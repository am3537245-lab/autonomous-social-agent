"""Multi-tenant SQLite storage engine with WAL mode and local perceptual matching."""

import json
import logging
import os
import re
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional, Tuple

logger = logging.getLogger("core.storage")

DEFAULT_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "storage.db")


ACTION_NORMALIZE = {
    "REPLY": "REPLIED",
    "REPLIED": "REPLIED",
    "HIDE": "HIDDEN",
    "HIDDEN": "HIDDEN",
    "DELETE": "DELETED",
    "DELETED": "DELETED",
    "IGNORE": "IGNORED",
    "IGNORED": "IGNORED",
    "HUMAN_HANDOFF": "ESCALATED",
    "ESCALATED": "ESCALATED",
    "DM_SENT": "DM_SENT",
}


def hamming_distance(hex1: str, hex2: str) -> int:
    """Calculate the Hamming distance between two hexadecimal hash strings."""
    if not hex1 or not hex2 or len(hex1) != len(hex2):
        return 999
    try:
        val1 = int(hex1, 16)
        val2 = int(hex2, 16)
        return bin(val1 ^ val2).count("1")
    except ValueError:
        return 999


class StorageEngine:
    """Production-grade SQLite engine configured with Write-Ahead Logging (WAL)

    and multi-tenant isolation.
    """

    def __init__(self, db_path: str = DEFAULT_DB_PATH) -> None:
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self._init_db()

    @contextmanager
    def get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        """Provide a transactional database connection with WAL mode."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA foreign_keys=ON;")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self) -> None:
        """Create all tables and performance indices if they do not exist."""
        with self.get_connection() as conn:
            # 1. Accounts
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS accounts (
                    id TEXT PRIMARY KEY,
                    platform TEXT CHECK(platform IN ('meta', 'tiktok')) NOT NULL,
                    page_name TEXT NOT NULL,
                    avatar_url TEXT DEFAULT '',
                    session_status TEXT CHECK(session_status IN ('active', 'expired', 'paused', 'needs_challenge')) DEFAULT 'active',
                    fingerprint_json TEXT DEFAULT '{}',
                    proxy_url TEXT DEFAULT '',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_active_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )

            # 2. Agent Personas
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS agent_personas (
                    account_id TEXT PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
                    system_prompt TEXT NOT NULL,
                    tone_of_voice TEXT DEFAULT 'friendly',
                    dialect TEXT DEFAULT 'Egyptian',
                    bad_words_list TEXT DEFAULT '[]',
                    auto_delete_toxic INTEGER DEFAULT 1,
                    auto_hide_spam INTEGER DEFAULT 1,
                    vision_enabled INTEGER DEFAULT 1,
                    confidence_threshold REAL DEFAULT 0.75
                );
                """
            )

            # 3. Posts Context Cache (Multimodal Memory)
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS posts_context_cache (
                    id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
                    external_post_id TEXT NOT NULL,
                    post_url TEXT NOT NULL,
                    caption_text TEXT DEFAULT '',
                    media_type TEXT CHECK(media_type IN ('image', 'video', 'carousel', 'text')) NOT NULL,
                    visual_digest_json TEXT DEFAULT '{}',
                    ocr_text TEXT DEFAULT '',
                    audio_summary TEXT DEFAULT '',
                    dhash_hex TEXT DEFAULT '',
                    phash_hex TEXT DEFAULT '',
                    custom_reply_rule TEXT DEFAULT '',
                    custom_dm_rule TEXT DEFAULT '',
                    is_custom_rule_active INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'INDEXED',
                    keyframe_hashes_json TEXT DEFAULT '[]',
                    detected_products_json TEXT DEFAULT '[]',
                    content_hash TEXT DEFAULT '',
                    last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(account_id, external_post_id)
                );
                """
            )

            # Auto-migrate existing posts_context_cache table columns if missing
            cur = conn.execute("PRAGMA table_info(posts_context_cache);")
            existing_cols = [r["name"] for r in cur.fetchall()]
            if "status" not in existing_cols:
                conn.execute("ALTER TABLE posts_context_cache ADD COLUMN status TEXT DEFAULT 'INDEXED';")
            if "keyframe_hashes_json" not in existing_cols:
                conn.execute("ALTER TABLE posts_context_cache ADD COLUMN keyframe_hashes_json TEXT DEFAULT '[]';")
            if "detected_products_json" not in existing_cols:
                conn.execute("ALTER TABLE posts_context_cache ADD COLUMN detected_products_json TEXT DEFAULT '[]';")
            if "content_hash" not in existing_cols:
                conn.execute("ALTER TABLE posts_context_cache ADD COLUMN content_hash TEXT DEFAULT '';")

            # 4. Saved Replies (Knowledge Base)
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS saved_replies (
                    id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
                    category TEXT NOT NULL,
                    trigger_keywords TEXT NOT NULL,
                    reply_text TEXT NOT NULL,
                    attachment_reference TEXT DEFAULT '',
                    usage_count INTEGER DEFAULT 0
                );
                """
            )

            # 5. Interaction Logs
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS interaction_logs (
                    id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
                    external_event_id TEXT NOT NULL,
                    thread_id TEXT DEFAULT '',
                    post_id TEXT DEFAULT '',
                    sender_name TEXT DEFAULT '',
                    interaction_type TEXT CHECK(interaction_type IN ('comment', 'dm')) NOT NULL,
                    inbound_text TEXT DEFAULT '',
                    inbound_media_type TEXT CHECK(inbound_media_type IN ('none', 'image', 'video', 'audio')) DEFAULT 'none',
                    inbound_media_analysis TEXT DEFAULT '',
                    agent_action TEXT CHECK(agent_action IN ('REPLIED', 'HIDDEN', 'DELETED', 'DM_SENT', 'ESCALATED', 'IGNORED')) NOT NULL,
                    agent_response TEXT DEFAULT '',
                    confidence REAL DEFAULT 1.0,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )

            # 6. Task Queue (Poisson & Priority Scheduler)
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS task_queue (
                    id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
                    task_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    priority INTEGER DEFAULT 1,
                    retry_count INTEGER DEFAULT 0,
                    scheduled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT CHECK(status IN ('PENDING', 'PROCESSING', 'DONE', 'FAILED')) DEFAULT 'PENDING',
                    error_message TEXT DEFAULT ''
                );
                """
            )

            # 7. Push Subscriptions (RFC 8291 Mobile Web Push)
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS push_subscriptions (
                    id TEXT PRIMARY KEY,
                    endpoint TEXT UNIQUE NOT NULL,
                    p256dh TEXT NOT NULL,
                    auth TEXT NOT NULL,
                    device_name TEXT DEFAULT 'Mobile Client',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )

            # Indices for low-latency lookups
            conn.execute("CREATE INDEX IF NOT EXISTS idx_posts_account_post ON posts_context_cache(account_id, external_post_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_account_time ON interaction_logs(account_id, timestamp DESC);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status_sched ON task_queue(status, scheduled_at, priority DESC);")

    # ==========================
    # ACCOUNT MANAGEMENT
    # ==========================

    def create_account(
        self,
        account_id: str,
        platform: str,
        page_name: str,
        avatar_url: str = "",
        proxy_url: str = "",
        fingerprint_json: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Register a new account/tenant with a default agent persona."""
        fp_str = json.dumps(fingerprint_json or {})
        with self.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO accounts (id, platform, page_name, avatar_url, proxy_url, fingerprint_json)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    platform=excluded.platform,
                    page_name=excluded.page_name,
                    avatar_url=excluded.avatar_url,
                    proxy_url=excluded.proxy_url,
                    fingerprint_json=excluded.fingerprint_json,
                    last_active_at=CURRENT_TIMESTAMP;
                """,
                (account_id, platform, page_name, avatar_url, proxy_url, fp_str),
            )

            # Insert default persona if missing
            conn.execute(
                """
                INSERT OR IGNORE INTO agent_personas (account_id, system_prompt, tone_of_voice, dialect)
                VALUES (?, ?, ?, ?);
                """,
                (
                    account_id,
                    "أنت المساعد الذكي وخدمة العملاء الرسمية للصفحة. أجب بود واحترافية وبسرعة بناء على تفاصيل المنشور والأسعار المذكورة.",
                    "friendly",
                    "Egyptian",
                ),
            )
        return self.get_account(account_id) or {}

    def get_account(self, account_id: str) -> Optional[Dict[str, Any]]:
        with self.get_connection() as conn:
            cur = conn.execute("SELECT * FROM accounts WHERE id = ?;", (account_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    def list_accounts(self) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cur = conn.execute("SELECT * FROM accounts ORDER BY created_at DESC;")
            return [dict(r) for r in cur.fetchall()]

    def update_account_status(self, account_id: str, status: str) -> None:
        with self.get_connection() as conn:
            conn.execute(
                "UPDATE accounts SET session_status = ?, last_active_at = CURRENT_TIMESTAMP WHERE id = ?;",
                (status, account_id),
            )

    # ==========================
    # PERSONA MANAGEMENT
    # ==========================

    def get_persona(self, account_id: str) -> Optional[Dict[str, Any]]:
        with self.get_connection() as conn:
            cur = conn.execute("SELECT * FROM agent_personas WHERE account_id = ?;", (account_id,))
            row = cur.fetchone()
            if not row:
                return None
            data = dict(row)
            data["bad_words_list"] = json.loads(data.get("bad_words_list") or "[]")
            return data

    def update_persona(self, account_id: str, updates: Dict[str, Any]) -> None:
        fields = []
        values = []
        for key in [
            "system_prompt",
            "tone_of_voice",
            "dialect",
            "auto_delete_toxic",
            "auto_hide_spam",
            "vision_enabled",
            "confidence_threshold",
        ]:
            if key in updates:
                fields.append(f"{key} = ?")
                values.append(updates[key])
        if "bad_words_list" in updates:
            fields.append("bad_words_list = ?")
            values.append(json.dumps(updates["bad_words_list"]))

        if not fields:
            return

        values.append(account_id)
        sql = f"UPDATE agent_personas SET {', '.join(fields)} WHERE account_id = ?;"
        with self.get_connection() as conn:
            conn.execute(sql, tuple(values))

    # ==========================
    # POSTS & PERCEPTUAL MATCHING
    # ==========================

    def upsert_post_cache(
        self,
        account_id: str,
        external_post_id: str,
        post_url: str,
        caption_text: str,
        media_type: str,
        visual_digest: Dict[str, Any],
        ocr_text: str = "",
        audio_summary: str = "",
        dhash_hex: str = "",
        phash_hex: str = "",
        custom_reply_rule: str = "",
        custom_dm_rule: str = "",
        is_custom_rule_active: bool = False,
    ) -> str:
        """Insert or update post cache with visual digests and perceptual hashes."""
        post_uuid = str(uuid.uuid4())
        digest_str = json.dumps(visual_digest, ensure_ascii=False)
        with self.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO posts_context_cache (
                    id, account_id, external_post_id, post_url, caption_text,
                    media_type, visual_digest_json, ocr_text, audio_summary,
                    dhash_hex, phash_hex, custom_reply_rule, custom_dm_rule,
                    is_custom_rule_active, last_synced_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(account_id, external_post_id) DO UPDATE SET
                    post_url=excluded.post_url,
                    caption_text=excluded.caption_text,
                    media_type=excluded.media_type,
                    visual_digest_json=excluded.visual_digest_json,
                    ocr_text=excluded.ocr_text,
                    audio_summary=excluded.audio_summary,
                    dhash_hex=CASE WHEN excluded.dhash_hex != '' THEN excluded.dhash_hex ELSE posts_context_cache.dhash_hex END,
                    phash_hex=CASE WHEN excluded.phash_hex != '' THEN excluded.phash_hex ELSE posts_context_cache.phash_hex END,
                    custom_reply_rule=excluded.custom_reply_rule,
                    custom_dm_rule=excluded.custom_dm_rule,
                    is_custom_rule_active=excluded.is_custom_rule_active,
                    last_synced_at=CURRENT_TIMESTAMP;
                """,
                (
                    post_uuid,
                    account_id,
                    external_post_id,
                    post_url,
                    caption_text,
                    media_type,
                    digest_str,
                    ocr_text,
                    audio_summary,
                    dhash_hex,
                    phash_hex,
                    custom_reply_rule,
                    custom_dm_rule,
                    1 if is_custom_rule_active else 0,
                ),
            )
        return post_uuid

    def get_post_by_external_id(self, account_id: str, external_post_id: str) -> Optional[Dict[str, Any]]:
        with self.get_connection() as conn:
            cur = conn.execute(
                "SELECT * FROM posts_context_cache WHERE account_id = ? AND external_post_id = ?;",
                (account_id, external_post_id),
            )
            row = cur.fetchone()
            if not row:
                return None
            data = dict(row)
            data["visual_digest"] = json.loads(data.get("visual_digest_json") or "{}")
            return data

    def list_posts(self, account_id: str) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cur = conn.execute(
                "SELECT * FROM posts_context_cache WHERE account_id = ? ORDER BY last_synced_at DESC;",
                (account_id,),
            )
            posts = []
            for r in cur.fetchall():
                d = dict(r)
                d["visual_digest"] = json.loads(d.get("visual_digest_json") or "{}")
                posts.append(d)
            return posts

    def match_post_by_hash(
        self, account_id: str, target_hash: str, hash_type: str = "phash", max_distance: int = 10
    ) -> Optional[Tuple[Dict[str, Any], int]]:
        """Find the closest post in the account's cache using Hamming distance.

        Bypasses LLM token usage if a match <= max_distance is found.
        """
        if not target_hash:
            return None

        col = "phash_hex" if hash_type == "phash" else "dhash_hex"
        with self.get_connection() as conn:
            cur = conn.execute(
                f"SELECT * FROM posts_context_cache WHERE account_id = ? AND {col} != '';",
                (account_id,),
            )
            best_post = None
            min_dist = 999

            for row in cur.fetchall():
                cached_hash = row[col]
                dist = hamming_distance(target_hash, cached_hash)
                if dist < min_dist:
                    min_dist = dist
                    best_post = dict(row)

            if best_post and min_dist <= max_distance:
                best_post["visual_digest"] = json.loads(best_post.get("visual_digest_json") or "{}")
                return (best_post, min_dist)
            return None

    def save_post_with_keyframes(
        self,
        account_id: str,
        external_post_id: str,
        post_url: str,
        caption_text: str,
        media_type: str,
        visual_digest: Dict[str, Any],
        keyframe_hashes: List[str],
        ocr_text: str = "",
        phash_hex: str = "",
    ) -> str:
        """Atomically persist an autonomously ingested post with 3 keyframe perceptual hashes."""
        post_uuid = str(uuid.uuid4())
        digest_str = json.dumps(visual_digest, ensure_ascii=False)
        keyframe_str = json.dumps(keyframe_hashes)
        products_str = json.dumps(visual_digest.get("detected_products_and_prices") or [], ensure_ascii=False)
        primary_hash = phash_hex or (keyframe_hashes[1] if len(keyframe_hashes) > 1 else (keyframe_hashes[0] if keyframe_hashes else ""))

        with self.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO posts_context_cache (
                    id, account_id, external_post_id, post_url, caption_text,
                    media_type, visual_digest_json, ocr_text, phash_hex,
                    status, keyframe_hashes_json, detected_products_json, last_synced_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'INDEXED', ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(account_id, external_post_id) DO UPDATE SET
                    post_url=excluded.post_url,
                    caption_text=excluded.caption_text,
                    media_type=excluded.media_type,
                    visual_digest_json=excluded.visual_digest_json,
                    ocr_text=excluded.ocr_text,
                    phash_hex=excluded.phash_hex,
                    status='INDEXED',
                    keyframe_hashes_json=excluded.keyframe_hashes_json,
                    detected_products_json=excluded.detected_products_json,
                    last_synced_at=CURRENT_TIMESTAMP;
                """,
                (
                    post_uuid,
                    account_id,
                    external_post_id,
                    post_url,
                    caption_text,
                    media_type,
                    digest_str,
                    ocr_text,
                    primary_hash,
                    keyframe_str,
                    products_str,
                ),
            )
        return post_uuid

    def acquire_indexing_lock(self, account_id: str, external_post_id: str) -> bool:
        """Atomic anti-stampede mutex. Returns True if this worker acquired the lock

        to index the post, or False if another worker is already indexing or post is indexed.
        """
        post_uuid = str(uuid.uuid4())
        with self.get_connection() as conn:
            cur = conn.execute(
                "SELECT status FROM posts_context_cache WHERE account_id = ? AND external_post_id = ?;",
                (account_id, external_post_id),
            )
            row = cur.fetchone()
            if row:
                st = row["status"]
                if st in ("INDEXED", "INDEXING"):
                    return False  # Already indexed or currently being indexed by another task
                conn.execute(
                    "UPDATE posts_context_cache SET status = 'INDEXING', last_synced_at = CURRENT_TIMESTAMP WHERE account_id = ? AND external_post_id = ?;",
                    (account_id, external_post_id),
                )
                return True

            # Post does not exist in cache: reserve it atomically with INDEXING
            conn.execute(
                """
                INSERT INTO posts_context_cache (
                    id, account_id, external_post_id, post_url, caption_text,
                    media_type, visual_digest_json, status, last_synced_at
                ) VALUES (?, ?, ?, '', '', 'image', '{}', 'INDEXING', CURRENT_TIMESTAMP);
                """,
                (post_uuid, account_id, external_post_id),
            )
            return True

    def release_indexing_lock(self, account_id: str, external_post_id: str, success: bool = True) -> None:
        """Release the indexing lock, setting status to INDEXED or FAILED."""
        new_status = "INDEXED" if success else "FAILED"
        with self.get_connection() as conn:
            conn.execute(
                "UPDATE posts_context_cache SET status = ?, last_synced_at = CURRENT_TIMESTAMP WHERE account_id = ? AND external_post_id = ?;",
                (new_status, account_id, external_post_id),
            )

    def update_post_price(self, account_id: str, external_post_id: str, new_price: str) -> bool:
        """Update product price in post visual digest (triggered by mobile lock-screen action)."""
        with self.get_connection() as conn:
            cur = conn.execute(
                "SELECT visual_digest_json FROM posts_context_cache WHERE account_id = ? AND external_post_id = ?;",
                (account_id, external_post_id),
            )
            row = cur.fetchone()
            if not row:
                return False

            digest = json.loads(row["visual_digest_json"] or "{}")
            products = digest.get("detected_products_and_prices", [])
            if products and isinstance(products, list):
                products[0]["price"] = new_price
            else:
                products = [{"item": "المنتج", "price": new_price}]

            digest["detected_products_and_prices"] = products
            new_digest_str = json.dumps(digest, ensure_ascii=False)
            new_products_str = json.dumps(products, ensure_ascii=False)

            conn.execute(
                """
                UPDATE posts_context_cache
                SET visual_digest_json = ?, detected_products_json = ?, status = 'INDEXED', last_synced_at = CURRENT_TIMESTAMP
                WHERE account_id = ? AND external_post_id = ?;
                """,
                (new_digest_str, new_products_str, account_id, external_post_id),
            )
            return True

    def search_inverted_index(
        self, account_id: str, query_text: str = "", phash_hex: Optional[str] = None
    ) -> Optional[Tuple[Dict[str, Any], int]]:
        """Inverted semantic and perceptual lookup matching posts by visual pHash,

        keyframe hashes, or product name entities.
        """
        # 1. Match by perceptual hash (primary or keyframes)
        if phash_hex:
            match = self.match_post_by_hash(account_id, phash_hex, max_distance=10)
            if match:
                return match

            # Also search through keyframe hashes
            with self.get_connection() as conn:
                cur = conn.execute(
                    "SELECT * FROM posts_context_cache WHERE account_id = ? AND keyframe_hashes_json != '[]';",
                    (account_id,),
                )
                for row in cur.fetchall():
                    kfs = json.loads(row["keyframe_hashes_json"] or "[]")
                    for kf_h in kfs:
                        dist = hamming_distance(phash_hex, kf_h)
                        if dist <= 10:
                            post = dict(row)
                            post["visual_digest"] = json.loads(post.get("visual_digest_json") or "{}")
                            return (post, dist)

        # 2. Textual entity matching
        if query_text:
            with self.get_connection() as conn:
                cur = conn.execute(
                    "SELECT * FROM posts_context_cache WHERE account_id = ? ORDER BY last_synced_at DESC;",
                    (account_id,),
                )
                for row in cur.fetchall():
                    caption = row["caption_text"] or ""
                    products = row["detected_products_json"] or ""
                    if any(word in caption or word in products for word in query_text.split() if len(word) > 2):
                        post = dict(row)
                        post["visual_digest"] = json.loads(post.get("visual_digest_json") or "{}")
                        return (post, 0)

        return None

    # ==========================
    # PRE-API ZERO-COST SHIELD
    # ==========================

    def check_zero_cost_shield(
        self, account_id: str, text: str
    ) -> Optional[Tuple[str, str]]:
        """Deterministic pre-API content validation against toxic words and spam links.

        Returns (action, reason) if triggered, otherwise None.
        """
        if not text:
            return None

        persona = self.get_persona(account_id)
        if not persona:
            return None

        bad_words: List[str] = persona.get("bad_words_list", [])
        auto_delete = bool(persona.get("auto_delete_toxic", 1))
        auto_hide = bool(persona.get("auto_hide_spam", 1))

        # Check for spam URLs / telegram / whatsapp invite links
        spam_pattern = r"(https?://|www\.|t\.me/|wa\.me/|bit\.ly/)"
        if auto_hide and re.search(spam_pattern, text, re.IGNORECASE):
            return ("HIDE", "Spam link or external channel detected by Zero-Cost Shield")

        # Check for explicit profanities or bad words
        for word in bad_words:
            if not word.strip():
                continue
            # Regex boundary or direct substring check for Arabic/English
            pattern = re.escape(word.strip())
            if re.search(pattern, text, re.IGNORECASE):
                action = "DELETE" if auto_delete else "HIDE"
                return (action, f"Blocked keyword '{word}' matched in Zero-Cost Shield")

        return None

    # ==========================
    # SAVED REPLIES
    # ==========================

    def add_saved_reply(
        self,
        account_id: str,
        category: str,
        trigger_keywords: List[str],
        reply_text: str,
        attachment_reference: str = "",
    ) -> str:
        reply_id = str(uuid.uuid4())
        kw_str = json.dumps(trigger_keywords, ensure_ascii=False)
        with self.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO saved_replies (id, account_id, category, trigger_keywords, reply_text, attachment_reference)
                VALUES (?, ?, ?, ?, ?, ?);
                """,
                (reply_id, account_id, category, kw_str, reply_text, attachment_reference),
            )
        return reply_id

    def get_saved_replies(self, account_id: str) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cur = conn.execute(
                "SELECT * FROM saved_replies WHERE account_id = ? ORDER BY usage_count DESC;",
                (account_id,),
            )
            replies = []
            for r in cur.fetchall():
                d = dict(r)
                d["trigger_keywords"] = json.loads(d.get("trigger_keywords") or "[]")
                replies.append(d)
            return replies

    def match_saved_reply(self, account_id: str, text: str) -> Optional[Dict[str, Any]]:
        """Fast keyword-based lookup for pre-written FAQ responses."""
        replies = self.get_saved_replies(account_id)
        text_lower = text.lower()
        for item in replies:
            for kw in item["trigger_keywords"]:
                if kw.lower() in text_lower:
                    # Increment usage count
                    with self.get_connection() as conn:
                        conn.execute(
                            "UPDATE saved_replies SET usage_count = usage_count + 1 WHERE id = ?;",
                            (item["id"],),
                        )
                    return item
        return None

    # ==========================
    # LOGS & RADAR METRICS
    # ==========================

    def log_interaction(
        self,
        account_id: str,
        external_event_id: str,
        interaction_type: str,
        agent_action: str,
        inbound_text: str = "",
        inbound_media_type: str = "none",
        inbound_media_analysis: str = "",
        agent_response: str = "",
        confidence: float = 1.0,
        thread_id: str = "",
        post_id: str = "",
        sender_name: str = "",
    ) -> str:
        log_id = str(uuid.uuid4())
        normalized_action = ACTION_NORMALIZE.get(agent_action.upper(), "REPLIED")
        with self.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO interaction_logs (
                    id, account_id, external_event_id, thread_id, post_id, sender_name,
                    interaction_type, inbound_text, inbound_media_type, inbound_media_analysis,
                    agent_action, agent_response, confidence
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    log_id,
                    account_id,
                    external_event_id,
                    thread_id,
                    post_id,
                    sender_name,
                    interaction_type,
                    inbound_text,
                    inbound_media_type,
                    inbound_media_analysis,
                    normalized_action,
                    agent_response,
                    confidence,
                ),
            )
        return log_id

    def get_recent_logs(self, account_id: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            if account_id:
                cur = conn.execute(
                    "SELECT * FROM interaction_logs WHERE account_id = ? ORDER BY timestamp DESC LIMIT ?;",
                    (account_id, limit),
                )
            else:
                cur = conn.execute(
                    "SELECT * FROM interaction_logs ORDER BY timestamp DESC LIMIT ?;",
                    (limit,),
                )
            return [dict(r) for r in cur.fetchall()]

    # ==========================
    # TASK QUEUE
    # ==========================

    def enqueue_task(
        self,
        account_id: str,
        task_type: str,
        payload: Dict[str, Any],
        priority: int = 1,
        delay_seconds: float = 0.0,
    ) -> str:
        task_id = str(uuid.uuid4())
        payload_str = json.dumps(payload, ensure_ascii=False)
        with self.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO task_queue (id, account_id, task_type, payload_json, priority, scheduled_at, status)
                VALUES (?, ?, ?, ?, ?, datetime('now', '+' || ? || ' seconds'), 'PENDING');
                """,
                (task_id, account_id, task_type, payload_str, priority, delay_seconds),
            )
        return task_id

    def pop_next_task(self) -> Optional[Dict[str, Any]]:
        """Transactionally pop the highest priority pending task whose schedule time has arrived."""
        with self.get_connection() as conn:
            cur = conn.execute(
                """
                SELECT * FROM task_queue
                WHERE status = 'PENDING' AND scheduled_at <= CURRENT_TIMESTAMP
                ORDER BY priority DESC, scheduled_at ASC
                LIMIT 1;
                """
            )
            row = cur.fetchone()
            if not row:
                return None
            task = dict(row)
            task["payload"] = json.loads(task.get("payload_json") or "{}")
            conn.execute(
                "UPDATE task_queue SET status = 'PROCESSING' WHERE id = ?;",
                (task["id"],),
            )
            return task

    def mark_task_done(self, task_id: str) -> None:
        with self.get_connection() as conn:
            conn.execute("UPDATE task_queue SET status = 'DONE' WHERE id = ?;", (task_id,))

    def mark_task_failed(self, task_id: str, error: str) -> None:
        with self.get_connection() as conn:
            conn.execute(
                """
                UPDATE task_queue
                SET status = 'FAILED', retry_count = retry_count + 1, error_message = ?
                WHERE id = ?;
                """,
                (error, task_id),
            )

    # ==========================
    # PUSH SUBSCRIPTIONS (MOBILE)
    # ==========================

    def save_push_subscription(
        self, endpoint: str, p256dh: str, auth: str, device_name: str = "Mobile Client"
    ) -> str:
        sub_id = str(uuid.uuid4())
        with self.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO push_subscriptions (id, endpoint, p256dh, auth, device_name)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(endpoint) DO UPDATE SET
                    p256dh=excluded.p256dh,
                    auth=excluded.auth,
                    device_name=excluded.device_name;
                """,
                (sub_id, endpoint, p256dh, auth, device_name),
            )
        return sub_id

    def list_push_subscriptions(self) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cur = conn.execute("SELECT * FROM push_subscriptions ORDER BY created_at DESC;")
            return [dict(r) for r in cur.fetchall()]

    def delete_push_subscription(self, endpoint: str) -> None:
        with self.get_connection() as conn:
            conn.execute("DELETE FROM push_subscriptions WHERE endpoint = ?;", (endpoint,))
