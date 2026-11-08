"""Real Meta Feed Harvester (Zero-Fabrication Public & Authenticated Ingestion).

Harvests real published posts from Facebook Pages via MetaSessionClient,
computes perceptual hashes (pHash/dHash) when media is available, and
caches real captions and grounded prices into posts_context_cache.
Never fabricates fake posts or synthetic prices.
"""

import asyncio
import logging
import re
from typing import Any, Dict, List, Optional
import requests

from core.engine import StealthBrowserManager
from core.llm import GeminiOrchestrator
from core.meta_client import MetaSessionClient
from core.storage import StorageEngine
from core.stream import compute_image_hashes

logger = logging.getLogger("core.crawler")


class PublicPageHarvester:
    """Harvests real posts from Meta profile URLs and seeds multimodal memory."""

    def __init__(
        self,
        storage: Optional[StorageEngine] = None,
        orchestrator: Optional[GeminiOrchestrator] = None,
        browser_manager: Optional[StealthBrowserManager] = None,
    ) -> None:
        self.storage = storage or StorageEngine()
        self.orchestrator = orchestrator or GeminiOrchestrator()
        self.browser_manager = browser_manager
        self.meta_client = MetaSessionClient(storage=self.storage, browser_manager=self.browser_manager)

    def purge_legacy_fake_posts(self, account_id: str = "act_1071349136056783") -> int:
        """Delete any previously seeded synthetic posts from SQLite."""
        fake_ids = ("post_dazaa_growth_01", "post_dazaa_reels_02", "post_dazaa_audit_03")
        with self.storage.get_connection() as conn:
            cur = conn.execute(
                "DELETE FROM posts_context_cache WHERE account_id = ? AND external_post_id IN (?, ?, ?);",
                (account_id, *fake_ids),
            )
            return cur.rowcount

    async def harvest_public_profile(
        self,
        account_id: str = "act_1071349136056783",
        profile_url: str = "https://www.facebook.com/profile.php?id=61579682971434",
        max_posts: int = 10,
    ) -> List[Dict[str, Any]]:
        """Harvest real posts from the Facebook Page and index them into SQLite."""
        self.purge_legacy_fake_posts(account_id)

        real_posts, _ = await asyncio.to_thread(
            self.meta_client.fetch_real_posts_and_comments_sync, account_id
        )
        ingested_records: List[Dict[str, Any]] = []

        for item in real_posts[:max_posts]:
            ext_id = item["external_post_id"]
            caption = item.get("caption_text", "")
            post_url = item.get("post_url", profile_url)
            media_url = item.get("media_url", "")
            media_type = item.get("media_type", "image")

            phash = ""
            kf_hashes: List[str] = []
            if media_url:
                try:
                    resp = await asyncio.to_thread(requests.get, media_url, timeout=10.0)
                    if resp.status_code == 200 and resp.content:
                        _, phash = compute_image_hashes(resp.content)
                        if phash:
                            kf_hashes = [phash]
                except Exception as img_err:
                    logger.debug(f"Could not download post media for pHash ({ext_id}): {img_err}")

            # Extract only genuinely stated prices from caption
            detected_prods: List[Dict[str, Any]] = []
            price_matches = re.findall(
                r"(\d+(?:[.,]\d+)?)\s*(?:ريال|ر\.س|SAR|درهم|دولار|جنيه|دينار|\$)",
                caption,
                re.IGNORECASE,
            )
            for pm in price_matches:
                detected_prods.append({"item": "عرض بالمنشور", "price": pm})

            visual_digest = {
                "detected_products_and_prices": detected_prods,
                "ocr_extracted_text": "",
                "visual_summary": f"منشور حقيقي من الصفحة: {caption[:120]}" if caption else "منشور حقيقي من الصفحة",
            }

            self.storage.save_post_with_keyframes(
                account_id=account_id,
                external_post_id=ext_id,
                post_url=post_url,
                caption_text=caption,
                media_type=media_type,
                visual_digest=visual_digest,
                keyframe_hashes=kf_hashes,
                ocr_text="",
                phash_hex=phash,
            )

            ingested_records.append({
                "external_post_id": ext_id,
                "caption_text": caption,
                "post_url": post_url,
                "detected_products": detected_prods,
                "phash": phash,
                "status": "INDEXED",
            })
            logger.info(f"Indexed real Facebook post {ext_id} for {account_id}")

        return ingested_records

    def harvest_sync(
        self,
        account_id: str = "act_1071349136056783",
        profile_url: str = "https://www.facebook.com/profile.php?id=61579682971434",
        max_posts: int = 10,
    ) -> List[Dict[str, Any]]:
        """Synchronous wrapper for harvesting real posts."""
        return asyncio.run(
            self.harvest_public_profile(
                account_id=account_id,
                profile_url=profile_url,
                max_posts=max_posts,
            )
        )
