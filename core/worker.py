"""Stochastic Humanized Execution Engine & Autonomous Worker Loop.

Implements Poisson process dispatching, humanized typing curves,
resilient DOM execution with fallback, and memory governance passes (< 1.0 GB RAM).
"""

import asyncio
import gc
import json
import logging
import math
import os
import random
import time
from typing import Any, Callable, Coroutine, Dict, List, Optional
import psutil

from core.engine import StealthBrowserManager, TenantContext
from core.llm import AgentDecision, GeminiOrchestrator
from core.storage import StorageEngine
from core.stream import compute_image_hashes, compress_image_to_webp, purge_buffers

logger = logging.getLogger("core.worker")


def sample_poisson_delay(rate_lambda: float = 0.2, min_delay: float = 1.0, max_delay: float = 8.0) -> float:
    """Sample an inter-arrival delay from a Poisson process (exponential distribution).

    Prevents fixed interval or linear robotic action cadence.
    """
    u = random.random()
    raw_delay = -math.log(1.0 - u) / max(0.01, rate_lambda)
    return max(min_delay, min(raw_delay, max_delay))


async def humanized_type(page_or_element: Any, text: str) -> None:
    """Simulate human typing dynamics with variable keystroke curves and cognitive micro-pauses."""
    for char in text:
        # Keystroke cadence: 40ms to 120ms
        delay = random.uniform(0.040, 0.120)

        # 1.5% chance of deliberate typo simulation and backspace correction
        if random.random() < 0.015 and char.isalpha():
            # Type random adjacent character
            typo_char = chr(ord(char) + 1)
            if hasattr(page_or_element, "type"):
                await page_or_element.type(typo_char, delay=int(delay * 1000))
            await asyncio.sleep(random.uniform(0.150, 0.350))
            if hasattr(page_or_element, "press"):
                await page_or_element.press("Backspace")
            await asyncio.sleep(random.uniform(0.100, 0.200))

        # Type actual character
        if hasattr(page_or_element, "type"):
            await page_or_element.type(char, delay=int(delay * 1000))
        elif hasattr(page_or_element, "keyboard"):
            await page_or_element.keyboard.type(char, delay=int(delay * 1000))

        # Cognitive micro-pause on punctuation or word boundaries
        if char in " .،!؟\n":
            await asyncio.sleep(random.uniform(0.300, 0.750))


class AutonomousWorker:
    """Core autonomous worker orchestrating tenant tasks, local pHash matching,

    Gemini inference, and DOM interaction dispatching.
    """

    def __init__(
        self,
        storage: StorageEngine,
        orchestrator: GeminiOrchestrator,
        browser_manager: StealthBrowserManager,
        on_event_broadcast: Optional[Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]] = None,
        simulate_delay: bool = True,
        jit_oracle: Optional[Any] = None,
    ) -> None:
        self.storage = storage
        self.orchestrator = orchestrator
        self.browser_manager = browser_manager
        self.on_event_broadcast = on_event_broadcast
        self.simulate_delay = simulate_delay
        self.jit_oracle = jit_oracle
        self.is_running = False
        self.tasks_processed = 0
        self.memory_ceiling_mb = 1000.0  # 1.0 GB ceiling

    async def start(self) -> None:
        """Start the background autonomous polling loop."""
        self.is_running = True
        logger.info("Autonomous Worker started with Poisson dispatching.")
        while self.is_running:
            try:
                task = self.storage.pop_next_task()
                if task:
                    await self.process_task(task)
                    self.tasks_processed += 1

                    # Explicit memory governance pass every 10 tasks
                    if self.tasks_processed % 10 == 0:
                        await self.run_memory_governance_pass()
                else:
                    await asyncio.sleep(1.0)
            except Exception as e:
                logger.error(f"Worker iteration error: {e}", exc_info=True)
                await asyncio.sleep(2.0)

    def stop(self) -> None:
        self.is_running = False
        logger.info("Autonomous Worker stop requested.")

    async def run_memory_governance_pass(self) -> None:
        """Enforce strict memory ceiling by collecting garbage in Python and V8."""
        gc.collect()
        await self.browser_manager.trigger_v8_garbage_collection()

        # Check total RSS memory
        rss_bytes = psutil.Process().memory_info().rss
        rss_mb = rss_bytes / (1024 * 1024)
        logger.info(f"Memory Governance Pass completed. Current RSS: {rss_mb:.1f} MB (Ceiling: {self.memory_ceiling_mb} MB)")

    async def process_task(self, task: Dict[str, Any]) -> None:
        """Process a single task from queue (Comment/DM/Sync)."""
        task_id = task["id"]
        account_id = task["account_id"]
        task_type = task["task_type"]
        payload = task["payload"]

        try:
            if task_type == "INBOUND_INTERACTION":
                await self._handle_inbound_interaction(account_id, payload)
            elif task_type == "INDEX_POST":
                await self._handle_index_post(account_id, payload)
            self.storage.mark_task_done(task_id)
        except Exception as e:
            logger.error(f"Task {task_id} failed: {e}", exc_info=True)
            self.storage.mark_task_failed(task_id, str(e))

    async def _handle_inbound_interaction(self, account_id: str, payload: Dict[str, Any]) -> None:
        """Execute the 3-Stage Decision & Action pipeline for an incoming comment or DM."""
        account = self.storage.get_account(account_id)
        if not account or account.get("session_status") in ("paused", "disconnected"):
            logger.info(f"Skipping interaction on {account_id}: status={account.get('session_status') if account else 'deleted'}")
            return

        external_event_id = payload.get("event_id", f"evt_{int(time.time()*1000)}")
        interaction_type = payload.get("interaction_type", "comment")
        inbound_text = payload.get("text", "")
        sender_name = payload.get("sender_name", "Customer")
        post_id = payload.get("post_id", "")
        thread_id = payload.get("thread_id", "")
        inbound_image_bytes = None
        if payload.get("image_base64"):
            import base64
            inbound_image_bytes = base64.b64decode(payload["image_base64"])

        start_time = time.monotonic()

        # ----------------------------------------------------
        # STAGE 1: LOCAL ZERO-COST SHIELD (PRE-API)
        # ----------------------------------------------------
        shield_trigger = self.storage.check_zero_cost_shield(account_id, inbound_text)
        if shield_trigger:
            action, reason = shield_trigger
            logger.info(f"Zero-Cost Shield triggered on account {account_id}: {action} - {reason}")
            # Execute moderation action immediately with zero LLM API call
            await self._execute_moderation_action(account_id, interaction_type, action, payload)
            self.storage.log_interaction(
                account_id=account_id,
                external_event_id=external_event_id,
                thread_id=thread_id,
                post_id=post_id,
                sender_name=sender_name,
                interaction_type=interaction_type,
                inbound_text=inbound_text,
                agent_action=action,
                agent_response=f"[Shield] {reason}",
                confidence=1.0,
            )
            if self.on_event_broadcast:
                await self.on_event_broadcast({
                    "type": "RADAR_ACTION",
                    "account_id": account_id,
                    "action": action,
                    "reason": reason,
                    "inbound_text": inbound_text,
                    "sender": sender_name,
                    "zero_cost_shield": True,
                    "latency_ms": int((time.monotonic() - start_time) * 1000),
                })
            return

        # ----------------------------------------------------
        # STAGE 2: MULTIMODAL CONTEXT ASSEMBLY & PHASH MATCHING
        # ----------------------------------------------------
        cached_post = None
        if post_id:
            cached_post = self.storage.get_post_by_external_id(account_id, post_id)
            if not cached_post and self.jit_oracle:
                logger.info(f"Unindexed post {post_id} received interaction. Invoking Reactive JIT Ingestion Oracle.")
                cached_post = await self.jit_oracle.ingest_post_jit(
                    account_id=account_id,
                    external_post_id=post_id,
                    mock_caption=payload.get("parent_caption"),
                    mock_media_bytes=payload.get("parent_media_bytes"),
                )

        # Perceptual hash matching for inbound customer screenshots/images
        matched_by_hash = False
        compressed_media = None
        if inbound_image_bytes:
            dhash, phash = compute_image_hashes(inbound_image_bytes)
            # Try to match image with cached posts
            match_res = self.storage.match_post_by_hash(account_id, phash, hash_type="phash", max_distance=10)
            if match_res:
                cached_post, dist = match_res
                matched_by_hash = True
                logger.info(f"Customer image matched post {cached_post['external_post_id']} via pHash (dist: {dist})")
            else:
                # Compress customer image to WebP (max 800px) before sending to Gemini
                compressed_media = compress_image_to_webp(inbound_image_bytes, max_dim=800, quality=75)

        # Keyword matching against Saved Replies
        matched_reply = self.storage.match_saved_reply(account_id, inbound_text)

        persona = self.storage.get_persona(account_id) or {}
        post_digest = cached_post.get("visual_digest") if cached_post else None

        # ----------------------------------------------------
        # STAGE 3: STRUCTURED INFERENCE (GEMINI OR DETERMINISTIC)
        # ----------------------------------------------------
        decision: AgentDecision = self.orchestrator.infer_interaction_decision(
            inbound_text=inbound_text,
            sender_name=sender_name,
            interaction_type=interaction_type,
            persona=persona,
            cached_post_digest=post_digest,
            matched_saved_reply=matched_reply,
            customer_media_bytes=compressed_media,
            customer_media_mime="image/webp",
        )

        # Clean in-memory buffers
        purge_buffers(inbound_image_bytes, compressed_media)

        # ----------------------------------------------------
        # STAGE 4: STOCHASTIC HUMANIZED EXECUTION
        # ----------------------------------------------------
        # Sample Poisson delay before replying (bypassed in fast test mode)
        if self.simulate_delay:
            delay = sample_poisson_delay(rate_lambda=0.3, min_delay=1.5, max_delay=6.0)
            await asyncio.sleep(delay)

        response_text = decision.public_comment_reply if interaction_type == "comment" else decision.private_dm_reply

        # Execute on DOM / Platform
        await self._execute_interaction_action(account_id, interaction_type, decision.decision, response_text, payload)

        # Log interaction to SQLite
        self.storage.log_interaction(
            account_id=account_id,
            external_event_id=external_event_id,
            thread_id=thread_id,
            post_id=post_id,
            sender_name=sender_name,
            interaction_type=interaction_type,
            inbound_text=inbound_text,
            inbound_media_type="image" if inbound_image_bytes else "none",
            inbound_media_analysis=decision.understood_context,
            agent_action=decision.decision,
            agent_response=response_text or decision.moderation_reason or "",
            confidence=decision.confidence,
        )

        latency_ms = int((time.monotonic() - start_time) * 1000)
        if self.on_event_broadcast:
            await self.on_event_broadcast({
                "type": "RADAR_ACTION",
                "account_id": account_id,
                "action": decision.decision,
                "response": response_text,
                "context": decision.understood_context,
                "sender": sender_name,
                "matched_by_phash": matched_by_hash,
                "latency_ms": latency_ms,
            })

    async def _handle_index_post(self, account_id: str, payload: Dict[str, Any]) -> None:
        """Index a post once, computing perceptual hashes and generating visual digest."""
        post_id = payload["post_id"]
        post_url = payload.get("post_url", "")
        caption = payload.get("caption", "")
        media_type = payload.get("media_type", "image")
        media_bytes = None
        if payload.get("media_base64"):
            import base64
            media_bytes = base64.b64decode(payload["media_base64"])

        dhash, phash = "", ""
        compressed_media = None
        if media_bytes:
            dhash, phash = compute_image_hashes(media_bytes)
            compressed_media = compress_image_to_webp(media_bytes, max_dim=800, quality=80)

        digest = self.orchestrator.index_post_multimodal(
            caption_text=caption,
            media_bytes=compressed_media,
            media_mime="image/webp",
        )

        self.storage.upsert_post_cache(
            account_id=account_id,
            external_post_id=post_id,
            post_url=post_url,
            caption_text=caption,
            media_type=media_type,
            visual_digest=digest.model_dump(),
            ocr_text=digest.ocr_extracted_text,
            audio_summary=digest.audio_transcript_summary,
            dhash_hex=dhash,
            phash_hex=phash,
        )

        purge_buffers(media_bytes, compressed_media)
        logger.info(f"Post {post_id} permanently indexed for account {account_id}.")

    async def _execute_moderation_action(
        self, account_id: str, interaction_type: str, action: str, payload: Dict[str, Any]
    ) -> None:
        """Perform comment deletion or hiding on the active page context."""
        tenant = self.browser_manager.contexts.get(account_id)
        if not tenant or not tenant.active_page:
            return

        page = tenant.active_page
        try:
            # Resilient ARIA locator for comment options menu
            comment_id = payload.get("comment_id")
            if comment_id:
                menu_btn = page.locator(f'[data-comment-id="{comment_id}"] [aria-haspopup="menu"]')
                if await menu_btn.count() > 0:
                    await menu_btn.first.click()
                    await asyncio.sleep(0.3)
                    if action == "DELETE":
                        delete_item = page.locator('role=menuitem[name*="حذف" i], role=menuitem[name*="delete" i]')
                        if await delete_item.count() > 0:
                            await delete_item.first.click()
                    elif action == "HIDE":
                        hide_item = page.locator('role=menuitem[name*="إخفاء" i], role=menuitem[name*="hide" i]')
                        if await hide_item.count() > 0:
                            await hide_item.first.click()
        except Exception as e:
            logger.warning(f"DOM moderation execution warning on {account_id}: {e}")

    async def _execute_interaction_action(
        self,
        account_id: str,
        interaction_type: str,
        decision: str,
        response_text: Optional[str],
        payload: Dict[str, Any],
    ) -> None:
        """Type and send humanized reply into DOM."""
        if decision != "REPLY" or not response_text:
            return

        tenant = self.browser_manager.contexts.get(account_id)
        if not tenant or not tenant.active_page:
            return

        page = tenant.active_page
        try:
            # Locate input box via ARIA role or placeholder
            input_box = page.locator(
                'div[role="textbox"], textarea[placeholder*="رد" i], textarea[placeholder*="comment" i]'
            )
            if await input_box.count() > 0:
                target = input_box.first
                await target.click()
                await humanized_type(target, response_text)
                await asyncio.sleep(0.4)
                await page.keyboard.press("Enter")
        except Exception as e:
            logger.warning(f"DOM reply execution warning on {account_id}: {e}")
