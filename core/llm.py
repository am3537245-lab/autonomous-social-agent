"""Resilient Gemini 2.5 Flash Free-Tier Orchestrator.

Enforces Multi-Key Token Bucket rate limits (15 RPM), exponential backoff on 429,
Pydantic v2 JSON Schema enforcement, and 'Analyze-Once, Cache-Forever' multimodal indexing.
"""

import asyncio
import base64
import json
import logging
import math
import os
import random
import re
import time
from typing import Any, Dict, List, Literal, Optional, Tuple
import requests
from pydantic import BaseModel, Field

logger = logging.getLogger("core.llm")


def load_env_file(filepath: Optional[str] = None) -> None:
    """Load key-value pairs from .env into os.environ if not already set."""
    if filepath is None:
        filepath = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        k = k.strip()
                        v = v.strip().strip('"').strip("'")
                        if k not in os.environ:
                            os.environ[k] = v
        except Exception as e:
            logger.warning(f"Could not load .env file: {e}")


# Automatically load .env on import
load_env_file()


class AgentDecision(BaseModel):
    """Strict Pydantic v2 schema for agent decisions."""

    understood_context: str = Field(
        ...,
        description="Brief summary of what post/item the user is referring to and what their media shows",
    )
    decision: Literal["REPLY", "HIDE", "DELETE", "IGNORE", "HUMAN_HANDOFF"] = Field(
        ..., description="Final operational action to take"
    )
    public_comment_reply: Optional[str] = Field(
        default=None, description="Text reply for comment if applicable"
    )
    private_dm_reply: Optional[str] = Field(
        default=None, description="Text reply for DM or private follow-up"
    )
    moderation_reason: Optional[str] = Field(
        default=None, description="Why comment was hidden/deleted or escalated"
    )
    toxicity_score: float = Field(
        default=0.0, ge=0.0, le=1.0, description="Toxicity rating between 0.0 and 1.0"
    )
    confidence: float = Field(
        default=0.95, ge=0.0, le=1.0, description="Confidence score between 0.0 and 1.0"
    )


class PostVisualDigest(BaseModel):
    """Structured multimodal post digest generated once per post."""

    visual_description: str = Field(
        ..., description="Detailed breakdown of products, colors, models, or visual scenes"
    )
    ocr_extracted_text: Optional[str] = Field(
        default="", description="Any text, prices, or phone numbers visible in the image/video"
    )
    audio_transcript_summary: Optional[str] = Field(
        default="", description="Summary of spoken voiceover or dialogue"
    )
    detected_products_and_prices: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Structured list of items and prices mentioned in caption, visual, or audio",
    )


class KeyBucket:
    """Token bucket rate limiter tracking 15 Requests Per Minute (RPM) per API key."""

    def __init__(self, key: str, max_rpm: int = 15) -> None:
        self.key = key
        self.max_rpm = max_rpm
        self.tokens = float(max_rpm)
        self.last_update = time.monotonic()
        self.cooldown_until = 0.0
        self.failure_count = 0

    def replenish(self) -> None:
        now = time.monotonic()
        elapsed = now - self.last_update
        self.tokens = min(float(self.max_rpm), self.tokens + elapsed * (self.max_rpm / 60.0))
        self.last_update = now

    def is_available(self) -> bool:
        now = time.monotonic()
        if now < self.cooldown_until:
            return False
        self.replenish()
        return self.tokens >= 1.0

    def consume(self) -> None:
        self.replenish()
        if self.tokens >= 1.0:
            self.tokens -= 1.0

    def trigger_cooldown(self, base_delay: float = 30.0) -> None:
        self.failure_count += 1
        jitter = random.uniform(0.5, 2.0)
        cooldown = (base_delay * (1.5 ** min(self.failure_count, 4))) + jitter
        self.cooldown_until = time.monotonic() + cooldown
        logger.warning(
            f"Key ending with '...{self.key[-4:]}' entered 429 cooldown for {cooldown:.1f}s"
        )

    def mark_success(self) -> None:
        self.failure_count = max(0, self.failure_count - 1)


class GeminiOrchestrator:
    """Manages Gemini API calls with multi-key rotation, model fallback, and structured output."""

    DEFAULT_MODEL = os.environ.get("DEFAULT_GEMINI_MODEL", "gemini-3.5-flash-lite")
    FALLBACK_MODELS = [
        m.strip()
        for m in os.environ.get(
            "FALLBACK_GEMINI_MODELS", "gemini-3.5-flash,gemini-3.1-flash-lite"
        ).split(",")
        if m.strip()
    ]

    def __init__(self, api_keys: Optional[List[str]] = None) -> None:
        raw_keys = api_keys if api_keys is not None else [
            k.strip() for k in os.environ.get("GEMINI_API_KEYS", "").split(",") if k.strip()
        ]
        self.keys = [k.strip() for k in raw_keys if k.strip()]
        self.buckets: List[KeyBucket] = [KeyBucket(k) for k in self.keys]
        self._current_index = 0

    def add_key(self, key: str) -> None:
        key = key.strip()
        if key and not any(b.key == key for b in self.buckets):
            self.keys.append(key)
            self.buckets.append(KeyBucket(key))

    def _get_next_available_bucket(self) -> Optional[KeyBucket]:
        if not self.buckets:
            return None

        # Check round-robin
        total = len(self.buckets)
        for _ in range(total):
            bucket = self.buckets[self._current_index]
            self._current_index = (self._current_index + 1) % total
            if bucket.is_available():
                return bucket

        return None

    def _clean_json_markdown(self, raw_text: str) -> str:
        """Strip markdown code fence blocks if LLM surrounds JSON with ```json ... ```."""
        cleaned = raw_text.strip()
        match = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", cleaned, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return cleaned

    def _call_gemini_raw(
        self,
        contents: List[Dict[str, Any]],
        system_instruction: str = "",
        model: Optional[str] = None,
    ) -> str:
        """Call Gemini REST endpoint with active key rotation and backoff."""
        bucket = self._get_next_available_bucket()
        if not bucket:
            # Check if all keys in cooldown or none configured
            if not self.buckets:
                raise RuntimeError("No Gemini API keys configured.")
            raise RuntimeError("All Gemini API keys are currently rate-limited (429 cooldown).")

        bucket.consume()
        target_models = [model] if model else [self.DEFAULT_MODEL] + self.FALLBACK_MODELS
        last_error = None

        for current_model in target_models:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{current_model}:generateContent?key={bucket.key}"

            payload: Dict[str, Any] = {
                "contents": contents,
                "generationConfig": {
                    "temperature": 0.2,
                    "topP": 0.95,
                    "maxOutputTokens": 2048,
                    "responseMimeType": "application/json",
                },
            }

            if system_instruction:
                payload["systemInstruction"] = {
                    "parts": [{"text": system_instruction}]
                }

            try:
                resp = requests.post(url, json=payload, timeout=20.0)
                if resp.status_code == 429:
                    bucket.trigger_cooldown(base_delay=20.0)
                    raise RuntimeError("Rate limit hit (429). Rotator queued backoff.")
                elif resp.status_code in (404, 503):
                    # Deprecated model or temporary demand spike; fallback to next model
                    logger.warning(
                        f"Model {current_model} returned status {resp.status_code}. Trying fallback..."
                    )
                    last_error = RuntimeError(f"Model {current_model} returned {resp.status_code}: {resp.text[:150]}")
                    continue

                resp.raise_for_status()
                data = resp.json()
                bucket.mark_success()

                # Parse candidate text
                candidates = data.get("candidates", [])
                if not candidates:
                    raise ValueError("Gemini returned no candidates.")

                parts = candidates[0].get("content", {}).get("parts", [])
                full_text = "".join(p.get("text", "") for p in parts)
                return self._clean_json_markdown(full_text)
            except requests.exceptions.RequestException as e:
                if hasattr(e, "response") and e.response is not None and e.response.status_code == 429:
                    bucket.trigger_cooldown(base_delay=25.0)
                last_error = e

        if last_error:
            raise last_error
        raise RuntimeError("All configured Gemini models failed to respond.")

    # ==========================
    # POST MULTIMODAL INDEXER
    # ==========================

    def index_post_multimodal(
        self,
        caption_text: str,
        media_bytes: Optional[bytes] = None,
        media_mime: str = "image/webp",
    ) -> PostVisualDigest:
        """Analyze-Once pipeline: Extract structured semantic digest from post media & caption.

        Stored permanently to cache so future comments require ZERO media tokens.
        """
        system_prompt = (
            "You are an expert Arabic social commerce catalog indexer. Analyze the post visual and caption.\n"
            "Return a strictly valid JSON matching this schema:\n"
            "{\n"
            '  "visual_description": "Detailed breakdown of products, colors, models, or scenes",\n'
            '  "ocr_extracted_text": "Any text, prices, or numbers visible in the image",\n'
            '  "audio_transcript_summary": "Summary of spoken dialogue if audio present",\n'
            '  "detected_products_and_prices": [{"item": "product name", "price": "detected price or offer"}]\n'
            "}"
        )

        parts: List[Dict[str, Any]] = []
        if media_bytes:
            b64_data = base64.b64encode(media_bytes).decode("utf-8")
            parts.append({
                "inlineData": {
                    "mimeType": media_mime,
                    "data": b64_data,
                }
            })

        parts.append({
            "text": f"Post Caption:\n{caption_text or '(No caption)'}\n\nIndex this post thoroughly."
        })

        # Offline / simulation fallback if no API keys are present
        if not self.buckets:
            return PostVisualDigest(
                visual_description=f"Auto-indexed post for: {caption_text[:60]}",
                ocr_extracted_text="Price and offer detected in local buffer",
                audio_transcript_summary="",
                detected_products_and_prices=[{"item": "Default Product", "price": "On Inquiry"}],
            )

        raw_json = self._call_gemini_raw([{"parts": parts}], system_instruction=system_prompt)
        parsed = json.loads(raw_json)
        if not parsed.get("audio_transcript_summary"):
            parsed["audio_transcript_summary"] = ""
        if not parsed.get("ocr_extracted_text"):
            parsed["ocr_extracted_text"] = ""
        return PostVisualDigest(**parsed)

    # ==========================
    # 3-STAGE DECISION ENGINE
    # ==========================

    def infer_interaction_decision(
        self,
        inbound_text: str,
        sender_name: str,
        interaction_type: str,  # 'comment' or 'dm'
        persona: Dict[str, Any],
        cached_post_digest: Optional[Dict[str, Any]] = None,
        matched_saved_reply: Optional[Dict[str, Any]] = None,
        customer_media_bytes: Optional[bytes] = None,
        customer_media_mime: str = "image/webp",
        conversation_history: Optional[List[Dict[str, str]]] = None,
    ) -> AgentDecision:
        """Stage 3: Infer structured customer support / moderation decision.

        Injects cached post digests as text context without re-sending post media tokens.
        """
        dialect = persona.get("dialect", "Egyptian")
        tone = persona.get("tone_of_voice", "friendly")
        system_instruction = (
            f"You are the official 24/7 customer support and moderation agent for this social page.\n"
            f"Dialect: {dialect} Arabic.\n"
            f"Tone: {tone}.\n"
            f"Persona Core: {persona.get('system_prompt', '')}\n\n"
            "Return a strictly valid JSON response conforming to this exact JSON schema:\n"
            "{\n"
            '  "understood_context": "Brief summary of what post/item the user is referring to and what their media shows",\n'
            '  "decision": "REPLY | HIDE | DELETE | IGNORE | HUMAN_HANDOFF",\n'
            '  "public_comment_reply": "Friendly response in dialect if comment",\n'
            '  "private_dm_reply": "Direct message response if DM or private follow-up",\n'
            '  "moderation_reason": "Explanation if hidden, deleted, or escalated",\n'
            '  "toxicity_score": 0.0,\n'
            '  "confidence": 0.95\n'
            "}\n"
            "Rules:\n"
            "1. If customer asks for price, look at the post's visual_digest or saved replies and answer directly with warmth.\n"
            "2. If customer is insulting or severely toxic, set decision to DELETE or HIDE with appropriate toxicity_score.\n"
            "3. If customer sends a payment receipt or complex dispute, set decision to HUMAN_HANDOFF.\n"
            "4. Never say you are an AI or bot. Speak as the actual store/page team."
        )

        parts: List[Dict[str, Any]] = []

        # If customer attached an image/screenshot in DM/comment
        if customer_media_bytes:
            b64_data = base64.b64encode(customer_media_bytes).decode("utf-8")
            parts.append({
                "inlineData": {
                    "mimeType": customer_media_mime,
                    "data": b64_data,
                }
            })

        # Assemble rich context as text
        context_prompt = f"--- INBOUND EVENT ---\n"
        context_prompt += f"Type: {interaction_type.upper()}\n"
        context_prompt += f"Sender: {sender_name or 'User'}\n"
        context_prompt += f"Message: {inbound_text or '(Attachment only)'}\n\n"

        if cached_post_digest:
            context_prompt += "--- CACHED POST KNOWLEDGE (ZERO MEDIA TOKENS) ---\n"
            context_prompt += f"Visual Breakdown: {cached_post_digest.get('visual_description', '')}\n"
            context_prompt += f"OCR Visible Text: {cached_post_digest.get('ocr_extracted_text', '')}\n"
            context_prompt += f"Products & Prices: {json.dumps(cached_post_digest.get('detected_products_and_prices', []), ensure_ascii=False)}\n\n"

        if matched_saved_reply:
            context_prompt += "--- SUGGESTED SAVED REPLY / FAQ ---\n"
            context_prompt += f"Category: {matched_saved_reply.get('category')}\n"
            context_prompt += f"Template Text: {matched_saved_reply.get('reply_text')}\n\n"

        if conversation_history:
            context_prompt += "--- RECENT THREAD HISTORY ---\n"
            for msg in conversation_history[-4:]:
                context_prompt += f"{msg.get('role', 'user')}: {msg.get('content', '')}\n"
            context_prompt += "\n"

        context_prompt += "Decide the action and formulate the response now."
        parts.append({"text": context_prompt})

        # Offline / deterministic fallback if no API keys are present
        if not self.buckets:
            # Deterministic fallback logic
            if matched_saved_reply:
                reply = matched_saved_reply.get("reply_text")
            elif cached_post_digest and cached_post_digest.get("detected_products_and_prices"):
                prods = cached_post_digest.get("detected_products_and_prices")
                price = prods[0].get("price", "متوفر حالياً")
                reply = f"أهلاً بك يا فندم! بخصوص استفسارك، السعر هو {price}. يسعدنا تواصلك معنا دائماً."
            else:
                reply = "أهلاً بحضرتك يا فندم! تم استلام رسالتك وفريق خدمة العملاء جاهز لمساعدتك في أي استفسار."

            return AgentDecision(
                understood_context=f"Inbound {interaction_type} from {sender_name}: {inbound_text[:50]}",
                decision="REPLY",
                public_comment_reply=reply if interaction_type == "comment" else None,
                private_dm_reply=reply if interaction_type == "dm" else None,
                moderation_reason=None,
                toxicity_score=0.0,
                confidence=0.98,
            )

        raw_json = self._call_gemini_raw([{"parts": parts}], system_instruction=system_instruction)
        parsed = json.loads(raw_json)
        return AgentDecision(**parsed)
