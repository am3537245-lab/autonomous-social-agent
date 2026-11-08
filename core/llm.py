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

ARABIC_DIGIT_MAP = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


def _extract_numeric_tokens(text: str) -> List[str]:
    """Normalize Arabic-Indic digits and extract numeric price/number tokens."""
    if not text:
        return []
    normalized = text.translate(ARABIC_DIGIT_MAP)
    return re.findall(r"\b\d+(?:[.,]\d+)?\b", normalized)


def validate_reply_price_grounding(
    reply_text: str,
    cached_post_digest: Optional[Dict[str, Any]] = None,
    matched_saved_reply: Optional[Dict[str, Any]] = None,
    all_saved_replies: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[bool, str, List[str]]:
    """Verify that every number/price mentioned in reply_text is grounded in real post context or saved replies.

    Returns (is_grounded, sanitized_reply_text, ungrounded_numbers).
    If any number is hallucinated, replaces the reply with a polite Fusha_Colloquial handoff message.
    """
    if not reply_text:
        return True, "", []

    # Strip any forbidden regional slang
    cleaned_reply = re.sub(r"يا\s*فندم|بكام\s*ده|عامل\s*كام|البيج|حضرتك\s*يا\s*فندم", "", reply_text).strip()

    reply_numbers = _extract_numeric_tokens(cleaned_reply)
    if not reply_numbers:
        return True, cleaned_reply, []

    # Build grounded corpus of allowed numbers
    grounded_corpus_parts: List[str] = []
    if cached_post_digest:
        grounded_corpus_parts.append(str(cached_post_digest.get("caption_text", "")))
        grounded_corpus_parts.append(str(cached_post_digest.get("ocr_text", "")))
        grounded_corpus_parts.append(str(cached_post_digest.get("ocr_extracted_text", "")))
        grounded_corpus_parts.append(str(cached_post_digest.get("visual_digest_json", "")))
        grounded_corpus_parts.append(str(cached_post_digest.get("detected_products_json", "")))
        if isinstance(cached_post_digest.get("detected_products_and_prices"), list):
            grounded_corpus_parts.append(json.dumps(cached_post_digest["detected_products_and_prices"], ensure_ascii=False))
        if isinstance(cached_post_digest.get("visual_digest"), dict):
            grounded_corpus_parts.append(json.dumps(cached_post_digest["visual_digest"], ensure_ascii=False))

    if matched_saved_reply:
        grounded_corpus_parts.append(str(matched_saved_reply.get("reply_text", "")))

    if all_saved_replies:
        for sr in all_saved_replies:
            grounded_corpus_parts.append(str(sr.get("reply_text", "")))

    allowed_numbers = set(_extract_numeric_tokens(" ".join(grounded_corpus_parts)))

    ungrounded = [n for n in reply_numbers if n not in allowed_numbers]
    if ungrounded:
        safe_handoff_reply = (
            "أهلاً وسهلاً بك! يسعدنا تواصلك معنا. سيقوم فريقنا بتأكيد السعر والتفاصيل الدقيقة لك فوراً عبر الرسائل."
        )
        return False, safe_handoff_reply, ungrounded

    return True, cleaned_reply, []



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
                resp = requests.post(url, json=payload, timeout=30.0)
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
                logger.warning(f"Request to {current_model} failed ({e}). Trying fallback if available...")
                if hasattr(e, "response") and e.response is not None and e.response.status_code == 429:
                    bucket.trigger_cooldown(base_delay=25.0)
                last_error = e
                continue

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
        dialect = persona.get("dialect", "Fusha_Colloquial")
        tone = persona.get("tone_of_voice", "friendly")

        dialect_instruction = "العامية الفصحى (اللهجة البيضاء الراقية - لغة عربية ميسرة وواضحة ومهذبة ومفهومة لجميع الدول العربية، بدون كلمات عامية إقليمية بحتة مثل 'بكام ده يا فندم' أو 'عامل كام'، بل بأسلوب مثل: 'أهلاً وسهلاً بك، يسعدنا خدمتك! سعر هذا المنتج هو... ومتاح التوصيل لكافة المناطق. تفضل بمراسلتنا لتأكيد طلبك')."
        if dialect == "Egyptian":
            dialect_instruction = "العامية المصرية الودودة."
        elif dialect == "Saudi" or dialect == "Gulf":
            dialect_instruction = "اللهجة السعودية والخليجية الراقية."
        elif dialect == "Levantine":
            dialect_instruction = "اللهجة الشامية المهذبة."
        elif dialect == "Standard":
            dialect_instruction = "العربية الفصحى الدقيقة."

        system_instruction = (
            f"You are the official 24/7 customer support and moderation agent for this social page.\n"
            f"Approved Dialect Style: {dialect_instruction}\n"
            f"Tone of Voice: {tone}.\n"
            f"Persona Core Instructions: {persona.get('system_prompt', '')}\n\n"
            "Return a strictly valid JSON response conforming to this exact JSON schema:\n"
            "{\n"
            '  "understood_context": "Brief summary of what post/item the user is referring to and what their media shows",\n'
            '  "decision": "REPLY | HIDE | DELETE | IGNORE | HUMAN_HANDOFF",\n'
            '  "public_comment_reply": "Friendly response in the approved dialect if comment",\n'
            '  "private_dm_reply": "Direct message response in the approved dialect if DM or private follow-up",\n'
            '  "moderation_reason": "Explanation if hidden, deleted, or escalated",\n'
            '  "toxicity_score": 0.0,\n'
            '  "confidence": 0.95\n'
            "}\n"
            "Rules:\n"
            "1. If customer asks for price and an exact price is explicitly present in CACHED POST KNOWLEDGE or SUGGESTED SAVED REPLY, quote that exact price warmly in public_comment_reply.\n"
            "2. STRICT ZERO-HALLUCINATION GUARD: NEVER invent, guess, or estimate any price or number that is not explicitly written in the CACHED POST KNOWLEDGE or SUGGESTED SAVED REPLY. If the customer asks for a price and no price is listed in the context, set decision to HUMAN_HANDOFF and write a polite Fusha_Colloquial reply stating that the team will confirm the exact price shortly.\n"
            "3. If customer is insulting or severely toxic, set decision to DELETE or HIDE with appropriate toxicity_score.\n"
            "4. If customer sends a payment receipt or complex dispute, set decision to HUMAN_HANDOFF.\n"
            "5. Never say you are an AI or bot. Speak as the actual store/page team in warm, professional Modern Simplified Arabic (العامية الفصحى الميسرة) without any regional Egyptian terms like 'يا فندم'."
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

        prods_list: List[Dict[str, Any]] = []
        if cached_post_digest:
            digest_dict = dict(cached_post_digest)
            if "visual_digest_json" in cached_post_digest and isinstance(cached_post_digest["visual_digest_json"], str):
                try:
                    vd = json.loads(cached_post_digest["visual_digest_json"])
                    if isinstance(vd, dict):
                        digest_dict.update(vd)
                except Exception:
                    pass

            caption = cached_post_digest.get("caption_text", "")
            raw_prods = digest_dict.get("detected_products_and_prices") or digest_dict.get("detected_products_json") or []
            if isinstance(raw_prods, str):
                try:
                    raw_prods = json.loads(raw_prods)
                except Exception:
                    raw_prods = []
            if isinstance(raw_prods, list):
                prods_list = raw_prods

            context_prompt += "--- CACHED POST KNOWLEDGE (ZERO MEDIA TOKENS) ---\n"
            if caption:
                context_prompt += f"Post Caption: {caption}\n"
            context_prompt += f"Visual Breakdown: {digest_dict.get('visual_summary') or digest_dict.get('visual_description', '')}\n"
            context_prompt += f"OCR Visible Text: {digest_dict.get('ocr_extracted_text') or digest_dict.get('ocr_text', '')}\n"
            context_prompt += f"Products & Prices: {json.dumps(prods_list, ensure_ascii=False)}\n\n"

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
        price_inquiry_keywords = ("سعر", "بكم", "بكام", "كم السعر", "الاسعار", "الأسعار", "تكلفة", "قيمة", "price", "how much")
        is_price_inquiry = any(kw in (inbound_text or "").lower() for kw in price_inquiry_keywords)

        if not self.buckets:
            # Deterministic fallback logic in Simplified Arabic (Fusha Colloquial)
            if matched_saved_reply:
                reply = matched_saved_reply.get("reply_text")
                decision_val = "REPLY"
            elif cached_post_digest and prods_list:
                prod = prods_list[0] if len(prods_list) > 0 else {}
                p_name = prod.get("product_name") or prod.get("item", "الباقة")
                price = prod.get("price_expression") or prod.get("price", "")
                if price:
                    reply = f"أهلاً وسهلاً بك يا {sender_name}! يسعدنا تواصلك معنا. سعر {p_name} هو {price}. والتفاصيل كاملة متوفرة لدينا، تفضل بمراسلتنا لتأكيد طلبك."
                    decision_val = "REPLY"
                else:
                    reply = "أهلاً وسهلاً بك! يسعدنا تواصلك معنا. سيقوم فريقنا بتأكيد السعر والتفاصيل الدقيقة لك فوراً عبر الرسائل."
                    decision_val = "HUMAN_HANDOFF"
            elif is_price_inquiry:
                reply = "أهلاً وسهلاً بك! يسعدنا تواصلك معنا. سيقوم فريقنا بتأكيد السعر والتفاصيل الدقيقة لك فوراً عبر الرسائل."
                decision_val = "HUMAN_HANDOFF"
            else:
                reply = f"أهلاً وسهلاً بك يا {sender_name}! يسعدنا تواصلك معنا وفريق خدمة العملاء جاهز لمساعدتك في أي استفسار."
                decision_val = "REPLY"

            return AgentDecision(
                understood_context=f"Inbound {interaction_type} from {sender_name}: {inbound_text[:50]}",
                decision=decision_val,
                public_comment_reply=reply if interaction_type == "comment" else None,
                private_dm_reply=reply if interaction_type == "dm" else None,
                moderation_reason="Ungrounded price inquiry handoff" if decision_val == "HUMAN_HANDOFF" else None,
                toxicity_score=0.0,
                confidence=0.98,
            )

        raw_json = self._call_gemini_raw([{"parts": parts}], system_instruction=system_instruction)
        parsed = json.loads(raw_json)
        decision_obj = AgentDecision(**parsed)

        # Enforce strict Zero-Hallucination Price Grounding Guard on LLM output
        for field_name in ("public_comment_reply", "private_dm_reply"):
            field_val = getattr(decision_obj, field_name)
            if field_val:
                is_grounded, sanitized_text, ungrounded_nums = validate_reply_price_grounding(
                    reply_text=field_val,
                    cached_post_digest=cached_post_digest,
                    matched_saved_reply=matched_saved_reply,
                )
                setattr(decision_obj, field_name, sanitized_text)
                if not is_grounded:
                    logger.warning(
                        f"Price Hallucination Guard intercepted ungrounded numbers {ungrounded_nums} in {field_name}. Escalating to HUMAN_HANDOFF."
                    )
                    decision_obj.decision = "HUMAN_HANDOFF"
                    decision_obj.moderation_reason = f"Intercepted ungrounded price numbers: {ungrounded_nums}"

        # Also if customer explicitly asked for price and neither post nor saved reply has any grounded numbers, ensure HUMAN_HANDOFF if LLM tried to reply without price confirmation
        if is_price_inquiry and not prods_list and not matched_saved_reply:
            has_grounded_digits = bool(_extract_numeric_tokens(str(cached_post_digest or "")))
            if not has_grounded_digits and decision_obj.decision == "REPLY":
                decision_obj.decision = "HUMAN_HANDOFF"
                decision_obj.moderation_reason = "Price inquiry on post with no listed price; escalated for human confirmation."

        return decision_obj
