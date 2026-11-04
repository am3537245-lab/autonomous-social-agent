"""Resolve the post a comment or DM is about, from the local post cache.

The agent must never answer a price or product question from a guessed
post. This module returns exactly what is in the cache, and says so when
nothing is there. It never synthesises a caption, a price, or a product:
``found=False`` is a real, reportable outcome that callers must surface
as "post not indexed yet", not fill in with a default catalogue item.
"""

import json
import re
from typing import Any, Dict, List, Optional


def normalize_post_id(raw: Any) -> str:
    """Extract a post identifier from a raw id, a URL, or a composite key."""
    if raw is None:
        return ""
    text = str(raw).strip()
    if not text:
        return ""
    # facebook.com/.../posts/<id> or ?...post=<id> or profile.php?...&post=<id>
    match = re.search(r"(?:posts/|post=|story_fbid=|fbid=)(\d+)", text)
    if match:
        return match.group(1)
    # A bare numeric id, or a post_<id> style id, is kept as-is.
    if re.fullmatch(r"[A-Za-z0-9_\-\.]+", text):
        return text
    return text


def _normalise_products(digest: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw = digest.get("detected_products_and_prices") or []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            raw = []
    products: List[Dict[str, Any]] = []
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            products.append(
                {
                    "product_name": item.get("product_name") or item.get("item") or "",
                    "price": item.get("price_expression") or item.get("price") or "",
                }
            )
    return [p for p in products if p["product_name"] or p["price"]]


def _post_to_context(post: Dict[str, Any], source: str) -> Dict[str, Any]:
    digest = post.get("visual_digest") or {}
    if isinstance(digest, str):
        try:
            digest = json.loads(digest)
        except Exception:
            digest = {}
    caption = post.get("caption_text") or ""
    return {
        "found": True,
        "source": source,
        "external_post_id": post.get("external_post_id") or "",
        "post_url": post.get("post_url") or "",
        "caption_text": caption,
        "media_type": post.get("media_type") or "",
        "products": _normalise_products(digest),
        "ocr_text": digest.get("ocr_extracted_text") or post.get("ocr_text") or "",
        "visual_summary": digest.get("visual_summary")
        or digest.get("visual_description")
        or "",
        "post": post,
    }


def resolve_post_context(storage: Any, account_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Resolve post context for an inbound interaction payload.

    Lookup order, each step only if the previous one found nothing:
    1. exact external_post_id from payload.post_id
    2. exact external_post_id from payload.post_url
    3. text search in the inverted index using the inbound text
    (only for words longer than 2 characters, via storage.search_inverted_index)
    """
    candidates: List[str] = []
    for key in ("post_id", "post_url", "parent_post_id"):
        value = payload.get(key)
        if value:
            normalised = normalize_post_id(value)
            if normalised and normalised not in candidates:
                candidates.append(normalised)
    for candidate in candidates:
        post = storage.get_post_by_external_id(account_id, candidate)
        if post:
            return _post_to_context(post, source="cache_exact")
    inbound_text = (payload.get("text") or "").strip()
    if inbound_text:
        try:
            match = storage.search_inverted_index(account_id, query_text=inbound_text)
        except Exception:
            match = None
        if match:
            post, _distance = match
            if post:
                return _post_to_context(post, source="cache_text_search")
    return {
        "found": False,
        "source": "not_indexed",
        "external_post_id": candidates[0] if candidates else "",
        "post_url": payload.get("post_url") or "",
        "caption_text": "",
        "media_type": "",
        "products": [],
        "ocr_text": "",
        "visual_summary": "",
        "post": None,
    }
