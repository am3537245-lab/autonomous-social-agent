"""Inbound-event parsing for the non-API (Stealth) rail and the Meta webhook rail.

This module is deliberately dependency-free (stdlib only) so its parsing can be
unit-tested without a browser, a Gemini key, or a Meta app. It never fabricates
an event: if a payload does not contain a recognisable message/comment node,
it returns an empty list, and callers must treat that as "no event", not as
a reason to invent one.
"""

import json
import re
from typing import Any, Dict, List, Optional


def _as_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _walk_nodes(node: Any, depth: int = 0):
    """Yield every dict in a nested JSON payload, up to a sane depth."""
    if depth > 12:
        return
    if isinstance(node, dict):
        yield node
        for child in node.values():
            yield from _walk_nodes(child, depth + 1)
    elif isinstance(node, list):
        for child in node:
            yield from _walk_nodes(child, depth + 1)


def _node_text(node: Dict[str, Any]) -> str:
    for key in ("text", "message", "body", "comment_text", "snippet"):
        value = node.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return ""


def _node_sender(node: Dict[str, Any]) -> str:
    for key in ("sender_name", "name"):
        value = node.get(key)
        if isinstance(value, str) and value.strip():
            return value
    sender = node.get("from") or node.get("sender") or {}
    if isinstance(sender, dict):
        name = sender.get("name")
        if isinstance(name, str) and name.strip():
            return name
    return "Customer"


def _node_post_id(node: Dict[str, Any]) -> str:
    for key in ("post_id", "parent_post_id", "story_id"):
        value = node.get(key)
        if value:
            return _as_str(value)
    return ""


def extract_inbound_events_from_network_payload(
    raw_payload: Any, account_id: str
) -> List[Dict[str, Any]]:
    """Extract comment/DM-like events from a Meta GraphQL/MQTT payload.

    Only nodes that carry both an identifier and a non-empty text (or an
    explicit comment/message marker) are returned. Nodes that look like
    generic feed metadata are ignored, so a page load does not flood the
    task queue with invented interactions.
    """
    if isinstance(raw_payload, str):
        try:
            raw_payload = json.loads(raw_payload)
        except Exception:
            return []
    events: List[Dict[str, Any]] = []
    seen = set()
    for node in _walk_nodes(raw_payload):
        text = _node_text(node)
        event_id = _as_str(
            node.get("comment_id")
            or node.get("message_id")
            or node.get("id")
            or ""
        )
        marker = _as_str(node.get("__typename") or node.get("item") or node.get("type") or "")
        is_comment_like = marker.lower() in (
            "comment",
            "feedback_comment_create",
            "message",
            "direct_message",
        ) or "comment" in marker.lower()
        if not text or not is_comment_like:
            # A bare message node with an id is still an event; a bare
            # metadata node without text is not.
            if not (event_id and marker.lower() in ("message", "direct_message")):
                continue
        if not event_id:
            continue
        if event_id in seen:
            continue
        seen.add(event_id)
        interaction_type = "comment" if "comment" in marker.lower() else "dm"
        events.append(
            {
                "event_id": event_id,
                "interaction_type": interaction_type,
                "text": text,
                "sender_name": _node_sender(node),
                "post_id": _node_post_id(node),
                "thread_id": _as_str(node.get("thread_id") or node.get("thread_key") or ""),
                "comment_id": event_id if interaction_type == "comment" else "",
            }
        )
    return events


def parse_meta_messaging_payload(
    payload: Dict[str, Any], resolve_account_id
) -> List[Dict[str, Any]]:
    """Parse Meta webhook ``entry[].messaging[]`` events into worker tasks.

    This is the webhook rail only. It does not create events for feed
    changes (handled elsewhere) and it does not invent a sender or a text:
    a messaging event without message text is skipped, except for a
    postback payload, which is kept as its payload text.
    """
    events: List[Dict[str, Any]] = []
    for entry in payload.get("entry", []) or []:
        page_asset_id = _as_str(entry.get("id", ""))
        account_id = resolve_account_id(page_asset_id)
        for messaging in entry.get("messaging", []) or []:
            sender = messaging.get("sender") or {}
            message = messaging.get("message") or {}
            postback = messaging.get("postback") or {}
            text = _as_str(message.get("text") or postback.get("payload") or "")
            if not text:
                continue
            mid = _as_str(message.get("mid") or messaging.get("timestamp") or "")
            if not mid:
                continue
            events.append(
                {
                    "account_id": account_id,
                    "event_id": mid,
                    "interaction_type": "dm",
                    "text": text,
                    "sender_name": _as_str(sender.get("name") or "Customer"),
                    "sender_id": _as_str(sender.get("id") or ""),
                    "thread_id": _as_str(sender.get("id") or ""),
                    "post_id": "",
                    "timestamp": messaging.get("timestamp"),
                }
            )
    return events


# ---------------------------------------------------------------------------
# Abuse / criticism classification.
#
# Deliberate distinction, documented here because it is a product decision,
# not just a regex: abuse (insult, threat, hate, harassment) may be hidden
# or deleted automatically. Civil criticism -- a complaint without insult,
# threat, or hate -- is NOT abuse and must not be hidden automatically;
# it is escalated for a human reply, because hiding real complaints hides
# real service problems and damages trust. Callers use the returned level
# to decide: "abuse" -> moderate, "critical" -> human handoff, "" -> normal.
# ---------------------------------------------------------------------------

_ABUSE_PATTERNS = [
    # Threats / violence (Arabic + English stems, kept generic on purpose).
    r"\b(بقتلك|سأقتلك|هقتلك|هدمر|سأدمر|موت|kill you|i will kill)\b",
    # Direct insults aimed at the page/team.
    r"(يا\s*(كذاب|نصاب|حرامي|لص|غشاش|محتال|زبالة|قذر|وسخ|حمار|كلب))",
    r"\b(scammer|fraudster|thief|liar|cheater)\b",
    # Hate / dehumanising slurs are matched by the page's own bad-words
    # list in the shield; this list stays to clearly abusive language only.
]

_CRITICAL_PATTERNS = [
    r"(سيء|سيئ|خيبة|مخيب|غير راض|مش راضي|شكوى|أشتكي|مشكلة|تأخير|تاخر|نصب|غش|مضحوك)",
    r"\b(bad|terrible|awful|disappointed|complaint|scam|fraud|late|broken|refund)\b",
]


def classify_inbound_text(text: str) -> Dict[str, Any]:
    """Classify inbound text as abuse, civil criticism, or neither.

    Returns {"level": "abuse"|"critical"|"", "reason": str}. Matching is
    case-insensitive and substring-based for Arabic (no word boundaries
    for Arabic script, where \\b does not behave like English).
    """
    if not text:
        return {"level": "", "reason": ""}
    lowered = text.lower()
    for pattern in _ABUSE_PATTERNS:
        if re.search(pattern, lowered, re.IGNORECASE):
            return {"level": "abuse", "reason": "Abusive language, insult, or threat detected"}
    for pattern in _CRITICAL_PATTERNS:
        if re.search(pattern, lowered, re.IGNORECASE):
            return {"level": "critical", "reason": "Civil criticism or complaint detected"}
    return {"level": "", "reason": ""}
