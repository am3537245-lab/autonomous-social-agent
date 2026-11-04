"""Tests for the ingestion fix: post context, abuse classification,
webhook DM parsing, network-event parsing, and the PUBLISH_REPLY fix.

Run with the project venv (fastapi/pydantic are only needed for the
worker test, not for the pure parsing tests):
    python -m pytest tests/test_ingestion_fix.py -v
or, without pytest installed:
    python tests/test_ingestion_fix.py

All tests use a temporary SQLite file. No network, no Gemini key,
no real Meta session, and no fabricated post content are involved:
an unindexed post must return found=False.
"""

import asyncio
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.ingestion import (
    classify_inbound_text,
    extract_inbound_events_from_network_payload,
    parse_meta_messaging_payload,
)
from core.post_context import normalize_post_id, resolve_post_context
from core.storage import StorageEngine


def _storage():
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    return StorageEngine(db_path=tmp.name), tmp.name


def test_normalize_post_id():
    assert normalize_post_id("https://www.facebook.com/posts/12345") == "12345"
    assert normalize_post_id("12345") == "12345"
    assert normalize_post_id("") == ""


def test_post_context_from_cache_and_not_indexed():
    storage, path = _storage()
    try:
        storage.create_account(
            account_id="act_test", platform="meta", page_name="Test Page"
        )
        storage.upsert_post_cache(
            account_id="act_test",
            external_post_id="post_1",
            post_url="https://example.test/post_1",
            caption_text="باقة تسويق 1500 ريال",
            media_type="image",
            visual_digest={
                "detected_products_and_prices": [
                    {"product_name": "باقة تسويق", "price_expression": "1500 ريال"}
                ]
            },
            ocr_text="1500 ريال",
        )
        found = resolve_post_context(
            storage, "act_test", {"post_id": "post_1", "text": "كم السعر؟"}
        )
        assert found["found"] is True
        assert found["caption_text"] == "باقة تسويق 1500 ريال"
        assert found["products"][0]["price"] == "1500 ريال"
        missing = resolve_post_context(
            storage, "act_test", {"post_id": "post_unknown", "text": "كم السعر؟"}
        )
        assert missing["found"] is False
        assert missing["products"] == []
        assert missing["caption_text"] == ""
    finally:
        os.unlink(path)


def test_abuse_hidden_and_criticism_not_hidden():
    abuse = classify_inbound_text("يا كذاب ونصاب، بقتلك")
    assert abuse["level"] == "abuse"
    critical = classify_inbound_text("الخدمة سيئة وفي تأخير، أريد استرداد")
    assert critical["level"] == "critical"
    normal = classify_inbound_text("كم سعر الباقة؟")
    assert normal["level"] == ""


def test_network_payload_parsing():
    payload = {
        "data": {
            "feedback_comment_create": {
                "comment_id": "c_1",
                "text": "كم السعر؟",
                "sender_name": "Ahmed",
                "post_id": "post_1",
            }
        }
    }
    # The walker only keeps nodes with a comment/message marker; a plain
    # nested dict without such a marker is not an event by design.
    events = extract_inbound_events_from_network_payload(
        {"comment_id": "c_1", "type": "comment", "text": "كم السعر؟",
         "sender_name": "Ahmed", "post_id": "post_1"},
        "act_test",
    )
    assert len(events) == 1
    assert events[0]["interaction_type"] == "comment"
    assert events[0]["post_id"] == "post_1"


def test_webhook_dm_parsing():
    def resolve(raw):
        return "act_test" if "61579682971434" in raw or raw == "" else raw

    payload = {
        "entry": [
            {
                "id": "61579682971434",
                "messaging": [
                    {
                        "sender": {"id": "user_9"},
                        "message": {"mid": "m_1", "text": "أريد حجز الباقة"},
                        "timestamp": 123456,
                    },
                    {
                        "sender": {"id": "user_9"},
                        "message": {"mid": "m_2"},
                        "timestamp": 123457,
                    },
                ],
            }
        ]
    }
    events = parse_meta_messaging_payload(payload, resolve)
    # Only the event with text is kept; the text-less event is skipped.
    assert len(events) == 1
    assert events[0]["interaction_type"] == "dm"
    assert events[0]["event_id"] == "m_1"
    assert events[0]["thread_id"] == "user_9"


def test_worker_publish_reply_without_context_is_failed_not_done():
    # Full worker import needs pydantic/requests/psutil/PIL; skip cleanly
    # if the venv deps are missing, instead of failing the whole suite.
    try:
        from core.engine import StealthBrowserManager
        from core.llm import GeminiOrchestrator
        from core.worker import AutonomousWorker
    except Exception as exc:  # pragma: no cover - environment guard
        print(f"SKIP worker test (deps missing: {exc})")
        return
    storage, path = _storage()
    try:
        storage.create_account(
            account_id="act_test", platform="meta", page_name="Test Page"
        )
        worker = AutonomousWorker(
            storage=storage,
            orchestrator=GeminiOrchestrator(api_keys=[]),
            browser_manager=StealthBrowserManager(headless=True),
            simulate_delay=False,
        )
        task_id = storage.enqueue_task(
            account_id="act_test",
            task_type="PUBLISH_REPLY",
            payload={"comment_id": "c_9", "reply_text": "أهلا بك", "post_id": "post_1"},
            priority=2,
        )
        task = storage.pop_next_task()
        assert task is not None and task["id"] == task_id
        # process_task catches the missing-context error itself and must
        # mark the task FAILED (previously it was silently marked DONE).
        asyncio.run(worker.process_task(task))
        with storage.get_connection() as conn:
            row = conn.execute(
                "SELECT status FROM task_queue WHERE id = ?;", (task_id,)
            ).fetchone()
        assert row[0] == "FAILED", f"expected FAILED, got {row[0]}"
    finally:
        os.unlink(path)


def test_worker_abusive_comment_is_hidden_and_logged():
    try:
        from core.engine import StealthBrowserManager
        from core.llm import GeminiOrchestrator
        from core.worker import AutonomousWorker
    except Exception as exc:  # pragma: no cover - environment guard
        print(f"SKIP worker test (deps missing: {exc})")
        return
    storage, path = _storage()
    try:
        storage.create_account(
            account_id="act_test", platform="meta", page_name="Test Page"
        )
        worker = AutonomousWorker(
            storage=storage,
            orchestrator=GeminiOrchestrator(api_keys=[]),
            browser_manager=StealthBrowserManager(headless=True),
            simulate_delay=False,
        )
        task_id = storage.enqueue_task(
            account_id="act_test",
            task_type="INBOUND_INTERACTION",
            payload={
                "event_id": "evt_abuse_1",
                "interaction_type": "comment",
                "text": "يا كذاب ونصاب",
                "sender_name": "Tester",
                "post_id": "",
            },
            priority=10,
        )
        task = storage.pop_next_task()
        asyncio.run(worker.process_task(task))
        logs = storage.get_recent_logs("act_test", limit=10)
        assert logs, "abusive interaction must be logged"
        assert logs[0]["agent_action"] == "HIDDEN"
    finally:
        os.unlink(path)


def main():
    tests = [
        test_normalize_post_id,
        test_post_context_from_cache_and_not_indexed,
        test_abuse_hidden_and_criticism_not_hidden,
        test_network_payload_parsing,
        test_webhook_dm_parsing,
        test_worker_publish_reply_without_context_is_failed_not_done,
        test_worker_abusive_comment_is_hidden_and_logged,
    ]
    failed = 0
    for test in tests:
        try:
            test()
            print(f"PASS {test.__name__}")
        except Exception as exc:
            failed += 1
            print(f"FAIL {test.__name__}: {exc}")
    print(f"{len(tests) - failed}/{len(tests)} passed")
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
