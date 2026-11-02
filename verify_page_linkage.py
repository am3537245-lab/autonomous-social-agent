"""Deterministic Verification Suite for Meta Business Asset Binding, Cold-Boot Feed Ingestion & Scope Verification.

Validates:
1. Account Asset Normalization & Persona Dialect Invariant in SQLite.
2. Encrypted Session Vault Cryptographic Integrity (Fernet AES-CBC/HMAC-SHA256).
3. Cold-Boot Multimodal Ingestion & Perceptual Hash Catalog Cache.
4. End-to-End Customer Interaction Simulation in Simplified Standard Arabic (Fusha Colloquial) with Price Accuracy.
5. Operational Scopes (CAN_MODERATE_COMMENTS, CAN_SEND_MESSAGES, ACTIVE health status, Instagram linkage).
"""

import asyncio
import codecs
import json
import logging
import os
import sys
import time

# Ensure UTF-8 output in Windows PowerShell terminal
sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, "replace")
sys.stderr = codecs.getwriter("utf-8")(sys.stderr.buffer, "replace")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("verify_page_linkage")

from core.engine import StealthBrowserManager, TenantContext, get_tenant_storage_path
from core.crawler import PublicPageHarvester
from core.llm import GeminiOrchestrator
from core.storage import StorageEngine
from core.verifier import ActiveAssociationOracle, PageHealthStatus


def run_test_1_account_normalization(storage: StorageEngine) -> bool:
    print("\n--- TEST 1: Account Asset Normalization & Persona Audit ---")
    account_id = "act_1071349136056783"
    acc = storage.get_account(account_id)

    assert acc is not None, f"FAIL: Account {account_id} not found in database!"
    assert acc["platform"] == "meta", f"FAIL: Expected platform meta, got {acc['platform']}"
    assert acc["page_name"] == "Dazaa Marketing", f"FAIL: Unexpected page_name {acc['page_name']}"
    assert acc["page_id"] == "1071349136056783", f"FAIL: Unexpected page_id {acc['page_id']}"
    assert acc["business_id"] == "766871999177306", f"FAIL: Unexpected business_id {acc['business_id']}"
    assert "61579682971434" in acc["profile_url"], f"FAIL: Unexpected profile_url {acc['profile_url']}"

    # Verify no legacy raw-URL account ID exists
    legacy_id = "https://business.facebook.com/latest/home?business_id=766871999177306&asset_id=1071349136056783"
    assert storage.get_account(legacy_id) is None, "FAIL: Legacy raw URL account entry still present in database!"

    persona = storage.get_persona(account_id)
    assert persona is not None, f"FAIL: Persona not found for {account_id}"
    assert persona["dialect"] == "Fusha_Colloquial", f"FAIL: Dialect must be Fusha_Colloquial, got {persona['dialect']}"
    assert persona["tone_of_voice"] == "friendly", f"FAIL: Tone must be friendly, got {persona['tone_of_voice']}"

    print(f"  [PASS] Account Normalized: {acc['id']}")
    print(f"         Page ID: {acc['page_id']} | Business ID: {acc['business_id']}")
    print(f"         Profile URL: {acc['profile_url']}")
    print(f"         Persona Dialect: {persona['dialect']} (العامية الفصحى الميسرة)")
    print(f"         Zero legacy URL entries confirmed.")
    return True


def run_test_2_session_vault_encryption(account_id: str = "act_1071349136056783") -> bool:
    print("\n--- TEST 2: Encrypted Session Vault Cryptographic Invariant ---")
    vault_file = get_tenant_storage_path(account_id)
    assert os.path.exists(vault_file), f"FAIL: Vault file {vault_file} does not exist!"

    tenant = TenantContext(account_id, "meta")
    state = tenant.load_decrypted_state()
    assert state is not None, "FAIL: Failed to decrypt storage state with Fernet master key!"
    assert "cookies" in state, "FAIL: Storage state missing 'cookies' array!"

    cookies = {c["name"]: c["value"] for c in state["cookies"]}
    assert "c_user" in cookies, "FAIL: 'c_user' cookie missing in encrypted vault!"
    assert "xs" in cookies, "FAIL: 'xs' session cookie missing in encrypted vault!"

    file_size = os.path.getsize(vault_file)
    print(f"  [PASS] Vault File: {vault_file} ({file_size} bytes encrypted)")
    print(f"         Decryption verified successfully via Fernet AES-CBC/HMAC-SHA256")
    print(f"         c_user: {cookies['c_user']} | xs: {cookies['xs'][:8]}... [REDACTED]")
    return True


def run_test_3_cold_boot_catalog(storage: StorageEngine) -> bool:
    print("\n--- TEST 3: Cold-Boot Feed Ingestion & Multimodal Memory Cache ---")
    account_id = "act_1071349136056783"

    posts = storage.list_posts(account_id)
    if len(posts) < 3:
        print("  Notice: Cache contains fewer than 3 posts. Running PublicPageHarvester...")
        harvester = PublicPageHarvester(storage=storage)
        harvester.harvest_sync(account_id=account_id)
        posts = storage.list_posts(account_id)

    assert len(posts) >= 3, f"FAIL: Expected >= 3 indexed posts, found {len(posts)}"

    for p in posts:
        assert p["status"] == "INDEXED", f"FAIL: Post {p['external_post_id']} not INDEXED"
        assert len(p["phash_hex"]) >= 8, f"FAIL: Post {p['external_post_id']} missing valid pHash"
        digest = json.loads(p["visual_digest_json"])
        prods = digest.get("detected_products_and_prices", [])
        assert len(prods) > 0, f"FAIL: Post {p['external_post_id']} missing detected products/prices"

    print(f"  [PASS] Catalog Cache Verified: {len(posts)} posts indexed for {account_id}")
    for p in posts[:3]:
        digest = json.loads(p["visual_digest_json"])
        prod = digest["detected_products_and_prices"][0]
        print(f"         * {p['external_post_id']} | pHash: {p['phash_hex']} | {prod['product_name']}: {prod['price_expression']}")
    return True


def run_test_4_customer_interaction(orchestrator: GeminiOrchestrator, storage: StorageEngine) -> bool:
    print("\n--- TEST 4: Low-Latency Customer Interaction (Fusha Colloquial & Price Accuracy) ---")
    account_id = "act_1071349136056783"
    post_context = storage.get_post_by_external_id(account_id, "post_dazaa_growth_01")
    assert post_context is not None, "FAIL: Growth package post not found in cache"

    persona = storage.get_persona(account_id)
    assert persona is not None, "FAIL: Persona not found for account"

    inbound_comment = "السلام عليكم، بكم باقة التسويق الرقمي المتكاملة وهل تشمل إدارة الإعلانات؟"
    sender_name = "أحمد القحطاني"

    start = time.perf_counter()
    decision = orchestrator.infer_interaction_decision(
        inbound_text=inbound_comment,
        sender_name=sender_name,
        interaction_type="comment",
        persona=persona,
        cached_post_digest=post_context,
    )
    latency_ms = int((time.perf_counter() - start) * 1000)

    reply = decision.public_comment_reply or decision.private_dm_reply or ""
    assert reply and len(reply.strip()) > 10, f"FAIL: Empty or invalid reply generated. Got: {reply}"
    assert "1500" in reply, f"FAIL: Reply failed to mention correct price 1500 ريال. Got: {reply}"

    # Dialect assertion: verify absence of heavy Egyptian colloquialisms
    forbidden_terms = ["يا فندم", "بكام ده", "منور", "حضرتك يا فندم", "البيج"]
    for term in forbidden_terms:
        assert term not in reply, f"FAIL: Reply contains forbidden non-Fusha dialect term '{term}'"

    print(f"  [PASS] Interaction Reply Generated in {latency_ms}ms")
    print(f"         Customer Inbound: '{inbound_comment}'")
    dec_val = decision.decision.value if hasattr(decision.decision, 'value') else decision.decision
    print(f"         Agent Decision  : {dec_val} (Confidence: {decision.confidence})")
    print(f"         Agent Response  : '{reply}'")
    print(f"         Price Accurately Quoted: 1500 ريال | Dialect: {persona['dialect']} (العامية الفصحى)")
    return True


async def run_test_5_operational_scopes(storage: StorageEngine) -> bool:
    print("\n--- TEST 5: Operational Scope Verification Oracle & Instagram Linkage ---")
    account_id = "act_1071349136056783"
    bm = StealthBrowserManager()
    oracle = ActiveAssociationOracle(storage=storage, browser_manager=bm)

    result = await oracle.verify_page_scopes(account_id)

    assert result.session_valid is True, f"FAIL: Session reported invalid. Details: {result.details}"
    assert result.can_moderate_comments is True, "FAIL: CAN_MODERATE_COMMENTS must be true"
    assert result.can_send_messages is True, "FAIL: CAN_SEND_MESSAGES must be true"
    assert result.health_status == PageHealthStatus.ACTIVE, f"FAIL: Expected ACTIVE health, got {result.health_status}"
    assert result.page_id == "1071349136056783", f"FAIL: Expected page_id 1071349136056783, got {result.page_id}"
    assert result.business_id == "766871999177306", f"FAIL: Expected business_id 766871999177306, got {result.business_id}"
    assert result.instagram_business_account_id != "", "FAIL: instagram_business_account_id must not be empty"

    print(f"  [PASS] Meta Business Suite Associated: asset_id={result.page_id}, business_id={result.business_id}")
    print(f"         CAN_MODERATE_COMMENTS: {result.can_moderate_comments}")
    print(f"         CAN_SEND_MESSAGES    : {result.can_send_messages}")
    print(f"         PAGE_HEALTH_STATUS   : {result.health_status.value}")
    print(f"         Instagram Linked ID  : {result.instagram_business_account_id}")
    return True


def main() -> None:
    print("================================================================================")
    print("  PRODUCTION META BUSINESS ASSET BINDING & VERIFICATION SUITE")
    print("  Target: Dazaa Marketing | Asset: 1071349136056783 | Business: 766871999177306")
    print("================================================================================")

    storage = StorageEngine()
    orchestrator = GeminiOrchestrator()

    # 1. Normalization
    t1 = run_test_1_account_normalization(storage)

    # 2. Vault Encryption
    t2 = run_test_2_session_vault_encryption("act_1071349136056783")

    # 3. Cold-Boot Catalog
    t3 = run_test_3_cold_boot_catalog(storage)

    # 4. Customer Interaction
    t4 = run_test_4_customer_interaction(orchestrator, storage)

    # 5. Operational Scopes
    t5 = asyncio.run(run_test_5_operational_scopes(storage))

    print("\n================================================================================")
    if all([t1, t2, t3, t4, t5]):
        print("  🎉 ALL 5/5 META ASSET BINDING & SCOPE VERIFICATION TESTS PASSED")
        print("  Status: PRODUCTION READY (Meta Page, Business Suite & IG Association Live)")
    else:
        print("  ❌ VERIFICATION SUITE FAILED")
        sys.exit(1)
    print("================================================================================\n")


if __name__ == "__main__":
    main()
