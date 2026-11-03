"""FastAPI Backend Server & Real-Time WebSocket Radar Broadcaster."""

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional
import psutil
import requests
from fastapi import FastAPI, HTTPException, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, PlainTextResponse
from pydantic import BaseModel, Field

from core.engine import StealthBrowserManager
from core.ingress import IngressManager
from core.llm import AgentDecision, GeminiOrchestrator, load_env_file
from core.storage import StorageEngine
from core.stream import compute_image_hashes, compress_image_to_webp
from core.verifier import ActiveAssociationOracle, DualPerspectiveVerifier
from core.worker import AutonomousWorker
from server.push import WebPushGateway

logger = logging.getLogger("server.api")

# Load environment configuration from .env
load_env_file()

app = FastAPI(
    title="Autonomous Social Media Multimodal Agent",
    version="2.1.0",
    description="Ultra-Low-Memory, Stealth Autonomous Social Media Daemon with Sovereign Mobile PWA Control Plane",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global runtime singletons
START_TIME = time.time()
storage = StorageEngine()
orchestrator = GeminiOrchestrator()
browser_manager = StealthBrowserManager(headless=True)
push_gateway = WebPushGateway(storage=storage)
oracle = ActiveAssociationOracle(storage=storage, browser_manager=browser_manager)
ghost_verifier = DualPerspectiveVerifier(browser_manager=browser_manager)
ingress_manager = IngressManager(port=8000, remote=False)
worker: Optional[AutonomousWorker] = None
worker_task: Optional[asyncio.Task] = None
active_websockets: List[WebSocket] = []


async def broadcast_radar_event(event: Dict[str, Any]) -> None:
    """Broadcast real-time events to all connected UI clients."""
    if not active_websockets:
        return
    message = json.dumps(event, ensure_ascii=False)
    disconnected = []
    for ws in active_websockets:
        try:
            await ws.send_text(message)
        except Exception:
            disconnected.append(ws)
    for ws in disconnected:
        if ws in active_websockets:
            active_websockets.remove(ws)


@app.on_event("startup")
async def startup_event() -> None:
    global worker, worker_task
    logger.info("Initializing Stealth Daemon and background worker...")
    await browser_manager.initialize()

    # Link security challenge callback to WebSocket alerts and Mobile Push
    async def on_challenge(acc_id: str, url: str) -> None:
        await broadcast_radar_event({
            "type": "HUMAN_INTERVENTION_REQUIRED",
            "account_id": acc_id,
            "url": url,
            "timestamp": time.time(),
        })
        push_gateway.broadcast_alert(
            title="⚠️ تنبيه أمني عاجل",
            body=f"الحساب {acc_id} يحتاج إلى حل تحدي أمني (Captcha / Checkpoint).",
            data={"url": "/", "account_id": acc_id},
        )

    browser_manager.on_challenge_callback = on_challenge

    worker = AutonomousWorker(
        storage=storage,
        orchestrator=orchestrator,
        browser_manager=browser_manager,
        on_event_broadcast=broadcast_radar_event,
    )
    worker_task = asyncio.create_task(worker.start())
    logger.info("Background worker loop successfully launched.")

    # Auto-provision normalized Dazaa Marketing account and purge any legacy raw URLs
    try:
        for a in storage.list_accounts():
            if "facebook.com" in a.get("id", ""):
                logger.info(f"Purging legacy URL account: {a['id']}")
                storage.purge_account_data(a["id"])

        if not storage.get_account("act_1071349136056783"):
            logger.info("Provisioning normalized act_1071349136056783 on startup...")
            storage.create_account(
                account_id="act_1071349136056783",
                platform="meta",
                page_name="Dazaa Marketing",
                page_id="1071349136056783",
                business_id="766871999177306",
                profile_url="https://www.facebook.com/profile.php?id=61579682971434",
                system_prompt="أنت خدمة العملاء الرسمية لصفحة Dazaa Marketing. أجب بلباقة وسرعة واحترافية بالعامية الفصحى الراقية بناء على أسعار وتفاصيل المنتجات.",
                dialect="Fusha_Colloquial",
                tone_of_voice="friendly",
            )
            try:
                from core.crawler import PublicPageHarvester
                PublicPageHarvester(storage=storage).harvest_sync("act_1071349136056783")
            except Exception as he:
                logger.warning(f"Startup harvest note: {he}")
    except Exception as se:
        logger.warning(f"Startup account provisioning note: {se}")


@app.on_event("shutdown")
async def shutdown_event() -> None:
    if worker:
        worker.stop()
    if worker_task:
        worker_task.cancel()
    await browser_manager.close()


# =======================================================
# WEBSOCKET RADAR
# =======================================================


@app.websocket("/ws/radar")
async def websocket_radar(websocket: WebSocket) -> None:
    await websocket.accept()
    active_websockets.append(websocket)
    try:
        # Send initial health ping
        rss_mb = psutil.Process().memory_info().rss / (1024 * 1024)
        await websocket.send_text(json.dumps({
            "type": "RADAR_INIT",
            "rss_mb": round(rss_mb, 2),
            "memory_ceiling_mb": 1000.0,
            "active_tenants": len(storage.list_accounts()),
        }))
        while True:
            data = await websocket.receive_text()
            # Handle incoming ping / messages
            await websocket.send_text(json.dumps({"type": "PONG", "received": data}))
    except WebSocketDisconnect:
        if websocket in active_websockets:
            active_websockets.remove(websocket)


# =======================================================
# REST API SCHEMAS & ROUTES
# =======================================================


class AccountCreateRequest(BaseModel):
    account_id: str
    platform: str
    page_name: str
    avatar_url: Optional[str] = ""
    proxy_url: Optional[str] = ""
    page_id: Optional[str] = ""
    business_id: Optional[str] = ""
    profile_url: Optional[str] = ""
    system_prompt: Optional[str] = None
    dialect: Optional[str] = "Fusha_Colloquial"


class AccountStatusUpdateRequest(BaseModel):
    status: str = Field(..., description="Target status: 'active', 'paused', or 'disconnected'")


class AccountSessionImportRequest(BaseModel):
    cookies: Optional[List[Dict[str, Any]]] = None
    raw_cookies_str: Optional[str] = None
    storage_state_json: Optional[Dict[str, Any]] = None


class PersonaUpdateRequest(BaseModel):
    system_prompt: Optional[str] = None
    tone_of_voice: Optional[str] = None
    dialect: Optional[str] = None
    bad_words_list: Optional[List[str]] = None
    auto_delete_toxic: Optional[bool] = None
    auto_hide_spam: Optional[bool] = None
    vision_enabled: Optional[bool] = None
    confidence_threshold: Optional[float] = None


class PostIndexRequest(BaseModel):
    post_id: str
    post_url: str
    caption: str
    media_type: str = "image"
    media_base64: Optional[str] = None


class SavedReplyCreateRequest(BaseModel):
    category: str
    trigger_keywords: List[str]
    reply_text: str
    attachment_reference: Optional[str] = ""


class SandboxSimulateRequest(BaseModel):
    account_id: str
    inbound_text: str
    sender_name: Optional[str] = "Customer"
    interaction_type: str = "comment"
    image_base64: Optional[str] = None
    post_id: Optional[str] = None


def _resolve_account_id(acc_id: str) -> str:
    """Normalize incoming raw URLs, encoded strings, or IDs to canonical tenant ID."""
    if not acc_id:
        return "act_1071349136056783"
    import urllib.parse
    decoded = urllib.parse.unquote(acc_id)
    if "facebook.com" in decoded or "1071349136056783" in decoded or "61579682971434" in decoded:
        return "act_1071349136056783"
    return decoded


@app.get("/api/accounts")
def list_accounts() -> List[Dict[str, Any]]:
    return storage.list_accounts()


@app.post("/api/accounts")
def create_account(req: AccountCreateRequest) -> Dict[str, Any]:
    acc = storage.create_account(
        account_id=req.account_id,
        platform=req.platform,
        page_name=req.page_name,
        avatar_url=req.avatar_url or "",
        proxy_url=req.proxy_url or "",
        page_id=req.page_id or "",
        business_id=req.business_id or "",
        profile_url=req.profile_url or "",
        system_prompt=req.system_prompt,
        dialect=req.dialect or "Fusha_Colloquial",
    )
    return acc


@app.post("/api/accounts/{account_id:path}/disconnect")
async def disconnect_account(account_id: str) -> Dict[str, Any]:
    account_id = _resolve_account_id(account_id)
    acc = storage.get_account(account_id)
    if not acc:
        raise HTTPException(status_code=404, detail="Account not found")
    await browser_manager.close_tenant_context(account_id)
    storage.disconnect_account_session(account_id)
    await broadcast_radar_event({
        "type": "ACCOUNT_UPDATED",
        "account_id": account_id,
        "session_status": "disconnected",
    })
    return {"status": "disconnected", "account_id": account_id}


@app.delete("/api/accounts/{account_id:path}")
async def delete_account(account_id: str) -> Dict[str, Any]:
    account_id = _resolve_account_id(account_id)
    acc = storage.get_account(account_id)
    if not acc:
        raise HTTPException(status_code=404, detail="Account not found")
    await browser_manager.close_tenant_context(account_id)
    purged_stats = storage.purge_account_data(account_id)
    await broadcast_radar_event({
        "type": "ACCOUNT_DELETED",
        "account_id": account_id,
    })
    return {"status": "deleted", "account_id": account_id, "purged": purged_stats}


@app.post("/api/accounts/{account_id:path}/status")
async def update_account_status_route(account_id: str, req: AccountStatusUpdateRequest) -> Dict[str, Any]:
    account_id = _resolve_account_id(account_id)
    acc = storage.get_account(account_id)
    if not acc:
        raise HTTPException(status_code=404, detail="Account not found")
    valid_statuses = ("active", "paused", "expired", "disconnected", "needs_challenge")
    if req.status not in valid_statuses:
        raise HTTPException(status_code=400, detail="Invalid status. Must be 'active' or 'paused'.")
    db_status = "expired" if req.status == "disconnected" else req.status
    storage.update_account_status(account_id, db_status)
    await broadcast_radar_event({
        "type": "ACCOUNT_UPDATED",
        "account_id": account_id,
        "session_status": req.status,
    })
    return {"status": "success", "account_id": account_id, "session_status": req.status}


@app.post("/api/accounts/{account_id:path}/session")
async def import_account_session(account_id: str, req: AccountSessionImportRequest) -> Dict[str, Any]:
    account_id = _resolve_account_id(account_id)
    acc = storage.get_account(account_id)
    if not acc:
        # Auto-provision canonical entity if missing
        acc = storage.create_account(
            account_id=account_id,
            platform="meta",
            page_name="Dazaa Marketing",
            page_id="1071349136056783",
            business_id="766871999177306",
            profile_url="https://www.facebook.com/profile.php?id=61579682971434",
            system_prompt="أنت خدمة العملاء الرسمية لصفحة Dazaa Marketing. أجب بلباقة وسرعة واحترافية بالعامية الفصحى الراقية بناء على أسعار وتفاصيل المنتجات.",
            dialect="Fusha_Colloquial",
            tone_of_voice="friendly",
        )

    state: Dict[str, Any] = {"cookies": [], "origins": []}

    if req.storage_state_json and isinstance(req.storage_state_json, dict):
        state = req.storage_state_json
    elif req.cookies:
        state["cookies"] = req.cookies
    elif req.raw_cookies_str:
        content = req.raw_cookies_str.strip()
        cookie_list = []
        domain = ".facebook.com" if acc.get("platform") == "meta" else ".tiktok.com"

        # 1. Check if user pasted JSON array or Playwright storageState object
        if content.startswith("[") or content.startswith("{"):
            try:
                parsed_json = json.loads(content)
                if isinstance(parsed_json, list):
                    cookie_list = parsed_json
                elif isinstance(parsed_json, dict):
                    cookie_list = parsed_json.get("cookies", [])
                    state["origins"] = parsed_json.get("origins", [])
            except json.JSONDecodeError:
                pass

        # 2. Check if Netscape/curl format
        if not cookie_list and "\t" in content:
            for line in content.splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split("\t")
                if len(parts) >= 7:
                    cookie_list.append({
                        "name": parts[5],
                        "value": parts[6],
                        "domain": parts[0],
                        "path": parts[2],
                        "httpOnly": parts[5] in ("xs", "datr", "fr", "sb"),
                        "secure": parts[3].upper() == "TRUE",
                        "sameSite": "Lax",
                    })

        # 3. Standard raw cookie string (e.g. c_user=...; xs=...)
        if not cookie_list:
            for item in content.split(";"):
                item = item.strip()
                if "=" in item:
                    k, v = item.split("=", 1)
                    cookie_list.append({
                        "name": k.strip(),
                        "value": v.strip(),
                        "domain": domain,
                        "path": "/",
                        "httpOnly": False,
                        "secure": True,
                        "sameSite": "Lax",
                    })
        state["cookies"] = cookie_list

    if not state.get("cookies"):
        raise HTTPException(status_code=400, detail="No valid cookies found in request payload.")

    from core.engine import TenantContext
    tenant = TenantContext(account_id, acc.get("platform", "meta"), {})
    tenant.save_encrypted_state(state)
    storage.update_account_status(account_id, "active")

    await broadcast_radar_event({
        "type": "ACCOUNT_UPDATED",
        "account_id": account_id,
        "session_status": "active",
    })

    return {
        "status": "session_imported",
        "account_id": account_id,
        "cookies_count": len(state.get("cookies", [])),
    }


@app.get("/api/accounts/{account_id:path}/persona")
def get_persona(account_id: str) -> Dict[str, Any]:
    account_id = _resolve_account_id(account_id)
    persona = storage.get_persona(account_id)
    if not persona:
        raise HTTPException(status_code=404, detail="Persona not found")
    return persona


@app.post("/api/accounts/{account_id:path}/persona")
def update_persona(account_id: str, req: PersonaUpdateRequest) -> Dict[str, Any]:
    account_id = _resolve_account_id(account_id)
    updates = req.model_dump(exclude_unset=True)
    storage.update_persona(account_id, updates)
    return {"status": "success", "persona": storage.get_persona(account_id)}


@app.get("/api/accounts/{account_id:path}/posts")
def list_posts(account_id: str) -> List[Dict[str, Any]]:
    account_id = _resolve_account_id(account_id)
    return storage.list_posts(account_id)


@app.post("/api/accounts/{account_id:path}/posts/index")
def index_post(account_id: str, req: PostIndexRequest) -> Dict[str, Any]:
    account_id = _resolve_account_id(account_id)
    task_id = storage.enqueue_task(
        account_id=account_id,
        task_type="INDEX_POST",
        payload=req.model_dump(),
        priority=2,
    )
    return {"status": "enqueued", "task_id": task_id}


@app.get("/api/accounts/{account_id:path}/saved-replies")
def get_saved_replies(account_id: str) -> List[Dict[str, Any]]:
    account_id = _resolve_account_id(account_id)
    return storage.get_saved_replies(account_id)


@app.post("/api/accounts/{account_id:path}/saved-replies")
def add_saved_reply(account_id: str, req: SavedReplyCreateRequest) -> Dict[str, Any]:
    account_id = _resolve_account_id(account_id)
    reply_id = storage.add_saved_reply(
        account_id=account_id,
        category=req.category,
        trigger_keywords=req.trigger_keywords,
        reply_text=req.reply_text,
        attachment_reference=req.attachment_reference or "",
    )
    return {"status": "created", "reply_id": reply_id}


@app.get("/api/accounts/{account_id:path}/logs")
def get_logs(account_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    account_id = _resolve_account_id(account_id)
    return storage.get_recent_logs(account_id, limit=limit)


@app.get("/api/system/metrics")
def get_system_metrics() -> Dict[str, Any]:
    process = psutil.Process()
    rss_mb = process.memory_info().rss / (1024 * 1024)
    return {
        "rss_mb": round(rss_mb, 2),
        "memory_ceiling_mb": 1000.0,
        "is_within_ceiling": rss_mb < 1000.0,
        "cpu_percent": process.cpu_percent(interval=0.1),
        "active_contexts": len(browser_manager.contexts),
        "total_accounts": len(storage.list_accounts()),
    }


# =======================================================
# MULTIMODAL SANDBOX SIMULATOR (TEST BEFORE GOING LIVE)
# =======================================================


@app.post("/api/sandbox/simulate")
def simulate_interaction(req: SandboxSimulateRequest) -> Dict[str, Any]:
    start_time = time.monotonic()
    account_id = req.account_id
    persona = storage.get_persona(account_id) or {
        "system_prompt": "خدمة عملاء ودودة واحترافية",
        "tone_of_voice": "friendly",
        "dialect": "Egyptian",
    }

    # 1. Zero-Cost Shield Check
    shield_res = storage.check_zero_cost_shield(account_id, req.inbound_text)
    if shield_res:
        action, reason = shield_res
        latency_ms = int((time.monotonic() - start_time) * 1000)
        return {
            "stage": "STAGE_1_ZERO_COST_SHIELD",
            "decision": action,
            "moderation_reason": reason,
            "toxicity_score": 1.0 if action == "DELETE" else 0.7,
            "confidence": 1.0,
            "matched_by_phash": False,
            "latency_ms": latency_ms,
            "zero_token_cost": True,
        }

    # 2. Local Perceptual Hash Check
    matched_by_hash = False
    cached_post = None
    inbound_image_bytes = None
    if req.image_base64:
        try:
            inbound_image_bytes = base64.b64decode(req.image_base64)
            dhash, phash = compute_image_hashes(inbound_image_bytes)
            match = storage.match_post_by_hash(account_id, phash, hash_type="phash", max_distance=10)
            if match:
                cached_post, dist = match
                matched_by_hash = True
        except Exception as e:
            logger.warning(f"Sandbox pHash match failed: {e}")

    if not cached_post and req.post_id:
        cached_post = storage.get_post_by_external_id(account_id, req.post_id)

    matched_reply = storage.match_saved_reply(account_id, req.inbound_text)
    post_digest = cached_post.get("visual_digest") if cached_post else None

    # Compress media in memory if sending to LLM
    compressed_media = None
    if inbound_image_bytes and not matched_by_hash:
        compressed_media = compress_image_to_webp(inbound_image_bytes, max_dim=800, quality=75)

    # 3. Structured Output Inference
    decision: AgentDecision = orchestrator.infer_interaction_decision(
        inbound_text=req.inbound_text,
        sender_name=req.sender_name or "Sandbox User",
        interaction_type=req.interaction_type,
        persona=persona,
        cached_post_digest=post_digest,
        matched_saved_reply=matched_reply,
        customer_media_bytes=compressed_media,
    )

    latency_ms = int((time.monotonic() - start_time) * 1000)

    return {
        "stage": "STAGE_3_LLM_INFERENCE",
        "decision": decision.decision,
        "understood_context": decision.understood_context,
        "public_comment_reply": decision.public_comment_reply,
        "private_dm_reply": decision.private_dm_reply,
        "moderation_reason": decision.moderation_reason,
        "toxicity_score": decision.toxicity_score,
        "confidence": decision.confidence,
        "matched_by_phash": matched_by_hash,
        "matched_post_id": cached_post.get("external_post_id") if cached_post else None,
        "latency_ms": latency_ms,
    }


# =======================================================
# PWA & SERVICE WORKER ASSETS
# =======================================================

MANIFEST_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dashboard", "manifest.json")
SW_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dashboard", "sw.js")
ICONS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dashboard", "icons")


@app.get("/manifest.json")
def get_manifest() -> Any:
    if os.path.exists(MANIFEST_FILE):
        return FileResponse(MANIFEST_FILE, media_type="application/manifest+json")
    raise HTTPException(status_code=404, detail="Manifest not found")


@app.get("/sw.js")
def get_sw() -> Any:
    if os.path.exists(SW_FILE):
        return FileResponse(
            SW_FILE,
            media_type="application/javascript",
            headers={"Service-Worker-Allowed": "/"},
        )
    raise HTTPException(status_code=404, detail="Service worker not found")


@app.get("/icons/{icon_name}")
def get_icon(icon_name: str) -> Any:
    path = os.path.join(ICONS_DIR, icon_name)
    if os.path.exists(path):
        return FileResponse(path, media_type="image/png")
    raise HTTPException(status_code=404, detail="Icon not found")


# =======================================================
# MOBILE WEB PUSH NOTIFICATIONS (RFC 8291)
# =======================================================


class PushSubscriptionRequest(BaseModel):
    endpoint: str
    keys: Dict[str, str]
    device_name: Optional[str] = "Mobile Client"


@app.get("/api/push/public-key")
def get_vapid_public_key() -> Dict[str, str]:
    return {"public_key": push_gateway.get_public_key()}


@app.post("/api/push/subscribe")
def subscribe_push(req: PushSubscriptionRequest) -> Dict[str, Any]:
    sub_id = push_gateway.register_device(
        subscription_info=req.model_dump(),
        device_name=req.device_name or "Mobile Client",
    )
    return {"status": "subscribed", "subscription_id": sub_id}


@app.post("/api/push/test")
def test_push_alert() -> Dict[str, Any]:
    count = push_gateway.broadcast_alert(
        title="🔔 اختبار إشعار الهاتف بنجاح",
        body="تم الاتصال بالموجّه الآلي السيادي واستلام الإشعار على شاشة القفل بنجاح.",
        data={"url": "/"},
    )
    return {"status": "broadcast_complete", "devices_notified": count}


class PushActionRequest(BaseModel):
    action: str
    data: Optional[Dict[str, Any]] = None


@app.post("/api/push/action")
def handle_push_action(req: PushActionRequest) -> Dict[str, Any]:
    """Resolve interactive lock-screen button actions (e.g. setting price directly)."""
    return push_gateway.resolve_lock_screen_action(action=req.action, data=req.data or {})


# =======================================================
# ACTIVE ASSOCIATION & SHADOWBAN ORACLE
# =======================================================


@app.post("/api/accounts/{account_id:path}/verify-connection")
async def verify_page_connection(account_id: str) -> Dict[str, Any]:
    account_id = _resolve_account_id(account_id)
    res = await oracle.verify_page_scopes(account_id)
    return res.model_dump()


class ShadowbanCheckRequest(BaseModel):
    post_url: str
    comment_id: str
    comment_text: str


@app.post("/api/accounts/{account_id:path}/check-shadowban")
async def check_shadowban(account_id: str, req: ShadowbanCheckRequest) -> Dict[str, Any]:
    account_id = _resolve_account_id(account_id)
    res = await ghost_verifier.verify_public_visibility(
        post_url=req.post_url,
        comment_id=req.comment_id,
        comment_text=req.comment_text,
    )
    if res.shadowban_detected:
        push_gateway.broadcast_alert(
            title="⚠️ تم رصد حظر خفي (Shadowban)!",
            body=f"التعليق غير مرئي للجمهور على المنشور: {req.post_url[:40]}",
            data={"post_url": req.post_url},
        )
    return res.model_dump()


@app.get("/api/health")
def health_check() -> Dict[str, Any]:
    rss_mb = psutil.Process().memory_info().rss / (1024 * 1024)
    return {
        "status": "ok",
        "service": "autonomous-social-agent",
        "uptime_seconds": round(time.time() - START_TIME, 2),
        "rss_mb": round(rss_mb, 2),
        "tenants_count": len(storage.list_accounts()),
        "cloud_mode": bool(os.environ.get("IS_CLOUD_DEPLOYMENT", False)),
    }


@app.get("/api/ingress/status")
def get_ingress_status() -> Dict[str, Any]:
    url = ingress_manager.public_url or ingress_manager.local_url
    return {
        "active_url": url,
        "is_remote": ingress_manager.remote,
        "lan_url": ingress_manager.local_url,
    }


# =======================================================
# META OFFICIAL EVENT-DRIVEN WEBHOOK (PRIMARY DUAL-RAIL INGRESS)
# =======================================================

META_VERIFY_TOKEN = os.environ.get("META_VERIFY_TOKEN", "dazaa_secret_verify_token_2026")
META_APP_SECRET = os.environ.get("META_APP_SECRET", "dazaa_app_secret_super_2026")
META_PAGE_ACCESS_TOKEN = os.environ.get("META_PAGE_ACCESS_TOKEN", "")


@app.get("/api/webhooks/meta")
def meta_webhook_verification(request: Request) -> Response:
    """RFC-compliant Meta Webhook Handshake Verification."""
    params = dict(request.query_params)
    mode = params.get("hub.mode")
    token = params.get("hub.verify_token")
    challenge = params.get("hub.challenge", "")

    if mode == "subscribe" and token == META_VERIFY_TOKEN:
        logger.info("Meta Webhook verification handshake succeeded.")
        return PlainTextResponse(challenge, status_code=200)

    logger.warning(f"Meta Webhook verification failed: mode={mode}, token_match={token == META_VERIFY_TOKEN}")
    raise HTTPException(status_code=403, detail="Verification token mismatch")


@app.post("/api/webhooks/meta")
async def meta_webhook_receiver(request: Request) -> Dict[str, Any]:
    """Inbound Meta Webhook Event Processor with HMAC-SHA256 signature verification,

    atomic idempotency, and dual-rail dispatch (Graph API -> Stealth Queue fallback).
    """
    raw_body = await request.body()
    signature_header = request.headers.get("X-Hub-Signature-256", "")

    # 1. Cryptographic Signature Assertion (HMAC-SHA256)
    if not signature_header or not signature_header.startswith("sha256="):
        logger.warning("Meta Webhook rejected: Missing or malformed X-Hub-Signature-256 header")
        raise HTTPException(status_code=403, detail="Missing or invalid signature header")

    expected_signature = "sha256=" + hmac.new(
        META_APP_SECRET.encode("utf-8"), raw_body, hashlib.sha256
    ).hexdigest()

    if not hmac.compare_digest(signature_header, expected_signature):
        logger.warning("Meta Webhook rejected: HMAC-SHA256 signature mismatch")
        raise HTTPException(status_code=403, detail="Cryptographic signature verification failed")

    # 2. Parse Inbound Payload
    try:
        payload = json.loads(raw_body.decode("utf-8"))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON body: {e}")

    entries = payload.get("entry", [])
    processed_events = []
    ignored_duplicates = 0

    for entry in entries:
        page_asset_id = str(entry.get("id", ""))
        account_id = _resolve_account_id(page_asset_id)

        # Process feed changes (comments)
        for change in entry.get("changes", []):
            if change.get("field") == "feed":
                val = change.get("value", {})
                item_type = val.get("item")
                if item_type in ("comment", "post"):
                    comment_id = str(val.get("comment_id") or val.get("post_id", ""))
                    if not comment_id:
                        continue

                    # 3. Atomic Idempotency Check & Replay Defense
                    is_new = storage.check_and_reserve_event(
                        account_id=account_id,
                        external_event_id=comment_id,
                        idempotency_key=f"meta_feed_{comment_id}",
                    )
                    if not is_new:
                        ignored_duplicates += 1
                        continue

                    inbound_text = val.get("message", "")
                    sender_name = val.get("sender_name") or val.get("from", {}).get("name", "Customer")
                    post_id = val.get("post_id", "")

                    # 4. Context Lookup & Multimodal Inference
                    cached_post = storage.get_post_by_external_id(account_id, post_id) if post_id else None
                    persona = storage.get_persona(account_id) or {
                        "system_prompt": "أنت خدمة العملاء الرسمية لصفحة Dazaa Marketing. أجب بلباقة وسرعة واحترافية بالعامية الفصحى.",
                        "dialect": "Fusha_Colloquial",
                        "tone_of_voice": "friendly",
                    }

                    decision = orchestrator.infer_interaction_decision(
                        inbound_text=inbound_text,
                        sender_name=sender_name,
                        interaction_type="comment",
                        persona=persona,
                        cached_post_digest=cached_post,
                    )

                    reply_text = decision.public_comment_reply or decision.private_dm_reply or ""

                    # 5. Dual-Rail Outbound Dispatch (Graph API Rail -> Stealth Worker Fallback)
                    dispatched_via = "STEALTH_QUEUE"
                    if META_PAGE_ACCESS_TOKEN:
                        try:
                            # Primary Rail: Instant Graph API v20.0
                            api_url = f"https://graph.facebook.com/v20.0/{comment_id}/comments"
                            resp = requests.post(
                                api_url,
                                params={"access_token": META_PAGE_ACCESS_TOKEN},
                                json={"message": reply_text},
                                timeout=6.0,
                            )
                            if resp.status_code in (200, 201):
                                dispatched_via = "GRAPH_API_V20"
                            else:
                                logger.warning(f"Graph API response {resp.status_code}, activating stealth circuit-breaker...")
                        except Exception as api_err:
                            logger.warning(f"Graph API rail network fault: {api_err}. Activating stealth fallback...")

                    if dispatched_via == "STEALTH_QUEUE":
                        # Secondary Rail: Enqueue for Autonomous Stealth Worker
                        storage.enqueue_task(
                            account_id=account_id,
                            task_type="PUBLISH_REPLY",
                            payload={
                                "comment_id": comment_id,
                                "reply_text": reply_text,
                                "post_id": post_id,
                                "target_url": f"https://www.facebook.com/{comment_id}",
                            },
                            priority=2,
                        )

                    # Log Interaction
                    storage.log_interaction(
                        account_id=account_id,
                        external_event_id=comment_id,
                        interaction_type="comment",
                        sender_name=sender_name,
                        inbound_text=inbound_text,
                        agent_action=decision.decision if isinstance(decision.decision, str) else str(decision.decision),
                        agent_response=reply_text,
                        confidence=decision.confidence,
                        post_id=post_id,
                    )

                    processed_events.append({
                        "event_id": comment_id,
                        "dispatched_via": dispatched_via,
                        "reply": reply_text,
                        "action": decision.decision,
                    })

    return {
        "status": "success",
        "processed_count": len(processed_events),
        "ignored_duplicates": ignored_duplicates,
        "events": processed_events,
    }


# =======================================================
# DASHBOARD HOSTING
# =======================================================


DASHBOARD_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dashboard", "index.html")


@app.get("/")
def serve_dashboard() -> Any:
    if os.path.exists(DASHBOARD_FILE):
        return FileResponse(DASHBOARD_FILE, media_type="text/html")
    return HTMLResponse("<h1>Autonomous Daemon Running</h1><p>Dashboard file missing.</p>")
