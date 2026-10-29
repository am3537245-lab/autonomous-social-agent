"""FastAPI Backend Server & Real-Time WebSocket Radar Broadcaster."""

import asyncio
import base64
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional
import psutil
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
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
    )
    return acc


@app.get("/api/accounts/{account_id}/persona")
def get_persona(account_id: str) -> Dict[str, Any]:
    persona = storage.get_persona(account_id)
    if not persona:
        raise HTTPException(status_code=404, detail="Persona not found")
    return persona


@app.post("/api/accounts/{account_id}/persona")
def update_persona(account_id: str, req: PersonaUpdateRequest) -> Dict[str, Any]:
    updates = req.model_dump(exclude_unset=True)
    storage.update_persona(account_id, updates)
    return {"status": "success", "persona": storage.get_persona(account_id)}


@app.get("/api/accounts/{account_id}/posts")
def list_posts(account_id: str) -> List[Dict[str, Any]]:
    return storage.list_posts(account_id)


@app.post("/api/accounts/{account_id}/posts/index")
def index_post(account_id: str, req: PostIndexRequest) -> Dict[str, Any]:
    task_id = storage.enqueue_task(
        account_id=account_id,
        task_type="INDEX_POST",
        payload=req.model_dump(),
        priority=2,
    )
    return {"status": "enqueued", "task_id": task_id}


@app.get("/api/accounts/{account_id}/saved-replies")
def get_saved_replies(account_id: str) -> List[Dict[str, Any]]:
    return storage.get_saved_replies(account_id)


@app.post("/api/accounts/{account_id}/saved-replies")
def add_saved_reply(account_id: str, req: SavedReplyCreateRequest) -> Dict[str, Any]:
    reply_id = storage.add_saved_reply(
        account_id=account_id,
        category=req.category,
        trigger_keywords=req.trigger_keywords,
        reply_text=req.reply_text,
        attachment_reference=req.attachment_reference or "",
    )
    return {"status": "created", "reply_id": reply_id}


@app.get("/api/accounts/{account_id}/logs")
def get_logs(account_id: str, limit: int = 50) -> List[Dict[str, Any]]:
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


@app.post("/api/accounts/{account_id}/verify-connection")
async def verify_page_connection(account_id: str) -> Dict[str, Any]:
    res = await oracle.verify_page_scopes(account_id)
    return res.model_dump()


class ShadowbanCheckRequest(BaseModel):
    post_url: str
    comment_id: str
    comment_text: str


@app.post("/api/accounts/{account_id}/check-shadowban")
async def check_shadowban(account_id: str, req: ShadowbanCheckRequest) -> Dict[str, Any]:
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
# DASHBOARD HOSTING
# =======================================================


DASHBOARD_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dashboard", "index.html")


@app.get("/")
def serve_dashboard() -> Any:
    if os.path.exists(DASHBOARD_FILE):
        return FileResponse(DASHBOARD_FILE, media_type="text/html")
    return HTMLResponse("<h1>Autonomous Daemon Running</h1><p>Dashboard file missing.</p>")
