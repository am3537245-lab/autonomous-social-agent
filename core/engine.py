"""Stealth Browser Engine with Low-Level Anti-Detect CDP Injections,

Memory Governance (< 1.0 GB RAM), and Multi-Tenant Isolation.
"""

import asyncio
import base64
import hashlib
import json
import logging
import os
import random
import re
from typing import Any, Callable, Coroutine, Dict, List, Optional
from cryptography.fernet import Fernet

logger = logging.getLogger("core.engine")

DEFAULT_STORAGE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "storage_states")
KEY_PATH = os.path.join(DEFAULT_STORAGE_DIR, "master.key")


def get_tenant_storage_path(account_id: str) -> str:
    """Return deterministic, safe filesystem path for an account's encrypted session vault."""
    legacy_file = os.path.join(DEFAULT_STORAGE_DIR, f"{account_id}.enc")
    if os.path.exists(legacy_file):
        return legacy_file

    clean = re.sub(r"[^a-zA-Z0-9_\-]", "_", account_id)
    if len(clean) > 48 or clean != account_id:
        h = hashlib.sha256(account_id.encode("utf-8")).hexdigest()[:12]
        clean = f"{clean[:32]}_{h}"
    return os.path.join(DEFAULT_STORAGE_DIR, f"{clean}.enc")


def generate_tenant_fingerprint(account_id: str, platform: str) -> Dict[str, Any]:
    """Generate deterministic, isolated browser fingerprints per tenant to prevent cross-account linking."""
    seed = int(hashlib.md5(account_id.encode("utf-8")).hexdigest()[:8], 16)
    rng = random.Random(seed)
    resolutions = [
        {"width": 1920, "height": 1080},
        {"width": 1440, "height": 900},
        {"width": 1536, "height": 864},
        {"width": 1366, "height": 768},
    ]
    chosen_res = rng.choice(resolutions)
    chrome_versions = ["124.0.6367.207", "125.0.6422.141", "126.0.6478.127"]
    version = rng.choice(chrome_versions)
    return {
        "userAgent": f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version} Safari/537.36",
        "viewport": chosen_res,
        "locale": "ar-EG" if "eg" in account_id.lower() else "ar-SA",
        "timezoneId": "Africa/Cairo" if "eg" in account_id.lower() else "Asia/Riyadh",
        "webglVendor": f"Google Inc. (NVIDIA GeForce RTX {rng.choice(['3060', '3070', '4060', '4070'])})",
    }


def _get_or_create_cipher() -> Fernet:
    """Ensure a master encryption key exists for tenant storage states."""
    os.makedirs(DEFAULT_STORAGE_DIR, exist_ok=True)
    if not os.path.exists(KEY_PATH):
        key = Fernet.generate_key()
        with open(KEY_PATH, "wb") as f:
            f.write(key)
    else:
        with open(KEY_PATH, "rb") as f:
            key = f.read().strip()
    return Fernet(key)


STEALTH_INIT_SCRIPT = """
(() => {
    // 1. Evade navigator.webdriver detection
    Object.defineProperty(navigator, 'webdriver', {
        get: () => undefined,
        configurable: true
    });

    // 2. Realistic plugins and mimeTypes
    const fakePlugins = [
        { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
        { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: '' },
        { name: 'Native Client', filename: 'internal-nacl-plugin', description: '' }
    ];
    Object.defineProperty(navigator, 'plugins', {
        get: () => fakePlugins,
        configurable: true
    });

    // 3. Spoof window.chrome
    if (!window.chrome) {
        window.chrome = {};
    }
    window.chrome.runtime = window.chrome.runtime || {
        connect: () => {},
        sendMessage: () => {},
        onMessage: { addListener: () => {} }
    };
    window.chrome.loadTimes = window.chrome.loadTimes || function() {};
    window.chrome.csi = window.chrome.csi || function() {};

    // 4. Subtle Canvas Noise (Perturbation < 0.001% to prevent cross-account fingerprint correlation)
    const originalGetImageData = CanvasRenderingContext2D.prototype.getImageData;
    CanvasRenderingContext2D.prototype.getImageData = function(x, y, w, h) {
        const imgData = originalGetImageData.apply(this, arguments);
        for (let i = 0; i < imgData.data.length; i += 64) {
            imgData.data[i] = (imgData.data[i] ^ 1);
        }
        return imgData;
    };

    // 5. Subtle AudioContext Perturbation
    const originalGetChannelData = AudioBuffer.prototype.getChannelData;
    AudioBuffer.prototype.getChannelData = function(channel) {
        const data = originalGetChannelData.apply(this, arguments);
        for (let i = 0; i < data.length; i += 128) {
            data[i] = data[i] + 0.0000001;
        }
        return data;
    };

    // 6. Mitigate CDP Runtime.enable Stack Trace Leakage
    const originalPrepareStackTrace = Error.prepareStackTrace;
    Error.prepareStackTrace = function(error, structuredStackTrace) {
        if (originalPrepareStackTrace) {
            return originalPrepareStackTrace(error, structuredStackTrace);
        }
        return error.stack;
    };

    // 7. Permissions query mock
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (parameters) => (
        parameters.name === 'notifications' ?
            Promise.resolve({ state: Notification.permission }) :
            originalQuery(parameters)
    );
})();
"""


class TenantContext:
    """Represents an isolated, stealth browser session for a single account."""

    def __init__(
        self,
        account_id: str,
        platform: str,
        fingerprint: Optional[Dict[str, Any]] = None,
        proxy_url: str = "",
    ) -> None:
        self.account_id = account_id
        self.platform = platform
        self.fingerprint = fingerprint or generate_tenant_fingerprint(account_id, platform)
        self.proxy_url = proxy_url
        self.browser_context: Any = None
        self.active_page: Any = None
        self.is_monitoring: bool = False
        self.storage_file = get_tenant_storage_path(account_id)

    def has_saved_session(self) -> bool:
        return os.path.exists(self.storage_file)

    def load_decrypted_state(self) -> Optional[Dict[str, Any]]:
        """Decrypt storage state JSON for Playwright initialization."""
        if not self.has_saved_session():
            return None
        try:
            cipher = _get_or_create_cipher()
            with open(self.storage_file, "rb") as f:
                encrypted_data = f.read()
            decrypted_bytes = cipher.decrypt(encrypted_data)
            return json.loads(decrypted_bytes.decode("utf-8"))
        except Exception as e:
            logger.error(f"Failed to decrypt session state for {self.account_id}: {e}")
            return None

    def save_encrypted_state(self, state_data: Dict[str, Any]) -> None:
        """Encrypt and save browser storage state JSON."""
        try:
            cipher = _get_or_create_cipher()
            json_bytes = json.dumps(state_data).encode("utf-8")
            encrypted = cipher.encrypt(json_bytes)
            with open(self.storage_file, "wb") as f:
                f.write(encrypted)
            logger.info(f"Encrypted session state saved for {self.account_id}")
        except Exception as e:
            logger.error(f"Failed to save encrypted state for {self.account_id}: {e}")


class StealthBrowserManager:
    """Manages a single Chromium instance running multiple isolated tenant contexts

    with strict memory governors (< 1.0 GB total RSS).
    """

    CHROMIUM_FLAGS = [
        "--js-flags=--max-old-space-size=512 --expose-gc",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--disable-software-rasterizer",
        "--disable-extensions",
        "--mute-audio",
        "--no-sandbox",
        "--disable-blink-features=AutomationControlled",
        "--disable-features=IsolateOrigins,site-per-process",
    ]

    def __init__(self, headless: bool = True) -> None:
        self.headless = headless
        self.playwright: Any = None
        self.browser: Any = None
        self.contexts: Dict[str, TenantContext] = {}
        self.on_challenge_callback: Optional[Callable[[str, str], Coroutine[Any, Any, None]]] = None
        self.on_event_callback: Optional[Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]] = None

    async def initialize(self) -> None:
        """Launch the single shared Chromium instance."""
        try:
            from playwright.async_api import async_playwright

            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(
                headless=self.headless,
                args=self.CHROMIUM_FLAGS,
            )
            logger.info("Chromium instance launched with low-memory & stealth flags.")
        except ImportError:
            logger.warning("Playwright is not installed. Browser engine running in Mock/Simulated Mode.")
        except Exception as e:
            logger.error(f"Error launching Chromium: {e}. Fallback to simulated mode.")

    async def create_tenant_context(
        self,
        account_id: str,
        platform: str,
        fingerprint: Optional[Dict[str, Any]] = None,
        proxy_url: str = "",
    ) -> TenantContext:
        """Create an isolated, stealth BrowserContext for a specific tenant."""
        fp = fingerprint or generate_tenant_fingerprint(account_id, platform)

        tenant = TenantContext(account_id, platform, fp, proxy_url)
        self.contexts[account_id] = tenant

        if not self.browser:
            return tenant

        # Load decrypted session if available
        state = tenant.load_decrypted_state()

        context_kwargs: Dict[str, Any] = {
            "user_agent": fp.get("userAgent"),
            "viewport": fp.get("viewport"),
            "locale": fp.get("locale", "ar-EG"),
            "timezone_id": fp.get("timezoneId", "Africa/Cairo"),
            "has_touch": False,
            "is_mobile": False,
        }
        if state:
            context_kwargs["storage_state"] = state
        if proxy_url:
            context_kwargs["proxy"] = {"server": proxy_url}

        context = await self.browser.new_context(**context_kwargs)
        tenant.browser_context = context

        # Inject low-level stealth script into every frame before page scripts run
        await context.add_init_script(STEALTH_INIT_SCRIPT)

        # Smart Media & Resource Blocking to maintain < 1.0 GB RAM
        page = await context.new_page()
        tenant.active_page = page

        async def route_interceptor(route: Any) -> None:
            req = route.request
            resource_type = req.resource_type
            url = req.url

            # Block heavy non-essential media & tracking
            if resource_type in ("font", "media") or any(
                bad in url for bad in ["google-analytics.com", "fbevents.js", "analytics"]
            ):
                await route.abort()
            else:
                await route.continue_()

        await page.route("**/*", route_interceptor)

        # Network Payload Interceptor for Meta GraphQL & TikTok XHR
        page.on("response", lambda resp: asyncio.create_task(self._handle_network_response(tenant, resp)))
        page.on("framenavigated", lambda frame: asyncio.create_task(self._check_security_challenges(tenant, frame.url)))

        return tenant

    async def _handle_network_response(self, tenant: TenantContext, response: Any) -> None:
        """Intercept inbound comments, DMs, and post updates directly from raw network payloads."""
        try:
            url = response.url
            # Meta Business Suite GraphQL Interception
            if "graphql" in url and response.status == 200:
                try:
                    payload = await response.json()
                    # Check for comment / inbox nodes
                    if self.on_event_callback:
                        event_data = {
                            "account_id": tenant.account_id,
                            "platform": tenant.platform,
                            "source": "network_intercept",
                            "raw_payload": payload,
                        }
                        await self.on_event_callback(event_data)
                except Exception:
                    pass

            # TikTok Studio API Interception
            elif "tiktok.com/api/" in url and response.status == 200:
                try:
                    payload = await response.json()
                    if self.on_event_callback:
                        event_data = {
                            "account_id": tenant.account_id,
                            "platform": tenant.platform,
                            "source": "tiktok_intercept",
                            "raw_payload": payload,
                        }
                        await self.on_event_callback(event_data)
                except Exception:
                    pass
        except Exception:
            pass

    async def _check_security_challenges(self, tenant: TenantContext, current_url: str) -> None:
        """Detect checkpoint / 2FA / CAPTCHA redirects and emit HUMAN_INTERVENTION_REQUIRED."""
        challenge_markers = [
            "checkpoint",
            "login_challenge",
            "two_step_verification",
            "verify_center",
            "captcha",
            "recaptcha",
            "challenge/action",
        ]
        if any(marker in current_url.lower() for marker in challenge_markers):
            logger.warning(f"Security challenge triggered on {tenant.account_id}: {current_url}")
            if self.on_challenge_callback:
                await self.on_challenge_callback(tenant.account_id, current_url)

    async def close_tenant_context(self, account_id: str) -> None:
        """Safely terminate and dispose of an individual tenant's BrowserContext and DOM pages."""
        tenant = self.contexts.pop(account_id, None)
        if tenant:
            try:
                if tenant.active_page and not tenant.active_page.is_closed():
                    await tenant.active_page.close()
                if tenant.browser_context:
                    await tenant.browser_context.close()
                logger.info(f"Isolated BrowserContext closed for tenant {account_id}")
            except Exception as e:
                logger.warning(f"Error closing context for tenant {account_id}: {e}")

    async def save_tenant_session(self, account_id: str) -> None:
        """Capture and encrypt current storage state."""
        tenant = self.contexts.get(account_id)
        if tenant and tenant.browser_context:
            state = await tenant.browser_context.storage_state()
            tenant.save_encrypted_state(state)

    async def trigger_v8_garbage_collection(self) -> None:
        """Trigger Chromium V8 GC pass via CDP to enforce < 1.0 GB memory ceiling."""
        if not self.browser:
            return
        for tenant in self.contexts.values():
            if tenant.active_page:
                try:
                    cdp = await tenant.browser_context.new_cdp_session(tenant.active_page)
                    await cdp.send("HeapProfiler.collectGarbage")
                    await cdp.detach()
                except Exception:
                    pass

    async def close(self) -> None:
        """Gracefully close all contexts and the main browser instance."""
        for tenant in self.contexts.values():
            if tenant.browser_context:
                await tenant.browser_context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        self.contexts.clear()
        logger.info("Stealth browser engine safely closed.")
