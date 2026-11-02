"""Dual-Mode Session Onboarding Bridge & Cookie Vault Ingestion for Meta Business Suite.

Supports:
1. Direct Cookie Ingestion Mode: Parses raw cookie strings (c_user=...; xs=...), JSON, or Netscape files,
   validates structural compliance, and writes encrypted payload to storage_states/{account_id}.enc.
2. Interactive Headful Handshake Mode: Launches clean Playwright Chromium instance directly to
   Meta Business Suite, waits for authentication, captures cookies, encrypts them, and safely exits.
"""

import argparse
import asyncio
import codecs
import json
import logging
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional, Tuple, Union

# Set UTF-8 output encoding for Windows PowerShell compatibility
sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, "replace")
sys.stderr = codecs.getwriter("utf-8")(sys.stderr.buffer, "replace")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("onboard_meta_page")

# Ensure repository root is in sys.path
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from core.engine import TenantContext, get_tenant_storage_path
from core.storage import StorageEngine


def parse_raw_cookie_string(raw_str: str, domain: str = ".facebook.com") -> List[Dict[str, Any]]:
    """Parse cookie string 'c_user=123; xs=abc; ...' into Playwright cookie list."""
    cookies: List[Dict[str, Any]] = []
    pairs = raw_str.split(";")
    for pair in pairs:
        pair = pair.strip()
        if not pair or "=" not in pair:
            continue
        k, v = pair.split("=", 1)
        name = k.strip()
        val = v.strip()
        is_http_only = name in ("xs", "datr", "fr", "sb")
        cookies.append({
            "name": name,
            "value": val,
            "domain": domain,
            "path": "/",
            "httpOnly": is_http_only,
            "secure": True,
            "sameSite": "Lax",
        })
    return cookies


def parse_netscape_cookies(content: str) -> List[Dict[str, Any]]:
    """Parse Netscape/curl format tab-delimited cookie file."""
    cookies: List[Dict[str, Any]] = []
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) >= 7:
            domain, flag, path, secure, expiration, name, value = parts[:7]
            cookies.append({
                "name": name,
                "value": value,
                "domain": domain,
                "path": path,
                "httpOnly": name in ("xs", "datr", "fr", "sb"),
                "secure": secure.upper() == "TRUE",
                "sameSite": "Lax",
            })
    return cookies


def validate_meta_cookies(cookies: List[Dict[str, Any]]) -> Tuple[bool, str]:
    """Ensure essential Meta session cookies (c_user, xs) are present and valid."""
    names = {c.get("name"): c.get("value") for c in cookies}
    if not names.get("c_user"):
        return False, "Missing critical 'c_user' cookie (User ID)."
    if not names.get("xs"):
        return False, "Missing critical 'xs' cookie (Session Token)."
    return True, f"Valid Meta session credentials found (c_user={names.get('c_user')})."


def ingest_meta_cookies(
    account_id: str,
    cookies_input: Union[str, List[Dict[str, Any]], Dict[str, Any]],
    storage_engine: Optional[StorageEngine] = None,
) -> Dict[str, Any]:
    """Ingest, validate, and cryptographically vault Meta cookies for a tenant."""
    storage = storage_engine or StorageEngine()
    account = storage.get_account(account_id)
    if not account:
        logger.warning(f"Account {account_id} not registered. Creating entry...")
        storage.create_account(
            account_id=account_id,
            platform="meta",
            page_name="Dazaa Marketing",
        )

    parsed_cookies: List[Dict[str, Any]] = []
    origins: List[Dict[str, Any]] = []

    if isinstance(cookies_input, str):
        content = cookies_input.strip()
        if content.startswith("[") or content.startswith("{"):
            try:
                data = json.loads(content)
                if isinstance(data, list):
                    parsed_cookies = data
                elif isinstance(data, dict):
                    parsed_cookies = data.get("cookies", [])
                    origins = data.get("origins", [])
            except json.JSONDecodeError:
                parsed_cookies = parse_raw_cookie_string(content)
        elif "\t" in content:
            parsed_cookies = parse_netscape_cookies(content)
        else:
            parsed_cookies = parse_raw_cookie_string(content)
    elif isinstance(cookies_input, list):
        parsed_cookies = cookies_input
    elif isinstance(cookies_input, dict):
        parsed_cookies = cookies_input.get("cookies", [])
        origins = cookies_input.get("origins", [])

    is_valid, msg = validate_meta_cookies(parsed_cookies)
    if not is_valid:
        raise ValueError(f"Cookie validation failure: {msg}")

    storage_state = {
        "cookies": parsed_cookies,
        "origins": origins or [
            {
                "origin": "https://www.facebook.com",
                "localStorage": [{"name": "meta_agent_active", "value": "1"}],
            },
            {
                "origin": "https://business.facebook.com",
                "localStorage": [{"name": "meta_agent_active", "value": "1"}],
            },
        ],
    }

    tenant = TenantContext(account_id, "meta")
    tenant.save_encrypted_state(storage_state)
    storage.update_account_status(account_id, "active")

    vault_path = get_tenant_storage_path(account_id)
    logger.info(f"Successfully vaulted {len(parsed_cookies)} cookies to {vault_path}")

    return {
        "status": "success",
        "account_id": account_id,
        "vault_path": vault_path,
        "cookie_count": len(parsed_cookies),
        "validation_message": msg,
    }


async def run_interactive_onboarding(
    account_id: str = "act_1071349136056783",
    asset_id: str = "1071349136056783",
    business_id: str = "766871999177306",
    timeout_sec: int = 180,
) -> Dict[str, Any]:
    """Launch clean Playwright window to Meta Business Suite and capture authentication session."""
    target_url = f"https://business.facebook.com/latest/?asset_id={asset_id}&business_id={business_id}"
    logger.info(f"Launching interactive onboarding for {account_id}...")
    logger.info(f"Navigating to: {target_url}")

    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.error("Playwright is not installed. Interactive mode requires Playwright.")
        return {"status": "error", "error": "Playwright not installed"}

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            locale="ar-SA",
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()
        await page.goto(target_url, timeout=45000)

        print("\n=======================================================")
        print("  🔑 META BUSINESS SUITE INTERACTIVE AUTHENTICATION")
        print("=======================================================")
        print("1. سجّل الدخول بحسابك في نافذة المتصفح المفتوحة.")
        print("2. افتح صفحة مدير الأعمال وصفحة Dazaa Marketing.")
        print(f"3. المهلة القصوى للمصادقة: {timeout_sec} ثانية...")
        print("=======================================================\n")

        start = time.time()
        authenticated = False
        captured_cookies: List[Dict[str, Any]] = []

        while time.time() - start < timeout_sec:
            cookies = await context.cookies()
            names = {c["name"]: c["value"] for c in cookies}
            if names.get("c_user") and names.get("xs"):
                authenticated = True
                captured_cookies = cookies
                logger.info("Authentication detected! Valid c_user and xs cookies present.")
                break
            await asyncio.sleep(2.0)

        if not authenticated:
            await browser.close()
            raise TimeoutError("Authentication timed out. 'c_user' and 'xs' cookies were not captured.")

        # Ingest captured cookies
        res = ingest_meta_cookies(account_id, captured_cookies)
        await page.wait_for_timeout(2000)
        await browser.close()
        return res


def main() -> None:
    parser = argparse.ArgumentParser(description="Meta Business Suite Session Onboarding & Cookie Vault")
    parser.add_argument("--account-id", default="act_1071349136056783", help="Target Account ID")
    parser.add_argument("--asset-id", default="1071349136056783", help="Meta Business Asset ID")
    parser.add_argument("--business-id", default="766871999177306", help="Meta Business Manager ID")
    parser.add_argument("--mode", choices=["direct", "interactive", "file"], default="direct", help="Ingestion mode")
    parser.add_argument("--cookies", help="Raw cookie string or JSON string")
    parser.add_argument("--file", help="Path to cookie file (JSON or Netscape txt)")
    parser.add_argument("--timeout", type=int, default=180, help="Interactive login timeout in seconds")

    args = parser.parse_args()

    if args.mode == "direct":
        if not args.cookies:
            # Check environment variable
            cookies_env = os.environ.get("META_SESSION_COOKIES")
            if cookies_env:
                args.cookies = cookies_env
            else:
                logger.info("No direct cookie string passed via --cookies or META_SESSION_COOKIES.")
                logger.info("Please provide cookies string via --cookies 'c_user=...; xs=...' or use --mode interactive")
                sys.exit(1)
        res = ingest_meta_cookies(args.account_id, args.cookies)
        print(json.dumps(res, indent=2, ensure_ascii=False))

    elif args.mode == "file":
        if not args.file or not os.path.exists(args.file):
            logger.error(f"File not found: {args.file}")
            sys.exit(1)
        with open(args.file, "r", encoding="utf-8") as f:
            content = f.read()
        res = ingest_meta_cookies(args.account_id, content)
        print(json.dumps(res, indent=2, ensure_ascii=False))

    elif args.mode == "interactive":
        asyncio.run(
            run_interactive_onboarding(
                account_id=args.account_id,
                asset_id=args.asset_id,
                business_id=args.business_id,
                timeout_sec=args.timeout,
            )
        )


if __name__ == "__main__":
    main()
