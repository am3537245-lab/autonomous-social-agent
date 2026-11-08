"""Cloud State & Vault Persistence Synchronizer (Render 24/7 Zero-Loss Engine).

Ensures that when a mobile user updates cookies or product prices on a cloud container
with an ephemeral filesystem, the encrypted vault and saved replies can be serialized into
an encrypted bundle (`CLOUD_VAULT_BUNDLE`) and automatically rehydrated on cold-boot.
Also supports syncing to Render API when `RENDER_API_KEY` is configured.
"""

import base64
import json
import logging
import os
import zlib
from typing import Any, Dict, List, Optional
import requests
from cryptography.fernet import Fernet

from core.engine import _get_or_create_cipher, KEY_PATH, TenantContext
from core.storage import StorageEngine

logger = logging.getLogger("core.cloud_sync")


class CloudStateSynchronizer:
    """Packs, encrypts, and rehydrates account vaults and saved replies across cold-boots."""

    def __init__(self, storage: Optional[StorageEngine] = None) -> None:
        self.storage = storage or StorageEngine()

    def export_encrypted_bundle(self, account_id: str = "act_1071349136056783") -> str:
        """Create a compressed, Fernet-encrypted bundle containing the account's cookies,
        saved replies, and persona for zero-loss cloud persistence.
        """
        acc = self.storage.get_account(account_id) or {}
        tenant = TenantContext(account_id, acc.get("platform", "meta"), {})
        state = tenant.load_decrypted_state() or {"cookies": [], "origins": []}
        saved_replies = self.storage.get_saved_replies(account_id)
        persona = self.storage.get_persona(account_id) or {}

        payload = {
            "account_id": account_id,
            "page_id": acc.get("page_id", "1071349136056783"),
            "business_id": acc.get("business_id", "766871999177306"),
            "profile_url": acc.get("profile_url", "https://www.facebook.com/profile.php?id=61579682971434"),
            "page_name": acc.get("page_name", "Dazaa Marketing"),
            "state": state,
            "saved_replies": saved_replies,
            "persona": persona,
        }
        raw_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        compressed = zlib.compress(raw_bytes, level=9)
        fernet = _get_or_create_cipher()
        encrypted = fernet.encrypt(compressed)
        return base64.urlsafe_b64encode(encrypted).decode("ascii")

    def rehydrate_from_bundle(self, bundle_b64: Optional[str] = None) -> bool:
        """Rehydrate account session vault, persona, and saved replies from CLOUD_VAULT_BUNDLE."""
        raw_bundle = bundle_b64 or os.environ.get("CLOUD_VAULT_BUNDLE", "").strip()
        if not raw_bundle:
            return False

        try:
            encrypted = base64.urlsafe_b64decode(raw_bundle.encode("ascii"))
            fernet = _get_or_create_cipher()
            compressed = fernet.decrypt(encrypted)
            payload = json.loads(zlib.decompress(compressed).decode("utf-8"))

            account_id = payload.get("account_id", "act_1071349136056783")
            self.storage.create_account(
                account_id=account_id,
                platform="meta",
                page_name=payload.get("page_name", "Dazaa Marketing"),
                page_id=payload.get("page_id", "1071349136056783"),
                business_id=payload.get("business_id", "766871999177306"),
                profile_url=payload.get("profile_url", "https://www.facebook.com/profile.php?id=61579682971434"),
            )

            state = payload.get("state")
            if state and state.get("cookies"):
                tenant = TenantContext(account_id, "meta", {})
                tenant.save_encrypted_state(state)
                self.storage.update_account_status(account_id, "active")

            persona = payload.get("persona")
            if persona and isinstance(persona, dict):
                self.storage.update_persona(account_id, persona)

            existing_replies = self.storage.get_saved_replies(account_id)
            if not existing_replies and payload.get("saved_replies"):
                for sr in payload["saved_replies"]:
                    kws = sr.get("trigger_keywords", [])
                    if isinstance(kws, str):
                        try:
                            kws = json.loads(kws)
                        except Exception:
                            kws = [kws]
                    self.storage.add_saved_reply(
                        account_id=account_id,
                        category=sr.get("category", "عام"),
                        trigger_keywords=kws,
                        reply_text=sr.get("reply_text", ""),
                        attachment_reference=sr.get("attachment_reference", ""),
                    )

            logger.info(f"Successfully rehydrated cloud bundle for {account_id}")
            return True
        except Exception as e:
            logger.warning(f"Failed to rehydrate CLOUD_VAULT_BUNDLE: {e}")
            return False

    def push_to_render_env(self, account_id: str = "act_1071349136056783") -> Dict[str, Any]:
        """Push APP_MASTER_KEY and CLOUD_VAULT_BUNDLE to Render service environment variables if RENDER_API_KEY is available."""
        api_key = os.environ.get("RENDER_API_KEY", "").strip()
        if not api_key:
            return {"synced": False, "reason": "No RENDER_API_KEY configured"}

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        try:
            # Find service ID for autonomous-social-agent
            r_srv = requests.get("https://api.render.com/v1/services?limit=20", headers=headers, timeout=12.0)
            if r_srv.status_code != 200:
                return {"synced": False, "reason": f"Render services lookup returned {r_srv.status_code}"}

            services = r_srv.json()
            service_id = ""
            for item in services:
                srv = item.get("service", item)
                if "autonomous-social-agent" in srv.get("name", "") or "social" in srv.get("name", "").lower():
                    service_id = srv.get("id", "")
                    break

            if not service_id:
                return {"synced": False, "reason": "Service autonomous-social-agent not found in Render account"}

            _get_or_create_cipher()
            master_key_str = os.environ.get("APP_MASTER_KEY")
            if not master_key_str and os.path.exists(KEY_PATH):
                with open(KEY_PATH, "rb") as kf:
                    master_key_str = kf.read().strip().decode("utf-8")
            bundle_str = self.export_encrypted_bundle(account_id)

            # Upsert individual env vars via Render API
            for env_key, env_val in [("APP_MASTER_KEY", master_key_str), ("CLOUD_VAULT_BUNDLE", bundle_str)]:
                requests.put(
                    f"https://api.render.com/v1/services/{service_id}/env-vars/{env_key}",
                    headers=headers,
                    json={"value": env_val},
                    timeout=12.0,
                )

            return {"synced": True, "service_id": service_id, "bundle_size": len(bundle_str)}
        except Exception as e:
            logger.warning(f"Render API env sync note: {e}")
            return {"synced": False, "reason": str(e)}
