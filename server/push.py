"""Self-Hosted Web Push Gateway (RFC 8291 / RFC 8292 VAPID).

Dispatches instant, encrypted push notifications to mobile lock screens
(Android, iOS, Desktop) upon checkpoints, security challenges, or high-value leads.
"""

import base64
import json
import logging
import os
from typing import Any, Dict, List, Optional
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization
from pywebpush import webpush, WebPushException

from core.storage import StorageEngine

logger = logging.getLogger("server.push")

DEFAULT_STORAGE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "storage_states")
VAPID_FILE = os.path.join(DEFAULT_STORAGE_DIR, "vapid_keys.json")


def _b64url_encode(data: bytes) -> str:
    """Encode bytes into base64url string without padding."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


class WebPushGateway:
    """Manages VAPID keys and dispatches RFC 8291 encrypted Web Push payloads."""

    def __init__(self, storage: StorageEngine) -> None:
        self.storage = storage
        self.vapid_data = self._get_or_create_vapid_keys()

    def _get_or_create_vapid_keys(self) -> Dict[str, str]:
        """Ensure ECDSA P-256 VAPID keypair exists on disk in standard format."""
        os.makedirs(DEFAULT_STORAGE_DIR, exist_ok=True)
        if os.path.exists(VAPID_FILE):
            try:
                with open(VAPID_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Corrupt VAPID file, regenerating: {e}")

        # Generate standard NIST P-256 (prime256v1) keypair
        private_key = ec.generate_private_key(ec.SECP256R1())
        public_key = private_key.public_key()

        # Raw uncompressed public key (65 bytes starting with 0x04)
        raw_pub = public_key.public_bytes(
            encoding=serialization.Encoding.X962,
            format=serialization.PublicFormat.UncompressedPoint,
        )
        pub_b64url = _b64url_encode(raw_pub)

        # Private key in PEM format for pywebpush
        pem_priv = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ).decode("ascii")

        vapid_dict = {
            "public_key": pub_b64url,
            "private_key_pem": pem_priv,
            "subject": "mailto:operator@autonomous-agent.internal",
        }

        with open(VAPID_FILE, "w", encoding="utf-8") as f:
            json.dump(vapid_dict, f, indent=2)

        logger.info("New VAPID keypair successfully generated and stored.")
        return vapid_dict

    def get_public_key(self) -> str:
        """Return base64url-encoded applicationServerKey for browser registration."""
        return self.vapid_data["public_key"]

    def register_device(
        self, subscription_info: Dict[str, Any], device_name: str = "Mobile Client"
    ) -> str:
        """Persist a browser PushSubscription object."""
        endpoint = subscription_info.get("endpoint", "")
        keys = subscription_info.get("keys", {})
        p256dh = keys.get("p256dh", "")
        auth = keys.get("auth", "")

        if not endpoint or not p256dh or not auth:
            raise ValueError("Invalid PushSubscription payload: missing endpoint, p256dh, or auth.")

        return self.storage.save_push_subscription(
            endpoint=endpoint,
            p256dh=p256dh,
            auth=auth,
            device_name=device_name,
        )

    def send_notification(
        self,
        subscription: Dict[str, Any],
        title: str,
        body: str,
        data: Optional[Dict[str, Any]] = None,
        actions: Optional[List[Dict[str, str]]] = None,
    ) -> bool:
        """Dispatch an RFC 8291 encrypted Web Push notification with optional lock-screen actions."""
        sub_info = {
            "endpoint": subscription["endpoint"],
            "keys": {
                "p256dh": subscription["p256dh"],
                "auth": subscription["auth"],
            },
        }

        payload_dict = {
            "title": title,
            "body": body,
            "icon": "/icons/icon-192.png",
            "badge": "/icons/icon-192.png",
            "vibrate": [120, 60, 120, 60, 240],
            "data": data or {"url": "/"},
        }
        if actions:
            payload_dict["actions"] = actions

        payload = json.dumps(payload_dict)

        try:
            webpush(
                subscription_info=sub_info,
                data=payload,
                vapid_private_key=self.vapid_data["private_key_pem"],
                vapid_claims={"sub": self.vapid_data["subject"]},
                timeout=8,
            )
            return True
        except WebPushException as e:
            if e.response is not None and e.response.status_code in (404, 410):
                logger.info(f"Purging expired push subscription: {subscription['endpoint'][:30]}...")
                self.storage.delete_push_subscription(subscription["endpoint"])
            else:
                logger.warning(f"WebPush delivery error: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected push delivery exception: {e}")
            return False

    def broadcast_alert(
        self,
        title: str,
        body: str,
        data: Optional[Dict[str, Any]] = None,
        actions: Optional[List[Dict[str, str]]] = None,
    ) -> int:
        """Broadcast alert with optional interactive action buttons to all registered devices."""
        subs = self.storage.list_push_subscriptions()
        success_count = 0
        for sub in subs:
            if self.send_notification(sub, title, body, data, actions):
                success_count += 1
        return success_count

    def resolve_lock_screen_action(self, action: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process mobile lock-screen quick actions (e.g. set_price:post_101:350)."""
        logger.info(f"Resolving lock-screen action: {action} with data: {data}")
        if action.startswith("set_price:"):
            parts = action.split(":")
            if len(parts) >= 3:
                post_id = parts[1]
                price = parts[2]
            else:
                post_id = data.get("post_id", "")
                price = parts[1]

            account_id = data.get("account_id") or "meta_store_official"
            success = self.storage.update_post_price(account_id, post_id, f"{price} جنيه")
            return {
                "status": "RESOLVED" if success else "FAILED",
                "post_id": post_id,
                "price": price,
                "action": "PRICE_UPDATED",
            }

        return {"status": "ACK", "action": action}
