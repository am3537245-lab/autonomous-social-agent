"""Zero-Trust Remote Ingress & Mobile QR Pairing Gateway.

Provides automated Cloudflare Tunnels (or LAN interface binding) with scannable
terminal QR codes to instantly pair mobile devices (Android, iPhone) with the server daemon.
"""

import io
import logging
import os
import re
import socket
import subprocess
import sys
import threading
import time
from typing import Optional
import qrcode

logger = logging.getLogger("core.ingress")


def get_local_lan_ip() -> str:
    """Retrieve the host machine's primary local LAN IP address."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Does not actually establish a connection; determines route
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip


def generate_terminal_qr(url: str) -> str:
    """Generate a clean ASCII QR code scannable by mobile cameras."""
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_L,
        box_size=1,
        border=1,
    )
    qr.add_data(url)
    qr.make(fit=True)
    out_buf = io.StringIO()
    qr.print_ascii(out=out_buf, invert=True)
    return out_buf.getvalue()


class IngressManager:
    """Manages secure ingress tunnels and mobile terminal QR pairing."""

    def __init__(self, port: int = 8000, remote: bool = False) -> None:
        self.port = port
        self.remote = remote
        self.tunnel_process: Optional[subprocess.Popen] = None
        self.public_url: Optional[str] = None
        self.local_url = f"http://{get_local_lan_ip()}:{port}"

    def start_tunnel(self) -> str:
        """Start Cloudflare Tunnel if requested and available, else use LAN ingress."""
        if not self.remote:
            self.public_url = self.local_url
            return self.public_url

        # Check for cloudflared executable
        cloudflared_bin = "cloudflared"
        local_bin = os.path.join(os.path.dirname(os.path.dirname(__file__)), "cloudflared.exe")
        if os.path.exists(local_bin):
            cloudflared_bin = local_bin

        try:
            logger.info("Spawning ephemeral Cloudflare Zero-Trust Tunnel...")
            self.tunnel_process = subprocess.Popen(
                [cloudflared_bin, "tunnel", "--url", f"http://localhost:{self.port}"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
            )

            # Read stderr/stdout for trycloudflare.com URL
            start_wait = time.time()
            for line in self.tunnel_process.stdout:
                match = re.search(r"https://[a-zA-Z0-9-]+\.trycloudflare\.com", line)
                if match:
                    self.public_url = match.group(0)
                    logger.info(f"Cloudflare Tunnel active: {self.public_url}")
                    break
                if time.time() - start_wait > 15:
                    break
        except Exception as e:
            logger.warning(f"Could not launch cloudflared ({e}). Falling back to LAN IP.")

        if not self.public_url:
            self.public_url = self.local_url

        return self.public_url

    def print_mobile_pairing_banner(self) -> None:
        """Render a terminal banner with pairing instructions and scannable QR code."""
        try:
            url = self.public_url or self.local_url
            qr_ascii = generate_terminal_qr(url)

            print("\n" + "=" * 68)
            print("[MOBILE CONTROL PLANE - PAIRING GATEWAY]")
            print("=" * 68)
            print(f"  * Target Gateway URL: {url}")
            print("  * Instructions for Android: Scan QR -> Chrome -> 'Install app' / 'Add to Home'")
            print("  * Instructions for iPhone:  Scan QR -> Safari -> Share -> 'Add to Home Screen'")
            print("=" * 68)
            try:
                print(qr_ascii)
            except Exception:
                pass
            print("=" * 68 + "\n")
        except Exception as e:
            logger.info(f"Mobile pairing banner bypassed: {e}")

    def stop(self) -> None:
        """Terminate active tunnel process if running."""
        if self.tunnel_process and self.tunnel_process.poll() is None:
            self.tunnel_process.terminate()
            logger.info("Ingress tunnel process terminated.")
