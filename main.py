"""Main Entrypoint for the Autonomous Social Media Multimodal Agent Daemon."""

import argparse
import logging
import os
import sys
import uvicorn

# Configure high-performance logging format
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")


def main() -> None:
    default_port = int(os.environ.get("PORT", "8000"))
    parser = argparse.ArgumentParser(
        description="Autonomous Multimodal Social Media Agent for Meta & TikTok Studio"
    )
    parser.add_argument("--host", default="0.0.0.0", help="Host interface to bind (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=default_port, help=f"Port to serve dashboard and API (default: {default_port})")
    parser.add_argument("--headless", action="store_true", default=True, help="Run browser in headless mode")
    parser.add_argument("--remote", action="store_true", default=False, help="Enable Cloudflare Tunnel for secure remote mobile access")
    args = parser.parse_args()

    from core.ingress import IngressManager
    ingress = IngressManager(port=args.port, remote=args.remote)
    active_url = ingress.start_tunnel()

    logger.info("=" * 65)
    logger.info("STARTING AUTONOMOUS MULTIMODAL SOCIAL MEDIA AGENT CORE")
    logger.info(f"Gateway URL: {active_url}")
    logger.info("Hardware Memory Ceiling: < 1.0 GB RAM")
    logger.info("Stealth Engine: Anti-Detect CDP Mitigation Active")
    logger.info("Multimodal Pipeline: 'Analyze-Once, Cache-Forever' with Gemini 3.5 Flash")
    logger.info("Mobile Control Plane: Production PWA + WebPush (RFC 8291)")
    logger.info("=" * 65)

    # Print scannable mobile pairing banner and QR Code
    ingress.print_mobile_pairing_banner()

    uvicorn.run(
        "server.api:app",
        host=args.host,
        port=args.port,
        reload=False,
        log_level="info",
        access_log=False,
    )


if __name__ == "__main__":
    main()
