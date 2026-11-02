"""Automated Render Cloud Provisioner & GitHub Repository Linker."""

import json
import logging
import os
import subprocess
import sys
import time
import urllib.request
import urllib.error

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("render_provisioner")

# Load environment configuration from .env safely
def load_env_dict():
    env_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    res = {}
    if os.path.exists(env_file):
        with open(env_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    res[k.strip()] = v.strip().strip('"').strip("'")
    return res

ENV_VARS = load_env_dict()
RENDER_API_KEY = ENV_VARS.get("RENDER_API_KEY") or os.environ.get("RENDER_API_KEY", "")
RENDER_OWNER_ID = ENV_VARS.get("RENDER_OWNER_ID") or os.environ.get("RENDER_OWNER_ID", "")
GITHUB_PAT = ENV_VARS.get("GITHUB_PAT") or os.environ.get("GITHUB_PAT", "")
GITHUB_USER = ENV_VARS.get("GITHUB_USER", "am3537245-lab")
REPO_NAME = "autonomous-social-agent"
GEMINI_KEY = ENV_VARS.get("GEMINI_API_KEYS", "")


def github_api_request(endpoint: str, data: dict = None, method: str = "GET") -> dict:
    url = f"https://api.github.com/{endpoint.lstrip('/')}"
    headers = {
        "Authorization": f"token {GITHUB_PAT}",
        "User-Agent": "Antigravity-Cloud-Provisioner",
        "Accept": "application/vnd.github.v3+json",
        "Content-Type": "application/json",
    }
    body = json.dumps(data).encode("utf-8") if data else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            content = resp.read().decode("utf-8")
            return json.loads(content) if content else {}
    except urllib.error.HTTPError as e:
        err_msg = e.read().decode("utf-8")
        logger.warning(f"GitHub API {method} {endpoint} returned {e.code}: {err_msg}")
        raise


def render_api_request(endpoint: str, data: dict = None, method: str = "GET") -> dict:
    url = f"https://api.render.com/v1/{endpoint.lstrip('/')}"
    headers = {
        "Authorization": f"Bearer {RENDER_API_KEY}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    body = json.dumps(data).encode("utf-8") if data else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            content = resp.read().decode("utf-8")
            return json.loads(content) if content else {}
    except urllib.error.HTTPError as e:
        err_msg = e.read().decode("utf-8")
        logger.error(f"Render API {method} {endpoint} returned {e.code}: {err_msg}")
        raise


def ensure_github_repo() -> str:
    """Ensure the GitHub repository exists, or create it if missing."""
    logger.info(f"Checking GitHub repo {GITHUB_USER}/{REPO_NAME}...")
    try:
        repo = github_api_request(f"repos/{GITHUB_USER}/{REPO_NAME}")
        logger.info(f"Repository verified: {repo.get('html_url')}")
        return repo.get("clone_url")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            logger.info(f"Creating new GitHub repo {REPO_NAME}...")
            new_repo = github_api_request(
                "user/repos",
                data={
                    "name": REPO_NAME,
                    "description": "Autonomous Multimodal Social Media Agent Control Plane",
                    "private": False,
                    "auto_init": False,
                },
                method="POST",
            )
            logger.info(f"Created repository: {new_repo.get('html_url')}")
            return new_repo.get("clone_url")
        raise


def sync_and_push_repo(clone_url: str) -> None:
    """Initialize clean git repo and push to GitHub."""
    repo_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(repo_dir)

    # Initialize fresh git repository
    logger.info("Initializing fresh git repository...")
    subprocess.run(["git", "init"], check=True)
    subprocess.run(["git", "config", "user.name", "am3537245-lab"], check=True)
    subprocess.run(["git", "config", "user.email", "oper49473@gmail.com"], check=True)

    # Remote with auth
    authed_remote = f"https://{GITHUB_USER}:{GITHUB_PAT}@github.com/{GITHUB_USER}/{REPO_NAME}.git"
    subprocess.run(["git", "remote", "add", "origin", authed_remote], check=True)

    # Add all files respected by .gitignore
    subprocess.run(["git", "branch", "-M", "main"], check=True)
    subprocess.run(["git", "add", "."], check=True)
    
    # Commit
    logger.info("Creating clean initial commit without secrets...")
    subprocess.run(["git", "commit", "-m", "Production Render Docker release with PWA, LLM and Ingress Tap"], check=True)

    logger.info("Pushing clean codebase to GitHub main branch...")
    subprocess.run(["git", "push", "-u", "origin", "main", "--force"], check=True)
    logger.info("GitHub clean push successful!")


def provision_render_service() -> dict:
    """Create or retrieve the Render Web Service and attach environment variables."""
    logger.info("Checking existing Render services...")
    services = render_api_request(f"services?ownerId={RENDER_OWNER_ID}")
    
    for item in services:
        srv = item.get("service", {})
        if srv.get("name") == "autonomous-social-agent":
            logger.info(f"Render service already exists: {srv.get('id')} ({srv.get('serviceDetails', {}).get('url')})")
            return srv

    logger.info("Creating new Docker-based Web Service on Render...")
    payload = {
        "type": "web_service",
        "name": "autonomous-social-agent",
        "ownerId": RENDER_OWNER_ID,
        "repo": f"https://github.com/{GITHUB_USER}/{REPO_NAME}",
        "branch": "main",
        "autoDeploy": "yes",
        "serviceDetails": {
            "env": "docker",
            "plan": "free",
            "region": "frankfurt",
            "dockerfilePath": "Dockerfile",
            "healthCheckPath": "/api/health",
            "envVars": [
                {"key": "PORT", "value": "10000"},
                {"key": "IS_CLOUD_DEPLOYMENT", "value": "true"},
                {"key": "GEMINI_API_KEYS", "value": GEMINI_KEY},
                {"key": "GOOGLE_PROJECT_ID", "value": "projects/131902077995"},
                {"key": "DEFAULT_GEMINI_MODEL", "value": "gemini-3.5-flash-lite"},
            ],
        },
    }

    res = render_api_request("services", data=payload, method="POST")
    logger.info(f"Render Web Service successfully provisioned! Service ID: {res.get('service', {}).get('id')}")
    return res.get("service", {})


def main():
    logger.info("=" * 65)
    logger.info("STARTING AUTOMATED RENDER CLOUD PROVISIONING PIPELINE")
    logger.info("=" * 65)
    
    clone_url = ensure_github_repo()
    sync_and_push_repo(clone_url)
    service = provision_render_service()
    
    details = service.get("serviceDetails", {})
    url = details.get("url") or f"https://{service.get('slug')}.onrender.com"
    dashboard_url = service.get("dashboardUrl") or f"https://dashboard.render.com/web/{service.get('id')}"

    print("\n" + "=" * 65)
    print("[DEPLOYMENT INITIATED ON RENDER CLOUD 24/7]")
    print(f"* Service Name:    {service.get('name')}")
    print(f"* Service ID:      {service.get('id')}")
    print(f"* Public App URL:  {url}")
    print(f"* Render Dashboard:{dashboard_url}")
    print("=" * 65 + "\n")


if __name__ == "__main__":
    main()
