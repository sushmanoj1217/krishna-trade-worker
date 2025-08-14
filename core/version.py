# path: core/version.py
import os, requests

def git_sha() -> str:
    return os.getenv("RENDER_GIT_COMMIT","") or os.getenv("GIT_REV","") or ""

def maybe_trigger_deploy():
    url = os.getenv("RENDER_DEPLOY_HOOK_URL","").strip()
    if not url: return False
    try:
        r = requests.post(url, timeout=10)
        return r.status_code in (200, 201, 202)
    except Exception:
        return False
