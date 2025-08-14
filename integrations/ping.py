# path: integrations/ping.py
import os, requests

def ping() -> bool:
    url = (os.getenv("PING_URL","") or "").strip()
    if not url: return False
    try:
        requests.get(url, timeout=5)
        return True
    except Exception:
        return False
