import os, requests
from tenacity import retry, stop_after_attempt, wait_fixed
from utils.logger import log

BASE = os.getenv("DHAN_BASE", "https://api.dhan.co/data")

def _headers():
    token = os.getenv("DHAN_ACCESS_TOKEN", "")
    cid = os.getenv("DHAN_CLIENT_ID", "")
    if not token or not cid:
        raise RuntimeError("DHAN credentials missing")
    return {
        "Authorization": f"Bearer {token}",
        "clientId": cid,
        "Content-Type": "application/json",
    }

@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
def get_option_chain(symbol: str) -> dict:
    url = f"{BASE}/v1/optionchain/{symbol}"
    r = requests.get(url, headers=_headers(), timeout=10)
    if r.status_code >= 400:
        log.warning(f"Dhan OC {r.status_code}: {r.text[:200]}")
    r.raise_for_status()
    return r.json()

def compute_levels_from_oc(oc_json: dict) -> dict:
    strikes = oc_json.get("strikes") or []
    pe = sorted([(x.get("strike"), x.get("pe_oi", 0)) for x in strikes], key=lambda t: t[1], reverse=True)
    ce = sorted([(x.get("strike"), x.get("ce_oi", 0)) for x in strikes], key=lambda t: t[1], reverse=True)
    s1, s2 = (pe[0][0], pe[1][0]) if len(pe) >= 2 else (None, None)
    r1, r2 = (ce[0][0], ce[1][0]) if len(ce) >= 2 else (None, None)
    pcr = oc_json.get("pcr")
    max_pain = oc_json.get("max_pain")
    spot = oc_json.get("spot")
    expiry = oc_json.get("expiry")
    return {"spot": spot, "s1": s1, "s2": s2, "r1": r1, "r2": r2, "pcr": pcr, "max_pain": max_pain, "expiry": expiry}
