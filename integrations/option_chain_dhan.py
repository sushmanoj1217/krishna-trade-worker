# integrations/option_chain_dhan.py
import os
import requests
from datetime import date
from tenacity import retry, stop_after_attempt, wait_fixed
from utils.logger import log

# --- ENV ---
DHAN_BASE = os.getenv("DHAN_BASE", "https://api.dhan.co").rstrip("/")
DHAN_UNDERLYING_SCRIP = os.getenv("DHAN_UNDERLYING_SCRIP", "").strip()   # REQUIRED (int as string)
DHAN_UNDERLYING_SEG = os.getenv("DHAN_UNDERLYING_SEG", "IDX_I").strip()  # e.g., IDX_I (indices)
DHAN_EXPIRY_ENV = os.getenv("DHAN_EXPIRY", "").strip()                   # optional override YYYY-MM-DD

# --- HEADERS ---
def _headers():
    token = os.getenv("DHAN_ACCESS_TOKEN", "")
    cid = os.getenv("DHAN_CLIENT_ID", "")
    if not token or not cid:
        raise RuntimeError("DHAN credentials missing (DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN)")
    return {
        "access-token": token,
        "client-id": cid,
        "Content-Type": "application/json",
    }

# --- Helpers ---
def _post_json(url: str, json_payload: dict, timeout=10):
    r = requests.post(url, headers=_headers(), json=json_payload, timeout=timeout)
    if r.status_code >= 400:
        log.warning(f"Dhan {r.status_code}: {r.text[:300]} @ {url}")
    r.raise_for_status()
    return r.json()

@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
def get_expiry_list(underlying_scrip: int, underlying_seg: str) -> list[str]:
    url = f"{DHAN_BASE}/v2/optionchain/expirylist"
    payload = {"UnderlyingScrip": underlying_scrip, "UnderlyingSeg": underlying_seg}
    data = _post_json(url, payload)
    return (data.get("data") or []) if isinstance(data, dict) else []

def _pick_nearest_expiry(expiries: list[str]) -> str | None:
    """Pick earliest expiry >= today."""
    if not expiries:
        return None
    today = date.today().isoformat()
    future = sorted([d for d in expiries if d >= today])
    return future[0] if future else sorted(expiries)[0]  # fallback earliest

@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
def get_option_chain_v2(underlying_scrip: int, underlying_seg: str, expiry: str) -> dict:
    url = f"{DHAN_BASE}/v2/optionchain"
    payload = {
        "UnderlyingScrip": underlying_scrip,
        "UnderlyingSeg": underlying_seg,
        "Expiry": expiry
    }
    data = _post_json(url, payload)
    return data

def ensure_inputs():
    if not DHAN_UNDERLYING_SCRIP.isdigit():
        raise RuntimeError("DHAN_UNDERLYING_SCRIP missing/invalid. Set int Security ID from Dhan scrip master CSV.")
    return int(DHAN_UNDERLYING_SCRIP), DHAN_UNDERLYING_SEG, DHAN_EXPIRY_ENV or None

def compute_levels_from_oc_v2(oc_json: dict, used_expiry: str) -> dict:
    """
    oc_json shape (per Dhan v2 docs):
    { "data": { "last_price": float, "oc": { "25000.000000": { "ce": {...,"oi":int}, "pe": {...,"oi":int} }, ... } } }
    We compute:
      - S1,S2: top-2 PE OI strikes (supports)
      - R1,R2: top-2 CE OI strikes (resistances)
      - PCR: sum(PE OI)/sum(CE OI)
      - Max Pain (approx): strike with max (CE OI + PE OI)
    """
    data = oc_json.get("data") or {}
    oc = data.get("oc") or {}
    spot = float(data.get("last_price") or 0.0)

    rows = []
    pe_sum = 0
    ce_sum = 0
    for k, v in oc.items():
        try:
            strike = float(k)
        except Exception:
            continue
        ce_oi = int((v.get("ce") or {}).get("oi") or 0)
        pe_oi = int((v.get("pe") or {}).get("oi") or 0)
        rows.append((strike, ce_oi, pe_oi))
        ce_sum += ce_oi
        pe_sum += pe_oi

    if not rows:
        raise RuntimeError("Empty option chain data")

    # Sorts
    top_pe = sorted(rows, key=lambda t: t[2], reverse=True)  # by pe_oi
    top_ce = sorted(rows, key=lambda t: t[1], reverse=True)  # by ce_oi
    s1, s2 = (top_pe[0][0], top_pe[1][0]) if len(top_pe) >= 2 else (None, None)
    r1, r2 = (top_ce[0][0], top_ce[1][0]) if len(top_ce) >= 2 else (None, None)

    # PCR
    pcr = round(pe_sum / ce_sum, 4) if ce_sum > 0 else None

    # Max Pain (approx): max (CE OI + PE OI)
    max_pain = max(rows, key=lambda t: (t[1] + t[2]))[0] if rows else None

    return {
        "spot": spot,
        "s1": s1, "s2": s2, "r1": r1, "r2": r2,
        "pcr": pcr,
        "max_pain": max_pain,
        "expiry": used_expiry,
    }

def fetch_levels() -> dict:
    """Top-level helper used by analytics.oc_refresh"""
    u_scrip, u_seg, expiry_override = ensure_inputs()
    expiry = expiry_override
    if not expiry:
        expiries = get_expiry_list(u_scrip, u_seg)
        expiry = _pick_nearest_expiry(expiries)
        if not expiry:
            raise RuntimeError("No expiry available from Dhan")
    oc = get_option_chain_v2(u_scrip, u_seg, expiry)
    return compute_levels_from_oc_v2(oc, expiry)
