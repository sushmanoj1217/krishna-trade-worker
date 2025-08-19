import os, requests, re
from datetime import date
from tenacity import retry, stop_after_attempt, wait_fixed
from utils.logger import log

# --- ENV ---
DHAN_BASE = os.getenv("DHAN_BASE", "https://api.dhan.co").rstrip("/")
DHAN_UNDERLYING_SCRIP = os.getenv("DHAN_UNDERLYING_SCRIP", "").strip()   # optional; if empty we auto-resolve
DHAN_UNDERLYING_SEG = os.getenv("DHAN_UNDERLYING_SEG", "IDX_I").strip()  # indices: IDX_I (per Annexure)
DHAN_EXPIRY_ENV = os.getenv("DHAN_EXPIRY", "").strip()                   # optional YYYY-MM-DD
OC_SYMBOL = os.getenv("OC_SYMBOL", "NIFTY").strip().upper()
MATCH_HINT = os.getenv("DHAN_MATCH_STRING", "").strip()                  # optional, e.g. "NIFTY 50"

# ---------- HTTP helpers ----------
def _headers():
    token = os.getenv("DHAN_ACCESS_TOKEN", "")
    cid = os.getenv("DHAN_CLIENT_ID", "")
    if not token or not cid:
        raise RuntimeError("DHAN credentials missing (DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN)")
    return {"access-token": token, "client-id": cid, "Content-Type": "application/json"}

def _post_json(url: str, payload: dict, timeout=12):
    r = requests.post(url, headers=_headers(), json=payload, timeout=timeout)
    if r.status_code >= 400:
        log.warning(f"Dhan {r.status_code}: {r.text[:300]} @ {url}")
    r.raise_for_status()
    return r.json()

def _get_json(url: str, timeout=12):
    r = requests.get(url, headers=_headers(), timeout=timeout)
    if r.status_code >= 400:
        log.warning(f"Dhan {r.status_code}: {r.text[:300]} @ {url}")
    r.raise_for_status()
    return r.json()

# ---------- v2 APIs ----------
@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
def get_expiry_list(underlying_scrip: int, underlying_seg: str) -> list[str]:
    url = f"{DHAN_BASE}/v2/optionchain/expirylist"
    data = _post_json(url, {"UnderlyingScrip": underlying_scrip, "UnderlyingSeg": underlying_seg})
    return (data.get("data") or []) if isinstance(data, dict) else []

def _pick_nearest_expiry(expiries: list[str]) -> str | None:
    if not expiries:
        return None
    today = date.today().isoformat()
    future = sorted([d for d in expiries if d >= today])
    return future[0] if future else sorted(expiries)[0]

@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
def get_option_chain_v2(underlying_scrip: int, underlying_seg: str, expiry: str) -> dict:
    url = f"{DHAN_BASE}/v2/optionchain"
    return _post_json(url, {"UnderlyingScrip": underlying_scrip, "UnderlyingSeg": underlying_seg, "Expiry": expiry})

# ---------- Security ID resolver via v2 instrument API ----------
def _norm(x: str) -> str:
    x = (x or "").upper()
    x = re.sub(r"[^A-Z0-9 ]+", " ", x)
    x = re.sub(r"\s+", " ", x).strip()
    return x

def _targets_for_symbol(sym: str) -> list[str]:
    if MATCH_HINT:
        return [_norm(MATCH_HINT)]
    s = (sym or "").upper()
    if s == "NIFTY":
        return [_norm("NIFTY 50"), "NIFTY50", "NIFTY-50"]
    if s == "BANKNIFTY":
        return [_norm("NIFTY BANK"), "BANK NIFTY", "BANKNIFTY", "NIFTYBANK"]
    if s == "FINNIFTY":
        return [_norm("NIFTY FINANCIAL SERVICES"), "FINNIFTY", _norm("NIFTY FIN SERVICES")]
    return [s]

@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
def _fetch_instruments(segment: str) -> list[dict]:
    """
    v2 instruments endpoint (segment-wise).
    Docs: /v2/instruments (Segmentwise List), GET /v2/instrument/{exchangeSegment}
    """
    url = f"{DHAN_BASE}/v2/instrument/{segment}"
    data = _get_json(url)
    # Expect data to be a list[object]; keep as-is
    if isinstance(data, dict) and "data" in data:
        return data["data"] or []
    if isinstance(data, list):
        return data
    return []

def _extract_security_id(row: dict) -> int | None:
    # Try common keys
    for key in ["SecurityID", "SecurityId", "security_id", "SECURITY_ID", "SEM_SMST_SECURITY_ID"]:
        val = row.get(key)
        if isinstance(val, int):
            return val
        if isinstance(val, str) and val.isdigit():
            return int(val)
    # sometimes nested under 'instrument' or similar
    for k, v in row.items():
        if isinstance(v, dict):
            sid = _extract_security_id(v)
            if sid:
                return sid
    return None

def _name_blob(row: dict) -> str:
    # Concatenate a bunch of likely name fields
    keys = [
        "DisplayName","DISPLAY_NAME","display_name",
        "SymbolName","SYMBOL_NAME","symbol_name",
        "TradingSymbol","TRADING_SYMBOL","trading_symbol",
        "ScripName","SCRIP_NAME","scrip_name",
        "Name","NAME","name","Instrument","INSTRUMENT","instrument"
    ]
    parts = []
    for k in keys:
        v = row.get(k)
        if isinstance(v, str) and v:
            parts.append(v)
    return _norm(" ".join(parts))

def _looks_like_index(row: dict) -> bool:
    # prefer index instruments
    for k in ["InstrumentType","INSTRUMENT_TYPE","instrument_type","Instrument","INSTRUMENT","instrument"]:
        v = row.get(k)
        if isinstance(v, str) and "INDEX" in v.upper():
            return True
    return False  # not strict, just a hint

def _resolve_security_id_via_api(sym: str, segment: str) -> int | None:
    targets = _targets_for_symbol(sym)
    rows = _fetch_instruments(segment)
    if not rows:
        log.warning("Instrument API returned empty list")
        return None

    # Hard match first
    for row in rows:
        blob = _name_blob(row)
        if any(t == blob for t in targets):
            sid = _extract_security_id(row)
            if sid:
                log.info(f"Resolved SecurityID {sid} via exact name match: {blob}")
                return sid

    # Contains match with index preference
    best_sid = None
    for row in rows:
        blob = _name_blob(row)
        if any(t in blob for t in targets):
            sid = _extract_security_id(row)
            if sid:
                if _looks_like_index(row):
                    log.info(f"Resolved SecurityID {sid} via index match: {blob}")
                    return sid
                best_sid = best_sid or sid
    if best_sid:
        log.info(f"Resolved SecurityID {best_sid} via fuzzy name match")
        return best_sid
    return None

def ensure_inputs() -> tuple[int, str, str | None]:
    # If provided explicitly, use it
    if DHAN_UNDERLYING_SCRIP.isdigit():
        sid = int(DHAN_UNDERLYING_SCRIP)
        log.info(f"Using SecurityID from env: {sid}")
        return sid, DHAN_UNDERLYING_SEG, (DHAN_EXPIRY_ENV or None)

    # Resolve via v2 instrument API (no CSV dependency)
    sid = _resolve_security_id_via_api(OC_SYMBOL, DHAN_UNDERLYING_SEG)
    if sid:
        return sid, DHAN_UNDERLYING_SEG, (DHAN_EXPIRY_ENV or None)

    # Last-resort known defaults (may vary by feed; override if wrong)
    fallback = {"NIFTY": 13, "BANKNIFTY": 25, "FINNIFTY": 27}.get(OC_SYMBOL)
    if fallback:
        log.warning(f"Auto-resolve failed; falling back to hardcoded ID {fallback} for {OC_SYMBOL}")
        return fallback, DHAN_UNDERLYING_SEG, (DHAN_EXPIRY_ENV or None)

    raise RuntimeError("DHAN_UNDERLYING_SCRIP missing/invalid and auto-resolve failed. "
                       "Set Security ID via env or provide DHAN_MATCH_STRING.")

# ---------- OC compute ----------
def compute_levels_from_oc_v2(oc_json: dict, used_expiry: str) -> dict:
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

    top_pe = sorted(rows, key=lambda t: t[2], reverse=True)
    top_ce = sorted(rows, key=lambda t: t[1], reverse=True)
    s1, s2 = (top_pe[0][0], top_pe[1][0]) if len(top_pe) >= 2 else (None, None)
    r1, r2 = (top_ce[0][0], top_ce[1][0]) if len(top_ce) >= 2 else (None, None)
    pcr = round(pe_sum / ce_sum, 4) if ce_sum > 0 else None
    max_pain = max(rows, key=lambda t: (t[1] + t[2]))[0] if rows else None

    return {
        "spot": spot,
        "s1": s1, "s2": s2, "r1": r1, "r2": r2,
        "pcr": pcr,
        "max_pain": max_pain,
        "expiry": used_expiry,
    }

def fetch_levels() -> dict:
    u_scrip, u_seg, expiry_override = ensure_inputs()
    expiry = expiry_override
    if not expiry:
        expiries = get_expiry_list(u_scrip, u_seg)
        expiry = _pick_nearest_expiry(expiries)
        if not expiry:
            raise RuntimeError("No expiry available from Dhan")
    oc = get_option_chain_v2(u_scrip, u_seg, expiry)
    return compute_levels_from_oc_v2(oc, expiry)
