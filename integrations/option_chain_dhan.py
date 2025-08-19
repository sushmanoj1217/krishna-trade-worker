import os
import re
import csv
import io
import time
import requests
from datetime import date
from tenacity import retry, stop_after_attempt, wait_fixed
from utils.logger import log

# =============================================================================
# Dhan v2 Option Chain integration (POST JSON) + Instrument resolver (CSV)
# Rate-limit safe: expiry/OC caching + 429 cooldown
# =============================================================================

# --- ENV ---
DHAN_BASE = os.getenv("DHAN_BASE", "https://api.dhan.co").rstrip("/")
DHAN_UNDERLYING_SCRIP = os.getenv("DHAN_UNDERLYING_SCRIP", "").strip()   # int OR mapping OR empty
DHAN_UNDERLYING_SEG = os.getenv("DHAN_UNDERLYING_SEG", "IDX_I").strip()  # indices: IDX_I
DHAN_EXPIRY_ENV = os.getenv("DHAN_EXPIRY", "").strip()                   # optional YYYY-MM-DD
OC_SYMBOL = os.getenv("OC_SYMBOL", "NIFTY").strip().upper()
MATCH_HINT = os.getenv("DHAN_MATCH_STRING", "").strip()                  # optional, e.g. "NIFTY 50"

# Caching / rate-limit knobs
EXPIRY_TTL_SECS = int(os.getenv("EXPIRY_TTL_SECS", "300"))               # cache expiry list for 5 min
_MIN_INTERVAL = os.getenv("DHAN_MIN_INTERVAL_SECS", os.getenv("OC_REFRESH_SECS", "10"))
try:
    MIN_INTERVAL_SECS = max(3, int(_MIN_INTERVAL))                       # at least 3s between OC calls
except Exception:
    MIN_INTERVAL_SECS = 10
COOLDOWN_429_SECS = int(os.getenv("DHAN_429_COOLDOWN_SECS", "30"))       # back off on 429 for 30s

_last_sid_logged = None
_expiry_cache = {"value": None, "ts": 0.0}
_oc_cache = {"data": None, "ts": 0.0, "expiry": None}
_cooldown_until = 0.0  # epoch seconds


# ---------- HTTP helpers ----------
def _headers():
    token = os.getenv("DHAN_ACCESS_TOKEN", "")
    cid = os.getenv("DHAN_CLIENT_ID", "")
    if not token or not cid:
        raise RuntimeError("DHAN credentials missing (DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN)")
    # Dhan v2 headers
    return {"access-token": token, "client-id": cid, "Content-Type": "application/json"}

def _post_json(url: str, payload: dict, timeout=12):
    r = requests.post(url, headers=_headers(), json=payload, timeout=timeout)
    if r.status_code >= 400:
        log.warning(f"Dhan {r.status_code}: {r.text[:300]} @ {url}")
    r.raise_for_status()
    return r.json()

def _get_text(url: str, timeout=12):
    # For instrument CSV endpoints
    r = requests.get(url, headers=_headers(), timeout=timeout)
    if r.status_code >= 400:
        log.warning(f"Dhan {r.status_code}: {r.text[:300]} @ {url}")
    r.raise_for_status()
    r.encoding = "utf-8"
    return r.text


# ---------- v2 APIs (JSON) ----------
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

def _get_option_chain_once(underlying_scrip: int, underlying_seg: str, expiry: str) -> dict:
    """Single attempt; we handle 429 cooldown ourselves (no tenacity here)."""
    url = f"{DHAN_BASE}/v2/optionchain"
    r = requests.post(url, headers=_headers(),
                      json={"UnderlyingScrip": underlying_scrip, "UnderlyingSeg": underlying_seg, "Expiry": expiry},
                      timeout=12)
    if r.status_code == 429:
        # Too many requests — set cooldown and return cached if available
        global _cooldown_until
        _cooldown_until = time.time() + COOLDOWN_429_SECS
        log.warning(f"Dhan 429: {r.text[:200]} @ {url} → cooldown {COOLDOWN_429_SECS}s")
        r.raise_for_status()
    if r.status_code >= 400:
        log.warning(f"Dhan {r.status_code}: {r.text[:300]} @ {url}")
    r.raise_for_status()
    return r.json()


# ---------- Security ID resolver (CSV via v2 instrument endpoint) ----------
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
def _fetch_instruments_csv_for_segment(segment: str) -> list[dict]:
    """
    Dhan v2 segment-wise instrument list returns CSV:
      GET /v2/instrument/{exchangeSegment}
    We'll parse CSV into list[dict].
    """
    url = f"{DHAN_BASE}/v2/instrument/{segment}"
    text = _get_text(url)
    f = io.StringIO(text)
    reader = csv.DictReader(f)
    return [row for row in reader]

def _extract_security_id(row: dict) -> int | None:
    # Common id columns seen in Dhan CSVs
    candidates = [
        "Security ID", "SecurityID", "SecurityId",
        "SEM_SMST_SECURITY_ID", "SEM_SECURITY_ID", "SECURITY_ID",
        "SEM_SEC_ID", "SEM_SECID", "SEC_ID",
    ]
    for k in candidates:
        v = row.get(k)
        if isinstance(v, str) and v.strip().isdigit():
            return int(v.strip())
        if isinstance(v, int):
            return v
    # last resort: first numeric field
    for k, v in row.items():
        if isinstance(v, str) and v.strip().isdigit():
            return int(v.strip())
    return None

def _name_blob(row: dict) -> str:
    # Concatenate several likely name fields
    name_cols = [
        "Display Name", "DISPLAY_NAME", "display_name",
        "Trading Symbol", "TRADING_SYMBOL", "SEM_TRADING_SYMBOL", "TradingSymbol",
        "Scrip Name", "SCRIP_NAME", "ScripName",
        "Symbol Name", "SYMBOL_NAME", "symbol_name",
        "Instrument Name", "INSTRUMENT_NAME", "Instrument",
        "Name", "NAME",
    ]
    parts = []
    for c in name_cols:
        v = row.get(c)
        if isinstance(v, str) and v:
            parts.append(v)
    return _norm(" ".join(parts))

def _looks_like_index(row: dict) -> bool:
    for k in ["Instrument Type", "INSTRUMENT_TYPE", "InstrumentType", "Instrument", "INSTRUMENT"]:
        v = row.get(k)
        if isinstance(v, str) and "INDEX" in v.upper():
            return True
    return False

def _sid_from_env_map(sym: str) -> int | None:
    """
    Accepts:
      - single integer: "13"
      - mapping: "NIFTY=13,BANKNIFTY=25; FINNIFTY=27"
    """
    raw = DHAN_UNDERLYING_SCRIP
    if not raw:
        return None
    if raw.isdigit():
        return int(raw)
    parts = re.split(r"[;,]", raw)
    kv = {}
    for p in parts:
        if "=" in p:
            k, v = p.split("=", 1)
            k = k.strip().upper()
            v = v.strip()
            if v.isdigit():
                kv[k] = int(v)
    return kv.get(sym.upper())

def _resolve_security_id(sym: str, segment: str) -> int | None:
    targets = _targets_for_symbol(sym)
    rows = _fetch_instruments_csv_for_segment(segment)
    if not rows:
        log.warning("Instrument CSV empty")
        return None

    # Exact match first
    for row in rows:
        blob = _name_blob(row)
        if any(t == blob for t in targets):
            sid = _extract_security_id(row)
            if sid:
                log.info(f"Resolved SecurityID {sid} via exact name match: {blob}")
                return sid

    # Contains match (prefer index rows)
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


# ---------- Inputs (with quiet logging) ----------
def ensure_inputs() -> tuple[int, str, str | None]:
    """
    Decide UnderlyingScrip (SecurityID), Segment, and Expiry override.
    Priority:
      1) DHAN_UNDERLYING_SCRIP env (single int or mapping)
      2) Resolve via /v2/instrument/{segment} CSV
      3) Fallback to known IDs (NIFTY=13, BANKNIFTY=25, FINNIFTY=27)
    """
    global _last_sid_logged

    # 1) env: single or mapping
    sid_from_env = _sid_from_env_map(OC_SYMBOL)
    if sid_from_env:
        if sid_from_env != _last_sid_logged:
            log.info(f"Using SecurityID from env: {sid_from_env}")
            _last_sid_logged = sid_from_env
        return sid_from_env, DHAN_UNDERLYING_SEG, (DHAN_EXPIRY_ENV or None)

    # 2) resolve via instrument CSV
    sid = _resolve_security_id(OC_SYMBOL, DHAN_UNDERLYING_SEG)
    if sid:
        if sid != _last_sid_logged:
            log.info(f"Resolved SecurityID {sid} for {OC_SYMBOL}")
            _last_sid_logged = sid
        return sid, DHAN_UNDERLYING_SEG, (DHAN_EXPIRY_ENV or None)

    # 3) fallback IDs
    fallback = {"NIFTY": 13, "BANKNIFTY": 25, "FINNIFTY": 27}.get(OC_SYMBOL)
    if fallback:
        if fallback != _last_sid_logged:
            log.warning(f"Auto-resolve failed; falling back to hardcoded ID {fallback} for {OC_SYMBOL}")
            _last_sid_logged = fallback
        return fallback, DHAN_UNDERLYING_SEG, (DHAN_EXPIRY_ENV or None)

    raise RuntimeError(
        "DHAN_UNDERLYING_SCRIP invalid and auto-resolve failed. "
        "Set a single ID or use map like 'NIFTY=13,BANKNIFTY=25,FINNIFTY=27'."
    )


# ---------- OC compute ----------
def compute_levels_from_oc_v2(oc_json: dict, used_expiry: str) -> dict:
    """
    oc_json expected (per Dhan v2):
    {
      "data": {
        "last_price": float,
        "oc": {
          "25000.000000": {"ce": {"oi": int, ...}, "pe": {"oi": int, ...}},
          ...
        }
      }
    }
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


# ---------- Public API (rate-limit aware) ----------
def fetch_levels() -> dict:
    """
    High-level helper used by analytics.oc_refresh.refresh_once()
    Caches expiry for EXPIRY_TTL_SECS and OC for MIN_INTERVAL_SECS (same expiry).
    On HTTP 429, respects cooldown and returns cached data if available.
    """
    global _expiry_cache, _oc_cache, _cooldown_until

    u_scrip, u_seg, expiry_override = ensure_inputs()

    # Expiry cache
    now = time.time()
    if expiry_override:
        expiry = expiry_override
    else:
        if (_expiry_cache["value"] is None) or (now - _expiry_cache["ts"] > EXPIRY_TTL_SECS):
            expiries = get_expiry_list(u_scrip, u_seg)
            picked = _pick_nearest_expiry(expiries)
            if not picked:
                raise RuntimeError("No expiry available from Dhan")
            _expiry_cache = {"value": picked, "ts": now}
            log.info(f"Picked expiry {picked} (cache {EXPIRY_TTL_SECS}s)")
        expiry = _expiry_cache["value"]

    # Respect cooldown after 429
    if now < _cooldown_until:
        if _oc_cache["data"] is not None and _oc_cache["expiry"] == expiry:
            log.warning(f"In cooldown ({int(_cooldown_until - now)}s left) → serving OC from cache")
            return compute_levels_from_oc_v2(_oc_cache["data"], expiry)
        raise RuntimeError("In 429 cooldown and no OC cache available")

    # If same-expiry and called within min interval → serve cache
    if _oc_cache["data"] is not None and _oc_cache["expiry"] == expiry:
        if (now - _oc_cache["ts"]) < MIN_INTERVAL_SECS:
            return compute_levels_from_oc_v2(_oc_cache["data"], expiry)

    # Fresh call
    try:
        oc = _get_option_chain_once(u_scrip, u_seg, expiry)
    except requests.HTTPError as e:
        # If 429 handled above, try cache
        if _oc_cache["data"] is not None and _oc_cache["expiry"] == expiry:
            log.warning("HTTP error on OC fetch → serving cache")
            return compute_levels_from_oc_v2(_oc_cache["data"], expiry)
        raise

    _oc_cache = {"data": oc, "ts": now, "expiry": expiry}
    return compute_levels_from_oc_v2(oc, expiry)
