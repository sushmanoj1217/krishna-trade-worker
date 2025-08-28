# integrations/option_chain_dhan.py
from __future__ import annotations

import os
import time
import math
import json
from typing import Any, Dict, Tuple, List, Optional

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

# ---------- Config helpers ----------

def _get_env_str(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(name, default)
    if v is None:
        return None
    # remove BOM/whitespace/newlines just in case
    return v.encode("utf-8").decode("utf-8").strip()

def _get_env_int(name: str, default: Optional[int] = None) -> Optional[int]:
    v = _get_env_str(name, None)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except Exception:
        return default

def _ist_now_str() -> str:
    # Render container likely UTC; we just annotate with IST in label to match /oc_now UI.
    # (If pytz not guaranteed, keep simple.)
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    return f"{ts} IST"

# ---------- HTTP helpers ----------

def _hdr() -> Dict[str, str]:
    cid = _get_env_str("DHAN_CLIENT_ID", "") or ""
    atok = _get_env_str("DHAN_ACCESS_TOKEN", "") or ""
    return {
        "accept": "application/json",
        "content-type": "application/json",
        "client-id": cid.strip(),
        "access-token": atok.strip(),
    }

class _RetryableHTTP(Exception):
    pass

def _should_retry(e: Exception) -> bool:
    # Retry only when server/ratelimit/temp issues
    if isinstance(e, _RetryableHTTP):
        return True
    return False

@retry(
    reraise=True,
    stop=stop_after_attempt(int(_get_env_int("DHAN_HTTP_MAX_RETRIES", 3) or 3)),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception(_should_retry),
)
def _post(url: str, payload: Dict[str, Any], timeout: Optional[float] = None) -> Dict[str, Any]:
    to = timeout or float(_get_env_int("DHAN_HTTP_TIMEOUT", 12) or 12)
    h = _hdr()
    try:
        r = requests.post(url, headers=h, json=payload, timeout=to)
        # Explicit handling
        if r.status_code == 429 or (500 <= r.status_code < 600):
            # Raise retryable
            raise _RetryableHTTP(f"HTTP {r.status_code}: {r.text[:200]}")
        r.raise_for_status()  # 4xx/5xx
    except requests.HTTPError as e:
        # Keep server's response body for clarity
        txt = ""
        try:
            txt = r.text  # type: ignore
        except Exception:
            pass
        # 401/400 should bubble up as non-retryable
        raise requests.HTTPError(txt or str(e)) from e
    try:
        return r.json()
    except Exception as e:
        raise requests.HTTPError(f"Invalid JSON from DHAN: {r.text[:200]}") from e

# ---------- DHAN endpoints ----------

_BASE = "https://api.dhan.co/v2"

def _expiry_list(under_scrip: int, under_seg: str) -> List[str]:
    url = f"{_BASE}/optionchain/expirylist"
    body = {"UnderlyingScrip": under_scrip, "UnderlyingSeg": under_seg}
    data = _post(url, body)
    # Expect: {"data": ["YYYY-MM-DD", ...], "status":"success"}
    exps = data.get("data") or data.get("Data") or []
    if not isinstance(exps, list):
        return []
    return [str(x) for x in exps]

def _fetch_oc(under_scrip: int, under_seg: str, expiry: str) -> Dict[str, Any]:
    url = f"{_BASE}/optionchain"
    body = {"UnderlyingScrip": under_scrip, "UnderlyingSeg": under_seg, "Expiry": expiry}
    data = _post(url, body)
    # Expect: {"data": {...}, "status":"success"}
    return data

# ---------- OC computations ----------

def _nearest_levels_from_spot(spot: float, strikes: List[float]) -> Tuple[float, float, float, float]:
    """
    Choose S1/S2 as nearest LOWER strikes, R1/R2 as nearest HIGHER strikes.
    Assumes strikes sorted asc.
    """
    if not strikes:
        return (0.0, 0.0, 0.0, 0.0)
    # find insertion point
    lo, hi = 0, len(strikes)
    while lo < hi:
        mid = (lo + hi) // 2
        if strikes[mid] < spot:
            lo = mid + 1
        else:
            hi = mid
    # lo is first strike >= spot
    r1 = strikes[lo] if lo < len(strikes) else strikes[-1]
    r2 = strikes[lo + 1] if (lo + 1) < len(strikes) else r1
    s1 = strikes[lo - 1] if lo - 1 >= 0 else strikes[0]
    s2 = strikes[lo - 2] if lo - 2 >= 0 else s1
    # normalize (S1<=S2? ensure S1>S2)
    if s2 > s1:
        s1, s2 = s2, s1
    return (float(s1), float(s2), float(r1), float(r2))

def _sum_oi(chain: Dict[str, Any]) -> Tuple[float, float, float, float]:
    """
    Return (total_ce_oi, total_pe_oi, ce_delta_sum, pe_delta_sum)
    Using (oi - previous_oi) if available; otherwise 0 delta.
    """
    t_ce = t_pe = d_ce = d_pe = 0.0
    for k, node in chain.items():
        ce = (node or {}).get("ce") or {}
        pe = (node or {}).get("pe") or {}
        ce_oi = float(ce.get("oi") or 0.0)
        pe_oi = float(pe.get("oi") or 0.0)
        t_ce += ce_oi
        t_pe += pe_oi
        ce_prev = float(ce.get("previous_oi") or 0.0)
        pe_prev = float(pe.get("previous_oi") or 0.0)
        d_ce += (ce_oi - ce_prev)
        d_pe += (pe_oi - pe_prev)
    return t_ce, t_pe, d_ce, d_pe

def _max_pain(chain: Dict[str, Any]) -> float:
    """
    Simplified: strike with max (CE OI + PE OI).
    """
    best = None
    best_sum = -1.0
    for k, node in chain.items():
        try:
            strike = float(k)
        except Exception:
            continue
        ce = (node or {}).get("ce") or {}
        pe = (node or {}).get("pe") or {}
        s = float(ce.get("oi") or 0.0) + float(pe.get("oi") or 0.0)
        if s >= best_sum:
            best_sum = s
            best = strike
    return float(best or 0.0)

def _mv_tag(pcr: float) -> str:
    # Simple heuristic; your downstream uses it as a tag (C2 gate logic handles exact sets)
    if pcr <= 0.75:
        return "bearish"
    if pcr >= 1.1:
        return "bullish"
    return ""

# ---------- Public: fetch_levels ----------

async def fetch_levels(p: Any) -> Dict[str, Any]:
    """
    Main provider entrypoint (async by signature, internally sync HTTP).
    Returns a dict consumed by /oc_now & checks:
      keys: status, source, symbol, expiry, spot, s1,s2,r1,r2, pcr, mp, asof, age_sec, ce_oi_delta, pe_oi_delta, mv
    On failure: status='provider_error', error='<message>'
    """
    symbol = _get_env_str("OC_SYMBOL", "NIFTY") or "NIFTY"
    seg = _get_env_str("DHAN_UNDERLYING_SEG", "IDX_I") or "IDX_I"

    # UnderlyingScrip: prefer explicit, else map, else default NIFTY=13
    scrip = _get_env_int("DHAN_UNDERLYING_SCRIP", None)
    if scrip is None:
        m = _get_env_str("DHAN_UNDERLYING_SCRIP_MAP", "") or ""
        # e.g., "NIFTY=13,BANKNIFTY=25"
        try:
            parts = [x.strip() for x in m.split(",") if x.strip()]
            kv = dict(
                (a.strip().upper(), int(b.strip()))
                for a, b in (item.split("=") for item in parts if "=" in item)
            )
            scrip = kv.get(symbol.upper(), None)
        except Exception:
            scrip = None
    if scrip is None:
        # sensible default
        scrip = 13

    asof = _ist_now_str()
    try:
        # Choose expiry: prefer ENV override, else nearest from API
        expiry = _get_env_str("OC_EXPIRY", "") or ""
        if not expiry:
            exps = _expiry_list(scrip, seg)
            expiry = exps[0] if exps else ""

        data = _fetch_oc(scrip, seg, expiry)
        payload = data.get("data") or data.get("Data") or {}
        spot = float(payload.get("last_price") or payload.get("lastPrice") or 0.0)
        chain = payload.get("oc") or payload.get("OC") or {}

        # Build strikes list
        strikes: List[float] = []
        for k in chain.keys():
            try:
                strikes.append(float(k))
            except Exception:
                continue
        strikes.sort()

        s1, s2, r1, r2 = _nearest_levels_from_spot(spot, strikes)
        t_ce, t_pe, d_ce, d_pe = _sum_oi(chain)
        pcr = round((t_pe / t_ce), 2) if t_ce > 0 else 0.0
        mp = _max_pain(chain)
        mv = _mv_tag(pcr)

        return {
            "status": "ok",
            "source": "provider",
            "symbol": symbol,
            "expiry": expiry,
            "spot": round(spot, 2),
            "s1": s1,
            "s2": s2,
            "r1": r1,
            "r2": r2,
            "pcr": pcr,
            "mp": mp,
            "asof": asof,
            "age_sec": 0,
            "ce_oi_delta": d_ce,
            "pe_oi_delta": d_pe,
            "mv": mv,
        }

    except requests.HTTPError as e:
        # Give precise provider error (auth or id)
        return {
            "status": "provider_error",
            "source": "provider",
            "symbol": symbol,
            "expiry": _get_env_str("OC_EXPIRY", "") or "",
            "error": str(e),
            "asof": asof,
            "age_sec": None,
        }
    except Exception as e:
        return {
            "status": "provider_error",
            "source": "provider",
            "symbol": symbol,
            "expiry": _get_env_str("OC_EXPIRY", "") or "",
            "error": f"Unhandled: {e}",
            "asof": asof,
            "age_sec": None,
        }

__all__ = ["fetch_levels"]
