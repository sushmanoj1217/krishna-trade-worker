import os
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, Any

import requests
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

from utils.logger import log
from utils.params import Params

class TooManyRequests(Exception):
    pass

@dataclass
class OCResult:
    spot: float
    s1: float
    s2: float
    r1: float
    r2: float
    expiry: str
    pcr: float | None
    max_pain: float
    bias_tag: str | None
    vix: float | None

def _resolve_security_id(p: Params) -> int:
    sym = p.symbol.upper()
    # SCRIP_MAP overrides SCRIP numeric
    map_str = os.getenv("DHAN_UNDERLYING_SCRIP_MAP", "").strip()
    if map_str:
        mp = {}
        for part in map_str.split(","):
            if "=" in part:
                k, v = part.split("=")
                mp[k.strip().upper()] = int(v.strip())
        if sym in mp:
            log.info(f"Using SecurityID from map: {sym}={mp[sym]}")
            return mp[sym]
    scrip_env = os.getenv("DHAN_UNDERLYING_SCRIP", "").strip()
    if scrip_env.isdigit():
        log.info(f"Using SecurityID from env: {scrip_env}")
        return int(scrip_env)
    raise RuntimeError("DHAN_UNDERLYING_SCRIP invalid and auto-resolve failed. Set '13' or map 'NIFTY=13,...'.")

def _headers() -> Dict[str, str]:
    cid = os.getenv("DHAN_CLIENT_ID", "")
    at = os.getenv("DHAN_ACCESS_TOKEN", "")
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "ClientId": cid,
        "AccessToken": at,
    }

def _endpoint() -> str:
    # v2 endpoint for optionchain snapshot (as per Dhan docs; you may adjust if required)
    return "https://api.dhan.co/v2/optionchain"

@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    retry=retry_if_exception_type((requests.HTTPError,)),
    reraise=True
)
def _post(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.post(url, headers=_headers(), data=json.dumps(payload), timeout=10)
    if r.status_code == 429:
        raise TooManyRequests(r.text)
    if r.status_code >= 400:
        try:
            r.raise_for_status()
        except Exception as e:
            raise requests.HTTPError(r.text) from e
    return r.json()

def _compute_levels_from_oc_v2(data: Dict[str, Any], p: Params) -> Dict[str, Any]:
    """
    Expect Dhan OC v2 response with CE/PE OI by strike.
    Compute:
    - spot (from underlyingPrice)
    - S1/S2/R1/R2 (100-pt grid around MaxPain / nearest walls)
    - PCR (sum OI)
    - MaxPain (strike of min CE+PE pain)
    - bias tags (simple rules)
    """
    underlying = data.get("underlyingPrice") or data.get("underlying_index_price")
    if underlying is None:
        raise RuntimeError("No underlying price in response")
    spot = float(underlying)

    # Build OI maps
    ce_map: Dict[int, float] = {}
    pe_map: Dict[int, float] = {}
    for row in data.get("optionChain", []):
        k = int(row["strikePrice"])
        ce_map[k] = float(row.get("ceOpenInterest", 0))
        pe_map[k] = float(row.get("peOpenInterest", 0))

    # PCR
    total_ce = sum(ce_map.values()) or 1.0
    total_pe = sum(pe_map.values())
    pcr = round(total_pe / total_ce, 2) if total_ce else None

    # Max Pain (min total OI around strike) – very rough
    all_strikes = sorted(set(ce_map.keys()) | set(pe_map.keys()))
    if not all_strikes:
        raise RuntimeError("Empty OC strikes")

    pain = {k: ce_map.get(k, 0) + pe_map.get(k, 0) for k in all_strikes}
    max_pain = min(pain, key=pain.get)

    # levels around max_pain on round-100 grid (adjust by band)
    # For indexes like NIFTY (50/100 step), BANKNIFTY (100), FINNIFTY (50)
    step = 50 if p.symbol.upper() in ("NIFTY", "FINNIFTY") else 100
    base = (max_pain // step) * step
    s1 = float(base)
    s2 = float(base - step)
    r1 = float(base + step)
    r2 = float(base + 2 * step)

    # Bias tags
    bias = None
    mp_dist = abs(spot - max_pain)
    if pcr is not None:
        if pcr >= p.pcr_bull_high:
            bias = "mvbullpcr"
        elif pcr <= p.pcr_bear_low:
            bias = "mvbearpcr"
    # MP-based bias
    mp_tag = "mvbullmp" if spot >= (max_pain + p.mp_support_dist) else ("mvbearmp" if spot <= (max_pain - p.mp_support_dist) else None)
    if mp_tag:
        bias = mp_tag

    # VIX – Dhan OC may not include; leave None
    vix = None

    return {
        "spot": spot,
        "s1": s1, "s2": s2, "r1": r1, "r2": r2,
        "expiry": data.get("expiryDate") or data.get("expiry") or "",
        "pcr": pcr,
        "max_pain": float(max_pain),
        "bias_tag": bias,
        "vix": vix,
        "oi": {"ce": ce_map, "pe": pe_map},  # exposed for pattern checks
    }

async def fetch_levels(p: Params) -> Dict[str, Any]:
    sec = _resolve_security_id(p)
    seg = os.getenv("DHAN_UNDERLYING_SEG", "IDX_I")
    payload = {
        "underlying": {
            "securityType": seg,
            "securityId": sec
        }
    }
    url = _endpoint()
    data = _post(url, payload)
    return _compute_levels_from_oc_v2(data, p)
