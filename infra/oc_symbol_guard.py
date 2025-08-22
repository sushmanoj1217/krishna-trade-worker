# infra/oc_symbol_guard.py
# ------------------------------------------------------------
# Enforce single OC_SYMBOL and normalize Dhan env:
# - OC_SYMBOL: pick one (NIFTY/BANKNIFTY/FINNIFTY), trim combos like "NIFTY/BANKNIFTY".
# - DHAN_UNDERLYING_SCRIP_MAP: "NIFTY=13,BANKNIFTY=25,FINNIFTY=27" -> pick SecurityID by OC_SYMBOL.
# - DHAN_UNDERLYING_SCRIP: set from MAP if present; else keep existing.
# - DHAN_UNDERLYING_SEG: default to IDX_I if empty.
# Logs a clear one-liner: "Using symbol: NIFTY (SecurityID: 13, SEG: IDX_I)".
# ------------------------------------------------------------
from __future__ import annotations
import os, logging, re
from typing import Dict, Optional, Tuple

_log = logging.getLogger(__name__)

_ALLOWED = {"NIFTY", "BANKNIFTY", "FINNIFTY"}

def _pick_single_symbol(raw: Optional[str]) -> Tuple[Optional[str], bool]:
    """
    From strings like "NIFTY/BANKNIFTY", " NIFTY , FINNIFTY ", return one symbol.
    Picks the first allowed one. Returns (symbol, changed?)
    """
    if not raw:
        return None, False
    s = raw.strip().upper()
    # split on common separators
    parts = re.split(r"[\/,\s]+", s)
    for p in parts:
        if p in _ALLOWED:
            # if more than one token originally -> changed = True when extras existed
            changed = len([x for x in parts if x]) > 1
            return p, changed
    # not matching allowed → keep original as-is (but warn)
    return s, False

def _parse_map(raw: Optional[str]) -> Dict[str, str]:
    """
    "NIFTY=13,BANKNIFTY=25,FINNIFTY=27" → {"NIFTY":"13",...}
    """
    if not raw:
        return {}
    out: Dict[str, str] = {}
    for tok in raw.split(","):
        tok = tok.strip()
        if not tok or "=" not in tok:
            continue
        k, v = tok.split("=", 1)
        k = k.strip().upper()
        v = v.strip()
        if k and v:
            out[k] = v
    return out

def apply() -> Dict[str, str]:
    """
    Normalize env in-place (os.environ). Returns a short info dict for logging/inspection.
    """
    env = os.environ
    raw_sym = env.get("OC_SYMBOL", "")
    sym, changed = _pick_single_symbol(raw_sym)
    info: Dict[str, str] = {}

    if not sym:
        _log.warning("OC_SYMBOL not set; set one of %s", sorted(_ALLOWED))
        return {"status": "missing"}

    if sym not in _ALLOWED:
        _log.warning("OC_SYMBOL '%s' not in allowed %s; proceeding as-is", sym, sorted(_ALLOWED))

    if changed or raw_sym != sym:
        env["OC_SYMBOL"] = sym
        _log.info("OC_SYMBOL normalized from '%s' -> '%s'", raw_sym, sym)

    # Segment default
    seg = env.get("DHAN_UNDERLYING_SEG") or "IDX_I"
    env["DHAN_UNDERLYING_SEG"] = seg

    # Resolve SecurityID
    secid = env.get("DHAN_UNDERLYING_SCRIP", "").strip()
    map_raw = env.get("DHAN_UNDERLYING_SCRIP_MAP", "")
    mp = _parse_map(map_raw)

    if mp and sym in mp:
        secid = mp[sym]
        env["DHAN_UNDERLYING_SCRIP"] = secid
    elif not secid:
        _log.warning("No DHAN_UNDERLYING_SCRIP_MAP entry for %s and DHAN_UNDERLYING_SCRIP empty.", sym)

    # Final one-liner for logs
    if secid:
        _log.info("Using symbol: %s (SecurityID: %s, SEG: %s)", sym, secid, seg)
    else:
        _log.info("Using symbol: %s (SecurityID: —, SEG: %s)", sym, seg)

    info.update({"symbol": sym, "segment": seg, "security_id": secid or ""})
    return info
