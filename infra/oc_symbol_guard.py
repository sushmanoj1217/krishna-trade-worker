# infra/oc_symbol_guard.py
# ------------------------------------------------------------
# Enforce single OC_SYMBOL and normalize Dhan env:
# - OC_SYMBOL: pick one (NIFTY/BANKNIFTY/FINNIFTY), trim combos like "NIFTY/BANKNIFTY".
# - DHAN_UNDERLYING_SCRIP_MAP: "NIFTY=13,BANKNIFTY=25,FINNIFTY=27" -> pick SecurityID by OC_SYMBOL.
# - DHAN_UNDERLYING_SCRIP: set from MAP or sensible defaults if empty/placeholder.
# - DHAN_UNDERLYING_SEG: default to IDX_I if empty.
# Logs a clear one-liner: "Using symbol: NIFTY (SecurityID: 13, SEG: IDX_I)".
# ------------------------------------------------------------
from __future__ import annotations
import os, logging, re
from typing import Dict, Optional, Tuple

_log = logging.getLogger(__name__)

_ALLOWED = {"NIFTY", "BANKNIFTY", "FINNIFTY"}
_DEFAULT_IDS = {"NIFTY": "13", "BANKNIFTY": "25", "FINNIFTY": "27"}
_PLACEHOLDERS = {"", "none", "null", "nil", "na", "-", "--"}

def _is_placeholder(val: Optional[str]) -> bool:
    if val is None:
        return True
    s = str(val).strip().lower()
    return s in _PLACEHOLDERS

def _pick_single_symbol(raw: Optional[str]) -> Tuple[Optional[str], bool]:
    """
    From strings like "NIFTY/BANKNIFTY", " NIFTY , FINNIFTY " return one symbol.
    Picks the first allowed one. Returns (symbol, changed?)
    """
    if not raw:
        return None, False
    s = raw.strip().upper()
    parts = [p for p in re.split(r"[\/,\s]+", s) if p]
    for p in parts:
        if p in _ALLOWED:
            changed = len(parts) > 1 or p != s
            return p, changed
    # not matching allowed → keep original as-is (but warn)
    return s, False

def _parse_map(raw: Optional[str]) -> Dict[str, str]:
    """
    "NIFTY=13,BANKNIFTY=25,FINNIFTY=27" → {"NIFTY":"13",...}
    """
    out: Dict[str, str] = {}
    if not raw:
        return out
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

    # ---- OC_SYMBOL ----
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

    # ---- SEGMENT ----
    seg = env.get("DHAN_UNDERLYING_SEG")
    if _is_placeholder(seg):
        seg = "IDX_I"
        env["DHAN_UNDERLYING_SEG"] = seg

    # ---- SECURITY ID ----
    map_raw = env.get("DHAN_UNDERLYING_SCRIP_MAP", "")
    mp = _parse_map(map_raw)

    cur_secid = env.get("DHAN_UNDERLYING_SCRIP")
    # Treat placeholders like "None"/"null"/"-" as empty
    if _is_placeholder(cur_secid):
        cur_secid = ""

    new_secid = cur_secid

    # 1) Prefer map entry if available for selected symbol
    if sym in mp and mp[sym].strip():
        new_secid = mp[sym].strip()
    # 2) Else if empty, use sensible defaults
    elif not new_secid:
        if sym in _DEFAULT_IDS:
            new_secid = _DEFAULT_IDS[sym]

    # Write back if changed
    if (cur_secid or "") != (new_secid or ""):
        env["DHAN_UNDERLYING_SCRIP"] = new_secid or ""
        _log.info("DHAN_UNDERLYING_SCRIP set -> '%s' (via %s)",
                  new_secid or "''", "MAP" if sym in mp else "DEFAULTS")

    # Final one-liner
    if new_secid:
        _log.info("Using symbol: %s (SecurityID: %s, SEG: %s)", sym, new_secid, seg)
    else:
        _log.info("Using symbol: %s (SecurityID: —, SEG: %s)", sym, seg)

    info.update({"symbol": sym, "segment": seg, "security_id": new_secid or ""})
    return info
