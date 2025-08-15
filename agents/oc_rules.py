# agents/oc_rules.py
# Option-Chain Based Trading Rules (as per plan):
# - S/R from OI: S1/S2 (PE highest/2nd), R1/R2 (CE highest/2nd)  -> scanner provides these.
# - Entry:
#     * CE BUY at S1/S2 when Bullish (CE OI↓, PE OI↑) or Strong Bullish (CE OI↓, PE OI↓)
#     * PE BUY at R1/R2 when Bearish (CE OI↑, PE OI↑) or Strong Bearish (CE OI↑, PE OI↓)
#   Only ONE trade per level per day (S1 once, S2 once, R1 once, R2 once).
# - Filters: 6 OC conditions (Bullish/StrongBullish/Bearish/StrongBearish/Sideways/BigMovePossible)
#   Sideways => avoid; BigMovePossible => wait for breakout (no entry).
# - Exit guides returned with signal:
#     * target_pct = 0.30  (min 30% premium gain)   OR nearest next S/R touch (executor may handle)
#     * sl_pct     = 0.35  (30–40% loss -> mid = 35%)
#     * time_exit_ist = "15:15"
#
# This module is stateless. Dedup (one-trade-per-level-per-day) should be enforced by caller
# using `dedup_key` returned with the signal.

from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, time as dtime, timezone, timedelta

# ===== Tunables =====
NEAR_BAND_POINTS = 6           # "level par aaya" ke liye +/- band
OI_UP_TH   =  2.0              # % change considered UP
OI_DOWN_TH = -2.0              # % change considered DOWN
BLOCK_WINDOWS = [("09:15","09:20"), ("15:15","15:30")]  # avoid new entries
TARGET_PCT = 0.30
SL_PCT      = 0.35
EXIT_BY_IST = "15:15"

@dataclass
class OCCtx:
    symbol: str
    spot: float
    s1: float
    s2: float
    r1: float
    r2: float
    ce_oi_pct: Optional[float] = None   # near-ATM CE OI % delta
    pe_oi_pct: Optional[float] = None   # near-ATM PE OI % delta
    volume_low: Optional[bool] = None   # optional: low volume -> sideways bias
    now: Optional[datetime] = None      # current time (IST). If None, use system.

# ---------- time helpers ----------
def _parse_hhmm(x: str) -> Tuple[int,int]:
    h,m = x.split(":"); return int(h), int(m)

def _in_block_window(now: datetime) -> bool:
    t = now.time()
    for a,b in BLOCK_WINDOWS:
        h1,m1 = _parse_hhmm(a); h2,m2 = _parse_hhmm(b)
        if dtime(h1,m1) <= t <= dtime(h2,m2):
            return True
    return False

def _today_str_ist(now: Optional[datetime] = None) -> str:
    n = now or datetime.now()
    return n.date().isoformat()

# ---------- condition classifier (6 views) ----------
def _cat(val: Optional[float]) -> str:
    if val is None:
        return "na"
    if val >= OI_UP_TH:
        return "up"
    if val <= OI_DOWN_TH:
        return "down"
    return "flat"

def classify_view(ce_pct: Optional[float], pe_pct: Optional[float], volume_low: Optional[bool]) -> str:
    c = _cat(ce_pct)
    p = _cat(pe_pct)
    vol_low = bool(volume_low)

    # Explicit table mapping
    # Bullish: CE↓, PE↑
    if c == "down" and p == "up":
        return "Bullish"
    # Strong Bullish: CE↓, PE↓
    if c == "down" and p == "down":
        return "Strong Bullish"
    # Bearish: CE↑, PE↑  (if not sideways volume-low)
    if c == "up" and p == "up" and not vol_low:
        return "Bearish"
    # Sideways: CE↑, PE↑ with low volume
    if c == "up" and p == "up" and vol_low:
        return "Sideways"
    # Strong Bearish: CE↑, PE↓
    if c == "up" and p == "down":
        return "Strong Bearish"
    # Big Move Possible: CE↓, PE↓ but not near S/R (usually compressions)
    if c == "down" and p == "down":
        return "Big Move Possible"
    # Fallback
    return "Unknown"

# ---------- level proximity ----------
def _near_level(spot: float, level: float, band: float = NEAR_BAND_POINTS) -> bool:
    return abs(spot - level) <= band

def _dedup_key(symbol: str, level_tag: str, now: Optional[datetime]) -> str:
    return f"{symbol}:{level_tag}:{_today_str_ist(now)}"

# ---------- public API ----------
def evaluate(ctx: OCCtx) -> Optional[Dict[str, Any]]:
    """
    Returns a CE/PE BUY signal dict or None.
    Output keys:
      side: "BUY_CE" | "BUY_PE"
      reason: text
      level_tag: "S1"|"S2"|"R1"|"R2"
      level: float
      view: one of 6 OC conditions
      target_pct: float (e.g., 0.30)
      sl_pct: float (e.g., 0.35)
      exit_by_ist: "15:15"
      dedup_key: unique key for one-trade-per-level-per-day
    """
    now = ctx.now or datetime.now()
    if _in_block_window(now):
        return None

    view = classify_view(ctx.ce_oi_pct, ctx.pe_oi_pct, ctx.volume_low)

    # Sideways or Big Move -> wait/avoid
    if view in ("Sideways", "Big Move Possible"):
        return None

    spot = ctx.spot
    # ---- CE BUY at S1/S2 with Bullish/Strong Bullish ----
    if view in ("Bullish", "Strong Bullish"):
        if _near_level(spot, ctx.s1):
            level_tag, level = "S1", ctx.s1
            return {
                "side": "BUY_CE",
                "reason": f"{view} near {level_tag}",
                "level_tag": level_tag,
                "level": level,
                "view": view,
                "target_pct": TARGET_PCT,
                "sl_pct": SL_PCT,
                "exit_by_ist": EXIT_BY_IST,
                "dedup_key": _dedup_key(ctx.symbol, level_tag, now),
            }
        if _near_level(spot, ctx.s2):
            level_tag, level = "S2", ctx.s2
            return {
                "side": "BUY_CE",
                "reason": f"{view} near {level_tag}",
                "level_tag": level_tag,
                "level": level,
                "view": view,
                "target_pct": TARGET_PCT,
                "sl_pct": SL_PCT,
                "exit_by_ist": EXIT_BY_IST,
                "dedup_key": _dedup_key(ctx.symbol, level_tag, now),
            }

    # ---- PE BUY at R1/R2 with Bearish/Strong Bearish ----
    if view in ("Bearish", "Strong Bearish"):
        if _near_level(spot, ctx.r1):
            level_tag, level = "R1", ctx.r1
            return {
                "side": "BUY_PE",
                "reason": f"{view} near {level_tag}",
                "level_tag": level_tag,
                "level": level,
                "view": view,
                "target_pct": TARGET_PCT,
                "sl_pct": SL_PCT,
                "exit_by_ist": EXIT_BY_IST,
                "dedup_key": _dedup_key(ctx.symbol, level_tag, now),
            }
        if _near_level(spot, ctx.r2):
            level_tag, level = "R2", ctx.r2
            return {
                "side": "BUY_PE",
                "reason": f"{view} near {level_tag}",
                "level_tag": level_tag,
                "level": level,
                "view": view,
                "target_pct": TARGET_PCT,
                "sl_pct": SL_PCT,
                "exit_by_ist": EXIT_BY_IST,
                "dedup_key": _dedup_key(ctx.symbol, level_tag, now),
            }

    # Else: no signal
    return None
