# path: agents/sl_manager.py
# Robust trailing-SL with safe defaults (no KeyError)

import os

def _bool_env(name: str, default: str = "on") -> bool:
    return (os.getenv(name, default) or default).lower() == "on"

def _int(val, fallback: int) -> int:
    try:
        return int(val)
    except Exception:
        return int(fallback)

def maybe_trail(side: str, buy: float, sl: float, cur: float, params: dict) -> float:
    """
    side: "CE" (long) or "PE" (short)
    buy:  entry price
    sl:   current stop
    cur:  current price
    params: expects params["exits"] dict but will fallback to ENV/defaults
    """
    exits = (params or {}).get("exits", {}) or {}

    # Defaults (ENV -> exits -> hard default)
    trailing_enabled = exits.get("trailing_enabled")
    if trailing_enabled is None:
        trailing_enabled = _bool_env("TRAILING_ENABLED", "on")
    else:
        trailing_enabled = bool(trailing_enabled)

    initial_sl = _int(exits.get("initial_sl_points"), 15)
    trail_after = exits.get("trail_after_points")
    if trail_after is None:
        trail_after = _int(os.getenv("TRAIL_AFTER_POINTS"), initial_sl)
    else:
        trail_after = _int(trail_after, initial_sl)

    trail_step = exits.get("trail_step_points")
    if trail_step is None:
        trail_step = _int(os.getenv("TRAIL_STEP_POINTS"), 5)
    else:
        trail_step = _int(trail_step, 5)

    if not trailing_enabled:
        return sl
    if any(v is None for v in (buy, sl, cur)):
        return sl
    try:
        buy = float(buy); sl = float(sl); cur = float(cur)
    except Exception:
        return sl

    new_sl = sl

    if side == "CE":
        # Long: trail after price moves +trail_after; lock sl to (cur - trail_step)
        if (cur - buy) >= trail_after:
            candidate = cur - trail_step
            if candidate > new_sl:
                new_sl = candidate
    else:  # "PE" short
        # Short: trail after price moves -trail_after; lock sl to (cur + trail_step)
        if (buy - cur) >= trail_after:
            candidate = cur + trail_step
            if candidate < new_sl:
                new_sl = candidate

    return new_sl
