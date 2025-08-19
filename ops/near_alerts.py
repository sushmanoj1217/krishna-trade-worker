# ops/near_alerts.py
from __future__ import annotations

import os
import time
from typing import Dict, Tuple

from agents.logger import get_latest_status_map
from ops.notify import send_telegram

NEAR_ALERT_COOLDOWN_SECS = int(os.getenv("NEAR_ALERT_COOLDOWN_SECS", "120"))
NEAR_ALERT_DEBUG = os.getenv("NEAR_ALERT_DEBUG", "0") == "1"

# Per-symbol entry band points (fallback to ENTRY_BAND_POINTS)
ENTRY_BAND_POINTS = float(os.getenv("ENTRY_BAND_POINTS", "12"))
ENTRY_BAND_POINTS_MAP = os.getenv("ENTRY_BAND_POINTS_MAP", "")  # e.g. "NIFTY=12,BANKNIFTY=30,FINNIFTY=15"

def _band_for(symbol: str) -> float:
    if ENTRY_BAND_POINTS_MAP:
        parts = [p.strip() for p in ENTRY_BAND_POINTS_MAP.split(",") if "=" in p]
        for p in parts:
            k, v = p.split("=", 1)
            if k.strip().upper() == symbol.upper():
                try:
                    return float(v.strip())
                except Exception:
                    pass
    return ENTRY_BAND_POINTS

# cooldown memory
_last_alert_at: Dict[str, float] = {}

def _cooldown_ok(key: str) -> bool:
    now = time.time()
    last = _last_alert_at.get(key, 0)
    if now - last >= NEAR_ALERT_COOLDOWN_SECS:
        _last_alert_at[key] = now
        return True
    return False

def _decorate(msg: str) -> str:
    ctx = get_latest_status_map()
    tails = []
    if ctx.get("PCR"): tails.append(f"PCR {ctx['PCR']}")
    if ctx.get("VIX"): tails.append(f"VIX {ctx['VIX']}")
    if tails:
        return f"{msg} | " + " • ".join(tails)
    return msg

def _fmt(x) -> str:
    try:
        return f"{float(x):.2f}"
    except Exception:
        return str(x)

def nudge(snapshot: Dict) -> None:
    """
    Called every OC tick, decides NEAR/CROSS alerts for S* / R*.
    snapshot keys used: symbol, spot, S1*,S2*,R1*,R2*, MV
    """
    symbol = snapshot.get("symbol", "NIFTY")
    spot = snapshot.get("spot")
    mv = snapshot.get("MV", "-")
    if spot is None:
        return

    band = _band_for(symbol)

    levels = []
    if snapshot.get("S1*") is not None: levels.append(("S1*", float(snapshot["S1*"])))
    if snapshot.get("S2*") is not None: levels.append(("S2*", float(snapshot["S2*"])))
    if snapshot.get("R1*") is not None: levels.append(("R1*", float(snapshot["R1*"])))
    if snapshot.get("R2*") is not None: levels.append(("R2*", float(snapshot["R2*"])))

    for tag, trig in levels:
        # Define zone & cross by side semantics
        if tag.startswith("S"):  # CE entries at supports
            zone_low, zone_high = trig, trig + band
            near = (spot >= zone_low) and (spot <= zone_high)
            cross = spot <= trig
        else:  # R* for PE
            zone_low, zone_high = trig - band, trig
            near = (spot >= zone_low) and (spot <= zone_high)
            cross = spot >= trig

        # NEAR
        key_near = f"NEAR:{symbol}:{tag}"
        if near and _cooldown_ok(key_near):
            delta = spot - trig
            msg = f"🟨 NEAR {symbol} {tag} | spot { _fmt(spot) } vs trigger { _fmt(trig) } (Δ { _fmt(delta) }) • MV {mv}"
            send_telegram(_decorate(msg))

        # CROSS
        key_cross = f"CROSS:{symbol}:{tag}"
        if cross and _cooldown_ok(key_cross):
            delta = spot - trig
            msg = f"🟩 CROSS {symbol} {tag} | spot { _fmt(spot) } vs trigger { _fmt(trig) } (Δ { _fmt(delta) }) • MV {mv}"
            send_telegram(_decorate(msg))

        if NEAR_ALERT_DEBUG:
            # helpful debug without cooldown
            msg = f"🔎 DEBUG {symbol} {tag} zone[{_fmt(zone_low)}..{_fmt(zone_high)}] spot={_fmt(spot)} cross={cross}"
            send_telegram(msg)
