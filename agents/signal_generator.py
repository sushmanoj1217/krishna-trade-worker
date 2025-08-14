# path: agents/signal_generator.py
import os, time
from core.state import AppState

def maybe_emit_signal(levels, params, state: AppState, bus, sheet, cfg):
    entry_band = params["entry_rules"].get("entry_band_points", 5)
    cooldown = int(os.getenv("MIN_SIGNAL_COOLDOWN_SECS", "60"))
    now = time.time()
    spot = levels.get("spot")
    s1, s2 = levels.get("s1"), levels.get("s2")
    r1, r2 = levels.get("r1"), levels.get("r2")
    sym = levels.get("symbol", cfg.symbol)

    if spot is None:
        return

    def near(a, b): 
        return (a is not None and b is not None and abs(a - b) <= entry_band)

    def can_fire(side, lvl):
        # Stronger key: per symbol + side + level (S1/S2/R1/R2)
        key = f"{sym}_{side}_{lvl}"
        last = state.last_signal_ts.get(key, 0)
        ok = (now - last) >= cooldown
        if ok:
            state.last_signal_ts[key] = now
        return ok

    if near(spot, s1) and can_fire("CE", "S1"):
        bus.emit("signal", {"side": "CE", "level_hit": "S1", "reason": f"Near support within {entry_band} pts", "spot": spot, "symbol": sym})
    elif near(spot, s2) and can_fire("CE", "S2"):
        bus.emit("signal", {"side": "CE", "level_hit": "S2", "reason": f"Near support within {entry_band} pts", "spot": spot, "symbol": sym})

    if near(spot, r1) and can_fire("PE", "R1"):
        bus.emit("signal", {"side": "PE", "level_hit": "R1", "reason": f"Near resistance within {entry_band} pts", "spot": spot, "symbol": sym})
    elif near(spot, r2) and can_fire("PE", "R2"):
        bus.emit("signal", {"side": "PE", "level_hit": "R2", "reason": f"Near resistance within {entry_band} pts", "spot": spot, "symbol": sym})

def on_levels(levels, params, state: AppState, bus, sheet, cfg):
    maybe_emit_signal(levels, params, state, bus, sheet, cfg)
