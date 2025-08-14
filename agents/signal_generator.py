
import os, time
from core.state import AppState

def maybe_emit_signal(levels, params, state: AppState, bus, sheet, cfg):
    entry_band = params["entry_rules"].get("entry_band_points", 5)
    cooldown = int(os.getenv("MIN_SIGNAL_COOLDOWN_SECS", "60"))
    now = time.time()
    spot = levels.get("spot")
    s1, s2 = levels.get("s1"), levels.get("s2")
    r1, r2 = levels.get("r1"), levels.get("r2")
    if spot is None:
        return

    def near(a, b): return (a is not None and b is not None and abs(a-b) <= entry_band)
    def can_fire(side):
        last = state.last_signal_ts.get(side, 0)
        return (now - last) >= cooldown

    if (near(spot, s1) or near(spot, s2)) and can_fire("CE"):
        bus.emit("signal", {"side":"CE","level_hit":"S1" if near(spot,s1) else "S2","reason":f"Near support within {entry_band} pts","spot":spot, "symbol": levels.get("symbol")})
        state.last_signal_ts["CE"] = now
    if (near(spot, r1) or near(spot, r2)) and can_fire("PE"):
        bus.emit("signal", {"side":"PE","level_hit":"R1" if near(spot,r1) else "R2","reason":f"Near resistance within {entry_band} pts","spot":spot, "symbol": levels.get("symbol")})
        state.last_signal_ts["PE"] = now

def on_levels(levels, params, state: AppState, bus, sheet, cfg):
    maybe_emit_signal(levels, params, state, bus, sheet, cfg)
