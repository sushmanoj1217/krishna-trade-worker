"""
Nightly params tuner (very lightweight placeholder):
- Reads last N trades from Sheets
- Computes simple stats and bumps MIN_TARGET_POINTS_* slightly if hit rate low
- Writes Params_Override tab keys (for operator review)
"""
import math, os
from utils.logger import log
from integrations import sheets as sh

N = int(os.getenv("TUNER_LOOKBACK_TRADES", "50"))
ADJ_STEP = int(os.getenv("TUNER_TARGET_STEP", "2"))  # points

def run():
    trades = sh.get_recent_trades(N)
    if not trades:
        log.info("EOD tuner: no trades to analyze")
        return
    wins = [t for t in trades if t.get("result") in ("tp", "mv_flip") and float(t.get("pnl") or 0) > 0]
    losses = [t for t in trades if t.get("result") in ("sl", "flat") and float(t.get("pnl") or 0) <= 0]
    wr = (len(wins) / max(1, (len(wins)+len(losses)))) * 100.0
    # naive rule: if WR < 45, increase min target by 2 pts; if > 60, decrease by 2 pts
    delta = -ADJ_STEP if wr > 60 else (ADJ_STEP if wr < 45 else 0)
    if delta != 0:
        override = {"key": "MIN_TARGET_POINTS_N", "value": str(max(5, sh.get_override_int("MIN_TARGET_POINTS_N", 20) + delta))}
        sh.upsert_override(override["key"], override["value"])
        log.info(f"EOD tuner: WR={wr:.1f}% → MIN_TARGET_POINTS_N {delta:+} -> {override['value']}")
    else:
        log.info(f"EOD tuner: WR OK {wr:.1f}% (no change)")
