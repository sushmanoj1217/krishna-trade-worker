# agents/tp_sl_watcher.py
from __future__ import annotations
import os, asyncio
from utils.logger import log
from utils.cache import get_snapshot
from integrations import sheets as sh

TRAIL_RATIO = float(os.getenv("TRAIL_RATIO", "2.0"))  # trail from 1:2

async def trail_tick():
    """
    Lightweight placeholder: checks open trades and adjusts SL towards entry+target/ratio.
    Real premium-based trailing would use option LTP—kept simple here.
    """
    open_trades = sh.get_open_trades()
    if not open_trades:
        return
    snap = get_snapshot()
    mark = float(snap.spot or 0.0) if snap else 0.0
    for t in open_trades:
        entry = float(t["buy_ltp"]); side = t["side"]; sl = float(t["sl"]); tp = float(t["tp"])
        # trail targetally
        if side == "CE":
            profit = max(0.0, mark - entry)
            trail_point = entry + (tp - entry) / TRAIL_RATIO
            new_sl = max(sl, trail_point if profit >= (tp - entry) / TRAIL_RATIO else sl)
        else:
            profit = max(0.0, entry - mark)
            trail_point = entry - (entry - tp) / TRAIL_RATIO
            new_sl = min(sl, trail_point if profit >= (entry - tp) / TRAIL_RATIO else sl)
        if new_sl != sl:
            sh.update_trade_sl(t["trade_id"], new_sl)
            log.info(f"Trail SL {t['trade_id']} → {new_sl}")
