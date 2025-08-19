# agents/trade_loop.py
from __future__ import annotations
import os, asyncio
from datetime import datetime
from zoneinfo import ZoneInfo
from utils.logger import log
from utils.params import Params
from utils.cache import get_snapshot
from utils.state import get_last_signal, is_last_signal_placed, mark_last_signal_placed
from integrations import sheets as sh

IST = ZoneInfo("Asia/Kolkata")

QTY_PER_TRADE = int(os.getenv("QTY_PER_TRADE", "15"))
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "10"))
MAX_EXPOSURE_PER_TRADE = float(os.getenv("MAX_EXPOSURE_PER_TRADE", "3000"))
SYMBOL = os.getenv("OC_SYMBOL", "NIFTY").upper()

def _within_market():
    t = datetime.now(tz=IST).time()
    return (t >= datetime.strptime("09:30", "%H:%M").time()
            and t < datetime.strptime("15:15", "%H:%M").time())

async def trade_tick():
    if not _within_market():
        return
    snap = get_snapshot()
    if not snap:
        return

    sig = get_last_signal()
    if not sig or not sig.get("eligible"):
        return
    if is_last_signal_placed():
        return

    if sh.count_today_trades() >= MAX_TRADES_PER_DAY:
        log.info("Daily cap hit; skip trade")
        return

    entry = float(sig["entry"]); side = sig["side"]
    approx_premium = max(5.0, min(300.0, abs((snap.spot or entry) - entry)))
    exposure = approx_premium * QTY_PER_TRADE
    if exposure > MAX_EXPOSURE_PER_TRADE:
        log.info(f"Exposure {exposure} > cap {MAX_EXPOSURE_PER_TRADE}; skip")
        return

    sl = float(sig["sl"]); tp = float(sig["tp"])

    tid = f"TR-{datetime.now(tz=IST).strftime('%Y%m%d-%H%M%S')}"
    row = [
        tid, sig["id"], SYMBOL, side, entry, "", sl, tp, f"oc:{sig['trigger']}",
        datetime.now(tz=IST).strftime("%Y-%m-%d %H:%M:%S"),
        "", "", "", ""  # exit/result/pnl/hash
    ]
    sh.append_row("Trades", row)
    mark_last_signal_placed()
    log.info(f"TRADE BUY {tid} {side} @ {entry} SL={sl} TP={tp} x{QTY_PER_TRADE}")

async def force_flat_all(reason="force_flat"):
    open_trades = sh.get_open_trades()
    if not open_trades:
        return
    snap = get_snapshot()
    mark = float(snap.spot or 0.0) if snap else 0.0
    for t in open_trades:
        buy = float(t["buy_ltp"]); side = t["side"]
        pnl = (mark - buy) if side == "CE" else (buy - mark)
        sh.close_trade(t["trade_id"], exit_ltp=mark, result="flat", pnl=round(pnl, 2), note=reason)
        log.info(f"FLAT {t['trade_id']} @ {mark} pnl={pnl} ({reason})")
