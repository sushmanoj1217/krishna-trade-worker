import os, asyncio
from datetime import datetime
from zoneinfo import ZoneInfo
from utils.logger import log
from utils.params import Params
from utils.cache import get_snapshot
from utils.ids import trade_id
from integrations import sheets as sh

IST = ZoneInfo("Asia/Kolkata")

QTY_PER_TRADE = int(os.getenv("QTY_PER_TRADE", "15"))
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "10"))
MAX_EXPOSURE_PER_TRADE = float(os.getenv("MAX_EXPOSURE_PER_TRADE", "3000"))  # ₹ cap
SYMBOL = os.getenv("OC_SYMBOL", "NIFTY").upper()

def _within_market():
    t = datetime.now(tz=IST).time()
    return (t >= datetime.strptime("09:30", "%H:%M").time()
            and t < datetime.strptime("15:15", "%H:%M").time())

async def trade_tick():
    """Place new paper trades on fresh eligible signals, respect caps."""
    if not _within_market():
        return
    snap = get_snapshot()
    if not snap:
        return
    p = Params()

    # read last signal (fast-path)
    sig = sh.last_signal()
    if not sig:
        return
    if sig.get("placed") == "1":
        return  # already acted

    side = sig["side"]     # CE|PE
    trigger = sig["trigger"]
    entry = float(sig.get("entry") or 0)
    sl = float(sig.get("sl") or 0)
    tp = float(sig.get("tp") or 0)
    eligible = str(sig.get("eligible")).lower() in ("true", "1", "yes")

    # gating
    if not eligible:
        return
    if sh.count_today_trades() >= MAX_TRADES_PER_DAY:
        log.info("Daily cap hit; skipping trade")
        return

    # price/exposure sanity (paper: use entry * qty)
    approx_premium = max(5.0, min(300.0, abs(snap.spot - entry)))  # crude proxy
    exposure = approx_premium * QTY_PER_TRADE
    if exposure > MAX_EXPOSURE_PER_TRADE:
        log.info(f"Exposure {exposure} > cap {MAX_EXPOSURE_PER_TRADE}; skip")
        return

    tid = trade_id()
    basis = f"oc:{trigger}"
    row = [
        tid, sig["id"], SYMBOL, side, entry, "", sl, tp, basis,
        datetime.now(tz=IST).strftime("%Y-%m-%d %H:%M:%S"),
        "", "", "", "",  # exit_time,result,pnl,dedupe_hash
    ]
    sh.append_row("Trades", row)
    sh.mark_signal_placed(sig["id"])
    log.info(f"TRADE BUY {tid} {side} @ {entry} SL={sl} TP={tp} x{QTY_PER_TRADE}")

async def force_flat_all(reason="force_flat"):
    """Exit all open paper trades with market mark and write to sheet."""
    open_trades = sh.get_open_trades()
    if not open_trades:
        return
    snap = get_snapshot()
    mark = float(snap.spot or 0.0) if snap else 0.0
    for t in open_trades:
        buy = float(t["buy_ltp"])
        side = t["side"]
        # PnL proxy: CE → (mark - buy), PE → (buy - mark) (scaled for paper)
        pnl = (mark - buy) if side == "CE" else (buy - mark)
        pnl = round(pnl, 2)
        sh.close_trade(t["trade_id"], exit_ltp=mark,
                       result="flat", pnl=pnl,
                       note=reason)
        log.info(f"FLAT {t['trade_id']} @ {mark} pnl={pnl} ({reason})")
