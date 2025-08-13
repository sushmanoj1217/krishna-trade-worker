import time, os
from datetime import datetime
from tzlocal import get_localzone
from core.state import AppState
from agents import risk_manager, sl_manager, logger

POINT_VALUE = float(os.getenv("POINT_VALUE", "1") or "1")

def now_ts_iso():
    return datetime.now(get_localzone()).isoformat()

def trade_id(ts_iso: str, symbol: str, side: str, ltp: float) -> str:
    base = ts_iso.replace(":","").replace("-","")[:15]
    return f"{base}_{symbol}_{side}_{int(ltp*100)}"

def current_ltp(levels):
    return levels.get("spot")

def on_signal(sig, params, state: AppState, bus, sheet, cfg):
    symbol = cfg.symbol
    side = sig["side"]
    levels = state.last_levels or {}
    ltp = current_ltp(levels)
    if not risk_manager.can_take_trade(side, ltp, cfg, params, state):
        return
    qty = risk_manager.compute_qty(ltp)
    sl_pts = params["exits"]["initial_sl_points"]
    rr = params["exits"]["target_rr"]
    tp_pts = sl_pts * rr

    tsb = now_ts_iso()
    tid = trade_id(tsb, symbol, side, ltp)
    if side == "CE":
        sl = ltp - sl_pts
        tp = ltp + tp_pts
    else:
        sl = ltp + sl_pts
        tp = ltp - tp_pts
    trade = {
        "trade_id": tid,
        "ts_buy": tsb,
        "symbol": symbol,
        "side": side,
        "buy_ltp": ltp,
        "qty": qty,
        "sl": sl,
        "tp": tp,
        "status": "OPEN",
        "reason_buy": sig.get("reason",""),
        "strat_ver": params.get("name","v1"),
        "worker_id": os.getenv("WORKER_ID","DAY_A")
    }
    if logger.log_trade_open(sheet, trade):
        state.open_trades[tid] = trade
        print(f"[trade] OPEN {tid} {side} qty={qty} ltp={ltp} sl={sl} tp={tp}")
    else:
        print(f"[trade] duplicate-skip {tid}")

def tick(state: AppState, sheet, cfg, params):
    if not state.open_trades:
        return
    levels = state.last_levels or {}
    spot = levels.get("spot")
    if spot is None:
        return
    closed = []
    for tid, tr in list(state.open_trades.items()):
        side = tr["side"]
        buy = tr["buy_ltp"]
        qty = tr["qty"]
        sl = tr["sl"]
        tp = tr["tp"]
        cur = spot
        new_sl = sl_manager.maybe_trail(side, buy, sl, cur, params)
        if new_sl != sl:
            tr["sl"] = new_sl
            logger.log_trade_update(sheet, tid, {"sl": new_sl})
        exit_reason = None
        if side == "CE":
            if cur <= tr["sl"]:
                exit_reason = "SL"
            elif cur >= tr["tp"]:
                exit_reason = "TP"
        else:
            if cur >= tr["sl"]:
                exit_reason = "SL"
            elif cur <= tr["tp"]:
                exit_reason = "TP"
        if exit_reason:
            pnl_points = (cur - buy) if side=="CE" else (buy - cur)
            pnl = pnl_points * qty * POINT_VALUE
            logger.log_trade_close(sheet, tid, cur, exit_reason, pnl)
            closed.append((tid, pnl))
    for tid, pnl in closed:
        state.open_trades.pop(tid, None)
        risk_manager.bump_daily(state, pnl)

def flatten_all(state: AppState, sheet):
    levels = state.last_levels or {}
    spot = levels.get("spot")
    if spot is None:
        return
    for tid, tr in list(state.open_trades.items()):
        side = tr["side"]
        buy = tr["buy_ltp"]
        qty = tr["qty"]
        pnl_points = (spot - buy) if side=="CE" else (buy - spot)
        pnl = pnl_points * qty * POINT_VALUE
        logger.log_trade_close(sheet, tid, spot, "EOD_FLAT", pnl)
        state.open_trades.pop(tid, None)
        risk_manager.bump_daily(state, pnl)
