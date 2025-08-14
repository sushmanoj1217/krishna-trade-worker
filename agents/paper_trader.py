import os
from datetime import datetime
from tzlocal import get_localzone

from core.state import AppState
from agents import risk_manager, sl_manager, logger

POINT_VALUE = float(os.getenv("POINT_VALUE", "1") or "1")


def now_ts_iso() -> str:
    return datetime.now(get_localzone()).isoformat()


def trade_id(ts_iso: str, symbol: str, side: str, ltp: float) -> str:
    base = ts_iso.replace(":", "").replace("-", "")[:15]
    return f"{base}_{symbol}_{side}_{int(ltp * 100)}"


def current_ltp(levels: dict):
    return levels.get("spot")


def fmt_trade(tr: dict) -> str:
    return f"{tr['symbol']} {tr['side']} qty={tr['qty']} @ {tr['buy_ltp']:.2f} SL={tr['sl']:.2f} TP={tr['tp']:.2f}"


def on_signal(sig: dict, params: dict, state: AppState, bus, sheet, cfg):
    symbol = sig.get("symbol", cfg.symbol)
    side = sig["side"]

    levels = state.last_levels or {}
    if levels.get("symbol") and levels["symbol"].upper() != symbol.upper():
        return

    ltp = current_ltp(levels)
    if not risk_manager.can_take_trade(side, ltp, cfg, params, state):
        return

    qty = risk_manager.compute_qty(ltp)
    sl_pts = params["exits"]["initial_sl_points"]
    rr = params["exits"]["target_rr"]
    tp_pts = sl_pts * rr

    tsb = now_ts_iso()
    tid = trade_id(tsb, symbol, side, float(ltp or 0))

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
        "reason_buy": sig.get("reason", ""),
        "strat_ver": params.get("name", "v1"),
        "worker_id": os.getenv("WORKER_ID", "DAY_A"),
    }

    if logger.log_trade_open(sheet, trade):
        state.open_trades[tid] = trade
        print(f"[trade] OPEN {tid} {side} {symbol} qty={qty} ltp={ltp} sl={sl} tp={tp}")
        # Notify via bus for alerts
        try:
            bus.emit("trade_open", {"trade": trade})
        except Exception:
            pass
    else:
        print(f"[trade] duplicate-skip {tid}")


def tick(state: AppState, sheet, cfg, params):
    if not state.open_trades:
        return

    levels = state.last_levels or {}
    spot = levels.get("spot")
    sym = levels.get("symbol", cfg.symbol)
    if spot is None:
        return

    closed = []

    for tid, tr in list(state.open_trades.items()):
        if tr.get("symbol") and tr.get("symbol").upper() != sym.upper():
            continue

        side = tr["side"]
        buy = float(tr["buy_ltp"])
        qty = int(tr["qty"])
        sl = float(tr["sl"])
        tp = float(tr["tp"])
        cur = float(spot)

        # Trailing SL
        new_sl = sl_manager.maybe_trail(side, buy, sl, cur, params)
        if new_sl != sl:
            tr["sl"] = new_sl
            logger.log_trade_update(sheet, tid, {"sl": new_sl})

        # Exit checks
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
            pnl_points = (cur - buy) if side == "CE" else (buy - cur)
            pnl = pnl_points * qty * POINT_VALUE
            logger.log_trade_close(sheet, tid, cur, exit_reason, pnl)
            closed.append((tid, pnl, tr, cur, exit_reason))
            # alert
            try:
                bus.emit("trade_close", {"trade": tr, "exit_ltp": cur, "reason": exit_reason, "pnl": pnl})
            except Exception:
                pass

    # finalize closures and update daily limits
    for tid, pnl, tr, cur, exit_reason in closed:
        state.open_trades.pop(tid, None)
        risk_manager.bump_daily(state, pnl)


def flatten_all(state: AppState, sheet):
    levels = state.last_levels or {}
    spot = levels.get("spot")
    sym = levels.get("symbol")
    if spot is None:
        return

    for tid, tr in list(state.open_trades.items()):
        if tr.get("symbol") and sym and tr.get("symbol").upper() != sym.upper():
            continue
        side = tr["side"]
        buy = float(tr["buy_ltp"])
        qty = int(tr["qty"])
        cur = float(spot)

        pnl_points = (cur - buy) if side == "CE" else (buy - cur)
        pnl = pnl_points * qty * POINT_VALUE
        logger.log_trade_close(sheet, tid, cur, "EOD_FLAT", pnl)
        # alert
        try:
            bus.emit("trade_close", {"trade": tr, "exit_ltp": cur, "reason": "EOD_FLAT", "pnl": pnl})
        except Exception:
            pass

        state.open_trades.pop(tid, None)
        risk_manager.bump_daily(state, pnl)
