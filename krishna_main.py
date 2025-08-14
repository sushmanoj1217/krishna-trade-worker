import os
import time
from datetime import datetime
from tzlocal import get_localzone

from core.bus import Bus
from core.config import load_settings, load_strategy_params
from core.state import AppState
from integrations.sheets import get_sheet
from housekeeping.schedulers import start_schedulers
from analytics.oc_refresh import oc_refresh_tick

# Agents & integrations
from agents import market_scanner, signal_generator, paper_trader, performance_tracker, backtester, logger, event_filter
from integrations import telegram


def today_str():
    return datetime.now(get_localzone()).strftime("%Y-%m-%d")


def pick_primary_symbol(cfg) -> str:
    raw = os.getenv("OC_SYMBOL", "") or cfg.symbol
    prim = os.getenv("OC_SYMBOL_PRIMARY", "").strip() or None
    if prim:
        return prim.strip().upper()
    if "," in raw:
        return raw.split(",")[0].strip().upper()
    return (raw or cfg.symbol).strip().upper()


def main():
    print("== Krishna Trade Worker v3 (Dhan OC) ==")

    cfg = load_settings()
    params = load_strategy_params()
    bus = Bus()
    state = AppState()
    sheet = get_sheet()

    worker_id = os.getenv("WORKER_ID", "DAY_A")
    shift_mode = os.getenv("SHIFT_MODE", "DAY").upper()

    # OC refresh cadence
    oc_secs_env = int(
        os.getenv(
            "OC_REFRESH_SECS",
            str(cfg.oc_refresh_secs_day if shift_mode == "DAY" else cfg.oc_refresh_secs_night),
        )
    )
    oc_secs = max(oc_secs_env, 3)

    # Primary symbol selection (this worker trades only primary)
    primary_symbol = pick_primary_symbol(cfg)
    cfg.symbol = primary_symbol

    # Ensure all sheet headers
    logger.ensure_all_headers(sheet, cfg)

    # -------- Bus Handlers --------
    def _levels_handler(levels: dict):
        # only process primary symbol
        if levels.get("symbol") and levels["symbol"].upper() != cfg.symbol.upper():
            return

        state.last_levels = levels
        if state.day_date != today_str():
            state.reset_if_new_day(today_str())

        # Event filter: hold signals/trades inside risky windows
        blocked, reason = event_filter.is_blocked_now(sheet, cfg)
        if blocked:
            logger.log_status(
                sheet,
                {"worker_id": worker_id, "shift_mode": shift_mode, "state": "HOLD", "message": f"events: {reason}"},
            )
            return

        market_scanner.on_levels(levels)
        signal_generator.on_levels(levels, params, state, bus, sheet, cfg)

    bus.on("levels", _levels_handler)

    def _signal_handler(sig: dict):
        if sig.get("symbol", "").upper() != cfg.symbol.upper():
            return

        # Safety: skip if events block is active
        blocked, _ = event_filter.is_blocked_now(sheet, cfg)
        if blocked:
            return

        logger.log_signal(sheet, cfg, sig, params, worker_id)
        telegram.send(f"Signal {sig['side']} {sig.get('level_hit','')} {cfg.symbol} @ {sig.get('spot')}")

        if os.getenv("AUTO_TRADE", "on").lower() == "on" and shift_mode == "DAY":
            paper_trader.on_signal(sig, params, state, bus, sheet, cfg)

    bus.on("signal", _signal_handler)

    def _on_trade_open(evt: dict):
        tr = evt.get("trade", {})
        telegram.send(
            f"OPEN {tr.get('symbol')} {tr.get('side')} qty={tr.get('qty')} @ {tr.get('buy_ltp')} "
            f"SL={tr.get('sl')} TP={tr.get('tp')}"
        )

    def _on_trade_close(evt: dict):
        tr = evt.get("trade", {})
        telegram.send(
            f"CLOSE {tr.get('symbol')} {tr.get('side')} exit={evt.get('exit_ltp')} "
            f"reason={evt.get('reason')} PnL={evt.get('pnl'):.2f}"
        )

    bus.on("trade_open", _on_trade_open)
    bus.on("trade_close", _on_trade_close)

    # -------- App Callbacks (Schedulers) --------
    def heartbeat():
        logger.log_status(sheet, {"worker_id": worker_id, "shift_mode": shift_mode, "state": "OK", "message": f"hb {cfg.symbol}"})

    def paper_tick():
        paper_trader.tick(state, sheet, cfg, params)

    def pre_eod_flatten():
        paper_trader.flatten_all(state, sheet)

    def eod():
        performance_tracker.eod(sheet, cfg, worker_id, today_str())

    def nightly():
        backtester.nightly(sheet, cfg, worker_id)

    app = {
        "oc_secs": oc_secs,
        "oc_refresh": lambda: oc_refresh_tick(bus),
        "paper_tick": paper_tick,
        "pre_eod_flatten": pre_eod_flatten,
        "eod": eod,
        "nightly": nightly,
        "heartbeat": heartbeat,
    }

    # Start schedulers
    start_schedulers(app, shift_mode)

    # Main loop
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("shutting down...")


if __name__ == "__main__":
    main()
