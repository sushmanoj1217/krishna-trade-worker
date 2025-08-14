
import os, time
from datetime import datetime
from tzlocal import get_localzone
from core.bus import Bus
from core.config import load_settings, load_strategy_params
from core.state import AppState
from integrations.sheets import get_sheet
from housekeeping.schedulers import start_schedulers
from analytics.oc_refresh import oc_refresh_tick
from agents import market_scanner, signal_generator, paper_trader, performance_tracker, backtester, logger

def today_str():
    return datetime.now(get_localzone()).strftime("%Y-%m-%d")

def pick_primary_symbol(cfg) -> str:
    raw = os.getenv("OC_SYMBOL","") or cfg.symbol
    prim = os.getenv("OC_SYMBOL_PRIMARY","").strip() or None
    if prim: return prim.strip().upper()
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
    worker_id = os.getenv("WORKER_ID","DAY_A")
    shift_mode = os.getenv("SHIFT_MODE","DAY").upper()
    oc_secs_env = int(os.getenv("OC_REFRESH_SECS", str(cfg.oc_refresh_secs_day if shift_mode=='DAY' else cfg.oc_refresh_secs_night)))
    oc_secs = max(oc_secs_env, 3)

    primary_symbol = pick_primary_symbol(cfg)
    cfg.symbol = primary_symbol

    logger.ensure_all_headers(sheet, cfg)

    def _levels_handler(levels):
        if levels.get("symbol") and levels["symbol"].upper() != cfg.symbol.upper():
            return
        state.last_levels = levels
        if state.day_date != today_str():
            state.reset_if_new_day(today_str())
        market_scanner.on_levels(levels)
        signal_generator.on_levels(levels, params, state, bus, sheet, cfg)
    bus.on("levels", _levels_handler)

    def _signal_handler(sig):
        if sig.get("symbol","").upper() != cfg.symbol.upper():
            return
        logger.log_signal(sheet, cfg, sig, params, worker_id)
        if os.getenv("AUTO_TRADE","on").lower() == "on" and shift_mode == "DAY":
            paper_trader.on_signal(sig, params, state, bus, sheet, cfg)
    bus.on("signal", _signal_handler)

    def heartbeat():
        logger.log_status(sheet, {"worker_id": worker_id, "shift_mode": shift_mode, "state":"OK", "message":f"hb {cfg.symbol}"})
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

    start_schedulers(app, shift_mode)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("shutting down...")

if __name__ == "__main__":
    main()
