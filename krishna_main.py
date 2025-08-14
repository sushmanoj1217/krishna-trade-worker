# path: krishna_main.py
import os, time
from datetime import datetime
from tzlocal import get_localzone

from core.bus import Bus
from core.config import load_settings, load_strategy_params
from core.state import AppState
from core.validate import print_startup_summary
from core.version import git_sha
from integrations.sheets import get_sheet
from integrations import ping as uptime_ping
from housekeeping.schedulers import start_schedulers
from analytics.oc_refresh import oc_refresh_tick

from agents import market_scanner, signal_generator, paper_trader, performance_tracker, backtester, logger, event_filter
from agents import daily_summary as daily_summary_agent
from agents import shift_snapshot, auto_heal
from integrations import telegram
from ops import teleops
from risk import circuit_breaker as cb
from storage import sheet_persistence  # NEW

def today_str():
    return datetime.now(get_localzone()).strftime("%Y-%m-%d")

def pick_primary_symbol(cfg) -> str:
    raw = os.getenv("OC_SYMBOL","") or cfg.symbol
    prim = os.getenv("OC_SYMBOL_PRIMARY","").strip() or None
    if prim: return prim.strip().upper()
    if "," in raw: return raw.split(",")[0].strip().upper()
    return (raw or cfg.symbol).strip().upper()

def main():
    print("== Krishna Trade Worker v3 (Dhan OC) ==")
    print_startup_summary()

    cfg = load_settings()

    # Get sheet FIRST, sync overrides from Sheet, THEN load params
    sheet = get_sheet()
    try:
        if sheet_persistence.sync_params_override_from_sheet(sheet):
            print("[boot] params_override: loaded from Sheet")
    except Exception as e:
        print("[boot] params_override sync error:", e)

    params = load_strategy_params()

    bus = Bus(); state = AppState()
    worker_id = os.getenv("WORKER_ID","DAY_A"); shift_mode = os.getenv("SHIFT_MODE","DAY").upper()

    oc_secs_env = int(os.getenv("OC_REFRESH_SECS", str(cfg.oc_refresh_secs_day if shift_mode=='DAY' else cfg.oc_refresh_secs_night)))
    oc_secs = max(oc_secs_env, 3)
    cfg.symbol = pick_primary_symbol(cfg)

    logger.ensure_all_headers(sheet, cfg)

    # Startup ping
    try: telegram.send(f"🚀 Worker UP [{cfg.symbol}] {git_sha()[:10]}")
    except Exception: pass

    def _levels_handler(levels: dict):
        if levels.get("symbol") and levels["symbol"].upper() != cfg.symbol.upper(): return
        state.last_levels = levels
        if state.day_date != today_str(): state.reset_if_new_day(today_str())
        blocked, reason = event_filter.is_blocked_now(sheet, cfg)
        if blocked:
            logger.log_status(sheet, {"worker_id": worker_id, "shift_mode": shift_mode, "state":"HOLD", "message": f"events: {reason}"})
            return
        paused, why = cb.is_paused()
        if paused:
            logger.log_status(sheet, {"worker_id": worker_id, "shift_mode": shift_mode, "state":"HOLD", "message": why})
            return
        market_scanner.on_levels(levels)
        signal_generator.on_levels(levels, params, state, bus, sheet, cfg)
    bus.on("levels", _levels_handler)

    def _signal_handler(sig: dict):
        if sig.get("symbol","").upper() != cfg.symbol.upper(): return
        blocked, _ = event_filter.is_blocked_now(sheet, cfg)
        if blocked: return
        paused, _ = cb.is_paused()
        if paused: return
        logger.log_signal(sheet, cfg, sig, params, worker_id)
        telegram.send(f"Signal {sig['side']} {sig.get('level_hit','')} {cfg.symbol} @ {sig.get('spot')}")
        if os.getenv("AUTO_TRADE","on").lower()=="on" and shift_mode=="DAY":
            paper_trader.on_signal(sig, params, state, bus, sheet, cfg)
    bus.on("signal", _signal_handler)

    def _on_trade_open(evt: dict):
        tr = evt.get("trade", {})
        telegram.send(f"OPEN {tr.get('symbol')} {tr.get('side')} qty={tr.get('qty')} @ {tr.get('buy_ltp')} SL={tr.get('sl')} TP={tr.get('tp')}")
    def _on_trade_close(evt: dict):
        tr = evt.get("trade", {})
        telegram.send(f"CLOSE {tr.get('symbol')} {tr.get('side')} exit={evt.get('exit_ltp')} reason={evt.get('reason')} PnL={evt.get('pnl'):.2f}")
        try: cb.on_trade_close(evt, sheet, cfg)
        except Exception: pass
    bus.on("trade_open", _on_trade_open)
    bus.on("trade_close", _on_trade_close)

    def heartbeat():
        logger.log_status(sheet, {"worker_id": worker_id, "shift_mode": shift_mode, "state":"OK", "message": f"hb {cfg.symbol}"})
        try: uptime_ping.ping()
        except Exception: pass

    def paper_tick(): paper_trader.tick(state, sheet, cfg, params)
    def pre_eod_flatten(): paper_trader.flatten_all(state, sheet)
    def eod():
        performance_tracker.eod(sheet, cfg, worker_id, today_str())
        shift_snapshot.day_end_snapshot(sheet, cfg)
        auto_heal.generate_suggestions(sheet, cfg)
    def nightly(): backtester.nightly(sheet, cfg, worker_id)
    def send_daily_summary(): daily_summary_agent.push_telegram(sheet, cfg)
    def tele_ops(): teleops.tick(sheet, cfg, state)

    def on_job_error(job_id, exc):
        msg = f"❌ Job error [{job_id}] {exc}"
        print(msg)
        try:
            logger.log_status(sheet, {"state":"ERR", "message": msg})
            telegram.send(msg)
        except Exception: pass

    app = {
        "oc_secs": oc_secs,
        "oc_refresh": lambda: oc_refresh_tick(bus),
        "paper_tick": paper_tick,
        "pre_eod_flatten": pre_eod_flatten,
        "eod": eod,
        "nightly": nightly,
        "heartbeat": heartbeat,
        "daily_summary": send_daily_summary,
        "tele_ops": tele_ops,
        "on_job_error": on_job_error,
    }

    start_schedulers(app, shift_mode)

    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        print("shutting down...")

if __name__ == "__main__":
    main()
