# krishna_main.py
from __future__ import annotations

import os
import random
import signal
import sys
import threading
import time
from datetime import datetime, timedelta, date

# Local modules
from agents.logger import ensure_all_headers, log_oc_live, log_signal
from agents.trade_executor import TradeExecutor
from analytics.market_context import write_context
from analytics import oc_refresh  # must expose a refresh() -> (snapshot, oc_items)
from agents import signal_generator  # must expose generate_signals(snapshot) -> List[dict]
from ops import near_alerts
from ops.oc_format import format_oc_reply
from ops import eod_perf
from ops.notify import send_telegram

# ----------------- ENV -----------------
TZ = os.getenv("TZ", "Asia/Kolkata")
SYMBOL = os.getenv("OC_SYMBOL", os.getenv("OC_SYMBOL_PRIMARY", "NIFTY"))

OC_MIN_INTERVAL_SECS = int(os.getenv("OC_MIN_INTERVAL_SECS", "18"))
OC_REFRESH_SECS = int(os.getenv("OC_REFRESH_SECS", "12"))
OC_JITTER_SECS = os.getenv("OC_JITTER_SECS", "3-5")
try:
    JITTER_A, JITTER_B = [int(x) for x in OC_JITTER_SECS.split("-")]
except Exception:
    JITTER_A, JITTER_B = 3, 5

TIME_EXIT_IST = os.getenv("TIME_EXIT_IST", "15:15")
EOD_WRITE_IST = os.getenv("EOD_WRITE_IST", "15:31")
EOD_SUMMARY_IST = os.getenv("EOD_SUMMARY_IST", "15:35")

TELE_OC_ON_BOOT = os.getenv("TELE_OC_ON_BOOT", "1") == "1"

# ----------------- RUNTIME -----------------
executor = TradeExecutor()
_shutdown = False

def _ist_now() -> datetime:
    # naive local time is fine on Render when TZ is set
    return datetime.now()

def _parse_hhmm(hhmm: str) -> Tuple[int, int]:
    h, m = hhmm.split(":")
    return int(h), int(m)

def _today_ist_at(hhmm: str) -> datetime:
    h, m = _parse_hhmm(hhmm)
    now = _ist_now()
    return now.replace(hour=h, minute=m, second=0, microsecond=0)

def _sleep_until(ts: datetime) -> None:
    while not _shutdown:
        delta = (ts - _ist_now()).total_seconds()
        if delta <= 0:
            break
        time.sleep(min(1.0, delta))

def _safe_call(fn, *a, **kw):
    try:
        return fn(*a, **kw)
    except Exception as e:
        send_telegram(f"⚠️ Runtime error in {getattr(fn, '__name__', 'fn')}: {e}")
        return None

# ----------------- CORE TICK -----------------
def oc_tick_once():
    """Single OC cycle: fetch → log → near alerts → signals → executor tick → context write."""
    # 1) Refresh OC (snapshot + raw chain)
    data = _safe_call(oc_refresh.refresh)
    if not data:
        return
    snapshot, oc_items = data  # snapshot dict, oc_items list
    # Sanity defaults
    snapshot.setdefault("symbol", SYMBOL)
    snapshot.setdefault("ts", datetime.now().isoformat(timespec="seconds"))

    # 2) Log OC to sheet (also shows S*/R* in oc_now)
    _safe_call(log_oc_live, snapshot)

    # 3) Alerts (NEAR / CROSS)
    _safe_call(near_alerts.nudge, snapshot)

    # 4) Signals
    signals = _safe_call(signal_generator.generate_signals, snapshot) or []
    for sig in signals:
        _safe_call(log_signal, sig)
        executor.place_limit(sig)

    # 5) Executor LTP / trail updates using spot
    executor.on_oc_tick(snapshot)

    # 6) Market context → PCR & VIX into Status
    _safe_call(write_context, oc_items=oc_items)

# ----------------- SCHEDULERS -----------------
def day_loop():
    """Runs during market hours. Keeps ticking until _shutdown."""
    # Initial jitter so clusters don't align
    time.sleep(random.uniform(JITTER_A, JITTER_B))

    # On boot greeting
    if TELE_OC_ON_BOOT:
        try:
            snap, _ = oc_refresh.peek_last() if hasattr(oc_refresh, "peek_last") else (None, None)
            if snap:
                send_telegram("🚀 Day worker online\n" + format_oc_reply(snap))
        except Exception:
            pass

    # Main loop
    while not _shutdown:
        start = time.time()
        oc_tick_once()
        # cadence
        elapsed = time.time() - start
        wait_for = max(OC_MIN_INTERVAL_SECS, OC_REFRESH_SECS) - elapsed
        wait_for += random.uniform(JITTER_A, JITTER_B)
        if wait_for > 0:
            time.sleep(wait_for)

def schedule_time_exit_and_eod():
    """Schedules time-exit and EOD summaries each day."""
    while not _shutdown:
        now = _ist_now()
        # Time Exit
        te = _today_ist_at(TIME_EXIT_IST)
        if (te - now).total_seconds() > 0:
            _sleep_until(te)
            if _shutdown: break
            executor.close_all("TIME_EXIT")
            send_telegram("⏳ Time-exit 15:15 IST: all trades flattened.")

        # EOD write
        eodw = _today_ist_at(EOD_WRITE_IST)
        if (eodw - _ist_now()).total_seconds() > 0:
            _sleep_until(eodw)
            if _shutdown: break
            try:
                _safe_call(eod_perf.write_eod_summary)
            except Exception:
                pass

        # EOD TG summary
        eods = _today_ist_at(EOD_SUMMARY_IST)
        if (eods - _ist_now()).total_seconds() > 0:
            _sleep_until(eods)
            if _shutdown: break
            try:
                _safe_call(eod_perf.send_eod_summary_telegram)
            except Exception:
                pass

        # roll to next day
        _sleep_until(_today_ist_at("00:00") + timedelta(days=1))

# ----------------- TELEGRAM ROUTER -----------------
def start_tele_router():
    try:
        from ops.tele_router import run_tele_router
    except Exception:
        send_telegram("⚠️ tele_router unavailable; TG commands disabled.")
        return
    threading.Thread(target=run_tele_router, name="tele_router", daemon=True).start()

# ----------------- BOOT -----------------
def _sigterm(_signo, _frame):
    global _shutdown
    _shutdown = True
    try:
        executor.close_all("SHUTDOWN")
    except Exception:
        pass
    send_telegram("🔻 Shutting down worker…")
    # give threads a moment to exit
    time.sleep(1.0)
    sys.exit(0)

def main():
    ensure_all_headers()

    # Start helpers
    start_tele_router()

    # Background schedulers
    threading.Thread(target=schedule_time_exit_and_eod, name="time_exit_eod", daemon=True).start()
    threading.Thread(target=day_loop, name="day_loop", daemon=True).start()

    # Keep process alive
    signal.signal(signal.SIGTERM, _sigterm)
    signal.signal(signal.SIGINT, _sigterm)
    send_telegram("✅ Worker started.")
    while not _shutdown:
        time.sleep(1.5)

if __name__ == "__main__":
    main()
