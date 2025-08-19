# krishna_main.py  — with pre-import debug so we can see import-time failures
from __future__ import annotations

# ---- Pre-import debug (runs before anything else) ----
from datetime import datetime
import sys, traceback

def _ts():
    return datetime.now().isoformat(sep=" ", timespec="seconds")

print(f"[{_ts()}] [BOOT] Pre-import starting…", flush=True)

try:
    # stdlib first
    import os
    import random
    import signal
    import threading
    import time
    from datetime import timedelta
    from typing import Tuple

    # local imports that might fail — keep inside try
    from agents.logger import ensure_all_headers, log_oc_live, log_signal
    from agents.trade_executor import TradeExecutor
    from analytics.market_context import write_context
    from analytics import oc_refresh  # must expose refresh() -> (snapshot, oc_items)
    from agents import signal_generator  # must expose generate_signals(snapshot) -> List[dict]
    from ops import near_alerts
    from ops.oc_format import format_oc_reply
    from ops import eod_perf
    from ops.notify import send_telegram

    print(f"[{_ts()}] [BOOT] Imports OK", flush=True)

except Exception as e:
    print(f"[{_ts()}] [FATAL] Import failed: {e}", flush=True)
    traceback.print_exc()
    sys.exit(1)

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
LOG_STDOUT = os.getenv("LOG_STDOUT", "1") == "1"

# ----------------- RUNTIME -----------------
executor = TradeExecutor()
_shutdown = False

def _now():
    return datetime.now().isoformat(sep=" ", timespec="seconds")

def _ist_now():
    return datetime.now()  # TZ env should be set on Render

def _parse_hhmm(hhmm: str) -> Tuple[int, int]:
    h, m = hhmm.split(":")
    return int(h), int(m)

def _today_ist_at(hhmm: str):
    h, m = _parse_hhmm(hhmm)
    now = _ist_now()
    return now.replace(hour=h, minute=m, second=0, microsecond=0)

def _sleep_until(ts):
    while not _shutdown:
        delta = (ts - _ist_now()).total_seconds()
        if delta <= 0:
            break
        time.sleep(min(1.0, delta))

def _safe_call(fn, *a, **kw):
    try:
        return fn(*a, **kw)
    except Exception as e:
        msg = f"Runtime error in {getattr(fn, '__name__', 'fn')}: {e}"
        print(f"[{_now()}] [ERR] {msg}", flush=True)
        traceback.print_exc()
        try:
            send_telegram(f"⚠️ {msg}")
        except Exception:
            pass
        return None

def _log_tick(snapshot: dict):
    if not LOG_STDOUT:
        return
    sym = snapshot.get("symbol", SYMBOL)
    spot = snapshot.get("spot")
    mv = snapshot.get("MV")
    s1s = snapshot.get("S1*"); s2s = snapshot.get("S2*")
    r1s = snapshot.get("R1*"); r2s = snapshot.get("R2*")
    print(f"[{_now()}] [TICK] {sym} spot={spot} MV={mv} | S*({s1s},{s2s}) R*({r1s},{r2s})", flush=True)

# ----------------- CORE TICK -----------------
def oc_tick_once():
    """Single OC cycle: fetch → log → near alerts → signals → executor tick → context write."""
    data = _safe_call(oc_refresh.refresh)
    if not data:
        return
    snapshot, oc_items = data  # snapshot dict, oc_items list
    snapshot.setdefault("symbol", SYMBOL)
    snapshot.setdefault("ts", datetime.now().isoformat(timespec="seconds"))

    _log_tick(snapshot)
    _safe_call(log_oc_live, snapshot)
    _safe_call(near_alerts.nudge, snapshot)

    signals = _safe_call(signal_generator.generate_signals, snapshot) or []
    if LOG_STDOUT and signals:
        print(f"[{_now()}] [SIGNALS] {len(signals)} generated", flush=True)
    for sig in signals:
        _safe_call(log_signal, sig)
        executor.place_limit(sig)

    executor.on_oc_tick(snapshot)
    _safe_call(write_context, oc_items=oc_items)

# ----------------- SCHEDULERS -----------------
def day_loop():
    time.sleep(random.uniform(JITTER_A, JITTER_B))

    if TELE_OC_ON_BOOT:
        try:
            snap = None
            if hasattr(oc_refresh, "peek_last"):
                snap, _ = oc_refresh.peek_last()
            if snap:
                msg = "🚀 Day worker online\n" + format_oc_reply(snap)
                print(f"[{_now()}] [BOOT] {msg.replace(chr(10), ' | ')}", flush=True)
                send_telegram(msg)
        except Exception as e:
            print(f"[{_now()}] [BOOT-ERR] {e}", flush=True)

    while not _shutdown:
        start = time.time()
        oc_tick_once()
        elapsed = time.time() - start
        wait_for = max(OC_MIN_INTERVAL_SECS, OC_REFRESH_SECS) - elapsed
        wait_for += random.uniform(JITTER_A, JITTER_B)
        if wait_for > 0:
            time.sleep(wait_for)

def schedule_time_exit_and_eod():
    while not _shutdown:
        now = _ist_now()
        te = _today_ist_at(TIME_EXIT_IST)
        if (te - now).total_seconds() > 0:
            _sleep_until(te)
            if _shutdown: break
            print(f"[{_now()}] [TIME-EXIT] Flattening all trades", flush=True)
            executor.close_all("TIME_EXIT")
            send_telegram("⏳ Time-exit 15:15 IST: all trades flattened.")

        eodw = _today_ist_at(EOD_WRITE_IST)
        if (eodw - _ist_now()).total_seconds() > 0:
            _sleep_until(eodw)
            if _shutdown: break
            print(f"[{_now()}] [EOD] Writing EOD performance", flush=True)
            _safe_call(eod_perf.write_eod_summary)

        eods = _today_ist_at(EOD_SUMMARY_IST)
        if (eods - _ist_now()).total_seconds() > 0:
            _sleep_until(eods)
            if _shutdown: break
            print(f"[{_now()}] [EOD] Sending EOD summary", flush=True)
            _safe_call(eod_perf.send_eod_summary_telegram)

        _sleep_until(_today_ist_at("00:00") + timedelta(days=1))

# ----------------- TELEGRAM ROUTER -----------------
def start_tele_router():
    try:
        from ops.tele_router import run_tele_router
        threading.Thread(target=run_tele_router, name="tele_router", daemon=True).start()
    except Exception as e:
        print(f"[{_now()}] [WARN] tele_router unavailable: {e}", flush=True)
        try:
            send_telegram("⚠️ tele_router unavailable; TG commands disabled.")
        except Exception:
            pass

# ----------------- BOOT -----------------
def _sigterm(_signo, _frame):
    global _shutdown
    _shutdown = True
    try:
        executor.close_all("SHUTDOWN")
    except Exception:
        pass
    print(f"[{_now()}] [SHUTDOWN] Stopping worker…", flush=True)
    try:
        send_telegram("🔻 Shutting down worker…")
    except Exception:
        pass
    time.sleep(1.0)
    sys.exit(0)

def main():
    print(f"[{_now()}] ✅ Starting worker…", flush=True)
    print(f"[{_now()}] 🔧 Ensuring Sheets tabs…", flush=True)
    ensure_all_headers()
    print(f"[{_now()}] ✅ Sheets tabs ensured", flush=True)

    start_tele_router()
    threading.Thread(target=schedule_time_exit_and_eod, name="time_exit_eod", daemon=True).start()
    threading.Thread(target=day_loop, name="day_loop", daemon=True).start()

    signal.signal(signal.SIGTERM, _sigterm)
    signal.signal(signal.SIGINT, _sigterm)
    print(f"[{_now()}] ✅ Worker started", flush=True)
    try:
        send_telegram("✅ Worker started.")
    except Exception:
        pass

    while not _shutdown:
        time.sleep(1.5)

if __name__ == "__main__":
    main()
