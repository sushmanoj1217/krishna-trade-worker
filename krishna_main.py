# krishna_main.py
# Krishna Trade Worker v3 — Main entry with NEAR alerts
# - OC snapshot, logging, and signal generation (directional buffer)
# - Gates: Events HOLD, No-trade windows (09:15–09:30, 14:45–15:15), Circuit pause
# - Condition-change exit, Daily trade cap, Dedup, Paper OPEN
# - NEAR alerts: on entering S/R alert zones, send TG with MV/PCR/VIX and action reason

import os, json, time, traceback
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta, time as dtime

# Third-party
try:
    import gspread
except Exception as e:
    print(f"[boot] gspread import failed: {e}", flush=True)
    gspread = None

try:
    from apscheduler.schedulers.background import BackgroundScheduler
except Exception as e:
    print(f"[boot] apscheduler import failed: {e}", flush=True)
    BackgroundScheduler = None

# Modules
from agents import logger
from agents.signal_generator import generate_signal_from_oc
from agents.signal_generator import classify_market_view, buffer_points  # for info/reasons

try:
    from agents.trade_executor import open_trade as open_paper_trade
except Exception:
    open_paper_trade = None

try:
    from analytics.oc_refresh import get_snapshot as _oc_get_snapshot
    OC_PLUGIN = _oc_get_snapshot
except Exception:
    OC_PLUGIN = None

try:
    from ops import tele_router
except Exception:
    tele_router = None

try:
    from ops import eod_perf
except Exception:
    eod_perf = None

try:
    from ops import params_override
except Exception:
    params_override = None

try:
    from ops import closer
except Exception:
    closer = None

try:
    from agents import circuit
except Exception:
    circuit = None

try:
    from ops import events_gate
except Exception:
    events_gate = None

try:
    from ops import near_alerts
except Exception:
    near_alerts = None

# ------------- Config -------------
@dataclass
class Config:
    tz: str
    sheet_id: str
    google_sa_json: str
    shift_mode: str
    worker_id: str
    auto_trade: str
    oc_mode: str
    symbol: str
    oc_min_interval_secs: int
    oc_refresh_secs: int
    oc_jitter_secs: int
    git_sha: str

def _getenv(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        return ""
    return v

def load_config() -> Config:
    return Config(
        tz=_getenv("TZ", "Asia/Kolkata"),
        sheet_id=_getenv("GSHEET_SPREADSHEET_ID", ""),
        google_sa_json=_getenv("GOOGLE_SA_JSON", ""),
        shift_mode=_getenv("SHIFT_MODE", "DAY"),
        worker_id=_getenv("WORKER_ID", "DAY_A"),
        auto_trade=_getenv("AUTO_TRADE", "on"),
        oc_mode=_getenv("OC_MODE", "dhan"),
        symbol=_getenv("OC_SYMBOL_PRIMARY", "NIFTY"),
        oc_min_interval_secs=int(_getenv("OC_MIN_INTERVAL_SECS", "18")),
        oc_refresh_secs=int(_getenv("OC_REFRESH_SECS", "15")),
        oc_jitter_secs=int(_getenv("OC_JITTER_SECS", "4")),
        git_sha=_getenv("GIT_SHA", _getenv("RENDER_GIT_COMMIT", ""))[:10],
    )

# ------------- Sheets wrapper -------------
class SheetsWrapper:
    def __init__(self, cfg: Config):
        if gspread is None:
            raise RuntimeError("gspread not available")
        try:
            info = json.loads(cfg.google_sa_json)
        except Exception:
            raise RuntimeError("Invalid GOOGLE_SA_JSON (must be one-line JSON)")
        gc = gspread.service_account_from_dict(info)
        self.ss = gc.open_by_key(cfg.sheet_id)

    def ensure_tab(self, title: str, headers: List[str]):
        try:
            ws = self.ss.worksheet(title)
        except Exception:
            ws = self.ss.add_worksheet(title=title, rows=10, cols=max(10, len(headers)))
            try:
                ws.append_row(headers)
            except Exception:
                pass
            return
        try:
            first = ws.row_values(1)
        except Exception:
            first = []
        if [h.strip() for h in first] != headers:
            try: ws.clear()
            except Exception: pass
            try: ws.append_row(headers)
            except Exception:
                end_col = chr(64 + len(headers))
                ws.update(f"A1:{end_col}1", [headers])

    def append_row(self, title: str, row: List[str]):
        ws = self.ss.worksheet(title)
        ws.append_row(row)

    def read_last_row(self, title: str) -> Optional[List[str]]:
        ws = self.ss.worksheet(title)
        rows = ws.get_all_values()
        return rows[-1] if rows and len(rows) >= 2 else None

# ------------- Helpers -------------
def get_oc_snapshot(cfg: Config, sheet: SheetsWrapper) -> Optional[Dict[str, Any]]:
    if OC_PLUGIN is not None:
        try:
            oc = OC_PLUGIN(cfg)
            if oc: return oc
        except Exception as e:
            print(f"[oc] plugin error: {e}", flush=True)
            traceback.print_exc()
    # fallback: last sheet row
    try:
        last = sheet.read_last_row("OC_Live")
        if not last or len(last) < 8: return None
        def f(v, d=None):
            try: return float(v)
            except: return d
        return {
            "ts": last[0],
            "symbol": last[1] or cfg.symbol,
            "spot": f(last[2], 0.0),
            "s1": f(last[3], None),
            "s2": f(last[4], None),
            "r1": f(last[5], None),
            "r2": f(last[6], None),
            "expiry": last[7],
            "signal": last[8] if len(last) > 8 else "",
            "ce_oi_pct": f(last[9], None) if len(last) > 9 else None,
            "pe_oi_pct": f(last[10], None) if len(last) > 10 else None,
            "volume_low": (str(last[11]).strip().lower() in ("1","true","yes","y")) if len(last) > 11 else None,
        }
    except Exception as e:
        print(f"[oc] sheet read failed: {e}", flush=True)
        return None

def _in_no_trade_window() -> bool:
    now = datetime.now().time()
    if dtime(9,15) <= now <= dtime(9,30): return True
    if dtime(14,45) <= now <= dtime(15,15): return True
    return False

def _today_trade_count(sheet) -> int:
    try:
        ws = sheet.ss.worksheet("Trades")
        rows = ws.get_all_values()
        if not rows or len(rows) < 2: return 0
        hdr = rows[0]
        ix_date = hdr.index("date") if "date" in hdr else None
        ix_event = hdr.index("event") if "event" in hdr else None
        today = datetime.now().date().isoformat()
        cnt = 0
        for r in rows[1:]:
            if ix_date is not None and len(r) > ix_date and r[ix_date] == today:
                ev = r[ix_event] if (ix_event is not None and len(r) > ix_event) else "OPEN"
                if ev == "OPEN":
                    cnt += 1
        return cnt
    except Exception:
        return 0

def dedup_exists(sheet: SheetsWrapper, dedup_key: str) -> bool:
    try:
        ws = sheet.ss.worksheet("Signals")
        rows = ws.get_all_values()
        if not rows or len(rows) < 2:
            return False
        headers = rows[0]
        try:
            idx = headers.index("signal_id")
        except ValueError:
            return False
        for r in reversed(rows[-500:]):
            if len(r) <= idx: continue
            if dedup_key and r[idx] == dedup_key:
                return True
        return False
    except Exception as e:
        print(f"[dedup] check failed: {e}", flush=True)
        return False

# ------------- Main -------------
def main():
    cfg = load_config()

    print("== Krishna Trade Worker v3 (OC Rules) ==")
    print(f"[boot] OC_MODE={cfg.oc_mode}")
    print(f"[boot] GSHEET_SPREADSHEET_ID={'set' if cfg.sheet_id else 'missing'}")
    print(f"[boot] TELEGRAM={'ready' if _getenv('TELEGRAM_BOT_TOKEN') else 'off'}")
    if _getenv("DHAN_CLIENT_ID"): print("[boot] DHAN_CLIENT_ID=set")
    if _getenv("DHAN_ACCESS_TOKEN"): print("[boot] DHAN_ACCESS_TOKEN=set")
    if _getenv("DHAN_USID_MAP"): print(f"[boot] DHAN_USID_MAP={_getenv('DHAN_USID_MAP')}")
    if cfg.git_sha: print(f"[boot] GIT_SHA={cfg.git_sha}")

    # Sheets
    sheet = SheetsWrapper(cfg)
    logger.ensure_all_headers(sheet, cfg)

    # Overrides
    if params_override is not None:
        try:
            applied = params_override.apply_overrides(sheet, cfg)
            if applied:
                print(f"[override] applied: {applied}", flush=True)
                logger.log_status(sheet, {
                    "worker_id": cfg.worker_id, "shift_mode": cfg.shift_mode,
                    "state": "OK", "message": f"params_override {len(applied)} applied"
                })
        except Exception as e:
            print(f"[override] failed: {e}", flush=True)

    # Telegram router
    if tele_router is not None:
        try:
            tele_router.start(sheet, cfg)
        except Exception as e:
            print(f"[boot] tele_router start failed: {e}", flush=True)

    # Scheduler
    if BackgroundScheduler is None:
        raise RuntimeError("apscheduler not available")
    sched = BackgroundScheduler(timezone=cfg.tz)

    # Heartbeat
    def heartbeat():
        try:
            logger.log_status(sheet, {
                "worker_id": cfg.worker_id, "shift_mode": cfg.shift_mode,
                "state": "OK", "message": f"hb {cfg.symbol}"
            })
        except Exception as e:
            print(f"❌ Job error [heartbeat] {e}", flush=True)

    sched.add_job(heartbeat, "interval", seconds=60, id="heartbeat")

    _last_oc_at = {"ts": 0.0}

    def oc_tick():
        try:
            now_ts = time.time()
            if now_ts - _last_oc_at["ts"] < cfg.oc_min_interval_secs:
                remain = round(cfg.oc_min_interval_secs - (now_ts - _last_oc_at["ts"]), 1)
                print(f"[oc] throttle {cfg.symbol}: wait {remain}s", flush=True)
                return

            oc = get_oc_snapshot(cfg, sheet)
            if not oc:
                print("[oc] no snapshot available", flush=True)
                return
            _last_oc_at["ts"] = now_ts

            spot, s1, r1 = oc.get("spot"), oc.get("s1"), oc.get("r1")
            print(f"[oc] {cfg.symbol} spot={spot} s1={s1} r1={r1}", flush=True)

            # Log OC to sheet (best-effort)
            try:
                logger.log_oc_live(sheet, {
                    "ts": oc.get("ts") or "",
                    "symbol": oc.get("symbol") or cfg.symbol,
                    "spot": oc.get("spot"), "s1": oc.get("s1"), "s2": oc.get("s2"),
                    "r1": oc.get("r1"), "r2": oc.get("r2"),
                    "expiry": oc.get("expiry") or "",
                    "signal": oc.get("signal") or "",
                    "ce_oi_pct": oc.get("ce_oi_pct") if "ce_oi_pct" in oc else "",
                    "pe_oi_pct": oc.get("pe_oi_pct") if "pe_oi_pct" in oc else "",
                    "volume_low": oc.get("volume_low") if "volume_low" in oc else "",
                })
            except Exception as e:
                print(f"[oc] log_oc_live failed: {e}", flush=True)

            # Auto close on condition change
            if closer is not None:
                try:
                    closer.condition_exit(sheet, cfg, oc)
                except Exception as e:
                    print(f"[closer] condition_exit failed: {e}", flush=True)

            # ---- Gate reasons (non-blocking list) ----
            reasons: List[str] = []
            hold_active = False
            if events_gate is not None:
                try:
                    hold_active, why = events_gate.is_hold_now(sheet)
                    if hold_active: reasons.append(f"HOLD:{why[:40]}")
                except Exception:
                    pass

            if _in_no_trade_window():
                reasons.append("no_trade_window")

            circ_paused = False
            if circuit is not None and circuit.should_pause():
                circ_paused = True
                reasons.append("circuit_pause")

            max_trades_str = os.getenv("MAX_TRADES_PER_DAY", "").strip()
            max_reached = False
            if max_trades_str.isdigit():
                try:
                    if _today_trade_count(sheet) >= int(max_trades_str):
                        max_reached = True
                        reasons.append(f"max_trades_day({max_trades_str})")
                except Exception:
                    pass

            auto_trade_off = (cfg.auto_trade.lower() != "on")
            if auto_trade_off:
                reasons.append("AUTO_TRADE=off")

            # ---- Try build signal (for decision + alert context)
            sig = generate_signal_from_oc({
                "symbol": oc.get("symbol") or cfg.symbol,
                "spot": oc.get("spot"),
                "s1": oc.get("s1"), "s2": oc.get("s2"),
                "r1": oc.get("r1"), "r2": oc.get("r2"),
                "ce_oi_pct": oc.get("ce_oi_pct"),
                "pe_oi_pct": oc.get("pe_oi_pct"),
                "volume_low": oc.get("volume_low"),
            })
            dedup_hit = False
            trade_taken = False
            trade_tag = None
            trade_side = None

            # If there is a candidate signal but reasons block trade, skip logging/OPEN
            can_trade = (sig is not None) and (len(reasons) == 0)

            if sig is not None:
                trade_tag  = sig.get("level_tag")
                trade_side = sig.get("side")

            if sig is not None and len(reasons) == 0:
                dkey = sig.get("dedup_key") or ""
                if dkey and dedup_exists(sheet, dkey):
                    dedup_hit = True
                    print(f"[signal] skip duplicate for {dkey}", flush=True)
                else:
                    # Log signal
                    logger.log_signal(sheet, {
                        "ts": "", "symbol": sig.get("symbol"),
                        "side": sig.get("side"), "price": "",
                        "reason": sig.get("reason"),
                        "level": sig.get("level"),
                        "sl": "", "tp": "", "rr": "",
                        "signal_id": dkey,
                    })
                    print(f"[signal] {sig.get('side')} {cfg.symbol} @ {sig.get('level_tag')} ({sig.get('reason')})", flush=True)
                    # OPEN (paper)
                    if open_paper_trade and cfg.auto_trade.lower() == "on":
                        try:
                            open_paper_trade(sheet, sig)
                            trade_taken = True
                        except Exception as e:
                            reasons.append("executor_error")
                            print(f"[trade] open_paper_trade failed: {e}", flush=True)

            # ---- NEAR alert (send once per level with cooldown)
            if near_alerts is not None:
                try:
                    near_alerts.check_and_alert(oc, {
                        "trade_taken": trade_taken,
                        "trade_side": trade_side,
                        "trade_tag": trade_tag,
                        "dedup_hit": dedup_hit,
                        "reasons": reasons
                    })
                except Exception as e:
                    print(f"[alert] near_alerts failed: {e}", flush=True)

        except Exception as e:
            print(f"❌ Job error [oc_tick] {e}", flush=True)
            traceback.print_exc()

    # OC tick job
    sched.add_job(oc_tick, "interval",
                  seconds=max(5, cfg.oc_refresh_secs),
                  id="oc_tick",
                  next_run_time=datetime.now() + timedelta(seconds=1))

    # Time-exit 15:15 IST
    def time_exit_job():
        if closer is None: return
        try:
            closer.time_exit_all(sheet, cfg)
        except Exception as e:
            print(f"❌ Job error [time_exit] {e}", flush=True)
    sched.add_job(time_exit_job, "cron", day_of_week="mon-fri", hour=15, minute=15, id="time_exit")

    # EOD jobs
    def eod_write():
        if eod_perf is None: return
        try:
            perf = eod_perf.write_eod(sheet, cfg, cfg.symbol)
            print(f"[eod] performance row written: {perf}", flush=True)
        except Exception as e:
            print(f"❌ Job error [eod_write] {e}", flush=True)
    sched.add_job(eod_write, "cron", day_of_week="mon-fri", hour=15, minute=31, id="eod_write")

    def eod_summary():
        if eod_perf is None: return
        try:
            eod_perf.send_daily_summary(None, cfg)  # module reads latest row
            print("[eod] daily summary sent", flush=True)
        except Exception as e:
            print(f"❌ Job error [eod_summary] {e}", flush=True)
    sched.add_job(eod_summary, "cron", day_of_week="mon-fri", hour=15, minute=35, id="eod_summary")

    sched.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[main] KeyboardInterrupt, exiting...", flush=True)
    finally:
        try: sched.shutdown(wait=False)
        except Exception: pass

if __name__ == "__main__":
    main()
