# krishna_main.py
# Krishna Trade Worker v3 — Main entry
# - Env-driven config (Day/Night)
# - OC snapshot via plugin (analytics.oc_refresh) or fallback: Sheet OC_Live
# - Signal generation via agents/signal_generator (oc_rules)
# - Per-level-per-day dedup (Signals.signal_id)
# - Heartbeat + OC tick schedulers (APScheduler)
# - Telegram long-polling router (/status, /oc_now)
# - EOD Performance row @ 15:31 IST + TG summary @ 15:35 IST (Mon–Fri)

import os, json, time, sys, traceback
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta

# ---- Third-party ----
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

# ---- Our modules ----
from agents import logger
from agents.signal_generator import generate_signal_from_oc

# Optional executor (paper trading)
try:
    from agents.trade_executor import open_trade as open_paper_trade  # signature: open_trade(sheet, signal_dict)
except Exception:
    open_paper_trade = None

# Optional OC plugin (preferred)
OC_PLUGIN = None
try:
    # Expect a callable get_snapshot(cfg) -> dict with spot,s1,s2,r1,r2,ce_oi_pct,pe_oi_pct,expiry, symbol
    from analytics.oc_refresh import get_snapshot as _oc_get_snapshot
    OC_PLUGIN = _oc_get_snapshot
except Exception:
    OC_PLUGIN = None

# Optional Telegram router
try:
    from ops import tele_router
except Exception:
    tele_router = None

# Optional EOD writer
try:
    from ops import eod_perf
except Exception:
    eod_perf = None

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

    # Our logger uses these two if present
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
        # ensure headers
        try:
            first = ws.row_values(1)
        except Exception:
            first = []
        if [h.strip() for h in first] != headers:
            try:
                ws.clear()
            except Exception:
                pass
            try:
                ws.append_row(headers)
            except Exception:
                end_col = chr(64 + len(headers))
                ws.update(f"A1:{end_col}1", [headers])

    def append_row(self, title: str, row: List[str]):
        ws = self.ss.worksheet(title)
        ws.append_row(row)

    # helpers
    def read_last_row(self, title: str) -> Optional[List[str]]:
        ws = self.ss.worksheet(title)
        rows = ws.get_all_values()
        return rows[-1] if rows and len(rows) >= 2 else None

    def read_col(self, title: str, col_index: int) -> List[str]:
        ws = self.ss.worksheet(title)
        col = ws.col_values(col_index)
        return col

# ------------- OC snapshot acquisition -------------
def _to_float(v, default=None):
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default

def oc_from_sheet_latest(sheet: SheetsWrapper, symbol: str) -> Optional[Dict[str, Any]]:
    """
    Expect OC_Live headers (minimum):
      ts, symbol, spot, s1, s2, r1, r2, expiry, signal
    Optional extra columns (if present):
      ce_oi_pct, pe_oi_pct, volume_low
    """
    try:
        last = sheet.read_last_row("OC_Live")
        if not last or len(last) < 8:
            return None

        # Indices per enforced header order:
        # 0 ts | 1 symbol | 2 spot | 3 s1 | 4 s2 | 5 r1 | 6 r2 | 7 expiry | 8 signal | 9 ce_oi_pct? | 10 pe_oi_pct? | 11 volume_low?
        ts      = last[0] if len(last) > 0 else ""
        sym     = last[1] if len(last) > 1 and last[1] else symbol
        spot    = _to_float(last[2], 0.0)
        s1      = _to_float(last[3], 0.0)
        s2      = _to_float(last[4], 0.0)
        r1      = _to_float(last[5], 0.0)
        r2      = _to_float(last[6], 0.0)
        expiry  = last[7] if len(last) > 7 else ""
        signal  = last[8] if len(last) > 8 else ""

        ce_oi_pct = _to_float(last[9])  if len(last) > 9  and last[9]  != "" else None
        pe_oi_pct = _to_float(last[10]) if len(last) > 10 and last[10] != "" else None
        # Accept "true/false/1/0"
        volume_low = None
        if len(last) > 11 and last[11] != "":
            v = str(last[11]).strip().lower()
            volume_low = (v in ("1", "true", "yes", "y"))

        return {
            "ts": ts, "symbol": sym,
            "spot": spot, "s1": s1, "s2": s2, "r1": r1, "r2": r2,
            "expiry": expiry, "signal": signal,
            "ce_oi_pct": ce_oi_pct, "pe_oi_pct": pe_oi_pct, "volume_low": volume_low,
        }
    except Exception as e:
        print(f"[oc] sheet read failed: {e}", flush=True)
        return None

def get_oc_snapshot(cfg: Config, sheet: SheetsWrapper) -> Optional[Dict[str, Any]]:
    # Preferred plugin (if provided by your repo)
    if OC_PLUGIN is not None:
        try:
            oc = OC_PLUGIN(cfg)  # must return dict
            if oc:
                return oc
        except Exception as e:
            print(f"[oc] plugin error: {e}", flush=True)
            traceback.print_exc()
    # Fallback to Sheet (works for Night mode or if plugin absent)
    return oc_from_sheet_latest(sheet, cfg.symbol)

# ------------- Dedup helpers -------------
def dedup_exists(sheet: SheetsWrapper, dedup_key: str) -> bool:
    """
    Look into Signals tab, last ~500 rows for today's signal_id == dedup_key.
    """
    try:
        ws = sheet.ss.worksheet("Signals")
        rows = ws.get_all_values()
        if not rows or len(rows) < 2:
            return False
        # Find header index for signal_id
        headers = rows[0]
        try:
            idx = headers.index("signal_id")
        except ValueError:
            return False
        # Scan last limited rows
        for r in reversed(rows[-500:]):
            if len(r) <= idx:
                continue
            if dedup_key and r[idx] == dedup_key:
                return True
        return False
    except Exception as e:
        print(f"[dedup] check failed: {e}", flush=True)
        return False

# ------------- Main logic -------------
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
    # Ensure all tabs/headers
    logger.ensure_all_headers(sheet, cfg)

    # Start Telegram router (daemon thread)
    if tele_router is not None:
        try:
            tele_router.start(sheet, cfg)
        except Exception as e:
            print(f"[boot] tele_router start failed: {e}", flush=True)
    else:
        print("[boot] tele_router missing; TG commands disabled", flush=True)

    # ---- Schedulers ----
    if BackgroundScheduler is None:
        raise RuntimeError("apscheduler not available")
    sched = BackgroundScheduler(timezone=cfg.tz)

    # Heartbeat: every 60s
    def heartbeat():
        try:
            logger.log_status(sheet, {
                "worker_id": cfg.worker_id,
                "shift_mode": cfg.shift_mode,
                "state": "OK",
                "message": f"hb {cfg.symbol}"
            })
        except Exception as e:
            print(f"❌ Job error [heartbeat] {e}", flush=True)

    sched.add_job(heartbeat, "interval", seconds=60, id="heartbeat")

    # OC tick: refresh snapshot + evaluate signal
    _last_oc_at = {"ts": 0.0}  # throttle

    def oc_tick():
        try:
            # Simple throttle (min interval)
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

            # Print summary
            spot = oc.get("spot")
            s1, r1 = oc.get("s1"), oc.get("r1")
            print(f"[oc] {cfg.symbol} spot={spot} s1={s1} r1={r1}", flush=True)

            # Log OC live to sheet (optional)
            try:
                logger.log_oc_live(sheet, {
                    "ts": oc.get("ts") or "",
                    "symbol": oc.get("symbol") or cfg.symbol,
                    "spot": spot, "s1": s1, "s2": oc.get("s2"),
                    "r1": r1, "r2": oc.get("r2"),
                    "expiry": oc.get("expiry") or "",
                    "signal": oc.get("signal") or "",
                })
            except Exception as e:
                print(f"[oc] log_oc_live failed: {e}", flush=True)

            # Evaluate OC strategy → signal
            sig = generate_signal_from_oc({
                "symbol": oc.get("symbol") or cfg.symbol,
                "spot": spot,
                "s1": s1, "s2": oc.get("s2"),
                "r1": r1, "r2": oc.get("r2"),
                "ce_oi_pct": oc.get("ce_oi_pct"),
                "pe_oi_pct": oc.get("pe_oi_pct"),
                "volume_low": oc.get("volume_low"),
            })
            if not sig:
                return

            # Dedup per-level-per-day
            dkey = sig.get("dedup_key")
            if dkey and dedup_exists(sheet, dkey):
                print(f"[signal] skip duplicate for {dkey}", flush=True)
                return

            # Log to Signals (append)
            logger.log_signal(sheet, {
                "ts": "",  # auto fill
                "symbol": sig.get("symbol"),
                "side": sig.get("side"),
                "price": "",          # executor fills on open
                "reason": sig.get("reason"),
                "level": sig.get("level"),
                "sl": "",             # executor computes from sl_pct if needed
                "tp": "",             # executor computes from target_pct if needed
                "rr": "",             # optional
                "signal_id": dkey or "",
            })
            print(f"[signal] {sig.get('side')} {cfg.symbol} @ {sig.get('level_tag')} ({sig.get('reason')})", flush=True)

            # Try open paper trade
            if open_paper_trade and cfg.auto_trade.lower() == "on":
                try:
                    open_paper_trade(sheet, sig)
                except Exception as e:
                    print(f"[trade] open_paper_trade failed: {e}", flush=True)

        except Exception as e:
            print(f"❌ Job error [oc_tick] {e}", flush=True)
            traceback.print_exc()

    # OC tick interval
    sched.add_job(
        oc_tick,
        "interval",
        seconds=max(5, cfg.oc_refresh_secs),
        id="oc_tick",
        next_run_time=datetime.now() + timedelta(seconds=1),
    )

    # ---- EOD jobs (Mon–Fri) ----
    def eod_write():
        if eod_perf is None:
            print("[eod] eod_perf module missing", flush=True)
            return
        try:
            perf = eod_perf.write_eod(sheet, cfg, cfg.symbol)
            print(f"[eod] performance row written: {perf}", flush=True)
        except Exception as e:
            print(f"❌ Job error [eod_write] {e}", flush=True)

    sched.add_job(
        eod_write,
        "cron",
        day_of_week="mon-fri",
        hour=15, minute=31,
        id="eod_write",
    )

    def eod_summary():
        if eod_perf is None:
            return
        try:
            # Read last Performance row; if not for today, compute once (append) then send
            ws = sheet.ss.worksheet("Performance")
            rows = ws.get_all_values()
            perf = None
            if rows and len(rows) >= 2:
                last = rows[-1]
                if last and len(last) >= 10:
                    today = datetime.now().date().isoformat()
                    if str(last[0]).strip() == today:
                        # headers: date,symbol,trades,wins,losses,win_rate,avg_pnl,gross_pnl,net_pnl,max_dd,version,notes
                        def _f(x): 
                            try: return float(x)
                            except: return 0.0
                        perf = {
                            "trades": _f(last[2]),
                            "wins": _f(last[3]),
                            "losses": _f(last[4]),
                            "win_rate": _f(last[5]),
                            "avg_pnl": _f(last[6]),
                            "gross_pnl": _f(last[7]),
                            "net_pnl": _f(last[8]),
                            "max_dd": _f(last[9]),
                        }
            if perf is None:
                # no row yet -> write now (single append) then use that
                perf = eod_perf.write_eod(sheet, cfg, cfg.symbol)
            eod_perf.send_daily_summary(perf, cfg)
            print("[eod] daily summary sent", flush=True)
        except Exception as e:
            print(f"❌ Job error [eod_summary] {e}", flush=True)

    sched.add_job(
        eod_summary,
        "cron",
        day_of_week="mon-fri",
        hour=15, minute=35,
        id="eod_summary",
    )

    # Start
    sched.start()

    # Main loop keepalive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[main] KeyboardInterrupt, exiting...", flush=True)
    finally:
        try:
            sched.shutdown(wait=False)
        except Exception:
            pass

if __name__ == "__main__":
    main()
