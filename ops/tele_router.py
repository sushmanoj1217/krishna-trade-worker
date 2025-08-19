# ops/tele_router.py
# Lightweight Telegram router (long-polling thread)
# - Auth via TELEGRAM_USER_ID (single or comma-separated)
# - Commands: /start /help /status /oc_now /open_trades /events_today /perf_today
#             /close_time_exit /eod_now
# - /oc_now reply shows buffered trigger levels (S* = S - band, R* = R + band),
#   6-condition market view tag, and PCR/VIX if provided.
# - Also nudges near-level alert check once on /oc_now.

from __future__ import annotations
import os, json, threading, time, traceback
from typing import Any, Dict, List, Optional
import urllib.parse, urllib.request

# --- Optional modules (best-effort) ---
try:
    from ops.oc_format import format_oc_reply
except Exception:
    format_oc_reply = None

try:
    from analytics.oc_refresh import get_snapshot as _oc_plugin_get
except Exception:
    _oc_plugin_get = None

try:
    from ops import near_alerts
except Exception:
    near_alerts = None

try:
    from ops import closer
except Exception:
    closer = None

try:
    from ops import eod_perf
except Exception:
    eod_perf = None


# ---------------- Telegram client ----------------
class _TG:
    def __init__(self, token: str):
        self.token = token
        self.base = f"https://api.telegram.org/bot{token}"

    def send(self, chat_id: str, text: str) -> bool:
        try:
            url = f"{self.base}/sendMessage"
            data = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode("utf-8")
            req = urllib.request.Request(url, data=data)
            with urllib.request.urlopen(req, timeout=15) as _:
                return True
        except Exception:
            return False

    def get_updates(self, offset: Optional[int], timeout: int = 50) -> Dict[str, Any]:
        try:
            params = {"timeout": str(timeout)}
            if offset is not None:
                params["offset"] = str(offset)
            url = f"{self.base}/getUpdates?{urllib.parse.urlencode(params)}"
            with urllib.request.urlopen(url, timeout=timeout + 10) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            return {"ok": False, "error": str(e)}


# ---------------- Helpers ----------------
def _allowed_user(chat_id: int) -> bool:
    raw = os.getenv("TELEGRAM_USER_ID", "").strip()
    if not raw:
        return True  # if unset, allow (useful in dev)
    allow = {x.strip() for x in raw.split(",") if x.strip()}
    return str(chat_id) in allow

def _fmt_num(x: Any) -> str:
    try:
        return f"{float(x):.2f}"
    except Exception:
        return str(x)

def _sheet_last_row(sheet, title: str) -> Optional[List[str]]:
    try:
        ws = sheet.ss.worksheet(title)
        rows = ws.get_all_values()
        return rows[-1] if rows and len(rows) >= 2 else None
    except Exception:
        return None

def _get_oc_snapshot(cfg, sheet) -> Optional[Dict[str, Any]]:
    # 1) Try plugin (Dhan OC) if present
    if _oc_plugin_get is not None:
        try:
            oc = _oc_plugin_get(cfg)
            if oc:
                return oc
        except Exception as e:
            print(f"[tele_router] oc plugin error: {e}", flush=True)
    # 2) Fallback to Sheet → OC_Live last row
    last = _sheet_last_row(sheet, "OC_Live")
    if not last or len(last) < 8:
        return None
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

def _send_help(tg: _TG, chat_id: str):
    tg.send(chat_id, (
        "Krishna AI bot commands:\n"
        "/start – hello & help\n"
        "/status – worker info\n"
        "/oc_now – show latest OC (with buffered S*/R*, MV, PCR/VIX)\n"
        "/open_trades – list open paper trades\n"
        "/events_today – active HOLD windows\n"
        "/perf_today – today performance row\n"
        "/close_time_exit – force time-exit now\n"
        "/eod_now – write EOD row & summary"
    ))

def _handle_status(tg: _TG, chat_id: str, cfg):
    msg = (
        "OK ✅\n"
        f"shift={cfg.shift_mode} worker={cfg.worker_id}\n"
        f"symbol={cfg.symbol}"
    )
    tg.send(chat_id, msg)

def _handle_oc_now(tg: _TG, chat_id: str, cfg, sheet):
    oc = _get_oc_snapshot(cfg, sheet)
    if not oc:
        tg.send(chat_id, "OC snapshot failed ❌")
        return
    # Nice formatted reply
    if format_oc_reply is not None:
        msg = format_oc_reply(oc)
    else:
        # minimal fallback
        msg = (
            "OC updated ✅\n"
            f"spot={_fmt_num(oc.get('spot'))}  S1={_fmt_num(oc.get('s1'))}  S2={_fmt_num(oc.get('s2'))}\n"
            f"R1={_fmt_num(oc.get('r1'))}  R2={_fmt_num(oc.get('r2'))}\n"
            f"expiry={oc.get('expiry','')}"
        )
    tg.send(chat_id, msg)

    # Also trigger NEAR/CROSS alert check once (non-blocking)
    try:
        if near_alerts is not None:
            near_alerts.check_and_alert(oc, {
                "trade_taken": False,
                "trade_side": "",
                "trade_tag": None,
                "dedup_hit": False,
                "reasons": ["manual oc_now"]
            })
    except Exception as e:
        print(f"[tele_router] near_alerts check failed: {e}", flush=True)

def _handle_open_trades(tg: _TG, chat_id: str, sheet):
    try:
        ws = sheet.ss.worksheet("Trades")
        rows = ws.get_all_values()
        if not rows or len(rows) < 2:
            tg.send(chat_id, "No open trades.")
            return
        hdr = rows[0]
        ix_close = hdr.index("ts_close") if "ts_close" in hdr else None
        open_rows = []
        for r in rows[1:]:
            if ix_close is None or ix_close >= len(r) or str(r[ix_close]).strip() == "":
                open_rows.append(r)
        if not open_rows:
            tg.send(chat_id, "No open trades.")
            return
        # compact listing
        out = ["Open trades:"]
        def get(h, row, default=""):
            try:
                i = hdr.index(h); return row[i] if i < len(row) else default
            except ValueError:
                return default
        for r in open_rows[-10:]:
            out.append(f"{get('trade_id',r)} {get('symbol',r)} {get('side',r)} qty={get('qty',r)} @ {get('ts_open',r)}")
        tg.send(chat_id, "\n".join(out))
    except Exception as e:
        tg.send(chat_id, f"open_trades failed: {e}")

def _handle_events_today(tg: _TG, chat_id: str, sheet):
    try:
        ws = sheet.ss.worksheet("Events")
        rows = ws.get_all_values()
        if not rows or len(rows) < 2:
            tg.send(chat_id, "No events.")
            return
        hdr = rows[0]
        today = time.strftime("%Y-%m-%d")
        out = ["Events today:"]
        for r in rows[1:]:
            d = r[hdr.index("date")] if "date" in hdr and len(r) > hdr.index("date") else ""
            active = (str(r[hdr.index("active")] if "active" in hdr else "").strip().lower() in ("1","true","yes","y"))
            window = r[hdr.index("window")] if "window" in hdr and len(r) > hdr.index("window") else ""
            typ = r[hdr.index("type")] if "type" in hdr and len(r) > hdr.index("type") else ""
            note = r[hdr.index("note")] if "note" in hdr and len(r) > hdr.index("note") else ""
            if d == today:
                out.append(f"{'ON' if active else 'off'} {typ} {window} {note}".strip())
        tg.send(chat_id, "\n".join(out) if len(out) > 1 else "No events today.")
    except Exception as e:
        tg.send(chat_id, f"events_today failed: {e}")

def _handle_perf_today(tg: _TG, chat_id: str, sheet):
    try:
        ws = sheet.ss.worksheet("Performance")
        rows = ws.get_all_values()
        if not rows or len(rows) < 2:
            tg.send(chat_id, "No performance rows.")
            return
        hdr = rows[0]
        today = time.strftime("%Y-%m-%d")
        last = None
        for r in reversed(rows[1:]):
            if "date" in hdr:
                try:
                    if r[hdr.index("date")] == today:
                        last = r; break
                except Exception:
                    pass
        if not last:
            tg.send(chat_id, "No perf row for today.")
            return
        def get(h, row, default=""):
            try:
                i = hdr.index(h); return row[i] if i < len(row) else default
            except ValueError:
                return default
        msg = (
            f"Perf {today} ✅\n"
            f"trades={get('trades', last)}  win_rate={get('win_rate', last)}  "
            f"avg_pnl={get('avg_pnl', last)}  net_pnl={get('net_pnl', last)}"
        )
        tg.send(chat_id, msg)
    except Exception as e:
        tg.send(chat_id, f"perf_today failed: {e}")

def _handle_close_time_exit(tg: _TG, chat_id: str, sheet, cfg):
    if closer is None:
        tg.send(chat_id, "Closer not available.")
        return
    try:
        closer.time_exit_all(sheet, cfg)
        tg.send(chat_id, "Time-exit executed ✅")
    except Exception as e:
        tg.send(chat_id, f"time_exit failed: {e}")

def _handle_eod_now(tg: _TG, chat_id: str, sheet, cfg):
    if eod_perf is None:
        tg.send(chat_id, "EOD module not available.")
        return
    try:
        perf = eod_perf.write_eod(sheet, cfg, cfg.symbol)
        eod_perf.send_daily_summary(None, cfg)
        tg.send(chat_id, f"EOD done ✅ {perf}")
    except Exception as e:
        tg.send(chat_id, f"eod_now failed: {e}")


# ---------------- Poller ----------------
def _poll_loop(tg: _TG, sheet, cfg):
    offset: Optional[int] = None
    print("[tele_router] started polling", flush=True)

    while True:
        try:
            res = tg.get_updates(offset, timeout=50)
            if not res.get("ok"):
                time.sleep(2)
                continue
            for upd in res.get("result", []):
                offset = upd["update_id"] + 1
                msg = upd.get("message") or upd.get("edited_message")
                if not msg: continue
                chat_id = msg["chat"]["id"]
                if not _allowed_user(chat_id):
                    tg.send(chat_id, "Unauthorized.")
                    continue
                text = (msg.get("text") or "").strip()

                if text.startswith("/start") or text.startswith("/help"):
                    _send_help(tg, chat_id)
                elif text.startswith("/status"):
                    _handle_status(tg, chat_id, cfg)
                elif text.startswith("/oc_now"):
                    _handle_oc_now(tg, chat_id, cfg, sheet)
                elif text.startswith("/open_trades"):
                    _handle_open_trades(tg, chat_id, sheet)
                elif text.startswith("/events_today"):
                    _handle_events_today(tg, chat_id, sheet)
                elif text.startswith("/perf_today"):
                    _handle_perf_today(tg, chat_id, sheet)
                elif text.startswith("/close_time_exit"):
                    _handle_close_time_exit(tg, chat_id, sheet, cfg)
                elif text.startswith("/eod_now"):
                    _handle_eod_now(tg, chat_id, sheet, cfg)
                else:
                    _send_help(tg, chat_id)

        except Exception as e:
            print(f"[tele_router] poll error: {e}", flush=True)
            traceback.print_exc()
            time.sleep(2)


# ---------------- Public API ----------------
def start(sheet, cfg) -> None:
    """Start polling router in a background thread."""
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        print("[tele_router] TELEGRAM_BOT_TOKEN missing; router off", flush=True)
        return
    tgc = _TG(token)
    th = threading.Thread(target=_poll_loop, args=(tgc, sheet, cfg), daemon=True)
    th.start()
