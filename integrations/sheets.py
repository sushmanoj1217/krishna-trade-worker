# integrations/sheets.py
from __future__ import annotations
import os, json, time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from utils.logger import log

# Optional Google Sheets
_USE_MEMORY = False
_UGS_READY = False
_GS = None
_WB = None

IST = ZoneInfo("Asia/Kolkata")
TABS = ["OC_Live","Signals","Trades","Performance","Events","Status","Snapshots","Params_Override"]

# In-memory store
_DB = {t: [] for t in TABS}

def _connect_real():
    global _USE_MEMORY, _UGS_READY, _GS, _WB
    sa = os.getenv("GOOGLE_SA_JSON", "").strip()
    sid = os.getenv("GSHEET_TRADES_SPREADSHEET_ID", "").strip()
    if not sa or not sid:
        _USE_MEMORY = True
        return
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        info = json.loads(sa)
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        _GS = gspread.authorize(creds)
        _WB = _GS.open_by_key(sid)
        _UGS_READY = True
        _USE_MEMORY = False
    except Exception as e:
        log.warning(f"Google Sheets connect failed → memory mode: {e}")
        _USE_MEMORY = True

def _get_ws(name: str):
    assert name in TABS
    try:
        return _WB.worksheet(name)
    except Exception:
        try:
            return _WB.add_worksheet(title=name, rows=1_000, cols=30)
        except Exception as e:
            log.warning(f"create worksheet {name} failed: {e}")
            return None

def ensure_tabs():
    _connect_real()
    if _USE_MEMORY:
        log.info("Sheets OK for trading bot")
        return
    for t in TABS:
        _get_ws(t)
    log.info("Sheets OK for trading bot")

def now_str():
    return datetime.now(tz=IST).strftime("%Y-%m-%d %H:%M:%S")

# ----------------- Basic IO -----------------
def append_row(tab: str, row: list):
    if tab not in TABS: 
        return
    if _USE_MEMORY:
        _DB[tab].append(row); return
    ws = _get_ws(tab)
    if not ws: 
        _DB[tab].append(row); return
    try:
        ws.append_row(row, value_input_option="USER_ENTERED")
    except Exception as e:
        log.warning(f"append_row({tab}) failed → memory mirror: {e}")
        _DB[tab].append(row)

def last_row(tab: str) -> dict | None:
    rows = []
    if _USE_MEMORY:
        rows = _DB.get(tab) or []
    else:
        ws = _get_ws(tab)
        if ws:
            try:
                vals = ws.get_all_values()
                rows = vals if vals else []
            except Exception as e:
                log.warning(f"last_row({tab}) fetch failed: {e}")
                rows = _DB.get(tab) or []
        else:
            rows = _DB.get(tab) or []
    if not rows:
        return None
    if tab == "OC_Live":
        r = rows[-1]
        keys = ["timestamp","spot","s1","s2","r1","r2","expiry","signal","vix","pcr","pcr_bucket","max_pain","max_pain_dist","bias_tag","stale"]
        return {k: (r[i] if i < len(r) else None) for i,k in enumerate(keys)}
    return None

def get_oc_live_history(days=60) -> list[dict]:
    rows = []
    if _USE_MEMORY:
        rows = _DB.get("OC_Live") or []
    else:
        try:
            rows = _get_ws("OC_Live").get_all_values()
        except Exception:
            rows = _DB.get("OC_Live") or []
    out = []
    for r in rows[-days*50:] if rows else []:
        keys = ["timestamp","spot","s1","s2","r1","r2","expiry","signal","vix","pcr","pcr_bucket","max_pain","max_pain_dist","bias_tag","stale"]
        out.append({k: (r[i] if i < len(r) else None) for i,k in enumerate(keys)})
    return out

# ----------------- Signals (for logging only) -----------------
def log_signal_row(row: list):
    append_row("Signals", row)

def get_last_event_rows(n=5):
    rows = _DB.get("Events") or []
    return rows[-n:]

# ----------------- Trades -----------------
def get_open_trades() -> list[dict]:
    rows = []
    if _USE_MEMORY:
        rows = _DB.get("Trades") or []
    else:
        try:
            rows = _get_ws("Trades").get_all_values()
        except Exception:
            rows = _DB.get("Trades") or []
    out = []
    for r in rows:
        if len(r) < 11: 
            continue
        exit_time = r[10]
        if exit_time in ("", None):
            out.append({
                "trade_id": r[0], "signal_id": r[1], "symbol": r[2], "side": r[3],
                "buy_ltp": float(r[4]) if r[4] else 0.0,
                "exit_ltp": float(r[5]) if r[5] else 0.0,
                "sl": float(r[6]) if r[6] else 0.0,
                "tp": float(r[7]) if r[7] else 0.0,
                "basis": r[8] if len(r) > 8 else "",
            })
    return out

def close_trade(tid: str, exit_ltp: float, result: str, pnl: float, note: str = ""):
    if _USE_MEMORY:
        rows = _DB.get("Trades") or []
        for i,r in enumerate(rows):
            if r[0] == tid and r[10] in ("", None):
                rows[i][5] = exit_ltp
                rows[i][10] = now_str()
                rows[i][11] = result
                rows[i][12] = pnl
                rows[i][13] = ""  # dedupe_hash
                _DB["Trades"] = rows
                append_row("Status", [now_str(), "trade_closed", tid, result, pnl, note])
                break
        return
    # Real sheet naive update: append a status row (simpler than row mutation)
    append_row("Status", [now_str(), "trade_closed", tid, result, pnl, note])

def update_trade_sl(tid: str, new_sl: float):
    append_row("Status", [now_str(), "trail_sl", tid, new_sl])

def count_today_trades() -> int:
    rows = _DB.get("Trades") or []
    today = datetime.now(tz=IST).date().isoformat()
    cnt = 0
    for r in rows:
        if len(r) >= 10 and str(r[9]).startswith(today):
            cnt += 1
    return cnt

def get_recent_trades(n=50) -> list[dict]:
    rows = _DB.get("Trades") or []
    out = []
    for r in rows[-n:]:
        out.append({"trade_id": r[0], "result": r[11] if len(r)>11 else "", "pnl": float(r[12]) if len(r)>12 and r[12] else 0.0})
    return out

# ----------------- Overrides & Performance -----------------
def get_overrides_map() -> dict[str, str]:
    rows = []
    if _USE_MEMORY:
        rows = _DB.get("Params_Override") or []
    else:
        try:
            rows = _get_ws("Params_Override").get_all_values()
        except Exception:
            rows = _DB.get("Params_Override") or []
    m = {}
    for r in rows:
        if not r: continue
        k = (r[0] or "").strip()
        v = (r[1] if len(r) > 1 else "").strip()
        if k: m[k] = v
    return m

def get_override_int(key: str, default: int) -> int:
    try:
        v = get_overrides_map().get(key)
        return int(float(v)) if v is not None and str(v).strip() != "" else default
    except Exception:
        return default

def upsert_override(key: str, value: str):
    if _USE_MEMORY:
        m = get_overrides_map()
        m[key] = value
        # naive replace
        _DB["Params_Override"] = [[k, v, now_str()] for k, v in m.items()]
        return
    append_row("Params_Override", [key, value, now_str()])

def update_performance(metrics: dict):
    append_row("Performance", [
        now_str(), metrics.get("win_rate"), metrics.get("avg_pl"),
        metrics.get("drawdown"), metrics.get("version")
    ])
