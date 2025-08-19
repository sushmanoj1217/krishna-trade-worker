import os, json, time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from utils.logger import log

# Minimal sheet abstraction with in-memory fallback when GOOGLE_SA_JSON missing.
# Replace with gspread/pygsheets in production if needed.

IST = ZoneInfo("Asia/Kolkata")

# In-memory "sheets"
_DB = {
    "OC_Live": [],
    "Signals": [],
    "Trades": [],
    "Performance": [],
    "Events": [],
    "Status": [],
    "Snapshots": [],
    "Params_Override": [],
}

def _use_memory():
    return not bool(os.getenv("GOOGLE_SA_JSON"))

def ensure_tabs():
    # In-memory ensures dict keys. Real sheets would verify tabs/headers.
    if _use_memory():
        log.info("Sheets OK for trading bot")
        return
    # If using real Google Sheets, this is where you'd connect and ensure tabs.
    log.info("Sheets OK for trading bot")

def now_str():
    return datetime.now(tz=IST).strftime("%Y-%m-%d %H:%M:%S")

# ------------- Generic helpers -------------
def append_row(tab: str, row: list):
    if _use_memory():
        _DB.setdefault(tab, []).append(row)
    else:
        # TODO: real sheets write
        _DB.setdefault(tab, []).append(row)

def last_row(tab: str) -> dict | None:
    rows = _DB.get(tab) or []
    if not rows:
        return None
    head = rows[0] if rows and isinstance(rows[0], list) and not isinstance(rows[0][0], (int,float)) else []
    # For memory mode, map simple columns by position for known tabs
    if tab == "OC_Live":
        # timestamp, spot, s1, s2, r1, r2, expiry, signal, vix, pcr, pcr_bucket, max_pain, max_pain_dist, bias_tag, stale
        r = rows[-1]
        keys = ["timestamp","spot","s1","s2","r1","r2","expiry","signal","vix","pcr","pcr_bucket","max_pain","max_pain_dist","bias_tag","stale"]
        return {k: (r[i] if i < len(r) else None) for i,k in enumerate(keys)}
    return None

# ------------- OC Live history -------------
def get_oc_live_history(days=60) -> list[dict]:
    rows = _DB.get("OC_Live") or []
    out = []
    for r in rows[-days*50:]:
        keys = ["timestamp","spot","s1","s2","r1","r2","expiry","signal","vix","pcr","pcr_bucket","max_pain","max_pain_dist","bias_tag","stale"]
        out.append({k: (r[i] if i < len(r) else None) for i,k in enumerate(keys)})
    return out

# ------------- Signals -------------
def last_signal() -> dict | None:
    rows = _DB.get("Signals") or []
    if not rows:
        return None
    r = rows[-1]
    # signal_id, ts, side, trigger, entry, sl, tp, eligible, placed
    if len(r) < 8:
        return None
    # Compatibility with earlier writes
    try:
        return {
            "id": r[0], "ts": r[1], "side": r[2], "trigger": r[3],
            "entry": r[3 if isinstance(r[3], (int,float)) else 3] if False else r[3],  # safe
            "eligible": r[10] if len(r) > 10 else True,
            "sl": r[5] if len(r) > 5 else None,
            "tp": r[6] if len(r) > 6 else None,
            "placed": r[15] if len(r) > 15 else "0",
        }
    except Exception:
        return None

def mark_signal_placed(signal_id: str):
    rows = _DB.get("Signals") or []
    if not rows:
        return
    # memory mode: append status
    _DB.setdefault("Status", []).append([now_str(), "signal_placed", signal_id])

# ------------- Trades -------------
def get_open_trades() -> list[dict]:
    rows = _DB.get("Trades") or []
    out = []
    for r in rows:
        if len(r) < 11: 
            continue
        exit_time = r[10]
        if exit_time in ("", None):
            out.append({
                "trade_id": r[0], "signal_id": r[1], "symbol": r[2], "side": r[3],
                "buy_ltp": r[4], "exit_ltp": r[5] or "", "sl": r[6], "tp": r[7], "basis": r[8]
            })
    return out

def close_trade(tid: str, exit_ltp: float, result: str, pnl: float, note: str = ""):
    rows = _DB.get("Trades") or []
    for i,r in enumerate(rows):
        if r[0] == tid and r[10] in ("", None):
            rows[i][5] = exit_ltp
            rows[i][10] = now_str()
            rows[i][11] = result
            rows[i][12] = pnl
            rows[i][13] = ""  # dedupe_hash
            _DB["Trades"] = rows
            _DB.setdefault("Status", []).append([now_str(), "trade_closed", tid, result, pnl, note])
            break

def update_trade_sl(tid: str, new_sl: float):
    rows = _DB.get("Trades") or []
    for i,r in enumerate(rows):
        if r[0] == tid and r[10] in ("", None):
            rows[i][6] = new_sl
            _DB["Trades"] = rows
            break

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
        out.append({
            "trade_id": r[0], "result": r[11], "pnl": r[12]
        })
    return out

# ------------- Params Override -------------
def get_override_int(key: str, default: int) -> int:
    rows = _DB.get("Params_Override") or []
    for r in rows[::-1]:
        if r and r[0] == key:
            try: return int(float(r[1]))
            except: return default
    return default

def upsert_override(key: str, value: str):
    rows = _DB.get("Params_Override") or []
    for r in rows:
        if r and r[0] == key:
            r[1] = value
            break
    else:
        rows.append([key, value, now_str()])
    _DB["Params_Override"] = rows

# ------------- Performance -------------
def update_performance(metrics: dict):
    _DB.setdefault("Performance", []).append([
        now_str(), metrics.get("win_rate"), metrics.get("avg_pl"),
        metrics.get("drawdown"), metrics.get("version")
    ])
