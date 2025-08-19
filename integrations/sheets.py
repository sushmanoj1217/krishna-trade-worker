# integrations/sheets.py
from __future__ import annotations
import os, json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Optional
from utils.logger import log

# Optional Google Sheets (real I/O) with in-memory fallback
_USE_MEMORY = False
_GS = None
_WB = None

IST = ZoneInfo("Asia/Kolkata")
TABS = ["OC_Live","Signals","Trades","Performance","Events","Status","Snapshots","Params_Override"]

# In-memory store
_DB: Dict[str, List[List[Any]]] = {t: [] for t in TABS}

def _connect_real():
    """Try connect to Google Sheets; if missing/broken → memory mode."""
    global _USE_MEMORY, _GS, _WB
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
            return _WB.add_worksheet(title=name, rows=2000, cols=40)
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

# ----------------- Generic IO -----------------
def append_row(tab: str, row: list):
    if tab not in TABS: 
        return
    if _USE_MEMORY:
        _DB[tab].append(row); 
        return
    ws = _get_ws(tab)
    if not ws:
        _DB[tab].append(row); 
        return
    try:
        ws.append_row(row, value_input_option="USER_ENTERED")
    except Exception as e:
        log.warning(f"append_row({tab}) failed → memory mirror: {e}")
        _DB[tab].append(row)

def get_all_values(tab: str) -> List[List[str]]:
    if _USE_MEMORY:
        return _DB.get(tab, [])
    ws = _get_ws(tab)
    if not ws:
        return _DB.get(tab, [])
    try:
        return ws.get_all_values()
    except Exception as e:
        log.warning(f"get_all_values({tab}) failed: {e}")
        return _DB.get(tab, [])

def last_row(tab: str) -> Optional[Dict[str, Any]]:
    rows = get_all_values(tab)
    if not rows:
        return None
    if tab == "OC_Live":
        r = rows[-1]
        keys = ["timestamp","spot","s1","s2","r1","r2","expiry","signal","vix","pcr","pcr_bucket","max_pain","max_pain_dist","bias_tag","stale"]
        return {k: (r[i] if i < len(r) else None) for i,k in enumerate(keys)}
    if tab == "Signals":
        r = rows[-1]
        # signal_id, ts, side, trigger, c1..c6, eligible, reason, mv_ok, mv_basis, oc_ok, oc_basis, nearfar, notes
        keys = ["signal_id","ts","side","trigger","c1","c2","c3","c4","c5","c6","eligible","reason","mv_ok","mv_basis","oc_ok","oc_basis","nearfar","notes"]
        return {k: (r[i] if i < len(r) else None) for i,k in enumerate(keys)}
    return None

# ----------------- OC_Live helpers -----------------
def get_oc_live_history(days=60) -> List[Dict[str, Any]]:
    rows = get_all_values("OC_Live")
    out = []
    for r in rows[-days*50:] if rows else []:
        keys = ["timestamp","spot","s1","s2","r1","r2","expiry","signal","vix","pcr","pcr_bucket","max_pain","max_pain_dist","bias_tag","stale"]
        out.append({k: (r[i] if i < len(r) else None) for i,k in enumerate(keys)})
    return out

def _parse_ts(ts: str) -> Optional[datetime]:
    if not ts: return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S %Z"):
        try:
            dt = datetime.strptime(ts, fmt)
            return dt.replace(tzinfo=IST)
        except Exception:
            continue
    return None

def get_oc_live_last_minutes(minutes: int = 5) -> List[Dict[str, Any]]:
    """Return OC_Live rows within last `minutes` minutes (approx by timestamps)."""
    rows = get_all_values("OC_Live")
    if not rows:
        return []
    cutoff = datetime.now(tz=IST) - timedelta(minutes=minutes)
    out = []
    for r in reversed(rows):
        ts = _parse_ts(r[0] if len(r) > 0 else "")
        if not ts: break
        if ts >= cutoff:
            keys = ["timestamp","spot","s1","s2","r1","r2","expiry","signal","vix","pcr","pcr_bucket","max_pain","max_pain_dist","bias_tag","stale"]
            out.append({k: (r[i] if i < len(r) else None) for i,k in enumerate(keys)})
        else:
            break
    return list(reversed(out))

# ----------------- Signals -----------------
def log_signal_row(row: list):
    append_row("Signals", row)

# ----------------- Trades & performance -----------------
def get_open_trades() -> List[Dict[str, Any]]:
    rows = get_all_values("Trades")
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

def get_open_trades_count() -> int:
    return len(get_open_trades())

def _find_trade_row_index(tid: str) -> Optional[int]:
    if _USE_MEMORY:
        rows = _DB.get("Trades") or []
        for i, r in enumerate(rows, start=1):
            if r and r[0] == tid:
                return i
        return None
    try:
        ws = _get_ws("Trades")
        cells = ws.findall(tid)
        for c in cells:
            if c.col == 1:  # trade_id in col A
                return c.row
    except Exception:
        pass
    return None

def close_trade(tid: str, exit_ltp: float, result: str, pnl: float, note: str = ""):
    """Update the trade row in-place if possible; else append a Status row."""
    if _USE_MEMORY:
        rows = _DB.get("Trades") or []
        for i,r in enumerate(rows):
            if r[0] == tid and (len(r) < 11 or r[10] in ("", None)):
                # [.., exit_ltp(5), exit_time(10), result(11), pnl(12), dedupe(13)]
                r[5] = exit_ltp
                while len(r) < 14: r.append("")
                r[10] = now_str()
                r[11] = result
                r[12] = pnl
                r[13] = ""
                rows[i] = r
                _DB["Trades"] = rows
                append_row("Status", [now_str(), "trade_closed", tid, result, pnl, note])
                return
        append_row("Status", [now_str(), "trade_closed_missing", tid, result, pnl, note])
        return
    # Real sheet path
    try:
        ws = _get_ws("Trades")
        idx = _find_trade_row_index(tid)
        if idx:
            ws.update(f"F{idx}:N{idx}", [[exit_ltp, "", "", now_str(), result, pnl, "", note]])
        else:
            append_row("Status", [now_str(), "trade_closed_missing", tid, result, pnl, note])
    except Exception as e:
        log.warning(f"close_trade update failed: {e}")
        append_row("Status", [now_str(), "trade_closed_fallback", tid, result, pnl, note])

def update_trade_sl(tid: str, new_sl: float):
    if _USE_MEMORY:
        rows = _DB.get("Trades") or []
        for i,r in enumerate(rows):
            if r[0] == tid and (len(r) < 11 or r[10] in ("", None)):
                r[6] = new_sl; rows[i] = r; _DB["Trades"] = rows; break
        append_row("Status", [now_str(), "trail_sl", tid, new_sl])
        return
    try:
        ws = _get_ws("Trades")
        idx = _find_trade_row_index(tid)
        if idx:
            ws.update_acell(f"G{idx}", new_sl)
        append_row("Status", [now_str(), "trail_sl", tid, new_sl])
    except Exception as e:
        log.warning(f"update_trade_sl failed: {e}")
        append_row("Status", [now_str(), "trail_sl_fail", tid, new_sl])

def count_today_trades() -> int:
    rows = get_all_values("Trades")
    today = datetime.now(tz=IST).date().isoformat()
    cnt = 0
    for r in rows:
        if len(r) >= 10 and str(r[9]).startswith(today):
            cnt += 1
    return cnt

def get_recent_trades(n=50) -> List[Dict[str, Any]]:
    rows = get_all_values("Trades")
    out = []
    for r in rows[-n:]:
        out.append({
            "trade_id": r[0] if len(r)>0 else "",
            "result": r[11] if len(r)>11 else "",
            "pnl": float(r[12]) if len(r)>12 and r[12] else 0.0
        })
    return out

def update_performance(metrics: Dict[str, Any]):
    append_row("Performance", [
        now_str(), metrics.get("win_rate"), metrics.get("avg_pl"),
        metrics.get("drawdown"), metrics.get("version")
    ])

# ----------------- Overrides -----------------
def get_overrides_map() -> Dict[str, str]:
    rows = get_all_values("Params_Override")
    m: Dict[str,str] = {}
    for r in rows:
        if not r: continue
        k = (r[0] or "").strip()
        v = (r[1] if len(r) > 1 else "").strip()
        if k: m[k] = v
    return m

def upsert_override(key: str, value: str):
    """Naive upsert: in memory replace; in real mode append a new row."""
    if _USE_MEMORY:
        m = get_overrides_map()
        m[key] = value
        _DB["Params_Override"] = [[k, v, now_str()] for k, v in m.items()]
        return
    append_row("Params_Override", [key, value, now_str()])

# ----------------- Events -----------------
def get_last_event_rows(n=5):
    rows = get_all_values("Events")
    return rows[-n:] if rows else []
