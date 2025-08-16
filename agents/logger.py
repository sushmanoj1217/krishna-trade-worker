# agents/logger.py
from __future__ import annotations
import json, time
from datetime import datetime
from typing import List, Dict, Any

# ---- Headers (must stay in this order) ----
H_OC_LIVE = ["ts","symbol","spot","s1","s2","r1","r2","expiry","signal","ce_oi_pct","pe_oi_pct","volume_low"]
H_SIGNALS = ["ts","symbol","side","price","reason","level","sl","tp","rr","signal_id"]
H_TRADES  = ["ts_open","symbol","side","qty","entry","sl","tp","ts_close","exit_price","pnl","reason_open","reason_close","trade_id"]
H_PERF    = ["date","symbol","trades","wins","losses","win_rate","avg_pnl","gross_pnl","net_pnl","max_dd","version","notes"]
H_EVENTS  = ["date","type","window","note","active"]
H_STATUS  = ["ts","worker_id","shift_mode","state","message"]
H_SNAP    = ["ts","key","value","blob"]
H_POVR    = ["key","value","source","status"]

def _now():
    return datetime.now().replace(microsecond=0).isoformat(sep=" ")

def _ensure(ws, headers: List[str]):
    try:
        first = ws.row_values(1)
    except Exception:
        first = []
    if [h.strip() for h in first] != headers:
        ws.clear()
        try:
            ws.append_row(headers)
        except Exception:
            end = chr(64 + len(headers))
            ws.update(f"A1:{end}1", [headers])

def ensure_all_headers(sheet, cfg=None):
    ss = sheet.ss
    for title, hdr in [
        ("OC_Live", H_OC_LIVE),
        ("Signals", H_SIGNALS),
        ("Trades", H_TRADES),
        ("Performance", H_PERF),
        ("Events", H_EVENTS),
        ("Status", H_STATUS),
        ("Snapshots", H_SNAP),
        ("Params_Override", H_POVR),
    ]:
        try:
            ws = ss.worksheet(title)
        except Exception:
            ws = ss.add_worksheet(title=title, rows=10, cols=max(10, len(hdr)))
        _ensure(ws, hdr)

def _append(ws, headers: List[str], row_map: Dict[str, Any]):
    row = []
    for h in headers:
        v = row_map.get(h, "")
        if h == "ts" or h == "ts_open" or h == "ts_close":
            if not v: v = _now()
        row.append(v)
    ws.append_row(row)

def _update_row(ws, headers: List[str], match_idx: int, match_val: str, updates: Dict[str, Any]) -> bool:
    rows = ws.get_all_values()
    if not rows or len(rows) < 2: return False
    # find header map
    hdrs = rows[0]
    idxs = {name: i for i, name in enumerate(hdrs)}
    for r_i in range(1, len(rows)):
        r = rows[r_i]
        if len(r) <= match_idx: continue
        if r[match_idx] != match_val: continue
        # patch row in memory
        cur = r + [""] * max(0, len(hdrs) - len(r))
        for k, v in updates.items():
            if k in idxs:
                cur[idxs[k]] = v
        # write back (A{row}:..)
        end_col = chr(64 + len(hdrs))
        ws.update(f"A{r_i+1}:{end_col}{r_i+1}", [cur])
        return True
    return False

# ---- Public append helpers ----
def log_oc_live(sheet, m: Dict[str, Any]):      _append(sheet.ss.worksheet("OC_Live"), H_OC_LIVE, m)
def log_signal(sheet, m: Dict[str, Any]):       _append(sheet.ss.worksheet("Signals"), H_SIGNALS, m)
def log_trade(sheet, m: Dict[str, Any]):        _append(sheet.ss.worksheet("Trades"), H_TRADES, m)
def log_performance(sheet, m: Dict[str, Any]):  _append(sheet.ss.worksheet("Performance"), H_PERF, m)
def log_event(sheet, m: Dict[str, Any]):        _append(sheet.ss.worksheet("Events"), H_EVENTS, m)

def log_status(sheet, m: Dict[str, Any]):
    _append(sheet.ss.worksheet("Status"), H_STATUS, m)

# ---- Update helpers ----
def update_trade_by_id(sheet, trade_id: str, updates: Dict[str, Any]) -> bool:
    """
    Updates a single trade row by trade_id; returns True if updated.
    """
    ws = sheet.ss.worksheet("Trades")
    hdr = ws.row_values(1)
    try:
        match_idx = hdr.index("trade_id")
    except ValueError:
        return False
    return _update_row(ws, hdr, match_idx, trade_id, updates)
