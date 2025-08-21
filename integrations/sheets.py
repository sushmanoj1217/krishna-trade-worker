import os
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import gspread
from google.oauth2.service_account import Credentials

from utils.logger import log

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_ID = os.getenv("GSHEET_TRADES_SPREADSHEET_ID", "")
TAB_TRADES = os.getenv("GSHEET_TRADES_WORKSHEET", "Trades")
TAB_OC = "OC_Live"
TAB_SIGNALS = "Signals"
TAB_STATUS = "Status"

_gc = None
_sheet_full = None

def _client():
    global _gc
    if _gc:
        return _gc
    sa_json = os.getenv("GOOGLE_SA_JSON", "")
    if not sa_json:
        raise RuntimeError("GOOGLE_SA_JSON missing (one-line json)")
    info = json.loads(sa_json)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    _gc = gspread.authorize(creds)
    return _gc

def _open():
    global _sheet_full
    if _sheet_full:
        return _sheet_full
    if not SPREADSHEET_ID:
        raise RuntimeError("GSHEET_TRADES_SPREADSHEET_ID missing")
    _sheet_full = _client().open_by_key(SPREADSHEET_ID)
    return _sheet_full

async def ensure_tabs():
    sh = _open()
    need = {
        TAB_OC: ["timestamp","spot","s1","s2","r1","r2","expiry","signal","vix","pcr","pcr_bucket","max_pain","max_pain_dist","bias_tag","stale"],
        TAB_SIGNALS: ["signal_id","ts","side","trigger","c1","c2","c3","c4","c5","c6","eligible","reason","mv_pcr_ok","mv_mp_ok","mv_basis","oc_bull_normal","oc_bull_shortcover","oc_bear_normal","oc_bear_crash","oc_pattern_basis","near_cross","notes"],
        TAB_TRADES: ["trade_id","signal_id","symbol","side","buy_ltp","exit_ltp","sl","tp","basis","buy_time","exit_time","result","pnl","dedupe_hash"],
        TAB_STATUS: ["ts","component","msg"]
    }
    for title, header in need.items():
        try:
            ws = sh.worksheet(title)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title, rows=200, cols=len(header))
            ws.append_row(header, value_input_option="RAW")

async def log_status(component: str, msg: str):
    try:
        ws = _open().worksheet(TAB_STATUS)
        ws.append_row([datetime.now(timezone.utc).isoformat(), component, msg], value_input_option="RAW")
    except Exception as e:
        log.warning(f"status write failed: {e}")

async def log_oc_live(snap):
    try:
        ws = _open().worksheet(TAB_OC)
        row = [
            datetime.now(timezone.utc).isoformat(),
            snap.spot, snap.s1, snap.s2, snap.r1, snap.r2,
            snap.expiry, "", snap.vix or "", snap.pcr or "",
            "", snap.max_pain, abs(snap.spot - snap.max_pain),
            snap.bias or "", "TRUE" if snap.stale else "FALSE"
        ]
        ws.append_row(row, value_input_option="RAW")
    except Exception as e:
        log.warning(f"OC_Live append failed: {e}")

# ===== Signals helpers =====

async def log_signal_row(sig: Dict[str, Any]):
    """Append signal dict into Signals tab. Missing fields become ''."""
    ws = _open().worksheet(TAB_SIGNALS)
    cols = ["signal_id","ts","side","trigger","c1","c2","c3","c4","c5","c6","eligible","reason",
            "mv_pcr_ok","mv_mp_ok","mv_basis",
            "oc_bull_normal","oc_bull_shortcover","oc_bear_normal","oc_bear_crash","oc_pattern_basis",
            "near_cross","notes"]
    row = [sig.get(k, "") for k in cols]
    ws.append_row(row, value_input_option="RAW")

async def get_today_signal_dedupes() -> set:
    """Return a set of dedupe keys 'YYYYMMDD|SIDE|TRIGGER|PRICE' from Signals for today."""
    ws = _open().worksheet(TAB_SIGNALS)
    rows = ws.get_all_values()
    out = set()
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    if rows and len(rows) > 1:
        hdr = rows[0]
        idx_map = {h: i for i, h in enumerate(hdr)}
        for r in rows[1:]:
            ts = r[idx_map.get("ts", -1)] if idx_map.get("ts", -1) >= 0 else ""
            if ts.startswith(today):
                side = r[idx_map.get("side", -1)] if idx_map.get("side", -1) >= 0 else ""
                trig = r[idx_map.get("trigger", -1)] if idx_map.get("trigger", -1) >= 0 else ""
                notes = r[idx_map.get("notes", -1)] if idx_map.get("notes", -1) >= 0 else ""
                out.add(f"{today}|{side}|{trig}|{notes}")
    return out

async def get_last_event_rows(n=5) -> List[List[str]]:
    ws = _open().worksheet(TAB_STATUS)
    vals = ws.get_all_values()
    if not vals:
        return []
    return vals[-min(n, len(vals)-1):]

# ===== Trades helpers =====

async def get_open_trades() -> List[Dict[str, Any]]:
    ws = _open().worksheet(TAB_TRADES)
    rows = ws.get_all_values()
    if not rows or len(rows) == 1:
        return []
    hdr = rows[0]
    idx = {h: i for i, h in enumerate(hdr)}
    out = []
    for r in rows[1:]:
        if not r[idx.get("exit_time", -1)]:
            out.append({
                "trade_id": r[idx.get("trade_id", -1)],
                "signal_id": r[idx.get("signal_id", -1)],
                "symbol": r[idx.get("symbol", -1)],
                "side": r[idx.get("side", -1)],
                "buy_ltp": r[idx.get("buy_ltp", -1)],
                "sl": r[idx.get("sl", -1)],
                "tp": r[idx.get("tp", -1)],
                "basis": r[idx.get("basis", -1)],
                "buy_time": r[idx.get("buy_time", -1)],
            })
    return out

async def close_trade(trade_id: str, exit_ltp: float, result: str, pnl: float, note: str = ""):
    ws = _open().worksheet(TAB_TRADES)
    rows = ws.get_all_values()
    if not rows or len(rows) == 1:
        return
    hdr = rows[0]
    idx = {h: i for i, h in enumerate(hdr)}
    for i in range(1, len(rows)):
        if rows[i][idx.get("trade_id", -1)] == trade_id and not rows[i][idx.get("exit_time", -1)]:
            # update exit columns (simple: append a new row with exit populated)
            new_row = rows[i][:]
            new_row[idx["exit_ltp"]] = str(exit_ltp)
            new_row[idx["exit_time"]] = datetime.now(timezone.utc).isoformat()
            new_row[idx["result"]] = result
            new_row[idx["pnl"]] = str(pnl)
            # note into basis tail
            bpos = idx.get("basis", -1)
            if bpos >= 0:
                new_row[bpos] = f"{rows[i][bpos]} | {note}".strip()
            ws.append_row(new_row, value_input_option="RAW")
            break

async def count_today_trades() -> int:
    ws = _open().worksheet(TAB_TRADES)
    rows = ws.get_all_values()
    if not rows or len(rows) == 1:
        return 0
    hdr = rows[0]
    idx = {h: i for i, h in enumerate(hdr)}
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    c = 0
    for r in rows[1:]:
        bt = r[idx.get("buy_time", -1)]
        if bt.startswith(today):
            c += 1
    return c
