#!/usr/bin/env python3
from __future__ import annotations
import os, json, time
from typing import List, Dict, Any, Optional
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread
from gspread.exceptions import APIError

IST = ZoneInfo("Asia/Kolkata")

DEFAULT_SIGNALS_HDR = [
    "Timestamp", "Symbol", "Expiry", "Side", "Level",
    "TriggerPrice", "Spot", "MV", "PCR", "MP",
    "CE_OI_Delta", "PE_OI_Delta",
    "Source", "AsOf", "AgeSec",
    "Eligibility", "Reason", "Mode"
]

DEFAULT_TRADES_HDR = [
    "Timestamp", "Symbol", "Expiry", "Side", "Level",
    "EntryPrice", "SpotAtEntry", "QtyLots", "Mode",
    "Status", "ExitTime", "ExitPrice", "PnL"
]

def _gc():
    sa_json = os.environ.get("GOOGLE_SA_JSON")
    sid = os.environ.get("GSHEET_TRADES_SPREADSHEET_ID")
    if not sa_json or not sid:
        raise RuntimeError("Missing GOOGLE_SA_JSON or GSHEET_TRADES_SPREADSHEET_ID")
    sa = json.loads(sa_json)
    gc = gspread.service_account_from_dict(sa)
    sh = gc.open_by_key(sid)
    return sh

def _ensure_sheet(sh, title: str, header: List[str]):
    try:
        ws = sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=200, cols=max(26, len(header)))
        ws.update("A1", [header])
        return ws

    # ensure header columns (append any missing to the right)
    first = ws.row_values(1)
    changed = False
    for h in header:
        if h not in first:
            first.append(h)
            changed = True
    if changed:
        ws.update("A1", [first])
    return ws

def _sheet(title: str, header: List[str]):
    sh = _gc()
    return _ensure_sheet(sh, title, header)

def _header_index_map(ws) -> Dict[str, int]:
    hdr = ws.row_values(1)
    return {h: i for i, h in enumerate(hdr)}

def _append_row(ws, row_dict: Dict[str, Any]):
    hdr_map = _header_index_map(ws)
    idx_to_val: Dict[int, Any] = {}
    for k, v in row_dict.items():
        if k not in hdr_map:
            # add missing header at end
            hdr = ws.row_values(1)
            hdr.append(k)
            ws.update("A1", [hdr])
            hdr_map = _header_index_map(ws)
        idx_to_val[hdr_map[k]] = v

    # build full row
    max_idx = max(idx_to_val.keys()) if idx_to_val else -1
    row = ["" for _ in range(max_idx + 1)]
    for i, v in idx_to_val.items():
        row[i] = v

    # append
    ws.append_row(row, value_input_option="USER_ENTERED")

def _read_last_n(ws, n: int) -> List[List[str]]:
    vals = ws.get_all_values()
    if len(vals) <= 1:
        return []
    body = vals[1:]
    if n >= len(body):
        return body
    return body[-n:]

def _now_ist_str() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

# ---------- Public API ----------

def append_signal(row: Dict[str, Any]) -> None:
    ws = _sheet("Signals", DEFAULT_SIGNALS_HDR)
    row = {"Timestamp": _now_ist_str(), **row}
    _append_row(ws, row)

def append_trade_open(row: Dict[str, Any]) -> None:
    ws = _sheet("Trades", DEFAULT_TRADES_HDR)
    row = {"Timestamp": _now_ist_str(), "Status": "OPEN", **row}
    _append_row(ws, row)

def recent_signal_exists(key_fields: Dict[str, Any], lookback: int = 200) -> bool:
    """Basic dedupe guard using last N rows in Signals."""
    ws = _sheet("Signals", DEFAULT_SIGNALS_HDR)
    hdr = ws.row_values(1)
    last = _read_last_n(ws, lookback)
    def key_of(rec: List[str]) -> str:
        m = {h: (rec[i] if i < len(rec) else "") for i, h in enumerate(hdr)}
        return "|".join(str(key_fields.get(k, "")) for k in ["Symbol","Expiry","Side","Level","TriggerPrice","Mode"])
    target = "|".join(str(key_fields.get(k, "")) for k in ["Symbol","Expiry","Side","Level","TriggerPrice","Mode"])
    for rec in last:
        if key_of(rec) == target:
            return True
    return False
