# integrations/sheets.py
from __future__ import annotations

import json
import os
import threading
from typing import List, Optional

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError, WorksheetNotFound, SpreadsheetNotFound
from datetime import datetime

# ---------- Debug & timeouts ----------
SHEETS_DEBUG = os.getenv("SHEETS_DEBUG", "1") == "1"
REQUEST_TIMEOUT = int(os.getenv("GSPREAD_TIMEOUT", "10"))  # seconds

def _now():
    return datetime.now().isoformat(sep=" ", timespec="seconds")

def _log(msg: str):
    if SHEETS_DEBUG:
        print(f"[{_now()}] [SHEETS] {msg}", flush=True)

# ---------- Config ----------
_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

_SPREADSHEET_ID = os.getenv("GSHEET_SPREADSHEET_ID")
_GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON")  # one-line JSON (escaped)

_client_lock = threading.Lock()
_ws_lock = threading.Lock()
_client: Optional[gspread.Client] = None
_sheet_cache: dict[str, gspread.Spreadsheet] = {}
_ws_cache: dict[tuple[str, str], gspread.Worksheet] = {}

# ---------- Low-level helpers ----------
def _require_env():
    if not _SPREADSHEET_ID:
        raise RuntimeError("GSHEET_SPREADSHEET_ID is not set")
    if not _GOOGLE_SA_JSON:
        raise RuntimeError("GOOGLE_SA_JSON is not set (must be one-line JSON)")

def _mask_email(e: str) -> str:
    if not e or "@" not in e:
        return "?"
    name, dom = e.split("@", 1)
    return (name[:3] + "…" + name[-2:]) + "@" + dom

def _get_client() -> gspread.Client:
    global _client
    if _client:
        return _client
    with _client_lock:
        if _client:
            return _client
        _require_env()
        try:
            info = json.loads(_GOOGLE_SA_JSON)
        except Exception as e:
            raise RuntimeError(f"GOOGLE_SA_JSON is not valid JSON: {e}")
        email = info.get("client_email")
        _log(f"Authorizing SA {_mask_email(email)} with scopes={len(_SCOPES)}")
        creds = Credentials.from_service_account_info(info, scopes=_SCOPES)
        # Build client manually so we can set request timeout
        client = gspread.Client(auth=creds)
        client.login()
        # gspread respects this attribute in request()
        setattr(client, "request_timeout", REQUEST_TIMEOUT)
        _log(f"gspread client ready (timeout={REQUEST_TIMEOUT}s)")
        _client = client
        return _client

def _open_sheet(sheet_id: str) -> gspread.Spreadsheet:
    if sheet_id in _sheet_cache:
        return _sheet_cache[sheet_id]
    with _client_lock:
        if sheet_id in _sheet_cache:
            return _sheet_cache[sheet_id]
        gc = _get_client()
        _log(f"Opening spreadsheet {sheet_id}")
        try:
            sh = gc.open_by_key(sheet_id)
        except SpreadsheetNotFound as e:
            _log("SpreadsheetNotFound — did you share the sheet with the service account email?")
            raise
        _sheet_cache[sheet_id] = sh
        _log("Spreadsheet open OK")
        return sh

def _get_ws(tab_name: str) -> gspread.Worksheet:
    key = (_SPREADSHEET_ID, tab_name)
    if key in _ws_cache:
        return _ws_cache[key]
    with _ws_lock:
        if key in _ws_cache:
            return _ws_cache[key]
        sh = _open_sheet(_SPREADSHEET_ID)
        try:
            ws = sh.worksheet(tab_name)
            _log(f"Worksheet '{tab_name}' found")
        except WorksheetNotFound:
            _log(f"Worksheet '{tab_name}' not found — creating")
            ws = sh.add_worksheet(title=tab_name, rows=1000, cols=30)
            _log(f"Worksheet '{tab_name}' created")
        _ws_cache[key] = ws
        return ws

def _ensure_headers(ws: gspread.Worksheet, headers: List[str]) -> None:
    try:
        # Resize columns if needed
        need_cols = max(ws.col_count, len(headers))
        if need_cols != ws.col_count:
            _log(f"Resizing cols to {need_cols} for '{ws.title}'")
            ws.resize(rows=ws.row_count, cols=need_cols)
    except Exception as e:
        _log(f"Resize warn: {e}")

    try:
        first_row = ws.row_values(1)
    except Exception as e:
        _log(f"row_values(1) failed: {e}")
        first_row = []

    if first_row == headers:
        _log(f"Headers OK for '{ws.title}'")
        return

    rng = f"A1:{_col_letter(len(headers))}1"
    _log(f"Writing headers to '{ws.title}' {rng}")
    ws.update(rng, [headers])

def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

# ---------- Public API ----------
def ensure_tab(tab_name: str, headers: List[str]) -> None:
    try:
        ws = _get_ws(tab_name)
        _ensure_headers(ws, headers)
    except (APIError, SpreadsheetNotFound) as e:
        _log(f"API error in ensure_tab('{tab_name}'): {e}")
        raise
    except Exception as e:
        _log(f"Unknown error in ensure_tab('{tab_name}'): {e}")
        raise

def append_row(tab_name: str, row: List) -> None:
    ws = _get_ws(tab_name)
    clean = [("" if (v is None) else v) for v in row]
    try:
        ws.append_row(clean, table_range="A1")
        _log(f"Appended 1 row to '{tab_name}'")
    except APIError as e:
        _log(f"append_row APIError: {e} — trying to resize and retry")
        ws.resize(rows=ws.row_count + 1000, cols=max(ws.col_count, len(clean)))
        ws.append_row(clean, table_range="A1")

def tail_rows(tab_name: str, n: int) -> List[List]:
    ws = _get_ws(tab_name)
    try:
        data = ws.get_all_values()
    except Exception as e:
        _log(f"get_all_values error on '{tab_name}': {e}")
        return []
    if not data or n <= 0:
        return []
    start = max(0, len(data) - n)
    return data[start:]

# ---------- Optional compat class ----------
class SheetClient:
    def ensure_tab(self, tab_name: str, headers: List[str]) -> None:
        ensure_tab(tab_name, headers)
    def append_row(self, tab_name: str, row: List) -> None:
        append_row(tab_name, row)
    def tail_rows(self, tab_name: str, n: int) -> List[List]:
        return tail_rows(tab_name, n)
