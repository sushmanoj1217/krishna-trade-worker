# integrations/sheets.py
from __future__ import annotations

import json
import os
import threading
from typing import List, Optional

# Deps: gspread, google-auth
import gspread
from google.oauth2.service_account import Credentials

# ---------- Config ----------
_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

_SPREADSHEET_ID = os.getenv("GSHEET_SPREADSHEET_ID")
_GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON")  # one-line JSON

# Single global client/worksheet cache + lock for thread safety
_client_lock = threading.Lock()
_ws_lock = threading.Lock()
_client: Optional[gspread.Client] = None
_sheet_cache: dict[str, gspread.Spreadsheet] = {}
_ws_cache: dict[tuple[str, str], gspread.Worksheet] = {}  # (sheet_id, tab_name) -> ws


# ---------- Low-level helpers ----------
def _require_env():
    if not _SPREADSHEET_ID:
        raise RuntimeError("GSHEET_SPREADSHEET_ID is not set")
    if not _GOOGLE_SA_JSON:
        raise RuntimeError("GOOGLE_SA_JSON is not set (must be one-line JSON)")

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
        creds = Credentials.from_service_account_info(info, scopes=_SCOPES)
        _client = gspread.authorize(creds)
        return _client

def _open_sheet(sheet_id: str) -> gspread.Spreadsheet:
    if sheet_id in _sheet_cache:
        return _sheet_cache[sheet_id]
    with _client_lock:
        if sheet_id in _sheet_cache:
            return _sheet_cache[sheet_id]
        gc = _get_client()
        sh = gc.open_by_key(sheet_id)
        _sheet_cache[sheet_id] = sh
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
        except gspread.WorksheetNotFound:
            # create with default 1000 rows, len(headers) cols later
            ws = sh.add_worksheet(title=tab_name, rows=1000, cols=30)
        _ws_cache[key] = ws
        return ws

def _ensure_headers(ws: gspread.Worksheet, headers: List[str]) -> None:
    # Resize columns if needed
    try:
        current_cols = ws.col_count
        need_cols = max(current_cols, len(headers))
        if need_cols != current_cols:
            ws.resize(rows=ws.row_count, cols=need_cols)
    except Exception:
        pass

    # Read first row
    try:
        first_row = ws.row_values(1)
    except Exception:
        first_row = []

    if first_row == headers:
        return

    # If empty or different, overwrite row 1 with headers
    values = [headers]
    ws.update(f"A1:{_col_letter(len(headers))}1", values)


def _col_letter(n: int) -> str:
    # 1 -> A, 26 -> Z, 27 -> AA
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


# ---------- Public API ----------
def ensure_tab(tab_name: str, headers: List[str]) -> None:
    """
    Ensure worksheet exists with the provided headers in row 1.
    """
    ws = _get_ws(tab_name)
    _ensure_headers(ws, headers)

def append_row(tab_name: str, row: List) -> None:
    """
    Append a single row (list). Nones will be converted to ''.
    """
    ws = _get_ws(tab_name)
    clean = [("" if (v is None) else v) for v in row]
    try:
        ws.append_row(clean, table_range="A1")
    except gspread.exceptions.APIError as e:
        # In case of dimension mismatch, try resizing columns and retry once
        try:
            ws.resize(rows=ws.row_count + 1000, cols=max(ws.col_count, len(clean)))
            ws.append_row(clean, table_range="A1")
        except Exception:
            raise e

def tail_rows(tab_name: str, n: int) -> List[List]:
    """
    Return the last n rows (excluding empty trailing rows).
    """
    ws = _get_ws(tab_name)
    # Efficient range pull: read only used range
    try:
        data = ws.get_all_values()  # small enough for ops sheets
    except Exception:
        return []
    if not data:
        return []
    if n <= 0:
        return []
    start = max(0, len(data) - n)
    return data[start:]


# ---------- Optional: convenience class (compat) ----------
class SheetClient:
    """
    Backwards-compatible shim used by some modules.
    Methods delegate to the module functions above.
    """
    def ensure_tab(self, tab_name: str, headers: List[str]) -> None:
        ensure_tab(tab_name, headers)

    def append_row(self, tab_name: str, row: List) -> None:
        append_row(tab_name, row)

    def tail_rows(self, tab_name: str, n: int) -> List[List]:
        return tail_rows(tab_name, n)
