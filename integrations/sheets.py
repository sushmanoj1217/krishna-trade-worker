import gspread, json, os
from google.oauth2.service_account import Credentials
from typing import Dict, Any, List
from utils.logger import log

_scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

_SPREADSHEET_ID = os.getenv("GSHEET_TRADES_SPREADSHEET_ID", "")

def _client():
    sa_json = os.getenv("GOOGLE_SA_JSON", "").strip()
    if not sa_json:
        raise RuntimeError("GOOGLE_SA_JSON missing")
    info = json.loads(sa_json)
    creds = Credentials.from_service_account_info(info, scopes=_scopes)
    return gspread.authorize(creds)

def _sheet():
    if not _SPREADSHEET_ID:
        raise RuntimeError("GSHEET_TRADES_SPREADSHEET_ID missing")
    return _client().open_by_key(_SPREADSHEET_ID)

def ensure_tabs():
    sh = _sheet()
    needed = ["OC_Live","Signals","Trades","Performance","Events","Status","Snapshots","Params_Override"]
    for name in needed:
        try:
            sh.worksheet(name)
        except gspread.WorksheetNotFound:
            log.info(f"Sheets: creating tab {name}")
            sh.add_worksheet(name, rows=1000, cols=30)
    log.info(f"Sheets OK for {sh.title}")

def append_row(tab: str, row: List[Any]):
    ws = _sheet().worksheet(tab)
    ws.append_row(row, value_input_option="RAW")

def last_row(tab: str) -> Dict[str, Any] | None:
    ws = _sheet().worksheet(tab)
    vals = ws.get_all_records()
    return vals[-1] if vals else None

def read_override_json() -> str | None:
    row = last_row("Params_Override")
    if not row:
        return None
    return row.get("json") or row.get("JSON") or None
