
import os, json
from typing import List, Dict, Any

def get_sheet():
    sid = os.getenv("GSHEET_SPREADSHEET_ID")
    sa = os.getenv("GOOGLE_SA_JSON")
    if not (sid and sa):
        print("[sheet] not configured; using console stub")
        return ConsoleSheet()
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(json.loads(sa), scopes=scopes)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(sid)
        return GSheet(sh)
    except Exception as e:
        print("[sheet] failed, fallback console:", e)
        return ConsoleSheet()

class ConsoleSheet:
    def __init__(self):
        self.storage: Dict[str, List[Dict[str, Any]]] = {}
    def ensure_headers(self, tab: str, headers: List[str]):
        self.storage.setdefault(tab, [])
        print(f"[sheet:{tab}] headers -> {headers}")
    def append(self, tab: str, row: Dict[str, Any]):
        self.storage.setdefault(tab, [])
        self.storage[tab].append(row)
        print(f"[sheet:{tab}] {row}")
    def read_all(self, tab: str) -> List[Dict[str, Any]]:
        return list(self.storage.get(tab, []))

class GSheet:
    def __init__(self, sh):
        self.sh = sh

    def ensure_headers(self, tab: str, headers: List[str]):
        try:
            ws = self.sh.worksheet(tab)
        except Exception:
            ws = self.sh.add_worksheet(title=tab, rows=1, cols=max(10, len(headers)))
        existing = ws.row_values(1)
        if existing != headers:
            if existing:
                ws.delete_rows(1)
            ws.insert_row(headers, index=1)

    def append(self, tab: str, row: Dict[str, Any]):
        ws = self.sh.worksheet(tab)
        headers = ws.row_values(1)
        vals = [row.get(h, "") for h in headers]
        ws.append_row(vals, value_input_option="RAW")

    def read_all(self, tab: str) -> List[Dict[str, Any]]:
        try:
            ws = self.sh.worksheet(tab)
        except Exception:
            return []
        return ws.get_all_records()
