# path: storage/sheet_persistence.py
import json
from datetime import datetime
from tzlocal import get_localzone
from pathlib import Path

DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)
OV_PATH = DATA_DIR / "params_override.json"

TAB_OVR = "Params_Override"   # headers: ts,date,symbol,json
TAB_SNP = "Snapshots"         # headers: date,symbol,total_trades,sum_pnl,best_trade_id,best_pnl,worst_trade_id,worst_pnl,json

def _now_iso():
    return datetime.now(get_localzone()).isoformat()

def sync_params_override_from_sheet(sheet):
    """Read latest override JSON from sheet and write to data/params_override.json"""
    try:
        rows = sheet.read_all(TAB_OVR) or []
        if not rows: return False
        # pick last non-empty json
        for r in reversed(rows):
            js = (r.get("json") or "").strip()
            if js:
                OV_PATH.write_text(js)
                return True
    except Exception:
        pass
    return False

def write_params_override_to_sheet(sheet, symbol: str, overrides: dict):
    row = {
        "ts": _now_iso(),
        "date": _now_iso().split("T")[0],
        "symbol": symbol,
        "json": json.dumps(overrides, ensure_ascii=False),
    }
    sheet.append(TAB_OVR, row)

def write_snapshot_to_sheet(sheet, snap: dict):
    row = {
        "date": snap.get("date",""),
        "symbol": snap.get("symbol",""),
        "total_trades": snap.get("total_trades",""),
        "sum_pnl": snap.get("sum_pnl",""),
        "best_trade_id": (snap.get("best") or {}).get("trade_id",""),
        "best_pnl": (snap.get("best") or {}).get("pnl",""),
        "worst_trade_id": (snap.get("worst") or {}).get("trade_id",""),
        "worst_pnl": (snap.get("worst") or {}).get("pnl",""),
        "json": json.dumps(snap, ensure_ascii=False),
    }
    sheet.append(TAB_SNP, row)
