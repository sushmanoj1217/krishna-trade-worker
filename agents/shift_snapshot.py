# path: agents/shift_snapshot.py
import json, pathlib
from datetime import datetime
from tzlocal import get_localzone
from agents import logger
from storage.sheet_persistence import write_snapshot_to_sheet

SNAP_DIR = pathlib.Path("data/snapshots")
SNAP_DIR.mkdir(parents=True, exist_ok=True)

def _today() -> str:
    return datetime.now(get_localzone()).strftime("%Y-%m-%d")

def day_end_snapshot(sheet, cfg):
    day = _today()
    best = {"pnl": -1e18, "trade_id": ""}
    worst = {"pnl": 1e18, "trade_id": ""}
    total = 0.0; n = 0
    rows = sheet.read_all("Trades") or []
    for r in rows:
        ts = str(r.get("ts_buy",""))
        if not ts.startswith(day): continue
        try:
            pnl = float(r.get("pnl") or 0)
            total += pnl; n += 1
            if pnl > best["pnl"]: best = {"pnl": pnl, "trade_id": r.get("trade_id","")}
            if pnl < worst["pnl"]: worst = {"pnl": pnl, "trade_id": r.get("trade_id","")}
        except Exception:
            continue
    snap = {"date": day, "symbol": cfg.symbol, "total_trades": n, "sum_pnl": total,
            "best": best, "worst": worst}
    path = SNAP_DIR / f"{day}.json"
    path.write_text(json.dumps(snap, indent=2))
    logger.log_status(sheet, {"state":"OK", "message": f"snapshot saved {path}"})

    # Also persist to Sheet
    try:
        write_snapshot_to_sheet(sheet, snap)
    except Exception:
        pass
