# ensure repo root on sys.path (so 'stores', 'agents', etc. resolve)
import sys, os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# night/backtest_runner.py
# Skeleton for Night jobs:
# - Writes daily snapshot, a dummy backtest run, a small params pool, and a research summary to Firestore
#   when FIREBASE_SYNC=on (safe no-op otherwise).
# - Optionally appends a short snapshot to Google Sheet "Snapshots" tab.
#
# Run example:
#   python -u night/backtest_runner.py

from __future__ import annotations
import os, json, uuid, random
from datetime import datetime
from typing import Dict, Any, Optional

# Firestore helpers (graceful no-op if disabled)
from stores import firestore_io

def _now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat(sep=" ")

def _date_str() -> str:
    return datetime.now().date().isoformat()

# ---- Optional: Google Sheet snapshot (best-effort, skipped if gspread/env missing) ----
def _maybe_sheet_snapshot(payload: Dict[str, Any]) -> None:
    try:
        import gspread
        gs_id = os.getenv("GSHEET_SPREADSHEET_ID", "")
        sa = os.getenv("GOOGLE_SA_JSON", "")
        if not gs_id or not sa:
            return
        info = json.loads(sa)
        gc = gspread.service_account_from_dict(info)
        ss = gc.open_by_key(gs_id)
        # Ensure tab & headers
        try:
            ws = ss.worksheet("Snapshots")
        except Exception:
            ws = ss.add_worksheet(title="Snapshots", rows=10, cols=10)
            ws.append_row(["ts", "key", "value", "blob"])
        # Append
        ws.append_row([_now_iso(), "night_summary", "ok", json.dumps(payload)[:48000]])
    except Exception as e:
        print(f"[night] sheet snapshot skipped: {e}", flush=True)

def _dummy_backtest() -> Dict[str, Any]:
    wins = random.randint(35, 60)
    trades = random.randint(80, 140)
    losses = max(0, trades - wins)
    win_rate = round(wins * 100.0 / max(1, trades), 2)
    avg_pnl = round(random.uniform(15, 45), 2)
    gross = round(avg_pnl * trades, 2)
    net = round(gross * 0.98, 2)
    max_dd = round(random.uniform(150, 450), 2)
    return {
        "trades": trades, "wins": wins, "losses": losses, "win_rate": win_rate,
        "avg_pnl": avg_pnl, "gross_pnl": gross, "net_pnl": net, "max_dd": max_dd,
        "notes": "dummy backtest; replace with real engine",
        "version": os.getenv("GIT_SHA", "")[:10],
    }

def _dummy_params_pool(n: int = 3):
    base = [
        {"ENTRY_BAND_POINTS": 7, "RR_TARGET": 2.5, "TRAIL_AFTER_POINTS": 20, "TRAIL_STEP_POINTS": 6},
        {"ENTRY_BAND_POINTS": 6, "RR_TARGET": 3.0, "TRAIL_AFTER_POINTS": 18, "TRAIL_STEP_POINTS": 5},
        {"ENTRY_BAND_POINTS": 8, "RR_TARGET": 2.2, "TRAIL_AFTER_POINTS": 22, "TRAIL_STEP_POINTS": 7},
    ]
    out = []
    for i in range(min(n, len(base))):
        out.append({
            "rank": i + 1,
            "params": base[i],
            "metrics": _dummy_backtest(),
        })
    return out

def main():
    date_s = _date_str()
    run_id = f"{date_s}-{uuid.uuid4().hex[:6]}"
    symbol = os.getenv("OC_SYMBOL_PRIMARY", "NIFTY")

    # 1) Daily snapshot
    daily_snapshot = {"symbol": symbol, "ts": _now_iso(), "summary": "night job skeleton executed"}
    firestore_io.save_snapshot_daily(date_s, daily_snapshot)

    # 2) Backtest run (dummy)
    bt_metrics = _dummy_backtest()
    firestore_io.save_backtest_run(run_id, {"symbol": symbol, "metrics": bt_metrics, "artifacts": {}})

    # 3) Params pool (top-3 dummy)
    for i, item in enumerate(_dummy_params_pool(3), start=1):
        firestore_io.save_params_pool(f"{run_id}_rank{i}", item)

    # 4) Research summary (very short)
    firestore_io.save_research_summary(date_s, f"Night skeleton ran for {symbol}; backtest trades={bt_metrics['trades']}, win_rate={bt_metrics['win_rate']}%.")

    # 5) Optional: drop a compact snapshot into Sheet (best-effort)
    _maybe_sheet_snapshot({"run_id": run_id, "symbol": symbol, "bt": bt_metrics})

    print(f"[night] done: run_id={run_id}", flush=True)

if __name__ == "__main__":
    main()

      
