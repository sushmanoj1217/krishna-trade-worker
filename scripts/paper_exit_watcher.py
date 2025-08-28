#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Paper exit watcher (fixed PNL + result writing)

- Scans Trades sheet for OPEN rows (exit_time empty).
- If exit_ltp already present (manual/auto), closes the trade:
    * exit_time = now
    * pnl = (exit_ltp - buy_ltp) * qty         [if EXIT_WRITE_PNL=1]
    * result = win/loss/be
    * notes += [reason if available]

Env:
  GSHEET_TRADES_SPREADSHEET_ID   (required)
  GOOGLE_SA_JSON                 (required; service account JSON)
  EXIT_DRY_RUN                   (default: "1" → no write)
  LOOP_SECS                      (default: "18")
  OC_SYMBOL                      (optional; for filtering logs)
  EXIT_WRITE_PNL                 (default: "1"; set "0" to NOT write pnl cell)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Dict, List, Tuple, Optional

try:
    import gspread  # type: ignore
except Exception as e:
    print("[paper_exit_watcher] gspread import failed:", e, file=sys.stderr)
    raise

HDR_ALIASES = {
    "trade_id": ["trade_id", "id"],
    "signal_id": ["signal_id"],
    "symbol": ["symbol", "sym"],
    "side": ["side", "direction"],
    "qty": ["qty", "quantity", "size", "lots"],
    "buy_ltp": ["buy_ltp", "buy_price", "entry_ltp", "entry_price", "buy_spot", "entry_spot"],
    "exit_ltp": ["exit_ltp", "exit_price", "sell_ltp", "sell_price", "exit_spot"],
    "buy_time": ["buy_time", "entry_time"],
    "exit_time": ["exit_time", "close_time"],
    "result": ["result", "status", "winloss"],
    "pnl": ["pnl", "net pnl", "profit", "pl", "p/l"],
    "notes": ["notes", "note", "comment", "remarks"],
}

def _norm(s: str) -> str:
    return (s or "").strip().lower()

def _build_header_map(hdr: List[str]) -> Dict[str, int]:
    idx = {}
    for k, aliases in HDR_ALIASES.items():
        for i, name in enumerate(hdr):
            n = _norm(name)
            if n in [a.lower() for a in aliases]:
                idx[k] = i
                break
    return idx

def _read_env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name, "")
    if v == "":
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")

def _now_str() -> str:
    # Naive local time string; matches existing sheet pattern
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def _to_float(x: str) -> Optional[float]:
    try:
        return float(str(x).replace(",", ""))
    except Exception:
        return None

def _to_int(x: str) -> Optional[int]:
    try:
        return int(str(x).replace(",", ""))
    except Exception:
        return None

def _get_ws() -> "gspread.Worksheet":
    sa_json = os.environ.get("GOOGLE_SA_JSON")
    sid = os.environ.get("GSHEET_TRADES_SPREADSHEET_ID")
    if not sa_json or not sid:
        raise RuntimeError("Missing GOOGLE_SA_JSON / GSHEET_TRADES_SPREADSHEET_ID")
    sa = json.loads(sa_json)
    gc = gspread.service_account_from_dict(sa)
    sh = gc.open_by_key(sid)
    # Trades sheet is canonical here
    return sh.worksheet("Trades")

def _has_formula(ws: "gspread.Worksheet", row: int, col: int) -> bool:
    try:
        cell = ws.cell(row, col, value_render_option="FORMULA")
        val = getattr(cell, "input_value", None)
        if isinstance(val, str) and val.startswith("="):
            return True
    except Exception:
        pass
    return False

def _close_open_trades(ws: "gspread.Worksheet", dry_run: bool, write_pnl: bool) -> int:
    vals = ws.get_all_values()
    if not vals:
        print("[INFO] Trades sheet empty")
        return 0

    hdr = vals[0]
    rows = vals[1:]
    idx = _build_header_map(hdr)

    needed = ["symbol", "side", "buy_ltp", "exit_ltp", "exit_time", "pnl", "result", "qty", "notes"]
    for k in ["symbol", "side", "buy_ltp", "exit_ltp", "exit_time"]:
        if k not in idx:
            raise RuntimeError(f"Trades header missing required column: {k!r}")

    write_count = 0
    for rpos, row in enumerate(rows, start=2):  # 1-based with header
        buy_ltp = _to_float(row[idx["buy_ltp"]]) if idx.get("buy_ltp") is not None else None
        exit_ltp_raw = row[idx["exit_ltp"]] if idx.get("exit_ltp") is not None else ""
        exit_ltp = _to_float(exit_ltp_raw)
        exit_time = row[idx["exit_time"]] if idx.get("exit_time") is not None else ""

        if not buy_ltp:
            continue  # invalid/empty buy
        if exit_time.strip():
            continue  # already closed
        if exit_ltp is None:
            # No exit price present → cannot close; skip
            continue

        symbol = row[idx["symbol"]] if idx.get("symbol") is not None else ""
        side = _norm(row[idx["side"]]) if idx.get("side") is not None else ""
        qty = 1
        if idx.get("qty") is not None:
            qv = _to_int(row[idx["qty"]])
            if qv and qv > 0:
                qty = qv

        # Long-only CE/PE assumed
        pnl = (exit_ltp - buy_ltp) * qty

        result_val = "be"
        if pnl > 0:
            result_val = "win"
        elif pnl < 0:
            result_val = "loss"

        # Build updates
        updates: List[Tuple[str, str]] = []
        # exit_time
        a1_exit_time = gspread.utils.rowcol_to_a1(rpos, idx["exit_time"] + 1)
        updates.append((a1_exit_time, _now_str()))
        # result
        if idx.get("result") is not None:
            a1_result = gspread.utils.rowcol_to_a1(rpos, idx["result"] + 1)
            updates.append((a1_result, result_val))
        # pnl (only if allowed + not a formula cell)
        if write_pnl and idx.get("pnl") is not None:
            pnl_col = idx["pnl"] + 1
            a1_pnl = gspread.utils.rowcol_to_a1(rpos, pnl_col)
            if not _has_formula(ws, rpos, pnl_col):
                updates.append((a1_pnl, str(round(pnl, 2))))

        # notes (optional append)
        if idx.get("notes") is not None:
            old = row[idx["notes"]].strip() if row[idx["notes"]] else ""
            note_add = "exit=MANUAL"  # generic tag; refine if you encode reasons
            new_notes = (old + (" | " if old else "") + note_add).strip()
            a1_notes = gspread.utils.rowcol_to_a1(rpos, idx["notes"] + 1)
            updates.append((a1_notes, new_notes))

        # Write
        print(f"[INFO] [{symbol} {side.upper()}] EXIT @{exit_ltp} pnl={round(pnl,2)}")
        if dry_run:
            for a1, val in updates:
                print(f"[DRY_RUN] ws.update({a1} ← {val!r})")
        else:
            for a1, val in updates:
                # USER_ENTERED to let sheet parse numbers/timestamps if needed
                ws.update(a1, [[val]], value_input_option="USER_ENTERED")
            write_count += 1

    return write_count

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    parser.add_argument("--loop", type=int, default=None, help="Run continuously, sleeping N seconds")
    args = parser.parse_args()

    loop_secs = args.loop if args.loop is not None else int(os.environ.get("LOOP_SECS", "18"))
    dry_run = _read_env_bool("EXIT_DRY_RUN", True)
    write_pnl = _read_env_bool("EXIT_WRITE_PNL", True)
    symbol_hint = os.environ.get("OC_SYMBOL", "")

    print("[paper_exit_watcher] starting...")
    print(f"2025-08-28 00:00:00 [INFO] config: loop={loop_secs}s, dry_run={dry_run}, write_pnl={write_pnl}, symbol={symbol_hint or '-'}")

    ws = _get_ws()
    print("2025-08-28 00:00:00 [INFO] Sheets OK: Trades worksheet found")

    def _tick():
        try:
            n = _close_open_trades(ws, dry_run=dry_run, write_pnl=write_pnl)
            if n == 0:
                print("2025-08-28 00:00:00 [INFO] No OPEN trades")
        except Exception as e:
            print("2025-08-28 00:00:00 [ERROR] tick failed:", e, file=sys.stderr)

    if args.once:
        _tick()
        print("2025-08-28 00:00:00 [INFO] once done")
        return

    try:
        while True:
            _tick()
            time.sleep(max(3, loop_secs))
    except KeyboardInterrupt:
        print("\n[paper_exit_watcher] stopped")

if __name__ == "__main__":
    main()
