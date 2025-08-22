# skills/performance_formulas.py
# ------------------------------------------------------------
# Performance tab rollups + formulas (idempotent).
# - Detects Net PnL & Version columns via flexible aliases
# - Ensures helper columns: __CumPNL, __Peak, __DD
# - Writes per-row formulas for cum/peak/DD (rows 2..last)
# - Adds a Summary block (Total, Wins, Losses, WinRate, Avg, Net, MaxDD)
# - Adds a Version-wise pivot (count + sum NetPnL)
#
# Env:
#   GOOGLE_SA_JSON (service account JSON, inline)
#   GSHEET_TRADES_SPREADSHEET_ID (spreadsheet id)
#
# Run:
#   python -m skills.performance_formulas apply
# or:
#   python - <<'PY'
#   import skills.performance_formulas as pf; pf.apply()
#   PY
# ------------------------------------------------------------
from __future__ import annotations

import os, json, re
from typing import List, Dict, Any, Optional, Tuple

# gspread deps
try:
    import gspread  # type: ignore
except Exception as e:
    gspread = None  # type: ignore

# ---------- Small utils ----------
def _env(name: str) -> Optional[str]:
    v = os.environ.get(name)
    return v.strip() if v and v.strip() else None

def _col_letter(idx1: int) -> str:
    """1-based index -> A1 column letter"""
    s = ""
    n = idx1
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def _norm_key(s: str) -> str:
    s = s.strip().lower()
    s = s.replace("Δ", "delta").replace("∆", "delta")
    s = re.sub(r"[\s\-\.\(\)\[\]/]+", "_", s)
    s = re.sub(r"__+", "_", s).strip("_")
    return s

def _detect_col(headers: List[str], aliases: List[str]) -> Optional[int]:
    """
    Return 1-based column index whose normalized header matches any alias.
    """
    norm = [_norm_key(h) for h in headers]
    al = [_norm_key(a) for a in aliases]
    for i, n in enumerate(norm, start=1):
        if n in al:
            return i
    # looser: allow contains if not exact
    for i, n in enumerate(norm, start=1):
        if any(a in n for a in al):
            return i
    return None

def _open_perf_ws():
    if gspread is None:
        raise RuntimeError("gspread not installed: pip install gspread")
    raw = _env("GOOGLE_SA_JSON")
    sid = _env("GSHEET_TRADES_SPREADSHEET_ID")
    if not raw or not sid:
        raise RuntimeError("Sheets env missing: set GOOGLE_SA_JSON & GSHEET_TRADES_SPREADSHEET_ID")
    sa = json.loads(raw)
    gc = gspread.service_account_from_dict(sa)
    sh = gc.open_by_key(sid)
    try:
        ws = sh.worksheet("Performance")
    except Exception:
        ws = sh.add_worksheet(title="Performance", rows=1000, cols=26)
        # seed minimal headers if brand-new
        ws.update("A1", [["Date","Symbol","Side","EntryTime","ExitTime","Qty","EntryPrice","ExitPrice","Net PnL","Version","Note"]])
    return ws

def _last_row_with_data(ws, key_col_idx: int) -> int:
    """
    Find last row with any content in the 'key' column (e.g., NetPnL).
    Returns at least 1 (header row).
    """
    col_letter = _col_letter(key_col_idx)
    vals = ws.col_values(key_col_idx)  # includes header
    # Trim empty tail
    last = 0
    for i, v in enumerate(vals, start=1):
        if str(v).strip() != "":
            last = i
    return max(last, 1)

# ---------- Core ----------
HELPER_HEADERS = ["__CumPNL", "__Peak", "__DD"]

NETPNL_ALIASES = [
    "net_pnl", "net_p&l", "netpl", "pnl", "p&l", "pnl_rs", "netpnl",
]
VERSION_ALIASES = [
    "version", "ver", "build_version", "strategy_version",
]

def apply():
    ws = _open_perf_ws()
    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("Performance sheet has no headers (row 1 is empty)")

    # Detect key columns
    net_idx = _detect_col(headers, NETPNL_ALIASES)
    if not net_idx:
        raise RuntimeError("Couldn't detect Net PnL column. Add a header like 'Net PnL' or 'PNL'.")

    ver_idx = _detect_col(headers, VERSION_ALIASES)  # optional

    # Ensure helper headers exist (append if missing)
    hdr = headers[:]  # copy
    added = False
    for hh in HELPER_HEADERS:
        if hh not in hdr:
            hdr.append(hh)
            added = True
    if added:
        ws.update(f"A1:{_col_letter(len(hdr))}1", [hdr])
        headers = hdr  # refresh

    # Recompute helper indexes
    cum_idx = headers.index("__CumPNL") + 1
    peak_idx = headers.index("__Peak") + 1
    dd_idx = headers.index("__DD") + 1

    # Determine data extent
    last = _last_row_with_data(ws, net_idx)
    if last < 2:
        # Only header → still write summary block (empty)
        _write_summary_block(ws, net_idx, dd_idx, ver_idx, last_row=2)
        return

    # Build per-row formulas for Cum/Peak/DD (rows 2..last)
    net_col = _col_letter(net_idx)
    cum_col = _col_letter(cum_idx)
    peak_col = _col_letter(peak_idx)
    dd_col = _col_letter(dd_idx)

    # Cum formulas
    cum_vals = []
    for r in range(2, last + 1):
        if r == 2:
            f = f"=IFERROR(N({net_col}{r}),0)"
        else:
            f = f"=IFERROR({cum_col}{r-1},0)+IFERROR(N({net_col}{r}),0)"
        cum_vals.append([f])
    ws.update(f"{cum_col}2:{cum_col}{last}", cum_vals, value_input_option="USER_ENTERED")

    # Peak formulas
    peak_vals = []
    for r in range(2, last + 1):
        if r == 2:
            f = f"={cum_col}{r}"
        else:
            f = f"=MAX({peak_col}{r-1},{cum_col}{r})"
        peak_vals.append([f])
    ws.update(f"{peak_col}2:{peak_col}{last}", peak_vals, value_input_option="USER_ENTERED")

    # DD formulas
    dd_vals = []
    for r in range(2, last + 1):
        f = f"={cum_col}{r}-{peak_col}{r}"
        dd_vals.append([f])
    ws.update(f"{dd_col}2:{dd_col}{last}", dd_vals, value_input_option="USER_ENTERED")

    # Summary block (idempotent overwrite)
    _write_summary_block(ws, net_idx, dd_idx, ver_idx, last_row=last)

def _write_summary_block(ws, net_idx: int, dd_idx: int, ver_idx: Optional[int], last_row: int):
    """
    Writes/overwrites a compact summary at far-right side (2 cols gap after helper section).
    Also writes a version-wise QUERY block below it.
    """
    headers = ws.row_values(1)
    total_cols = len(headers)
    start_col = total_cols + 2  # two columns gap
    S = _col_letter(start_col)

    # Ranges
    net = f"{_col_letter(net_idx)}2:{_col_letter(net_idx)}"
    dd = f"{_col_letter(dd_idx)}2:{_col_letter(dd_idx)}"

    # Summary labels & formulas
    summary = [
        ["SUMMARY", ""],
        ["Total Trades", f"=COUNT({net})"],
        ["Wins (>0)", f"=COUNTIF({net},\">0\")"],
        ["Losses (<=0)", f"=COUNTIF({net},\"<=0\")"],
        ["Win Rate", f"=IFERROR(INDEX({S}{2+1},0,1)/INDEX({S}{2},0,1),)"],  # Wins / Total
        ["Avg Net P&L", f"=IFERROR(AVERAGEIF({net},\"<>\"),)"],
        ["Net P&L (Σ)", f"=IFERROR(SUM({net}),)"],
        ["Max Drawdown", f"=IFERROR(MIN({dd}),)"],
    ]
    ws.update(f"{S}1:{_col_letter(start_col+1)}{len(summary)}", summary, value_input_option="USER_ENTERED")

    # Version pivot (optional)
    start_row = len(summary) + 2
    if ver_idx:
        V = f"{_col_letter(ver_idx)}2:{_col_letter(ver_idx)}"
        # QUERY over {Version, NetPnL}
        formula = f'=QUERY({{{ {V} , {net} }}}, "select Col1, count(Col1), sum(Col2) where Col1 is not null group by Col1 order by count(Col1) desc label count(Col1) \'Trades\', sum(Col2) \'NetPnL\'", 0)'
        ws.update_acell(f"{S}{start_row}", "By Version")
        ws.update_acell(f"{S}{start_row+1}", formula)
    else:
        ws.update_acell(f"{S}{start_row}", "By Version (Version col not found)")

# ---------- CLI ----------
def _main():
    import sys
    cmd = sys.argv[1:] or ["apply"]
    if cmd[0] not in {"apply"}:
        print("Usage: python -m skills.performance_formulas apply")
        return
    apply()
    print("Performance formulas applied.")

if __name__ == "__main__":
    _main()
