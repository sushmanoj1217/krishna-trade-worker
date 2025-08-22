# skills/performance_formulas.py
# ------------------------------------------------------------
# Performance tab rollups + formulas (idempotent, auto-detect).
# - Detects Net PnL & Version columns via flexible aliases;
#   if not found, infers Net PnL by scanning data (+/- numeric column).
# - Ensures helper columns: __CumPNL, __Peak, __DD
# - Writes per-row formulas for cum/peak/DD (rows 2..last)
# - Adds a Summary block (Total, Wins, Losses, WinRate, Avg, Net, MaxDD)
# - Adds a Version-wise pivot (count + sum NetPnL) when Version present
#
# Env:
#   GOOGLE_SA_JSON (service account JSON)
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

import os, json, re, math, statistics
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
    s = str(s or "").strip().lower()
    s = s.replace("Δ", "delta").replace("∆", "delta")
    s = re.sub(r"[\s\-\.\(\)\[\]/]+", "_", s)
    s = re.sub(r"__+", "_", s).strip("_")
    return s

def _to_float(x) -> Optional[float]:
    try:
        if x in (None, "", "—"): return None
        return float(str(x).replace(",", "").strip())
    except Exception:
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

def _last_row_with_data(ws, key_col_idx_hint: Optional[int] = None) -> int:
    """Find last row that has any content (prefer hint column if provided)."""
    if key_col_idx_hint:
        vals = ws.col_values(key_col_idx_hint)  # includes header
    else:
        # fallback: scan first 10 columns to find deepest filled row
        depth = 1
        for c in range(1, 11):
            try:
                col = ws.col_values(c)
            except Exception:
                continue
            for i, v in enumerate(col, start=1):
                if str(v).strip() != "":
                    depth = max(depth, i)
        return max(depth, 1)
    last = 0
    for i, v in enumerate(vals, start=1):
        if str(v).strip() != "":
            last = i
    return max(last, 1)

# ---------- Detection ----------
NETPNL_ALIASES = [
    "net_pnl","net_p&l","netpl","pnl","p&l","pnl_rs","netpnl","profit","net_profit","result","results","p_l","p-l","p/l","net",
]
VERSION_ALIASES = [
    "version","ver","build_version","strategy_version","variant","model_ver","model_version",
]

def _detect_col(headers: List[str], aliases: List[str]) -> Optional[int]:
    """Return 1-based column index whose normalized header matches/contains any alias."""
    norm = [_norm_key(h) for h in headers]
    al = [_norm_key(a) for a in aliases]
    # exact
    for i, n in enumerate(norm, start=1):
        if n in al:
            return i
    # contains
    for i, n in enumerate(norm, start=1):
        if any(a in n for a in al):
            return i
    return None

def _infer_netpnl_from_data(ws, headers: List[str]) -> Optional[int]:
    """Heuristic: choose a column with mostly numeric values AND both + and - appear."""
    cols = len(headers)
    last = _last_row_with_data(ws)
    if last < 2:
        return None
    last_letter = _col_letter(cols)
    rng = f"A2:{last_letter}{last}"
    data = ws.get(rng)  # list of rows
    if not data:
        return None

    best_idx = None
    best_score = -1.0

    for c in range(1, cols + 1):
        # collect numeric values of column c
        nums: List[float] = []
        for r in range(0, len(data)):
            row = data[r]
            if c - 1 < len(row):
                v = _to_float(row[c - 1])
                if v is not None:
                    nums.append(v)
        if not nums:
            continue
        # metrics
        n = len(nums)
        pos = sum(1 for x in nums if x > 0)
        neg = sum(1 for x in nums if x < 0)
        ratio = n / max(1, len(data))
        var = statistics.pstdev(nums) if len(nums) > 1 else 0.0
        # score: prefer both signs + numeric coverage + variance
        score = (1.0 if (pos > 0 and neg > 0) else 0.0) * 3.0 + ratio * 2.0 + (1.0 if var > 0 else 0.0)
        if score > best_score:
            best_score = score
            best_idx = c

    # sanity: if score too low (no +/-), still accept the most numeric column with variance
    return best_idx

# ---------- Core ----------
HELPER_HEADERS = ["__CumPNL", "__Peak", "__DD"]

def apply():
    ws = _open_perf_ws()
    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("Performance sheet has no headers (row 1 is empty)")

    # Detect NetPnL column
    net_idx = _detect_col(headers, NETPNL_ALIASES)
    if not net_idx:
        net_idx = _infer_netpnl_from_data(ws, headers)

    if not net_idx:
        raise RuntimeError(
            "Couldn't detect Net PnL column from headers or data.\n"
            "Workarounds: rename your PnL column to 'Net PnL' / 'PNL' / 'Profit', "
            "or ensure it has numeric +/− values."
        )

    # Detect Version column (optional)
    ver_idx = _detect_col(headers, VERSION_ALIASES)

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
    last = _last_row_with_data(ws, key_col_idx_hint=net_idx)
    # Build A1 letters
    net_col = _col_letter(net_idx)
    cum_col = _col_letter(cum_idx)
    peak_col = _col_letter(peak_idx)
    dd_col = _col_letter(dd_idx)

    if last >= 2:
        # Cum formulas (rows 2..last)
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
    _write_summary_block(ws, net_idx, dd_idx, ver_idx)

def _write_summary_block(ws, net_idx: int, dd_idx: int, ver_idx: Optional[int]):
    """
    Writes/overwrites a compact summary at far-right side (2 cols gap after helper section).
    Also writes a version-wise QUERY block below it (if Version present).
    """
    headers = ws.row_values(1)
    total_cols = len(headers)
    start_col = total_cols + 2  # two columns gap
    S = _col_letter(start_col)
    Vcol = _col_letter(ver_idx) if ver_idx else None

    # net & dd ranges (open-ended from row 2)
    net = f"{_col_letter(net_idx)}2:{_col_letter(net_idx)}"
    dd = f"{_col_letter(dd_idx)}2:{_col_letter(dd_idx)}"

    # Build summary rows with exact cell references
    # Row layout:
    # S1: SUMMARY
    # S2: Total Trades
    # S3: Wins (>0)
    # S4: Losses (<=0)
    # S5: Win Rate = S3 / S2
    # S6: Avg Net P&L
    # S7: Net P&L (Σ)
    # S8: Max Drawdown
    summary = [
        ["SUMMARY", ""],                                         # row +0
        ["Total Trades",  f"=COUNT({net})"],                     # +1
        ["Wins (>0)",     f"=COUNTIF({net},\">0\")"],            # +2
        ["Losses (<=0)",  f"=COUNTIF({net},\"<=0\")"],           # +3
        ["Win Rate",      ""],                                   # +4 (filled after write)
        ["Avg Net P&L",   f"=IFERROR(AVERAGEIF({net},\"<>\"),)"],# +5
        ["Net P&L (Σ)",   f"=IFERROR(SUM({net}),)"],             # +6
        ["Max Drawdown",  f"=IFERROR(MIN({dd}),)"],              # +7
    ]
    ws.update(f"{S}1:{_col_letter(start_col+1)}{len(summary)}", summary, value_input_option="USER_ENTERED")

    # Now fill Win Rate = S3 / S2
    wins_addr = f"{S}{3}"   # S3 (value column is S+1, but formula goes in S+1)
    total_addr = f"{S}{2}"  # S2
    winrate_addr = f"{_col_letter(start_col+1)}{5}"  # value cell next to "Win Rate" label at row 5
    ws.update_acell(winrate_addr, f"=IFERROR({_col_letter(start_col+1)}3/{_col_letter(start_col+1)}2,)")

    # Version pivot (optional)
    start_row = len(summary) + 2
    if ver_idx:
        ws.update_acell(f"{S}{start_row}", "By Version")
        # QUERY over {Version, NetPnL}
        V = f"{_col_letter(ver_idx)}2:{_col_letter(ver_idx)}"
        formula = (
            f'=QUERY({{{ {V} , {net} }}}, '
            f'"select Col1, count(Col1), sum(Col2) '
            f' where Col1 is not null group by Col1 '
            f' order by count(Col1) desc '
            f' label count(Col1) \'Trades\', sum(Col2) \'NetPnL\'", 0)'
        )
        ws.update_acell(f"{S}{start_row+1}", formula)
    else:
        ws.update_acell(f"{S}{start_row}", "By Version (Version column not found)")

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
