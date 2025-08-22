# scripts/eod_tuner.py
# -----------------------------------------------------------------------------
# EOD Tuner: Read recent Performance, compute light heuristics, and append
# next-day parameters into "Params_Override" sheet.
#
# Inputs (ENV):
#   EOD_TUNE_SYMBOLS        = "NIFTY,BANKNIFTY" (default: OC_SYMBOL or NIFTY)
#   EOD_LOOKBACK_DAYS       = 10         # distinct trade-dates to look back
#   EOD_MIN_TRADES          = 8          # minimum trades in lookback to tune
#   EOD_TUNER_DRY_RUN       = 0/1        # 1 => don't write, only log
#   PERFORMANCE_SHEET_NAME  = Performance (override if your sheet is named differently)
#
# Sheets env:
#   GOOGLE_SA_JSON, GSHEET_TRADES_SPREADSHEET_ID
#
# Output Sheet: Params_Override (wide row per run)
# -----------------------------------------------------------------------------

from __future__ import annotations

import os, json, time, logging, statistics
from typing import Any, Dict, List, Tuple, Optional

try:
    import gspread  # type: ignore
except Exception:
    gspread = None  # type: ignore

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ---------------- ENV helpers ----------------
def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(name)
    return v.strip() if v and str(v).strip() else default

def _sym_env(sym: str, base: str, default: float) -> float:
    s = (sym or "").upper()
    val = _env(f"{base}_{s}")
    if val is None:
        val = _env(base)
    try:
        return float(val) if val is not None else float(default)
    except Exception:
        return float(default)

# ---------------- Sheets IO ----------------
REQ_HEADERS_PARAMS = [
    "date","symbol","LEVEL_BUFFER","ENTRY_BAND","TARGET_MIN_POINTS",
    "TP_POINTS","SL_POINTS","TRAIL_TRIGGER_POINTS","TRAIL_OFFSET_POINTS",
    "MV_REV_CONFIRM","lookback_days","src","notes"
]

def _open_spreadsheet():
    if gspread is None:
        raise RuntimeError("gspread not installed")
    raw = _env("GOOGLE_SA_JSON"); sid = _env("GSHEET_TRADES_SPREADSHEET_ID")
    if not raw or not sid:
        raise RuntimeError("Sheets env missing (GOOGLE_SA_JSON / GSHEET_TRADES_SPREADSHEET_ID)")
    sa = json.loads(raw)
    gc = gspread.service_account_from_dict(sa)
    sh = gc.open_by_key(sid)
    return sh, sid

def _open_ws_write(name: str):
    """For writing: create if missing."""
    sh, sid = _open_spreadsheet()
    try:
        return sh.worksheet(name)
    except Exception:
        log.info("Sheet '%s' not found in %s → creating", name, sid)
        return sh.add_worksheet(title=name, rows=1000, cols=40)

def _open_ws_read_must(name: str):
    """For reading: DO NOT create silently. Raise with helpful info."""
    sh, sid = _open_spreadsheet()
    try:
        return sh.worksheet(name)
    except Exception:
        names = [w.title for w in sh.worksheets()]
        raise RuntimeError(f"Worksheet '{name}' not found in spreadsheet {sid}. Available: {names}")

def _open_ws_read_optional(name: str):
    sh, sid = _open_spreadsheet()
    try:
        return sh.worksheet(name)
    except Exception:
        return None

def _ensure_params_headers(ws) -> List[str]:
    try:
        hdr = ws.row_values(1)
    except Exception:
        hdr = []
    if not hdr:
        ws.update("A1", [REQ_HEADERS_PARAMS])
        return REQ_HEADERS_PARAMS
    need = [h for h in REQ_HEADERS_PARAMS if h not in hdr]
    if need:
        ws.update("A1", [hdr + need])
        return hdr + need
    return hdr

def _append_params_row(row: Dict[str, Any]) -> None:
    ws = _open_ws_write("Params_Override")
    hdr = _ensure_params_headers(ws)
    vals = [row.get(h,"") for h in hdr]
    ws.append_row(vals, value_input_option="RAW")

# -------- tolerant numeric/string helpers --------
def _num(x) -> Optional[float]:
    try:
        if x in (None,"","—"): return None
        s = str(x).replace(",","").strip()
        return float(s)
    except Exception:
        return None

def _today_ist_str() -> str:
    t = time.time() + 5.5*3600
    return time.strftime("%Y-%m-%d", time.gmtime(t))

# ---------------- baselines & steps ----------------
BASE = {
    "NIFTY":     dict(BUF=12.0, BAND=3.0,  TGT=30.0, TP=40.0,  SL=20.0,  TR_TR=25.0, TR_OFF=15.0),
    "BANKNIFTY": dict(BUF=30.0, BAND=8.0,  TGT=80.0, TP=100.0, SL=60.0,  TR_TR=70.0, TR_OFF=40.0),
    "FINNIFTY":  dict(BUF=15.0, BAND=4.0,  TGT=50.0, TP=60.0,  SL=35.0,  TR_TR=45.0, TR_OFF=25.0),
}
STEP = {
    "NIFTY":     dict(BUF_UP=2.0, BUF_DN=2.0, TP_UP=5.0),
    "BANKNIFTY": dict(BUF_UP=5.0, BUF_DN=5.0, TP_UP=10.0),
    "FINNIFTY":  dict(BUF_UP=3.0, BUF_DN=3.0, TP_UP=7.0),
}

def _base(sym: str, key: str, fallback: float) -> float:
    s = (sym or "").upper()
    if s in BASE and key in BASE[s]:
        return float(BASE[s][key])
    return float(fallback)

# ---------------- core stats ----------------
def _last_n_dates(records: List[Dict[str,Any]], n: int, sym: str) -> List[str]:
    seen = []
    S = sym.upper()
    for r in records:
        rs = str(r.get("symbol") or r.get("Symbol") or "").upper()
        if rs and rs != S:
            continue
        d = str(r.get("date") or r.get("Date") or "").strip()
        if not d: continue
        if d not in seen:
            seen.append(d)
    return seen[-n:] if len(seen) >= n else seen

def _filter_by(records: List[Dict[str,Any]], sym: str, dates: List[str]) -> List[Dict[str,Any]]:
    out = []
    D = set(dates)
    S = sym.upper()
    for r in records:
        rs = str(r.get("symbol") or r.get("Symbol") or "").upper()
        if rs and rs != S:
            continue
        d = str(r.get("date") or r.get("Date") or "").strip()
        if d in D:
            out.append(r)
    return out

def _stats(records: List[Dict[str,Any]]) -> Dict[str,Any]:
    pnls = []
    wins = []; losses = []
    reasons = []
    for r in records:
        p = _num(r.get("pnl_points") or r.get("PNL") or r.get("Net PnL") or r.get("NetPNL"))
        if p is None: continue
        pnls.append(p)
        if p >= 0:
            wins.append(p)
        else:
            losses.append(-p)
        er = str(r.get("exit_reason") or r.get("ExitReason") or "").strip().upper()
        if er:
            reasons.append(er)
    n = len(pnls)
    wr = (len(wins)/n*100.0) if n>0 else 0.0
    avg_win = (sum(wins)/len(wins)) if wins else 0.0
    avg_loss = (sum(losses)/len(losses)) if losses else 0.0
    med_win = statistics.median(wins) if wins else 0.0
    med_loss = statistics.median(losses) if losses else 0.0
    mv_rev_cnt = sum(1 for x in reasons if "MV" in x)
    tp_cnt = sum(1 for x in reasons if "TP" in x)
    sl_cnt = sum(1 for x in reasons if x=="SL")
    return dict(
        n=n, wr=wr, avg_win=avg_win, avg_loss=avg_loss,
        med_win=med_win, med_loss=med_loss,
        mv_rev_cnt=mv_rev_cnt, tp_cnt=tp_cnt, sl_cnt=sl_cnt
    )

# ---------------- tuning ----------------
def _tune_for_symbol(sym: str, perf: List[Dict[str,Any]], lookback_days: int, min_trades: int) -> Optional[Dict[str,Any]]:
    BUF = _sym_env(sym, "LEVEL_BUFFER", _base(sym, "BUF", 12.0))
    BAND= _sym_env(sym, "ENTRY_BAND", _base(sym, "BAND", 3.0))
    TGT = _sym_env(sym, "TARGET_MIN_POINTS", _base(sym, "TGT", 30.0))
    TP  = _sym_env(sym, "TP_POINTS", _base(sym, "TP", 40.0))
    SL  = _sym_env(sym, "SL_POINTS", _base(sym, "SL", 20.0))
    TR  = _sym_env(sym, "TRAIL_TRIGGER_POINTS", _base(sym, "TR_TR", 25.0))
    TOF = _sym_env(sym, "TRAIL_OFFSET_POINTS",  _base(sym, "TR_OFF", 15.0))
    MVN = float(_env("MV_REV_CONFIRM","2") or "2")

    dates = _last_n_dates(perf, lookback_days, sym)
    rows = _filter_by(perf, sym, dates)
    if len(rows) < min_trades:
        log.info("[%s] Not enough trades in lookback (have %d, need %d) → skip tuning.", sym, len(rows), min_trades)
        return None

    st = _stats(rows)
    wr = st["wr"]; avg_win = st["avg_win"]; avg_loss = st["avg_loss"]
    med_win = st["med_win"]; med_loss = st["med_loss"]
    n = st["n"]; mv_rev_cnt = st["mv_rev_cnt"]; tp_cnt = st["tp_cnt"]; sl_cnt = st["sl_cnt"]

    steps = STEP.get(sym.upper(), STEP["NIFTY"])
    notes = []

    TRADES_PER_DAY = n / max(1, len(dates))
    if wr < 40.0 and avg_loss >= 0.6*TP:
        BUF = BUF + steps["BUF_UP"]
        SL = max(0.8*SL, SL - 0.05*TP)
        notes.append("WR<40 & loss>=0.6*TP → BUF↑, SL tighten")
    elif TRADES_PER_DAY < 0.6 and wr >= 50.0:
        BUF = max(1.0, BUF - steps["BUF_DN"])
        notes.append("Low trades/day & WR≥50 → BUF↓")

    if wr > 55.0 and avg_win >= 0.7*TP:
        TP = TP + steps["TP_UP"]
        notes.append("WR>55 & avg_win≥0.7*TP → TP↑")

    if med_win > 0:
        TR = min(TP - 5.0, max(0.5*SL, 0.6*med_win))
        TOF = max(0.4*TR, min(0.6*TR, TR - 5.0))
        notes.append("Trail tuned from median win")

    SL = min(SL, 0.7*TP)
    SL = max(SL, 0.4*TP)

    def rnd(x: float) -> float:
        return round(float(x), 2)
    BUF, BAND, TGT, TP, SL, TR, TOF, MVN = map(rnd, [BUF, BAND, TGT, TP, SL, TR, TOF, MVN])

    summary = f"n={n}, wr={wr:.1f}%, avgW={avg_win:.1f}, avgL={avg_loss:.1f}, medW={med_win:.1f}, medL={med_loss:.1f}, mvRev={mv_rev_cnt}, tp={tp_cnt}, sl={sl_cnt}"
    if not notes:
        notes.append("no major change")
    return dict(
        symbol=sym.upper(),
        LEVEL_BUFFER=BUF, ENTRY_BAND=BAND, TARGET_MIN_POINTS=TGT,
        TP_POINTS=TP, SL_POINTS=SL,
        TRAIL_TRIGGER_POINTS=TR, TRAIL_OFFSET_POINTS=TOF,
        MV_REV_CONFIRM=MVN,
        lookback_days=lookback_days,
        notes=summary + " | " + "; ".join(notes)
    )

# ---------------- read Performance (no auto-create) ----------------
ALT_PERF_NAMES = ["Performance","performance","PERFORMANCE","Perf","Trades","TRADES"]

def _read_performance() -> List[Dict[str,Any]]:
    name = _env("PERFORMANCE_SHEET_NAME","Performance")
    # first try explicit name (must-exist)
    try:
        ws = _open_ws_read_must(name)
        rows = ws.get_all_records() or []
        log.info("Performance sheet='%s' rows=%d", name, len(rows))
        return rows
    except Exception as e:
        log.warning("Preferred sheet '%s' not found (%s). Trying fallbacks...", name, e)

    # try fallbacks without creating
    for alt in [n for n in ALT_PERF_NAMES if n != name]:
        ws = _open_ws_read_optional(alt)
        if ws:
            rows = ws.get_all_records() or []
            log.info("Performance fallback sheet='%s' rows=%d", alt, len(rows))
            return rows

    # final: show helpful info
    sh, sid = _open_spreadsheet()
    names = [w.title for w in sh.worksheets()]
    raise RuntimeError(f"Performance sheet not found. Tried: {[name]+[n for n in ALT_PERF_NAMES if n!=name]}. "
                       f"Available in {sid}: {names}. Set PERFORMANCE_SHEET_NAME=... if needed.")

# ---------------- main ----------------
def run():
    symbols_csv = _env("EOD_TUNE_SYMBOLS") or _env("OC_SYMBOL","NIFTY")
    symbols = [s.strip().upper() for s in symbols_csv.split(",") if s.strip()]
    lookback = int(float(_env("EOD_LOOKBACK_DAYS","10")))
    min_trades = int(float(_env("EOD_MIN_TRADES","8")))
    dry = (_env("EOD_TUNER_DRY_RUN","0") in {"1","true","on","yes"})

    perf = _read_performance()
    if not perf:
        raise RuntimeError("Performance sheet is present but has no rows. Run backtest or live first.")

    today = _today_ist_str()
    wrote = 0
    for sym in symbols:
        rec = _tune_for_symbol(sym, perf, lookback, min_trades)
        if not rec:
            log.info("[%s] Skipped (insufficient data).", sym)
            continue
        row = {
            "date": today, "symbol": sym,
            "LEVEL_BUFFER": rec["LEVEL_BUFFER"],
            "ENTRY_BAND": rec["ENTRY_BAND"],
            "TARGET_MIN_POINTS": rec["TARGET_MIN_POINTS"],
            "TP_POINTS": rec["TP_POINTS"],
            "SL_POINTS": rec["SL_POINTS"],
            "TRAIL_TRIGGER_POINTS": rec["TRAIL_TRIGGER_POINTS"],
            "TRAIL_OFFSET_POINTS": rec["TRAIL_OFFSET_POINTS"],
            "MV_REV_CONFIRM": rec["MV_REV_CONFIRM"],
            "lookback_days": rec["lookback_days"],
            "src": "tuner-v1",
            "notes": rec["notes"],
        }
        log.info("[%s] EOD params: %s", sym, json.dumps(row, ensure_ascii=False))
        if not dry:
            _append_params_row(row)
            wrote += 1
        else:
            log.info("[DRY_RUN] Not writing to Params_Override")

    log.info("EOD Tuner done. rows_written=%d dry_run=%s", wrote, str(dry))
    return

if __name__ == "__main__":
    run()
