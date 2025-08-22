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
#   PERFORMANCE_SHEET_NAME  = Performance (override if needed)
#
# Sheets env:
#   GOOGLE_SA_JSON, GSHEET_TRADES_SPREADSHEET_ID
#
# Output Sheet: Params_Override (wide row per run)
# -----------------------------------------------------------------------------

from __future__ import annotations

import os, json, time, logging, statistics, re
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

# ---------------- date parsing helpers ----------------
_DATE_REs = [
    # 2025-08-22 14:05:26, 2025-08-22
    re.compile(r"(\d{4})[-/](\d{2})[-/](\d{2})"),
    # 22/08/2025 or 22-08-2025
    re.compile(r"(\d{2})[-/](\d{2})[-/](\d{4})"),
    # 22-08-25
    re.compile(r"(\d{2})[-/](\d{2})[-/](\d{2})"),
]

def _parse_date_str(s: str) -> Optional[str]:
    if not s: return None
    txt = s.strip()
    for pat in _DATE_REs:
        m = pat.search(txt)
        if not m: 
            continue
        g = m.groups()
        if len(g)==3 and len(g[0])==4:
            y,mo,da = g
            return f"{int(y):04d}-{int(mo):02d}-{int(da):02d}"
        if len(g)==3 and len(g[2])==4:
            da,mo,y = g
            return f"{int(y):04d}-{int(mo):02d}-{int(da):02d}"
        if len(g)==3 and len(g[2])==2:
            da,mo,y2 = g
            y = 2000 + int(y2)
            return f"{int(y):04d}-{int(mo):02d}-{int(da):02d}"
    return None

# ---------------- core stats ----------------
def _last_n_dates(records: List[Dict[str,Any]], n: int, sym: str) -> List[str]:
    seen = []
    S = sym.upper()
    for r in records:
        rs = str(r.get("symbol") or r.get("Symbol") or "").upper()
        if rs and rs != S:
            continue
        d = (r.get("date") or r.get("Date") or "")
        d = str(d).strip()
        if not d:
            continue
        if d not in seen:
            seen.append(d)
    if seen:
        return seen[-n:] if len(seen) >= n else seen
    # Fallback: no date columns at all -> use ALL bucket
    return ["ALL"]

def _filter_by(records: List[Dict[str,Any]], sym: str, dates: List[str]) -> List[Dict[str,Any]]:
    if dates == ["ALL"]:
        # don't filter by date; only by symbol (if present)
        out = []
        S = sym.upper()
        for r in records:
            rs = str(r.get("symbol") or r.get("Symbol") or "").upper()
            if rs and rs != S:
                continue
            out.append(r)
        return out
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
        p = _num(r.get("pnl_points") or r.get("PNL") or r.get("Net PnL") or r.get("NetPNL") or r.get("Profit") or r.get("Net P&L"))
        if p is None: continue
        pnls.append(p)
        if p >= 0:
            wins.append(p)
        else:
            losses.append(-p)
        er = str(r.get("exit_reason") or r.get("ExitReason") or r.get("Exit Reason") or r.get("reason") or r.get("Reason") or "").strip().upper()
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

    TRADES_PER_DAY = n / max(1, len(dates) if dates != ["ALL"] else 1)
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

# -------------- dupe/variant-safe Performance readers --------------
ALT_PERF_NAMES = ["Performance","performance","PERFORMANCE","Perf","Trades","TRADES"]

DATE_CANDS   = ["date","Date","trade_date","Trade Date","entry_time","Entry Time","open_time","Open Time",
                "entry_ts","EntryTS","exit_time","Exit Time","close_time","Close Time","timestamp","Timestamp","time","Time"]
SYMBOL_CANDS = ["symbol","Symbol","underlying","Underlying","index","Index","instrument","Instrument","ticker","Ticker","base","Base"]
PNL_CANDS    = ["pnl_points","PNL","Net PnL","NetPNL","Profit","Net P&L"]
EXIT_CANDS   = ["exit_reason","ExitReason","Exit Reason","reason","Reason"]

def _dupe_safe_from_ws(ws) -> List[Dict[str,Any]]:
    """Handle non-unique headers by manual mapping + robust date/symbol fallback."""
    vals = ws.get_all_values()
    if not vals or len(vals) < 2:
        return []
    header = [ (c or "").strip() for c in vals[0] ]
    rows = vals[1:]

    def _find_idx(cands: List[str]) -> Optional[int]:
        for cand in cands:
            cl = cand.strip().lower()
            for i,h in enumerate(header):
                if (h or "").strip().lower() == cl:
                    return i
        return None

    idx_date  = _find_idx(DATE_CANDS)
    idx_sym   = _find_idx(SYMBOL_CANDS)
    idx_pnl   = _find_idx(PNL_CANDS)
    idx_exit  = _find_idx(EXIT_CANDS)

    out: List[Dict[str,Any]] = []
    for r in rows:
        if not any((x.strip() if isinstance(x,str) else "") for x in r):
            continue
        rec: Dict[str,Any] = {}

        # 1) PNL
        if idx_pnl is not None and idx_pnl < len(r):
            rec["pnl_points"] = r[idx_pnl]

        # 2) SYMBOL (fallback to env)
        sym_val = None
        if idx_sym is not None and idx_sym < len(r):
            sym_val = r[idx_sym]
        if not sym_val:
            sym_val = _env("OC_SYMBOL","NIFTY")
        rec["symbol"] = (str(sym_val or "")).strip().upper()

        # 3) DATE — try direct; else try from any time column; else today
        date_val = None
        if idx_date is not None and idx_date < len(r):
            date_val = _parse_date_str(str(r[idx_date]))
        if not date_val:
            # scan other time-like columns in same row
            for i, h in enumerate(header):
                hn = (h or "").strip()
                if not hn: 
                    continue
                if any(k in hn.lower() for k in ["time","date","timestamp","ts"]):
                    date_val = _parse_date_str(str(r[i] if i < len(r) else ""))
                    if date_val:
                        break
        if not date_val:
            # extreme fallback: today IST (keeps tuner functional)
            date_val = _today_ist_str()
        rec["date"] = date_val

        # 4) exit reason (optional)
        if idx_exit is not None and idx_exit < len(r):
            rec["exit_reason"] = r[idx_exit]

        # keep raw too for safety:
        for i,h in enumerate(header):
            key = (h or f"col{i+1}").strip()
            if key and i < len(r):
                rec[key] = r[i]
        out.append(rec)

    log.info("Performance dupe-safe parsed rows=%d (idx: date=%s, symbol=%s, pnl=%s, exit=%s)", 
             len(out), idx_date, idx_sym, idx_pnl, idx_exit)
    return out

def _read_performance_from_name(name: str) -> List[Dict[str,Any]]:
    ws = _open_ws_read_must(name)
    try:
        rows = ws.get_all_records() or []
        if rows:
            log.info("Performance sheet='%s' rows=%d (records)", name, len(rows))
            return rows
        else:
            # if empty by records, try dupe-safe map from values
            log.info("Performance sheet='%s' empty via get_all_records() → trying dupe-safe", name)
            return _dupe_safe_from_ws(ws)
    except Exception as e:
        if "header" in str(e).lower() and "unique" in str(e).lower():
            log.warning("Sheet '%s' header not unique → using dupe-safe reader", name)
            return _dupe_safe_from_ws(ws)
        # any other error: re-raise
        raise

def _read_performance() -> List[Dict[str,Any]]:
    pref = _env("PERFORMANCE_SHEET_NAME","Performance")
    # Try preferred name
    try:
        return _read_performance_from_name(pref)
    except Exception as e:
        log.warning("Preferred sheet '%s' not usable (%s). Trying fallbacks...", pref, e)

    # Try fallbacks
    for alt in [n for n in ALT_PERF_NAMES if n != pref]:
        ws = _open_ws_read_optional(alt)
        if ws:
            try:
                rows = ws.get_all_records() or []
                if rows:
                    log.info("Performance fallback sheet='%s' rows=%d (records)", alt, len(rows))
                    return rows
                else:
                    log.info("Performance fallback sheet='%s' empty via records → dupe-safe", alt)
                    return _dupe_safe_from_ws(ws)
            except Exception as e:
                if "header" in str(e).lower() and "unique" in str(e).lower():
                    log.warning("Fallback sheet '%s' header not unique → using dupe-safe reader", alt)
                    return _dupe_safe_from_ws(ws)
                else:
                    log.warning("Fallback sheet '%s' read error: %s", alt, e)

    # final: show helpful info
    sh, sid = _open_spreadsheet()
    names = [w.title for w in sh.worksheets()]
    raise RuntimeError(f"Performance sheet not found/usable. Tried: {[pref]+[n for n in ALT_PERF_NAMES if n!=pref]}. "
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
