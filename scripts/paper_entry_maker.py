#!/usr/bin/env python3
"""
Paper Entry Maker
- Pulls live OC snapshot via analytics.oc_refresh_shim.get_refresh()
- Applies C1–C6 entry rules (strict)
- If eligible, appends an OPEN trade row to 'Trades' sheet (unless ENTRY_DRY_RUN=1)
- Supports --once or --loop N (seconds)

ENV (required):
  GOOGLE_SA_JSON                 -> service account json (string)
  GSHEET_TRADES_SPREADSHEET_ID   -> Google Sheet ID
  OC_SYMBOL                      -> e.g. NIFTY / BANKNIFTY / FINNIFTY

ENV (optional; defaults shown):
  ENTRY_DRY_RUN=1                -> 1=dry run (log only), 0=write rows
  LEVEL_BUFFER                   -> default: symbol map NIFTY=12, BANKNIFTY=30, FINNIFTY=15
  ENTRY_BAND=3                   -> trigger band (±pts) around shifted level
  TARGET_MIN_POINTS=30           -> space check (C6)
  OC_STALE_MAX_SEC=90            -> snapshot freshness gate (C4)
  NO_TRADE_WINDOWS="0915-0930,1445-1515"  -> IST time windows (inclusive) to block (C4)
  ONE_ATTEMPT_PER_LEVEL=1        -> dedupe per day per (symbol,side,level)
  MAX_TRADES_PER_DAY=0           -> 0=unlimited; else cap entries/day

Sheet 'Trades' expected headers:
  ['trade_id','signal_id','symbol','side','buy_ltp','exit_ltp','sl','tp',
   'basis','buy_time','exit_time','result','pnl','dedupe_hash','notes']

"""

from __future__ import annotations
import os, json, time, uuid, argparse, math, datetime as dt
from typing import Any, Dict, Optional, Tuple

# --- Sheets client ---
try:
    import gspread
except Exception as e:
    raise RuntimeError("gspread not installed") from e

# --- Live snapshot resolver ---
try:
    from analytics.oc_refresh_shim import get_refresh
except Exception as e:
    raise RuntimeError("analytics.oc_refresh_shim.get_refresh import failed") from e


# ---------- Helpers ----------

IST_OFFSET = dt.timedelta(hours=5, minutes=30)

def now_ist() -> dt.datetime:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc) + IST_OFFSET

def parse_windows(spec: str) -> list[Tuple[dt.time, dt.time]]:
    """Parse '0915-0930,1445-1515' into [(09:15,09:30),(14:45,15:15)]."""
    out = []
    if not spec:
        return out
    for chunk in spec.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            a, b = chunk.split("-")
            hh1, mm1 = int(a[:2]), int(a[2:])
            hh2, mm2 = int(b[:2]), int(b[2:])
            out.append((dt.time(hh1, mm1), dt.time(hh2, mm2)))
        except Exception:
            continue
    return out

def in_block_windows(t: dt.datetime, windows: list[Tuple[dt.time, dt.time]]) -> bool:
    cur = t.time()
    for a, b in windows:
        if a <= cur <= b:
            return True
    return False

def env_float(name: str, default: float) -> float:
    v = os.environ.get(name)
    if v is None or v == "":
        return default
    try:
        return float(v)
    except Exception:
        return default

def env_int(name: str, default: int) -> int:
    v = os.environ.get(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except Exception:
        return default

def symbol_default_buffer(symbol: str) -> float:
    s = (symbol or "").upper()
    if "BANK" in s:
        return 30.0
    if "FIN" in s:
        return 15.0
    return 12.0  # NIFTY default

def mv_allows(side: str, mv: Optional[str]) -> bool:
    if not mv:
        return False
    mvl = mv.lower()
    if side == "CE":
        # CE allowed when bullish/big move
        return ("bullish" in mvl) or ("big_move" in mvl) or ("big move" in mvl)
    if side == "PE":
        # PE allowed when bearish/strong_bearish
        return ("bearish" in mvl)
    return False

def oi_supports(side: str, ce_delta: Optional[float], pe_delta: Optional[float]) -> bool:
    """
    C3 supportive patterns:
    CE:  CEΔ↓ & PEΔ↑  OR  CEΔ↓ & PEΔ↓  OR  CEΔ↔/↓ & PEΔ↑
    PE:  CEΔ↑ & PEΔ↓  OR  CEΔ↓ & PEΔ↓  OR  CEΔ↑ & PEΔ↔/↓
    We'll treat:
      x↓: x < 0
      x↔: near 0 (|x| < eps)
      x↑: x > 0
    """
    def sign(x: Optional[float]) -> int:
        if x is None: return 0
        if abs(x) < 1e-9: return 0
        return -1 if x < 0 else 1

    c = sign(ce_delta)
    p = sign(pe_delta)

    if side == "CE":
        return (c < 0 and p > 0) or (c < 0 and p < 0) or (c <= 0 and p >= 0)
    if side == "PE":
        return (c > 0 and p < 0) or (c < 0 and p < 0) or (c >= 0 and p <= 0)
    return False

def nearest_entry_side_and_trigger(spot: float, s1: float, s2: float, r1: float, r2: float,
                                   buf: float, band: float, mv: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[float], Optional[float]]:
    """
    Compute shifted levels and decide which trigger is 'near' (within band) and consistent with MV.
    Returns (side, basis, price, space_points_target_side)
      basis ∈ {"S1*", "S2*", "R1*", "R2*"}
      price = shifted level
    Space calculation later uses basis to pick opposite barrier.
    """
    s1s = s1 - buf
    s2s = s2 - buf
    r1s = r1 + buf
    r2s = r2 + buf

    cand: list[Tuple[str, str, float]] = []  # (side, basis, level_price)
    # For CE: S1*, S2*
    cand.append(("CE", "S1*", s1s))
    cand.append(("CE", "S2*", s2s))
    # For PE: R1*, R2*
    cand.append(("PE", "R1*", r1s))
    cand.append(("PE", "R2*", r2s))

    # Filter by band first (C1: NEAR/CROSS)
    near = [(sd, bs, lv) for (sd, bs, lv) in cand if abs(spot - lv) <= band]

    if not near:
        return (None, None, None, None)

    # If multiple, pick closest
    near.sort(key=lambda t: abs(spot - t[2]))
    side, basis, price = near[0]

    # MV agreement (C2)
    if not mv_allows(side, mv):
        return (None, None, None, None)

    # Space (C6): require min points to next opposite barrier
    return (side, basis, price, None)

def space_ok(basis: str, price: float, s1: float, s2: float, r1: float, r2: float, target_min: float, side: str) -> Tuple[bool, float]:
    """
    C6: enough room to target
    CE @ S1* or S2*: space to R1
    PE @ R1* or R2*: space to S1
    """
    if side == "CE":
        space = (r1 - price)
    else:
        space = (price - s1)
    return (space >= target_min, space)

def day_key(dt_ist: dt.datetime) -> str:
    return dt_ist.strftime("%Y-%m-%d")

def dedupe_hash(symbol: str, side: str, basis: str, price: float, dkey: str) -> str:
    core = f"{dkey}:{symbol}:{side}:{basis}:{price:.2f}"
    return uuid.uuid5(uuid.NAMESPACE_DNS, core).hex[:12]

def trades_today_count(ws, dkey: str) -> int:
    # naive count by buy_time prefix
    vals = ws.get_all_values()
    if not vals or len(vals) < 2:
        return 0
    hdr = vals[0]
    rows = vals[1:]
    try:
        col_buy_time = hdr.index("buy_time")
    except ValueError:
        return 0
    c = 0
    for r in rows:
        if len(r) > col_buy_time and r[col_buy_time].startswith(dkey):
            c += 1
    return c

def row_exists_dedupe(ws, dhash: str) -> bool:
    vals = ws.get_all_values()
    if not vals or len(vals) < 2:
        return False
    hdr = vals[0]
    rows = vals[1:]
    try:
        col_dh = hdr.index("dedupe_hash")
    except ValueError:
        return False
    for r in rows:
        if len(r) > col_dh and r[col_dh] == dhash:
            return True
    return False


# ---------- Core run ----------

def load_params(symbol: str) -> Dict[str, Any]:
    buf = env_float("LEVEL_BUFFER", symbol_default_buffer(symbol))
    band = env_float("ENTRY_BAND", 3.0)
    tgt = env_float("TARGET_MIN_POINTS", 30.0)
    stale_max = env_int("OC_STALE_MAX_SEC", 90)
    windows = os.environ.get("NO_TRADE_WINDOWS", "0915-0930,1445-1515")
    one_attempt = env_int("ONE_ATTEMPT_PER_LEVEL", 1)
    max_day = env_int("MAX_TRADES_PER_DAY", 0)
    return dict(
        LEVEL_BUFFER=buf,
        ENTRY_BAND=band,
        TARGET_MIN_POINTS=tgt,
        OC_STALE_MAX_SEC=stale_max,
        NO_TRADE_WINDOWS=windows,
        ONE_ATTEMPT_PER_LEVEL=one_attempt,
        MAX_TRADES_PER_DAY=max_day,
    )

def decide_and_maybe_write(snap: Dict[str, Any], ws, symbol: str, dry_run: bool) -> Optional[Dict[str, Any]]:
    # Basic fields
    spot = float(snap.get("spot") or 0.0)
    s1 = float(snap.get("s1") or 0.0)
    s2 = float(snap.get("s2") or 0.0)
    r1 = float(snap.get("r1") or 0.0)
    r2 = float(snap.get("r2") or 0.0)
    mv = snap.get("mv")  # string or None
    ce_d = snap.get("ce_oi_delta")
    pe_d = snap.get("pe_oi_delta")
    age = int(snap.get("age_sec") or 0)

    P = load_params(symbol)
    buf = P["LEVEL_BUFFER"]
    band = P["ENTRY_BAND"]
    target_min = P["TARGET_MIN_POINTS"]
    stale_max = P["OC_STALE_MAX_SEC"]
    windows = parse_windows(P["NO_TRADE_WINDOWS"])
    one_attempt = bool(P["ONE_ATTEMPT_PER_LEVEL"])
    max_day = int(P["MAX_TRADES_PER_DAY"])

    now = now_ist()
    dkey = day_key(now)

    # C4: freshness + time window
    if age > stale_max:
        print(f"[ENTRY] Block: stale snapshot age={age}s > {stale_max}s")
        return None
    if in_block_windows(now, windows):
        print(f"[ENTRY] Block: within no-trade window ({P['NO_TRADE_WINDOWS']}) IST={now.time()}")
        return None

    # C1 + C2 (near/cross + MV gate)
    side, basis, price, _ = nearest_entry_side_and_trigger(
        spot, s1, s2, r1, r2, buf, band, mv
    )
    if not side:
        print("[ENTRY] C1/C2 fail: no near trigger or MV mismatch")
        return None

    # C3 OI confirmation
    if not oi_supports(side, ce_d, pe_d):
        print(f"[ENTRY] C3 fail: OI pattern not supportive (side={side}, CEΔ={ce_d}, PEΔ={pe_d})")
        return None

    # C6 space
    ok_space, space_pts = space_ok(basis, float(price), s1, s2, r1, r2, target_min, side)
    if not ok_space:
        print(f"[ENTRY] C6 fail: space {space_pts:.2f} < target {target_min:.0f}")
        return None

    # C5 hygiene: per-day cap & dedupe
    if max_day > 0:
        count = trades_today_count(ws, dkey)
        if count >= max_day:
            print(f"[ENTRY] C5 block: daily cap hit ({count}/{max_day})")
            return None

    # Dedupe per level/side/day
    dh = dedupe_hash(symbol, side, basis, float(price), dkey)
    if one_attempt and row_exists_dedupe(ws, dh):
        print(f"[ENTRY] C5 dedupe: already attempted {basis} {price:.2f} for {symbol} today")
        return None

    # All good → append row
    buy_ltp = float(price)
    trade_id = uuid.uuid4().hex[:8]
    row = [
        trade_id,           # trade_id
        "AUTO",             # signal_id
        symbol,             # symbol
        side,               # side ('CE' / 'PE')
        buy_ltp,            # buy_ltp
        "",                 # exit_ltp (blank)
        "",                 # sl (optional)
        "",                 # tp (optional)
        basis,              # basis ('S1*' / 'S2*' / 'R1*' / 'R2*')
        now.strftime("%Y-%m-%d %H:%M:%S"),  # buy_time (IST)
        "",                 # exit_time
        "",                 # result
        "",                 # pnl (formula or watcher fills)
        dh,                 # dedupe_hash
        f"C1–C6 OK | mv={mv} | space≈{space_pts:.0f}"  # notes
    ]

    if dry_run:
        print("[ENTRY][DRY] Would append:", row)
    else:
        ws.append_row(row, value_input_option="USER_ENTERED")
        print("[ENTRY] Appended:", row)

    return dict(
        trade_id=trade_id, side=side, basis=basis, buy_ltp=buy_ltp,
        notes=row[-1]
    )


def run_once(symbol: str, dry_run: bool) -> None:
    # Sheets
    sa = json.loads(os.environ["GOOGLE_SA_JSON"])
    gc = gspread.service_account_from_dict(sa)
    sh = gc.open_by_key(os.environ["GSHEET_TRADES_SPREADSHEET_ID"])
    ws = sh.worksheet("Trades")

    # Live snapshot
    snap = None
    try:
        snap = os.environ.get("TELEGRAM_DISABLED")  # just to silence lints
        # resolve live
        import asyncio
        snap = asyncio.run(get_refresh()({}))
    except Exception as e:
        print("[ENTRY] snapshot error:", e)
        return

    if not snap or snap.get("status") != "ok":
        print("[ENTRY] snapshot not ok:", snap.get("status") if isinstance(snap, dict) else "None")
        return

    decide_and_maybe_write(snap, ws, symbol, dry_run)


def run_loop(symbol: str, secs: int, dry_run: bool) -> None:
    print(f"[paper_entry_maker] loop every {secs}s, dry_run={dry_run}, symbol={symbol}")
    while True:
        try:
            run_once(symbol, dry_run)
        except Exception as e:
            print("[ENTRY] loop error:", e)
        time.sleep(secs)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="single pass")
    ap.add_argument("--loop", type=int, default=0, help="seconds")
    args = ap.parse_args()

    symbol = os.environ.get("OC_SYMBOL", "NIFTY").upper()
    dry_run = os.environ.get("ENTRY_DRY_RUN", "1") != "0"

    if args.once or not args.loop:
        print("[paper_entry_maker] starting once...")
        run_once(symbol, dry_run)
        print("[paper_entry_maker] once done")
        return

    run_loop(symbol, args.loop, dry_run)


if __name__ == "__main__":
    main()
