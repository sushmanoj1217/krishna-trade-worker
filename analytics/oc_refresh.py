# analytics/oc_refresh.py
# ------------------------------------------------------------
# Public API:
#   - async refresh_once(*args, **kwargs) -> dict {status, reason, snapshot, provider}
#   - get_snapshot(), set_snapshot()
#
# This version:
#   - Provider snapshot पर HOLD/daily_cap flags merge करता है (env/sheet)
#   - MV हमेशा derive करता है (PCR/MaxPain; tie-break via OIΔ)
#   - Summary हमेशा भरता है और **aliases** भी देता है:
#       summary, summary_text, summary_line, summary_str, final_summary
#   - Sheets fallback safe; expiry < today (IST) या age > OC_MAX_SNAPSHOT_AGE_SEC ⇒ STALE
#   - snapshot["c5_reason"] = "OK" / "HOLD" / "DailyCap"
# ------------------------------------------------------------
from __future__ import annotations
import importlib, inspect, logging, re, time, json, os
from typing import Any, Optional, Dict, Tuple, List

try:
    import gspread  # type: ignore
except Exception:
    gspread = None  # type: ignore

_log = logging.getLogger(__name__)
_SNAPSHOT: Optional[dict] = None

# ---------------- Public snapshot API ----------------
def set_snapshot(snap: dict) -> None:
    global _SNAPSHOT
    if isinstance(snap, dict):
        _SNAPSHOT = snap

def get_snapshot() -> Optional[dict]:
    return _SNAPSHOT

# ---------------- small utils ----------------
def _env(name: str) -> Optional[str]:
    v = os.environ.get(name)
    return v.strip() if v and v.strip() else None

def _to_float(x):
    try:
        if x in (None, "", "—"): return None
        return float(str(x).replace(",", "").strip())
    except Exception:
        return None

def _norm_key(k: str) -> str:
    s = str(k).lower()
    s = s.replace("Δ", "delta").replace("∆", "delta")
    s = re.sub(r"[\s\-\.\(\)\[\]/]+", "_", s)
    s = re.sub(r"__+", "_", s).strip("_")
    return s

def _fmt_ist(epoch_utc: Optional[int]) -> str:
    if not epoch_utc: return ""
    t = epoch_utc + int(5.5 * 3600)
    return time.strftime("%Y-%m-%d %H:%M:%S IST", time.gmtime(t))

def _today_ist_ymd() -> Tuple[int,int,int]:
    t = time.time() + 5.5*3600
    return int(time.strftime("%Y", time.gmtime(t))), int(time.strftime("%m", time.gmtime(t))), int(time.strftime("%d", time.gmtime(t)))

def _parse_ymd(s: str) -> Optional[Tuple[int,int,int]]:
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", s.strip())
    if not m: return None
    return int(m.group(1)), int(m.group(2)), int(m.group(3))

def _ymd_lt(a: Tuple[int,int,int], b: Tuple[int,int,int]) -> bool:
    return a < b

# ---------------- flags (env + sheet) ----------------
def _truthy(x: Any) -> Optional[bool]:
    if x is None: return None
    s = str(x).strip().lower()
    if s in {"1","true","yes","y","on","t"}: return True
    if s in {"0","false","no","n","off","f"}: return False
    return None

def _open_by_key():
    if gspread is None:
        raise RuntimeError("gspread not installed")
    raw = _env("GOOGLE_SA_JSON"); sid = _env("GSHEET_TRADES_SPREADSHEET_ID")
    if not raw or not sid:
        raise RuntimeError("Sheets env missing")
    sa = json.loads(raw); gc = gspread.service_account_from_dict(sa)
    return gc.open_by_key(sid)

def _open_ws(name: str):
    sh = _open_by_key()
    return sh.worksheet(name)

def _read_params_override() -> Dict[str, bool]:
    out = {"hold": False, "daily_cap_hit": False}
    try:
        ws = _open_ws("Params_Override")
        rows = ws.get_all_records() or []
    except Exception:
        return out
    if not rows: return out
    last = {_norm_key(k): v for k, v in rows[-1].items()}
    for k in ("hold","system_hold","manual_hold"):
        if k in last:
            tv = _truthy(last[k])
            if tv is not None: out["hold"] = tv; break
    for k in ("daily_cap_hit","daily_cap","cap_hit"):
        if k in last:
            tv = _truthy(last[k])
            if tv is not None: out["daily_cap_hit"] = tv; break
    return out

def _read_override_flags_env() -> Dict[str, bool]:
    out = {"hold_set": False, "cap_set": False, "hold": False, "daily_cap_hit": False}
    hv = _env("HOLD_OVERRIDE") or _env("SYSTEM_HOLD") or _env("HOLD")
    cv = _env("DAILY_CAP_HIT") or _env("DAILY_CAP") or _env("CAP_HIT")
    if hv is not None:
        out["hold_set"] = True
        tv = _truthy(hv)
        if tv is not None: out["hold"] = tv
    if cv is not None:
        out["cap_set"] = True
        tv = _truthy(cv)
        if tv is not None: out["daily_cap_hit"] = tv
    return out

# ---------------- MV + Summary helpers ----------------
def _derive_mv(pcr: Optional[float], mp: Optional[float], spot: Optional[float],
               ce_d: Optional[float], pe_d: Optional[float]) -> str:
    score = 0
    try:
        if isinstance(pcr,(int,float)):
            score += 1 if float(pcr) >= 1.0 else -1
    except Exception: pass
    try:
        if isinstance(mp,(int,float)) and isinstance(spot,(int,float)):
            score += 1 if float(mp) > float(spot) else -1
    except Exception: pass
    if score > 0: return "bullish"
    if score < 0: return "bearish"
    # tie → OIΔ tiebreak (PEΔ>CEΔ ⇒ bullish, else bearish)
    if isinstance(ce_d,(int,float)) and isinstance(pe_d,(int,float)) and ce_d != pe_d:
        return "bullish" if pe_d > ce_d else "bearish"
    return ""  # truly unknown

def _ensure_mv(snap: Dict[str, Any]) -> None:
    mv = str(snap.get("mv") or "").strip().lower()
    if not mv:
        pcr = _to_float(snap.get("pcr"))
        mp  = _to_float(snap.get("max_pain"))
        spot= _to_float(snap.get("spot"))
        ce_d= _to_float(snap.get("ce_oi_delta"))
        pe_d= _to_float(snap.get("pe_oi_delta"))
        mv2 = _derive_mv(pcr, mp, spot, ce_d, pe_d)
        snap["mv"] = mv2 or ""  # might still be "" if all missing

def _build_summary(s: Dict[str, Any]) -> str:
    # HARD guards first
    if s.get("stale"):
        rs = s.get("stale_reason") or []
        reason = "; ".join(rs) if rs else "stale"
        return f"⚠️ STALE DATA — live mismatch; no trade. (Reasons: {reason})"
    if s.get("hold") or s.get("daily_cap_hit"):
        tags = []
        if s.get("hold"): tags.append("HOLD")
        if s.get("daily_cap_hit"): tags.append("DailyCap")
        return f"🚫 System {' & '.join(tags)} — no trade."

    # Soft decision using MV + OIΔ alignment
    mv = (s.get("mv") or "").strip().lower()
    pcr = _to_float(s.get("pcr"))
    mp  = _to_float(s.get("max_pain"))
    spot= _to_float(s.get("spot"))
    ce_d= _to_float(s.get("ce_oi_delta"))
    pe_d= _to_float(s.get("pe_oi_delta"))

    # If MV unknown, try derive again
    if not mv:
        mv = _derive_mv(pcr, mp, spot, ce_d, pe_d)

    # OIΔ alignment (our C3 proxy)
    c3_ok = None
    if isinstance(ce_d,(int,float)) and isinstance(pe_d,(int,float)):
        if mv == "bearish":
            c3_ok = (ce_d > 0 and pe_d <= 0)
        elif mv == "bullish":
            c3_ok = (pe_d > 0 and ce_d <= 0)

    # PCR/MP gate proxy (our C2 proxy)
    c2_ok = None
    if isinstance(pcr,(int,float)) and isinstance(mp,(int,float)) and isinstance(spot,(int,float)):
        if mv == "bearish":
            c2_ok = (pcr < 1.0 and mp <= spot)
        elif mv == "bullish":
            c2_ok = (pcr >= 1.0 and mp >= spot)

    side = None; level = None
    if mv == "bearish":
        side, level = "CE", "S1*"
    elif mv == "bullish":
        side, level = "PE", "R1*"

    fails: List[str] = []
    if c2_ok is False: fails.append("C2")
    if c3_ok is False: fails.append("C3")

    if mv and (c2_ok is True) and (c3_ok is True):
        return f"✅ Eligible — {side} @ {level}"
    if mv:
        if fails:
            return f"❌ Not eligible — failed: {', '.join(fails)}"
        return f"⏳ Bias: {mv} — waiting for OIΔ/PCR alignment"
    return "❔ Insufficient data — waiting for live feed"

def _apply_summary_aliases(s: Dict[str, Any]) -> None:
    """Write the same summary into multiple commonly-seen keys so any renderer picks it up."""
    txt = s.get("summary") or ""
    for k in ("summary_text", "summary_line", "summary_str", "final_summary"):
        s[k] = txt

# ---------------- Sheets fallback builder ----------------
def _open_oc_ws():
    sh = _open_by_key()
    try:
        return sh.worksheet("OC_Live")
    except Exception:
        return sh.worksheet("Snapshots")

def _extract_asof_epoch(row: Dict[str, Any]) -> Optional[int]:
    cand = None
    for k, v in row.items():
        nk = _norm_key(k)
        if nk in {"ts","timestamp","time","asof","as_of","updated_at","last_update","last_updated"}:
            cand = v; break
    if cand is None: return None
    try:
        s = str(cand).strip()
        if re.fullmatch(r"[0-9]+", s):
            x = int(s); 
            if x > 10_000_000_000: x//=1000
            return x
        for f in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                tm = time.strptime(s, f); return int(time.mktime(tm))
            except Exception: pass
    except Exception:
        return None
    return None

def _read_oc_rows() -> Optional[List[dict]]:
    try:
        ws = _open_oc_ws()
        return ws.get_all_records()
    except Exception as e:
        _log.warning("oc_refresh: sheets read failed: %s", e)
        return None

def _build_from_sheet() -> Optional[dict]:
    rows = _read_oc_rows()
    if not rows: return None
    last = rows[-1]; prev = rows[-2] if len(rows) >= 2 else None
    norm = lambda d: {_norm_key(k): v for k, v in (d or {}).items()}
    lastn, prevn = norm(last), norm(prev)

    sym = (lastn.get("symbol") or lastn.get("sym") or _env("OC_SYMBOL") or "").upper()
    exp = lastn.get("expiry") or lastn.get("exp") or ""
    spot= _to_float(lastn.get("spot"))
    s1  = _to_float(lastn.get("s1")); s2 = _to_float(lastn.get("s2"))
    r1  = _to_float(lastn.get("r1")); r2 = _to_float(lastn.get("r2"))
    pcr = _to_float(lastn.get("pcr")); mp = _to_float(lastn.get("max_pain"))
    ce_d= _to_float(lastn.get("ce_oi_delta")); pe_d = _to_float(lastn.get("pe_oi_delta"))

    # flags
    f_sheet = _read_params_override()
    f_env   = _read_override_flags_env()
    hold = f_env["hold"] if f_env["hold_set"] else f_sheet.get("hold", False)
    cap  = f_env["daily_cap_hit"] if f_env["cap_set"] else f_sheet.get("daily_cap_hit", False)

    # staleness
    stale = False; reasons: List[str] = []
    if exp:
        eymd = _parse_ymd(str(exp)); today = _today_ist_ymd()
        if eymd and _ymd_lt(eymd, today):
            stale = True; reasons.append(f"expiry {exp} < today {today[0]:04d}-{today[1]:02d}-{today[2]:02d}")
    max_age = int(_env("OC_MAX_SNAPSHOT_AGE_SEC") or "300")
    asof_epoch = _extract_asof_epoch(last)
    age_sec = None; asof_str = ""
    if asof_epoch:
        now_utc = int(time.time())
        age_sec = max(0, now_utc - asof_epoch)
        if age_sec > max_age: stale = True; reasons.append(f"age>{max_age}s")
        asof_str = _fmt_ist(asof_epoch)

    snap = {
        "symbol": sym, "expiry": exp, "spot": spot,
        "s1": s1, "s2": s2, "r1": r1, "r2": r2,
        "pcr": pcr, "max_pain": mp,
        "ce_oi_delta": ce_d, "pe_oi_delta": pe_d,
        "source": "sheets", "ts": int(time.time()),
        "asof": asof_str, "age_sec": age_sec,
        "stale": stale, "stale_reason": reasons,
        "hold": bool(hold), "daily_cap_hit": bool(cap),
    }
    _ensure_mv(snap)
    snap["summary"] = _build_summary(snap)
    _apply_summary_aliases(snap)
    snap["c5_reason"] = "HOLD" if snap["hold"] else ("DailyCap" if snap["daily_cap_hit"] else "OK")
    return snap

# ---------------- provider discovery ----------------
_MODULE_CANDIDATES = [
    "providers.dhan_oc",
    "providers.oc",
]
_FN_CAND_NAMES = [
    "refresh_once","refresh_now","run_once","refresh","do_refresh",
    "refresh_tick","refresh_snapshot","oc_refresh","fetch_levels",
    "get_oc_snapshot","compute_levels","compute_snapshot","build_snapshot","get_levels",
]

def _discover_provider():
    for mname in _MODULE_CANDIDATES:
        try:
            m = importlib.import_module(mname)
        except Exception:
            continue
        for fnm in _FN_CAND_NAMES:
            if hasattr(m, fnm) and callable(getattr(m, fnm)):
                fn = getattr(m, fnm)
                _log.info("oc_refresh: provider %s.%s selected (async=%s)", mname, fnm, inspect.iscoroutinefunction(fn))
                return fn, f"{mname}.{fnm}", inspect.iscoroutinefunction(fn)
    return None, "", False

_PROVIDER_FN, _PROVIDER_NAME, _PROVIDER_IS_ASYNC = _discover_provider()

async def _call_provider(fn, is_async: bool):
    res = fn()
    if inspect.isawaitable(res):
        res = await res
    return res

# ---------------- main entry ----------------
async def refresh_once(*args, **kwargs) -> dict:
    status = "ok"; reason = ""; snap: Optional[dict] = None

    if _PROVIDER_FN is not None:
        try:
            ret = await _call_provider(_PROVIDER_FN, _PROVIDER_IS_ASYNC)
            if isinstance(ret, dict) and isinstance(ret.get("snapshot"), dict):
                psnap = ret["snapshot"]
            elif isinstance(ret, dict):
                psnap = ret
            else:
                psnap = getattr(ret, "snapshot", None)

            if isinstance(psnap, dict):
                snap = dict(psnap)
                snap.setdefault("source", "provider")
                # ts → age/asof
                ts = snap.get("ts")
                try:
                    ts_epoch = int(ts) if ts is not None else None
                    if ts_epoch and ts_epoch > 10_000_000_000: ts_epoch//=1000
                except Exception:
                    ts_epoch = None
                if ts_epoch:
                    snap["age_sec"] = max(0, int(time.time()) - ts_epoch)
                    snap["asof"] = _fmt_ist(ts_epoch)
                snap.setdefault("age_sec", 0)
                snap.setdefault("asof", snap.get("asof",""))

                # merge flags
                flags_sheet = _read_params_override()
                flags_env   = _read_override_flags_env()
                hold = flags_env["hold"] if flags_env["hold_set"] else flags_sheet.get("hold", False)
                cap  = flags_env["daily_cap_hit"] if flags_env["cap_set"] else flags_sheet.get("daily_cap_hit", False)
                snap["hold"] = bool(hold); snap["daily_cap_hit"] = bool(cap)

                # MV ensure + stale check
                _ensure_mv(snap)
                exp_s = str(snap.get("expiry") or "").strip()
                if exp_s:
                    eymd = _parse_ymd(exp_s); today = _today_ist_ymd()
                    if eymd and _ymd_lt(eymd, today):
                        snap["stale"] = True
                        snap.setdefault("stale_reason", []).append(
                            f"expiry {exp_s} < today {today[0]:04d}-{today[1]:02d}-{today[2]:02d}"
                        )
                snap.setdefault("stale", False)
                snap.setdefault("stale_reason", [])

                # Build summary + aliases + explicit C5 text
                snap["summary"] = _build_summary(snap)
                _apply_summary_aliases(snap)
                snap["c5_reason"] = "HOLD" if snap["hold"] else ("DailyCap" if snap["daily_cap_hit"] else "OK")
        except Exception as e:
            status, reason = "provider_error", str(e)

    if snap is None:
        s2 = _build_from_sheet()
        if s2 is not None:
            snap = s2
            if status == "ok" and reason == "":
                status, reason = "fallback", "sheets"

    if isinstance(snap, dict):
        set_snapshot(snap)

    return {"status": status, "reason": reason, "snapshot": snap, "provider": _PROVIDER_NAME}
