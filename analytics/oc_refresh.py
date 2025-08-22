# analytics/oc_refresh.py
# ------------------------------------------------------------
# Stable API:
#   - async refresh_once(*args, **kwargs) -> dict {status, reason, snapshot, provider}
#   - get_snapshot(), set_snapshot()
#
# Changes in this version:
#   - Provider snapshot पर HOLD/daily_cap flags merge (env/sheet से)
#   - MV खाली हो तो हल्का fallback (PCR/MP + OIΔ) derive
#   - STALE: expiry < today (IST) या age > OC_MAX_SNAPSHOT_AGE_SEC
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

# -------- Public API --------
def set_snapshot(snap: dict) -> None:
    global _SNAPSHOT
    if isinstance(snap, dict):
        _SNAPSHOT = snap

def get_snapshot() -> Optional[dict]:
    return _SNAPSHOT

# -------- Utils --------
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

def _today_ist_ymd_tuple() -> Tuple[int,int,int]:
    t = time.time() + 5.5 * 3600
    y = int(time.strftime("%Y", time.gmtime(t)))
    m = int(time.strftime("%m", time.gmtime(t)))
    d = int(time.strftime("%d", time.gmtime(t)))
    return y, m, d

def _parse_ymd(s: str) -> Optional[Tuple[int,int,int]]:
    s = s.strip()
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", s)
    if not m: return None
    return int(m.group(1)), int(m.group(2)), int(m.group(3))

def _ymd_lt(a: Tuple[int,int,int], b: Tuple[int,int,int]) -> bool:
    return a < b

def _fmt_ist_dt(epoch_utc: Optional[int]) -> str:
    if not epoch_utc:
        return ""
    t = epoch_utc + int(5.5 * 3600)
    return time.strftime("%Y-%m-%d %H:%M:%S IST", time.gmtime(t))

def _parse_epoch_like(val) -> Optional[int]:
    try:
        if isinstance(val, (int, float)):
            x = int(val)
        else:
            s = str(val).strip()
            if not s or not re.fullmatch(r"[0-9]+", s):
                return None
            x = int(s)
        if x > 10_000_000_000:
            x //= 1000
        return x
    except Exception:
        return None

# -------- Sheets I/O --------
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

def _open_oc_ws():
    sh = _open_by_key()
    try:
        return sh.worksheet("OC_Live")
    except Exception:
        return sh.worksheet("Snapshots")

def _open_ws_and_rows() -> Optional[List[dict]]:
    try:
        ws = _open_oc_ws()
        return ws.get_all_records()
    except Exception as e:
        _log.warning("oc_refresh: sheets read failed: %s", e)
        return None

def _truthy(x: Any) -> Optional[bool]:
    if x is None: return None
    s = str(x).strip().lower()
    if s in {"1","true","yes","y","on","t"}: return True
    if s in {"0","false","no","n","off","f"}: return False
    return None

def _read_override_flags() -> Dict[str, bool]:
    hold = None
    for k in ("HOLD_OVERRIDE","SYSTEM_HOLD","HOLD"):
        v = _env(k); tv = _truthy(v) if v is not None else None
        if tv is not None: hold = tv; break
    cap = None
    for k in ("DAILY_CAP_HIT","DAILY_CAP","CAP_HIT"):
        v = _env(k); tv = _truthy(v) if v is not None else None
        if tv is not None: cap = tv; break
    return {"hold": bool(hold) if hold is not None else False,
            "daily_cap_hit": bool(cap) if cap is not None else False,
            "hold_set": hold is not None, "cap_set": cap is not None}

def _read_params_override() -> Dict[str, bool]:
    out = {"hold": False, "daily_cap_hit": False}
    try:
        ws = _open_ws("Params_Override")
    except Exception:
        return out
    try:
        rows = ws.get_all_records()
    except Exception:
        return out
    if not rows:
        return out
    last = rows[-1]
    last_norm = { _norm_key(k): v for k, v in last.items() }
    for k in ("hold","system_hold","manual_hold"):
        if k in last_norm:
            tv = _truthy(last_norm[k])
            if tv is not None: out["hold"] = tv; break
    for k in ("daily_cap_hit","daily_cap","cap_hit"):
        if k in last_norm:
            tv = _truthy(last_norm[k])
            if tv is not None: out["daily_cap_hit"] = tv; break
    return out

# -------- MV fallback --------
def _ensure_mv(snap: Dict[str, Any]) -> None:
    mv = str(snap.get("mv") or "").strip().lower()
    if mv:
        return
    pcr = _to_float(snap.get("pcr"))
    mp  = _to_float(snap.get("max_pain"))
    spot= _to_float(snap.get("spot"))
    ce_d= _to_float(snap.get("ce_oi_delta"))
    pe_d= _to_float(snap.get("pe_oi_delta"))
    score = 0
    if isinstance(pcr, (int,float)):
        score += 1 if float(pcr) >= 1.0 else -1
    if isinstance(mp, (int,float)) and isinstance(spot,(int,float)):
        score += 1 if float(mp) > float(spot) else -1
    if score > 0: snap["mv"]="bullish"; return
    if score < 0: snap["mv"]="bearish"; return
    if isinstance(ce_d,(int,float)) and isinstance(pe_d,(int,float)) and ce_d != pe_d:
        snap["mv"] = "bullish" if pe_d > ce_d else "bearish"
    else:
        snap["mv"] = ""

# -------- Provider discovery (keep simple) --------
_MODULE_CANDIDATES = [
    "providers.dhan_oc",
    "providers.oc",
]
_FN_CANDIDATE_NAMES = [
    "refresh_once","refresh_now","run_once","refresh","do_refresh",
    "refresh_tick","refresh_snapshot","oc_refresh","fetch_levels",
    "get_oc_snapshot","compute_levels","compute_snapshot","build_snapshot","get_levels",
]

def _discover_provider():
    for mod_name in _MODULE_CANDIDATES:
        try:
            m = importlib.import_module(mod_name)
        except Exception:
            continue
        for nm in _FN_CANDIDATE_NAMES:
            if hasattr(m, nm) and callable(getattr(m, nm)):
                fn = getattr(m, nm)
                _log.info("oc_refresh: provider %s.%s selected (async=%s)", mod_name, nm, inspect.iscoroutinefunction(fn))
                return fn, f"{mod_name}.{nm}", inspect.iscoroutinefunction(fn)
    return None, "", False

_PROVIDER_FN, _PROVIDER_NAME, _PROVIDER_IS_ASYNC = _discover_provider()

async def _call_variants(fn, is_async: bool):
    res = fn()
    if inspect.isawaitable(res):
        res = await res
    return res

# -------- Build from Sheets (unchanged except staleness) --------
def _extract_asof_from_row(row: Dict[str, Any]) -> Optional[int]:
    cand = None
    for k, v in row.items():
        nk = _norm_key(k)
        if nk in {"ts","timestamp","time","asof","as_of","updated_at","last_update","last_updated"}:
            cand = v; break
    if cand is None: return None
    # epoch or parseable string
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

def _build_from_sheet() -> Optional[dict]:
    rows = _open_ws_and_rows()
    if not rows:
        return None
    last_raw = rows[-1]
    prev_raw = rows[-2] if len(rows) >= 2 else None
    asof_epoch = _extract_asof_from_row(last_raw)

    def norm(d): return {_norm_key(k): v for k, v in d.items()} if d else {}
    last = norm(last_raw); prev = norm(prev_raw)

    sym = (last.get("symbol") or last.get("sym") or _env("OC_SYMBOL") or "").upper()
    exp = last.get("expiry") or last.get("exp") or ""
    spot = _to_float(last.get("spot"))
    s1 = _to_float(last.get("s1")); s2 = _to_float(last.get("s2"))
    r1 = _to_float(last.get("r1")); r2 = _to_float(last.get("r2"))
    pcr= _to_float(last.get("pcr")); mp = _to_float(last.get("max_pain"))

    dpcr = None
    if prev:
        p_prev = _to_float(prev.get("pcr"))
        if p_prev is not None and pcr is not None:
            dpcr = pcr - p_prev

    mv_tag = (last.get("mv") or "").strip().lower()
    if not mv_tag:
        # fallback derive if blank
        ce_d = None; pe_d = None
        mv_tag = ""
        try:
            ce_d = _to_float(last.get("ce_oi_delta"))
            pe_d = _to_float(last.get("pe_oi_delta"))
        except Exception:
            pass
        score = 0
        if isinstance(pcr,(int,float)): score += 1 if pcr >= 1.0 else -1
        if isinstance(mp,(int,float)) and isinstance(spot,(int,float)): score += 1 if mp > spot else -1
        if score > 0: mv_tag="bullish"
        elif score < 0: mv_tag="bearish"
        elif isinstance(ce_d,(int,float)) and isinstance(pe_d,(int,float)) and ce_d != pe_d:
            mv_tag="bullish" if pe_d>ce_d else "bearish"

    # Flags
    flags_sheet = _read_params_override()
    flags_env   = _read_override_flags()
    hold = flags_env["hold"] if flags_env.get("hold_set") else flags_sheet.get("hold", False)
    cap  = flags_env["daily_cap_hit"] if flags_env.get("cap_set") else flags_sheet.get("daily_cap_hit", False)

    # Stale logic
    stale = False; reasons: List[str] = []
    exp_s = str(exp or "").strip()
    if exp_s:
        exp_ymd = _parse_ymd(exp_s); today = _today_ist_ymd_tuple()
        if exp_ymd and _ymd_lt(exp_ymd, today):
            stale = True; reasons.append(f"expiry {exp_s} < today {today[0]:04d}-{today[1]:02d}-{today[2]:02d}")
    max_age = int(_env("OC_MAX_SNAPSHOT_AGE_SEC") or "300")
    age_sec = None; asof_str = ""
    if asof_epoch:
        now_utc = int(time.time())
        age_sec = max(0, now_utc - int(asof_epoch))
        if age_sec > max_age: stale = True; reasons.append(f"age>{max_age}s")
        asof_str = _fmt_ist_dt(asof_epoch)

    return {
        "symbol": sym, "expiry": exp, "spot": spot,
        "s1": s1, "s2": s2, "r1": r1, "r2": r2,
        "pcr": pcr, "max_pain": mp,
        "ce_oi_delta": _to_float(last.get("ce_oi_delta")),
        "pe_oi_delta": _to_float(last.get("pe_oi_delta")),
        "mv": mv_tag,
        "hold": bool(hold), "daily_cap_hit": bool(cap),
        "source": "sheets",
        "ts": int(time.time()),
        "asof": asof_str, "age_sec": age_sec,
        "stale": stale, "stale_reason": reasons,
    }

# -------- Main entry --------
async def refresh_once(*args, **kwargs) -> dict:
    status = "ok"; reason = ""; snap: Optional[dict] = None

    fn = _PROVIDER_FN
    if fn is not None:
        try:
            ret = await _call_variants(fn, _PROVIDER_IS_ASYNC)
            psnap = None
            # extract snapshot
            if isinstance(ret, dict) and ("snapshot" in ret and isinstance(ret["snapshot"], dict)):
                psnap = ret["snapshot"]
            elif isinstance(ret, dict):
                psnap = ret
            else:
                try:
                    psnap = getattr(ret, "snapshot")
                except Exception:
                    psnap = None
            if isinstance(psnap, dict):
                snap = dict(psnap)
                snap.setdefault("source", "provider")
                snap.setdefault("ts", int(time.time()))
                # asof/age
                ts = psnap.get("ts")
                ts_epoch = None
                try:
                    if ts is not None:
                        ts_epoch = int(ts) if isinstance(ts,(int,float)) else int(str(ts))
                        if ts_epoch > 10_000_000_000: ts_epoch//=1000
                except Exception:
                    ts_epoch = None
                if ts_epoch:
                    snap["age_sec"] = max(0, int(time.time()) - int(ts_epoch))
                    snap["asof"] = _fmt_ist_dt(ts_epoch)
                snap.setdefault("age_sec", 0)
                snap.setdefault("asof", snap.get("asof",""))

                # Merge HOLD/daily-cap flags for C5
                flags_sheet = _read_params_override()
                flags_env   = _read_override_flags()
                hold = flags_env["hold"] if flags_env.get("hold_set") else flags_sheet.get("hold", False)
                cap  = flags_env["daily_cap_hit"] if flags_env.get("cap_set") else flags_sheet.get("daily_cap_hit", False)
                snap["hold"] = bool(hold); snap["daily_cap_hit"] = bool(cap)

                # MV fallback if empty
                _ensure_mv(snap)

                # expiry stale rule
                exp_s = str(snap.get("expiry") or "").strip()
                if exp_s:
                    exp_ymd = _parse_ymd(exp_s); today = _today_ist_ymd_tuple()
                    if exp_ymd and _ymd_lt(exp_ymd, today):
                        snap["stale"] = True
                        snap.setdefault("stale_reason", []).append(
                            f"expiry {exp_s} < today {today[0]:04d}-{today[1]:02d}-{today[2]:02d}"
                        )
                snap.setdefault("stale", False)
                snap.setdefault("stale_reason", [])
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
