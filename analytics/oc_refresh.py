# analytics/oc_refresh.py
# ------------------------------------------------------------
# Stable API:
#   - async refresh_once(*args, **kwargs) -> dict {status, reason, snapshot, provider}
#   - get_snapshot() -> dict|None
#   - set_snapshot(dict)
#
# What’s new:
#   - Staleness detection:
#       * Expiry date vs today's IST date
#       * Optional row timestamp -> asof (IST) + age_sec
#   - Snapshot fields added: source, ts (fetch time, epoch), asof (IST string),
#     age_sec, stale (bool), stale_reason (list[str])
#
# Provider-first; else Google Sheets fallback with robust OIΔ proxies.
# ------------------------------------------------------------
from __future__ import annotations
import importlib, inspect, logging, re, time
from typing import Any, Callable, Optional, Dict, Tuple, List
import json, os

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

# -------- Provider discovery --------
_MODULE_CANDIDATES = [
    "analytics.oc_sources", "analytics.oc_core", "analytics.oc_backend",
    "integrations.dhan_oc", "integrations.oc_feed",
    "providers.dhan_oc", "providers.oc",
    "dhan.oc", "oc.providers",
]
_FN_CANDIDATE_NAMES = [
    "refresh_once","refresh_now","run_once","refresh","do_refresh",
    "refresh_tick","refresh_snapshot","oc_refresh","fetch_levels",
    "get_oc_snapshot","compute_levels","compute_snapshot","build_snapshot","get_levels",
]
def _score_name(name: str) -> int:
    n = name.lower()
    order = {nm: i for i, nm in enumerate(_FN_CANDIDATE_NAMES)}
    if n in order: return order[n]
    if "refresh" in n: return 50
    if any(k in n for k in ("snapshot","levels","oc")): return 60
    return 999
def _discover_provider() -> Tuple[Optional[Callable[..., Any]], str, bool]:
    for mod_name in _MODULE_CANDIDATES:
        try:
            m = importlib.import_module(mod_name)
        except Exception:
            continue
        cands: list[tuple[int,int,str,Callable[...,Any],bool]] = []
        for nm in dir(m):
            obj = getattr(m, nm, None)
            if callable(obj):
                sc = _score_name(nm)
                if sc < 999:
                    try:
                        sig = inspect.signature(obj)
                        req = sum(
                            1 for p in sig.parameters.values()
                            if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
                            and p.default is inspect._empty
                        )
                    except Exception:
                        req = 0
                    cands.append((sc, req, nm, obj, inspect.iscoroutinefunction(obj)))
        if cands:
            cands.sort(key=lambda t: (t[0], t[1]))
            _, req, nm, fn, is_coro = cands[0]
            _log.info("oc_refresh: provider %s.%s (async=%s, req=%s)", mod_name, nm, is_coro, req)
            return fn, f"{mod_name}.{nm}", is_coro
    return None, "", False
_PROVIDER_FN, _PROVIDER_NAME, _PROVIDER_IS_ASYNC = _discover_provider()

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

def _ist_now_epoch() -> int:
    # UTC + 5:30
    return int(time.time() + 5.5 * 3600)

def _fmt_ist_dt(epoch_utc: Optional[int]) -> str:
    if not epoch_utc:
        return ""
    # format IST
    t = epoch_utc + int(5.5 * 3600)
    return time.strftime("%Y-%m-%d %H:%M:%S IST", time.gmtime(t))

def _today_ist_date_str() -> str:
    t = time.time() + 5.5 * 3600
    return time.strftime("%Y-%m-%d", time.gmtime(t))

def _parse_epoch_like(val) -> Optional[int]:
    # ints/floats or strings of digits (s / ms)
    try:
        if isinstance(val, (int, float)):
            x = int(val)
        else:
            s = str(val).strip()
            if not s or not re.fullmatch(r"[0-9]+", s):
                return None
            x = int(s)
        # if looks like ms:
        if x > 10_000_000_000:
            x //= 1000
        return x
    except Exception:
        return None

def _parse_any_timestamp(v) -> Optional[int]:
    # try epoch first
    ep = _parse_epoch_like(v)
    if ep: 
        return ep
    # try common date strings
    s = str(v or "").strip()
    if not s: 
        return None
    fmts = [
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
        "%d-%m-%Y %H:%M:%S", "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y",
    ]
    for f in fmts:
        try:
            tm = time.strptime(s, f)
            # interpret as local (UTC) -> epoch
            return int(time.mktime(tm))
        except Exception:
            continue
    return None

def _extract_asof_from_row(row: Dict[str, Any]) -> Optional[int]:
    # look for timestamp-like keys
    cand = None
    for k, v in row.items():
        nk = _norm_key(k)
        if nk in {"ts","timestamp","time","asof","as_of","updated_at","last_update","last_updated"}:
            cand = v
            break
    if cand is None:
        return None
    return _parse_any_timestamp(cand)

# -------- OIΔ helpers --------
_CE_TOKS = ("ce",); _PE_TOKS = ("pe",); _CALL_TOKS = ("call",); _PUT_TOKS = ("put",)
_OI_TOKS = ("oi", "openinterest", "open_interest")
_DELTA_TOKS = ("delta", "chg", "change", "d")
def _is_delta_key(norm: str, side: str) -> bool:
    side_ok = (side in norm) or (side == "ce" and any(t in norm for t in _CALL_TOKS)) or (side == "pe" and any(t in norm for t in _PUT_TOKS))
    if not side_ok: return False
    if not any(t in norm for t in _OI_TOKS): return False
    if not any(t in norm for t in _DELTA_TOKS): return False
    return True
def _is_abs_oi_key(norm: str, side: str) -> bool:
    side_ok = (side in norm) or (side == "ce" and any(t in norm for t in _CALL_TOKS)) or (side == "pe" and any(t in norm for t in _PUT_TOKS))
    if not side_ok: return False
    if not any(t in norm for t in _OI_TOKS): return False
    if any(t in norm for t in _DELTA_TOKS): return False
    return True

def _pick_oi_delta_any(row: Dict[str, Any], prev: Optional[Dict[str, Any]], side: str) -> Optional[float]:
    for k, v in row.items():
        nk = _norm_key(k)
        if _is_delta_key(nk, side):
            val = _to_float(v)
            if val is not None:
                return float(val)
    best_curr_val = best_prev_val = None
    for k, v in row.items():
        nk = _norm_key(k)
        if _is_abs_oi_key(nk, side):
            val = _to_float(v)
            if val is not None:
                best_curr_val = val; break
    if prev is not None:
        for k, v in prev.items():
            nk = _norm_key(k)
            if _is_abs_oi_key(nk, side):
                val = _to_float(v)
                if val is not None:
                    best_prev_val = val; break
    if best_curr_val is not None and best_prev_val is not None:
        try:
            return float(best_curr_val) - float(best_prev_val)
        except Exception:
            pass
    return None

def _derive_mv(pcr: Optional[float], max_pain: Optional[float], spot: Optional[float], dpcr: Optional[float]) -> str:
    score = 0
    if isinstance(pcr, (int,float)):
        score += 1 if pcr >= 1.0 else -1
    if isinstance(max_pain, (int,float)) and isinstance(spot, (int,float)):
        score += 1 if max_pain > spot else -1
    if score > 0: return "bullish"
    if score < 0: return "bearish"
    if isinstance(dpcr, (int,float)):
        return "bullish" if dpcr < 0 else "bearish"
    return ""

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

# -------- Flags (HOLD / Daily Cap) --------
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

# -------- Build snapshot from Sheets --------
def _build_from_sheet() -> Optional[dict]:
    rows = _open_ws_and_rows()
    if not rows:
        return None
    last_raw = rows[-1]
    prev_raw = rows[-2] if len(rows) >= 2 else None
    # for as-of detection, keep raw
    asof_epoch = _extract_asof_from_row(last_raw)

    # normalize for numeric extraction
    def norm(d): return {_norm_key(k): v for k, v in d.items()}
    last = norm(last_raw); prev = norm(prev_raw) if prev_raw else None

    sym = (last.get("symbol") or last.get("sym") or _env("OC_SYMBOL") or "")
    sym = str(sym).upper()
    exp = last.get("expiry") or last.get("exp") or ""

    spot = _to_float(last.get("spot"))
    s1 = _to_float(last.get("s1")); s2 = _to_float(last.get("s2"))
    r1 = _to_float(last.get("r1")); r2 = _to_float(last.get("r2"))
    pcr = _to_float(last.get("pcr")); mp = _to_float(last.get("max_pain"))

    dpcr = None
    if prev is not None:
        p_prev = _to_float(prev.get("pcr"))
        if p_prev is not None and pcr is not None:
            dpcr = pcr - p_prev

    mv_tag = (last.get("mv") or last.get("move") or last.get("trend") or "")
    mv_tag = str(mv_tag).strip().lower() or _derive_mv(pcr, mp, spot, dpcr)

    ce_d = _pick_oi_delta_any(last, prev, "ce") or _pick_oi_delta_any(last, prev, "call")
    pe_d = _pick_oi_delta_any(last, prev, "pe") or _pick_oi_delta_any(last, prev, "put")

    if ce_d is None and pe_d is None and isinstance(dpcr, (int,float)) and dpcr != 0:
        mag = max(1.0, abs(dpcr) * 1000.0)
        pe_d, ce_d = (mag, -mag) if dpcr > 0 else (-mag, mag)

    if ce_d is None and pe_d is None and mv_tag:
        sign = 1.0 if mv_tag == "bullish" else (-1.0 if mv_tag == "bearish" else 0.0)
        if sign != 0.0:
            ce_d, pe_d = -1.0 * sign, 1.0 * sign

    if ce_d is None and pe_d is None and isinstance(pcr, (int,float)) and pcr != 1.0:
        pe_d, ce_d = (1.0, -1.0) if pcr > 1.0 else (-1.0, 1.0)

    # Flags (sheet/env)
    flags_sheet = _read_params_override()
    flags_env = _read_override_flags()
    hold = flags_env["hold"] if flags_env.get("hold_set") else flags_sheet.get("hold", False)
    daily_cap_hit = flags_env["daily_cap_hit"] if flags_env.get("cap_set") else flags_sheet.get("daily_cap_hit", False)

    # Staleness checks
    stale = False; reasons: List[str] = []
    # 1) expiry mismatch
    today = _today_ist_date_str()
    exp_s = str(exp).strip()
    if exp_s and exp_s != today:
        stale = True
        reasons.append(f"expiry {exp_s} != today {today}")
    # 2) as-of age
    max_age = int(_env("OC_MAX_SNAPSHOT_AGE_SEC") or "300")
    age_sec = None
    asof_str = ""
    if asof_epoch:
        # asof_epoch is epoch in (local/UTC); we treat it as UTC here for simplicity
        now_utc = int(time.time())
        age_sec = max(0, now_utc - int(asof_epoch))
        if age_sec > max_age:
            stale = True; reasons.append(f"age>{max_age}s")
        asof_str = _fmt_ist_dt(asof_epoch)
    else:
        asof_str = ""  # unknown

    snap = {
        "symbol": sym,
        "expiry": exp,
        "spot": spot,
        "s1": s1, "s2": s2, "r1": r1, "r2": r2,
        "pcr": pcr, "max_pain": mp,
        "ce_oi_delta": ce_d, "pe_oi_delta": pe_d,
        "mv": mv_tag,
        "hold": bool(hold),
        "daily_cap_hit": bool(daily_cap_hit),
        "source": "sheets",
        "ts": int(time.time()),
        "asof": asof_str,
        "age_sec": age_sec,
        "stale": stale,
        "stale_reason": reasons,
    }
    return snap

# -------- Main entry --------
async def refresh_once(*args, **kwargs) -> dict:
    status = "ok"; reason = ""; snap: Optional[dict] = None

    if _PROVIDER_FN is not None:
        try:
            ret = _PROVIDER_FN(*args, **kwargs)
            if inspect.isawaitable(ret):
                ret = await ret
            # try direct dict or nested
            if isinstance(ret, dict) and "snapshot" in ret and isinstance(ret["snapshot"], dict):
                snap = ret["snapshot"]
            elif isinstance(ret, dict):
                snap = ret
            # enrich provider snapshot minimal fields if missing
            if isinstance(snap, dict):
                snap.setdefault("source", "provider")
                snap.setdefault("ts", int(time.time()))
                snap.setdefault("stale", False)
                snap.setdefault("stale_reason", [])
                # normalize expiry staleness check even for provider
                today = _today_ist_date_str()
                exp_s = str(snap.get("expiry") or "").strip()
                if exp_s and exp_s != today:
                    snap["stale"] = True
                    snap.setdefault("stale_reason", []).append(f"expiry {exp_s} != today {today}")
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
