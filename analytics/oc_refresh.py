# analytics/oc_refresh.py
# ------------------------------------------------------------
# Unified OC refresh core with stable API:
#   - refresh_once(*args, **kwargs) -> dict snapshot
#   - get_snapshot() -> dict|None
#   - set_snapshot(snap: dict) -> None
#
# How it works:
# 1) Discovers a provider function in common modules (Dhan/OC sources)
#    like fetch_levels / refresh / get_oc_snapshot etc., and calls it
#    with flexible args: (), (None,), ({},) -- whichever matches.
# 2) Extracts a snapshot (dict) from the return (dict / tuple / object).
# 3) Stores it in module-global _SNAPSHOT.
# 4) If no provider or it fails, falls back to Google Sheets "OC_Live"
#    last row (headers-based) to build a snapshot.
#
# Env used:
#   - GOOGLE_SA_JSON (service account JSON)
#   - GSHEET_TRADES_SPREADSHEET_ID (sheet id)
#   - OC_SYMBOL (for defaults)
# ------------------------------------------------------------
from __future__ import annotations
import importlib
import inspect
import logging
from typing import Any, Callable, Optional, Dict, Tuple

import json, os, time

# Sheets (optional)
try:
    import gspread  # type: ignore
except Exception:
    gspread = None  # type: ignore

_log = logging.getLogger(__name__)

# Module-global latest snapshot
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
    "analytics.oc_sources",
    "analytics.oc_core",
    "analytics.oc_backend",
    "integrations.dhan_oc",
    "integrations.oc_feed",
    "providers.dhan_oc",
    "providers.oc",
    "dhan.oc",
    "oc.providers",
]
_FN_CANDIDATE_NAMES = [
    "refresh_once",
    "refresh_now",
    "run_once",
    "refresh",
    "do_refresh",
    "refresh_tick",
    "refresh_snapshot",
    "oc_refresh",
    "fetch_levels",
    "get_oc_snapshot",
    "compute_levels",
    "compute_snapshot",
    "build_snapshot",
    "get_levels",
]

def _score_name(name: str) -> int:
    n = name.lower()
    order = {nm: i for i, nm in enumerate(_FN_CANDIDATE_NAMES)}
    if n in order: return order[n]
    if "refresh" in n: return 50
    if any(k in n for k in ("snapshot", "levels", "oc")): return 60
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
                score = _score_name(nm)
                if score < 999:
                    # required positional params (for arg-flex)
                    try:
                        sig = inspect.signature(obj)
                        req = sum(
                            1 for p in sig.parameters.values()
                            if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
                            and p.default is inspect._empty
                        )
                    except Exception:
                        req = 0
                    cands.append((score, req, nm, obj, inspect.iscoroutinefunction(obj)))
        if cands:
            cands.sort(key=lambda t: (t[0], t[1]))  # best score, fewer required args
            _, req, nm, fn, is_coro = cands[0]
            _log.info("oc_refresh: provider %s.%s (async=%s, req=%s)", mod_name, nm, is_coro, req)
            return fn, f"{mod_name}.{nm}", is_coro
    return None, "", False

_PROVIDER_FN, _PROVIDER_NAME, _PROVIDER_IS_ASYNC = _discover_provider()

def _extract_snapshot(ret: Any) -> Optional[dict]:
    """Heuristic: find a dict with expected OC keys."""
    def looks(d: Any) -> bool:
        if not isinstance(d, dict): return False
        k = set(x.lower() for x in d.keys())
        if "spot" in k and ({"s1","s2","r1","r2"} & k):
            return True
        if {"symbol","expiry","spot"} <= k:
            return True
        if "levels" in k and "spot" in k:
            return True
        return False

    if looks(ret): return ret
    if isinstance(ret, (tuple, list)):
        for x in ret:
            if looks(x): return x
    for attr in ("snapshot","data","result"):
        if hasattr(ret, attr):
            try:
                val = getattr(ret, attr)
                if looks(val): return val
            except Exception:
                pass
    return None

def _call_variants(fn: Callable, is_async: bool):
    """Return a coroutine that tries (), (None,), ({},) in order."""
    async def _runner():
        variants = [
            ((), {}),
            ((None,), {}),
            (({},), {}),
        ]
        for a,k in variants:
            try:
                if is_async:
                    res = fn(*a, **k)  # may or may not be awaitable
                    if inspect.isawaitable(res):
                        res = await res
                else:
                    res = fn(*a, **k)
                return res
            except TypeError:
                # try next variant (arg mismatch)
                continue
        # last resort: call without caring (may still work)
        res = fn()
        if inspect.isawaitable(res):
            res = await res
        return res
    return _runner()

# -------- Sheets fallback --------
def _env(name: str) -> Optional[str]:
    v = os.environ.get(name)
    return v if v and v.strip() else None

def _open_status_ws():
    """Open spreadsheet and return 'OC_Live' worksheet if present."""
    if gspread is None:
        raise RuntimeError("gspread not installed")
    raw = _env("GOOGLE_SA_JSON")
    sid = _env("GSHEET_TRADES_SPREADSHEET_ID")
    if not raw or not sid:
        raise RuntimeError("Sheets env missing")
    sa = json.loads(raw)
    gc = gspread.service_account_from_dict(sa)
    sh = gc.open_by_key(sid)
    try:
        ws = sh.worksheet("OC_Live")
    except Exception:
        # fallback to Snapshots
        ws = sh.worksheet("Snapshots")
    return ws

_NUMERIC_COLS = {
    "spot","s1","s2","r1","r2","pcr","max_pain","ce_oi_delta","pe_oi_delta",
}

def _to_float(x):
    try:
        if x in (None,"","—"): return None
        return float(str(x).replace(",","").strip())
    except Exception:
        return None

def _build_from_sheet() -> Optional[dict]:
    try:
        ws = _open_status_ws()
        vals = ws.get_all_records()  # list of dicts with header mapping
        if not vals:
            return None
        row = vals[-1]
        # normalize keys
        row_norm: Dict[str, Any] = {}
        for k, v in row.items():
            key = str(k).strip().lower()
            if key in _NUMERIC_COLS:
                row_norm[key] = _to_float(v)
            else:
                row_norm[key] = v
        # expected fields
        sym = (row_norm.get("symbol") or row_norm.get("sym") or _env("OC_SYMBOL") or "").upper()
        exp = row_norm.get("expiry") or row_norm.get("exp") or ""
        snap = {
            "symbol": sym,
            "expiry": exp,
            "spot": row_norm.get("spot"),
            "s1": row_norm.get("s1"),
            "s2": row_norm.get("s2"),
            "r1": row_norm.get("r1"),
            "r2": row_norm.get("r2"),
            "pcr": row_norm.get("pcr"),
            "max_pain": row_norm.get("max_pain"),
            "ce_oi_delta": row_norm.get("ce_oi_delta"),
            "pe_oi_delta": row_norm.get("pe_oi_delta"),
            "mv": row_norm.get("mv") or row_norm.get("move") or row_norm.get("trend"),
            "source": "sheets",
            "ts": int(time.time()),
        }
        return snap
    except Exception as e:
        _log.warning("oc_refresh: sheets fallback failed: %s", e)
        return None

# -------- Main entry --------
async def refresh_once(*args, **kwargs) -> dict:
    """
    Try provider; else read last OC snapshot from Sheets.
    Always returns a dict: {"status": "...", "snapshot": dict|None, ...}
    and publishes _SNAPSHOT if extracted.
    """
    status = "ok"
    reason = ""
    snap: Optional[dict] = None

    # 1) Provider
    if _PROVIDER_FN is not None:
        try:
            ret = await _call_variants(_PROVIDER_FN, _PROVIDER_IS_ASYNC)
            snap = _extract_snapshot(ret)
            if snap is None and isinstance(ret, dict) and "snapshot" in ret and isinstance(ret["snapshot"], dict):
                snap = ret["snapshot"]
        except Exception as e:
            status, reason = "provider_error", str(e)

    # 2) Sheets fallback
    if snap is None:
        s2 = _build_from_sheet()
        if s2 is not None:
            snap = s2
            if status == "ok" and reason == "":
                status = "fallback"
                reason = "sheets"

    # 3) Publish (if any)
    if isinstance(snap, dict):
        set_snapshot(snap)

    return {
        "status": status,
        "reason": reason,
        "snapshot": snap,
        "provider": _PROVIDER_NAME,
    }
