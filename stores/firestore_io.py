# stores/firestore_io.py
# Firestore read/write helpers for Night jobs (env-gated; safe offline).
# Collections layout (prefixable via FB_COLLECTION_PREFIX, default 'ktw_v3'):
#   overrides/current                 (doc id: 'current'; status: proposed/approved)
#   snapshots_daily/{YYYY-MM-DD}      (doc per date)
#   backtests/{run_id}                (doc per run)
#   params_pool/{run_id_rank}         (doc per candidate)
#   research_summaries/{YYYY-MM-DD}   (doc per date)
#
# Enable by env (add later when ready):
#   FIREBASE_SYNC=on
#   FIREBASE_PROJECT_ID=<id>
#   FIREBASE_SA_JSON=<service-account JSON one-line>  # falls back to GOOGLE_SA_JSON if unset
#   (optional) FB_COLLECTION_PREFIX=ktw_v3

from __future__ import annotations
import os, json, time
from typing import Any, Dict, Optional

_client = None
_prefix = None
_enabled = None

def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    return os.getenv(name, default)

def is_enabled() -> bool:
    global _enabled
    if _enabled is None:
        _enabled = (_env("FIREBASE_SYNC", "off").lower() == "on")
    return _enabled

def _get_client():
    global _client, _prefix
    if _client is not None:
        return _client
    if not is_enabled():
        raise RuntimeError("FIREBASE_SYNC is off")

    project = _env("FIREBASE_PROJECT_ID", "")
    if not project:
        raise RuntimeError("FIREBASE_PROJECT_ID missing")

    # Prefer FIREBASE_SA_JSON, else reuse GOOGLE_SA_JSON
    raw = _env("FIREBASE_SA_JSON") or _env("GOOGLE_SA_JSON")
    if not raw:
        raise RuntimeError("FIREBASE_SA_JSON/GOOGLE_SA_JSON missing")

    info = json.loads(raw)

    # Lazy import so module can exist without extra deps when disabled
    from google.cloud import firestore
    from google.oauth2 import service_account

    creds = service_account.Credentials.from_service_account_info(info)
    _client = firestore.Client(project=project, credentials=creds)
    _prefix = _env("FB_COLLECTION_PREFIX", "ktw_v3").strip() or "ktw_v3"
    return _client

def _full(name: str) -> str:
    # e.g., 'ktw_v3/overrides' logical prefix (used as collection name prefix)
    return f"{_prefix}/{name}" if _prefix else name

def _collect(name: str):
    client = _get_client()
    # Firestore doesn't support slashes inside collection IDs; emulate with single flat prefix
    # So we join prefix and name using '__' separator to keep things tidy in console
    col = f"{_prefix}__{name}" if _prefix else name
    return client.collection(col)

def _safe_now_iso() -> str:
    import datetime as _dt
    return _dt.datetime.now().replace(microsecond=0).isoformat(sep=" ")

# ---------- Public API (graceful no-ops when disabled) ----------

def save_overrides_current(data: Dict[str, Any]) -> bool:
    """
    Upsert overrides/current (status: proposed/approved).
    Example data: {"status":"proposed","params":{"ENTRY_BAND_POINTS":7}, "source":"night", "ts": "..."}
    """
    if not is_enabled():
        print("[firestore] off: overrides/current not written")
        return False
    try:
        col = _collect("overrides")
        col.document("current").set({
            **data,
            "ts": data.get("ts") or _safe_now_iso(),
        }, merge=True)
        return True
    except Exception as e:
        print(f"[firestore] overrides/current failed: {e}")
        return False

def save_snapshot_daily(date_str: str, snapshot: Dict[str, Any]) -> bool:
    """
    snapshots_daily/{YYYY-MM-DD} = snapshot blob (e.g., day summary, metrics)
    """
    if not is_enabled():
        print("[firestore] off: snapshots_daily not written")
        return False
    try:
        col = _collect("snapshots_daily")
        col.document(date_str).set({
            "ts": _safe_now_iso(),
            "data": snapshot,
        }, merge=True)
        return True
    except Exception as e:
        print(f"[firestore] snapshots_daily failed: {e}")
        return False

def save_backtest_run(run_id: str, payload: Dict[str, Any]) -> bool:
    """
    backtests/{run_id} = results, metrics, artifacts pointers
    """
    if not is_enabled():
        print("[firestore] off: backtests not written")
        return False
    try:
        col = _collect("backtests")
        col.document(run_id).set({
            "ts": _safe_now_iso(),
            **payload
        }, merge=True)
        return True
    except Exception as e:
        print(f"[firestore] backtests failed: {e}")
        return False

def save_params_pool(run_id_rank: str, payload: Dict[str, Any]) -> bool:
    """
    params_pool/{run_id_rank} = {"rank": n, "params": {...}, "metrics": {...}}
    """
    if not is_enabled():
        print("[firestore] off: params_pool not written")
        return False
    try:
        col = _collect("params_pool")
        col.document(run_id_rank).set({
            "ts": _safe_now_iso(),
            **payload
        }, merge=True)
        return True
    except Exception as e:
        print(f"[firestore] params_pool failed: {e}")
        return False

def save_research_summary(date_str: str, summary: str, extra: Optional[Dict[str, Any]] = None) -> bool:
    """
    research_summaries/{YYYY-MM-DD} = {"summary": str, ...}
    """
    if not is_enabled():
        print("[firestore] off: research_summaries not written")
        return False
    try:
        col = _collect("research_summaries")
        doc = {
            "ts": _safe_now_iso(),
            "summary": summary,
        }
        if extra:
            doc.update(extra)
        col.document(date_str).set(doc, merge=True)
        return True
    except Exception as e:
        print(f"[firestore] research_summaries failed: {e}")
        return False
