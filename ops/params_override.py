# ops/params_override.py
from __future__ import annotations
import os, json
from typing import Dict, Any, List

APPLY_TRUE = {"approved","on","true","yes","ok","1"}

def _trim(s): return str(s or "").strip()
def _norm_status(s): return _trim(s).lower()

def _read_sheet_overrides(sheet) -> Dict[str,str]:
    try:
        ws = sheet.ss.worksheet("Params_Override")
        rows = ws.get_all_values()
        if not rows or len(rows) < 2: return {}
        hdr = [h.strip().lower() for h in rows[0]]
        i_key = hdr.index("key") if "key" in hdr else -1
        i_val = hdr.index("value") if "value" in hdr else -1
        i_st  = hdr.index("status") if "status" in hdr else -1
        out = {}
        for r in rows[1:]:
            if i_key<0 or i_val<0 or i_st<0 or len(r)<=max(i_key,i_val,i_st): continue
            if _norm_status(r[i_st]) in APPLY_TRUE:
                k = _trim(r[i_key]); v = _trim(r[i_val])
                if k: out[k]=v
        return out
    except Exception as e:
        print(f"[params_override] sheet read failed: {e}", flush=True)
        return {}

def _read_firestore_overrides() -> Dict[str,str]:
    try:
        if os.getenv("FIREBASE_SYNC","off").lower()!="on": return {}
        raw = os.getenv("FIREBASE_SA_JSON") or os.getenv("GOOGLE_SA_JSON")
        proj = os.getenv("FIREBASE_PROJECT_ID","")
        if not raw or not proj: return {}
        info = json.loads(raw)
        from google.cloud import firestore
        from google.oauth2 import service_account
        creds = service_account.Credentials.from_service_account_info(info)
        client = firestore.Client(project=proj, credentials=creds)
        prefix = os.getenv("FB_COLLECTION_PREFIX","ktw_v3") or "ktw_v3"
        col = client.collection(f"{prefix}__overrides")
        doc = col.document("current").get()
        if not doc.exists: return {}
        data = doc.to_dict() or {}
        if _norm_status(data.get("status","")) not in APPLY_TRUE: return {}
        params = data.get("params") or {}
        return {str(k):str(v) for k,v in params.items()}
    except Exception as e:
        print(f"[params_override] firestore read failed: {e}", flush=True)
        return {}

def apply_overrides(sheet, cfg) -> Dict[str,str]:
    merged = {**_read_firestore_overrides(), **_read_sheet_overrides(sheet)}
    applied = {}
    for k,v in merged.items():
        os.environ[k]=v; applied[k]=v
        try:
            if hasattr(cfg, k.lower()): setattr(cfg, k.lower(), v)
        except Exception: pass
    return applied
