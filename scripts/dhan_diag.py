# scripts/dhan_diag.py
# Quick Dhan auth & option-chain probe. Prints clear PASS/FAIL.

from __future__ import annotations
import os, sys, json, time
import requests

BASE = "https://api.dhan.co/v2"
TIMEOUT = 8

def _hdr():
    cid = os.environ.get("DHAN_CLIENT_ID", "").strip()
    tok = os.environ.get("DHAN_ACCESS_TOKEN", "").strip()
    if not cid or not tok:
        print("FAIL: Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN in env")
        sys.exit(2)
    return {
        "accept": "application/json",
        "content-type": "application/json",
        "client-id": cid,
        "access-token": tok,
    }

def ping_profile():
    # lightweight auth check endpoint (fallback to optionchain if 401)
    url = f"{BASE}/optionchain"
    payload = {
        "exchangeSegment": "IDX_I",
        "securityId": int(os.environ.get("DHAN_UNDERLYING_SCRIP", "13") or 13),
        "expiryCode": 0,
    }
    try:
        r = requests.post(url, headers=_hdr(), data=json.dumps(payload), timeout=TIMEOUT)
        ok = (200 <= r.status_code < 300)
        print(f"HTTP {r.status_code} @ /optionchain")
        if not ok:
            print("Body:", r.text[:500])
        return ok, r
    except requests.RequestException as e:
        print("FAIL: request error:", e)
        return False, None

def main():
    # Echo env
    print("DHAN_CLIENT_ID len:", len(os.environ.get("DHAN_CLIENT_ID","")))
    print("DHAN_ACCESS_TOKEN len:", len(os.environ.get("DHAN_ACCESS_TOKEN","")))
    print("SEG:", os.environ.get("DHAN_UNDERLYING_SEG"))
    print("SCRIP:", os.environ.get("DHAN_UNDERLYING_SCRIP"))
    print("MAP:", os.environ.get("DHAN_UNDERLYING_SCRIP_MAP"))

    ok, r = ping_profile()
    if ok:
        try:
            data = r.json()
        except Exception:
            data = {}
        status = data.get("status") or "?"
        print("PASS: optionchain reachable. status:", status)
        # quick shape check
        asof = data.get("asOf") or data.get("timestamp") or ""
        print("asOf:", asof)
        sys.exit(0)
    else:
        print("FAIL: Auth/headers invalid or token expired. Fix env in SERVICE and redeploy.")
        print("Hint: ClientId must be your Dhan client id (digits), token must be active API token.")
        sys.exit(1)

if __name__ == "__main__":
    main()
