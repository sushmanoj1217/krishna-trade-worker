# path: integrations/dhan.py
# Dhan Option Chain client with streaming CSV resolve + rate-limit aware requests

import os, io, csv, time, random, requests
from typing import Dict, Any, Optional

API_BASE = "https://api.dhan.co/v2"
INSTR_CSV_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"

def _hdrs() -> Dict[str, str]:
    return {
        "access-token": os.getenv("DHAN_ACCESS_TOKEN", ""),
        "client-id": os.getenv("DHAN_CLIENT_ID", ""),
        "Content-Type": "application/json",
    }

def _sym_norm(s: str) -> str:
    s = (s or "").strip().upper()
    if s in ("BANK NIFTY", "NIFTY BANK", "BANK-NIFTY", "BANKNIFTY"): return "BANKNIFTY"
    if s in ("FINNIFTY", "FININFTY", "FININFTI", "FININIFTY", "FININFTIY", "FININNIFTY"): return "FINNIFTY"
    if s in ("NIFTY50", "NIFTY 50"): return "NIFTY"
    return s

def _env_usid(sym: str) -> Optional[int]:
    m = os.getenv("DHAN_USID_MAP", "")
    if m:
        for pair in m.split(","):
            if "=" in pair:
                k, v = pair.split("=", 1)
                if _sym_norm(k) == sym:
                    try: return int(v)
                    except: pass
    v = os.getenv(f"DHAN_USID_{sym}", "")
    if v:
        try: return int(v)
        except: pass
    return None

def _request_json(method: str, url: str, *, json_body: dict | None = None, timeout: int = 15, max_retries: int = None) -> dict:
    """
    Rate-limit safe requester:
      - Retries on 429 and 5xx with exponential backoff + jitter
      - Respects Retry-After when present
    """
    if max_retries is None:
        max_retries = int(os.getenv("OC_BACKOFF_MAX_RETRIES", "4") or "4")
    backoff = 1.5
    for attempt in range(max_retries + 1):
        try:
            r = requests.request(method, url, headers=_hdrs(), json=json_body, timeout=timeout)
            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                sleep_s = int(ra) if (ra and ra.isdigit()) else int(backoff) + random.randint(0, 2)
                time.sleep(max(1, sleep_s))
                backoff *= 2
                continue
            if 500 <= r.status_code < 600:
                time.sleep(int(backoff))
                backoff *= 2
                continue
            r.raise_for_status()
            return r.json() or {}
        except requests.RequestException as e:
            if attempt >= max_retries:
                raise
            # small jitter even on network errors
            time.sleep(int(backoff))
            backoff *= 2
    return {}

class DhanClient:
    def resolve_underlying_scrip(self, symbol: str) -> Optional[int]:
        """Memory-safe streaming resolve: reads CSV line-by-line and returns at first match."""
        sym = _sym_norm(symbol)

        usid_env = _env_usid(sym)
        if usid_env:
            return usid_env

        with requests.get(INSTR_CSV_URL, stream=True, timeout=20) as r:
            r.raise_for_status()
            r.raw.decode_content = True
            text_stream = io.TextIOWrapper(r.raw, encoding="utf-8", newline="")
            reader = csv.DictReader(text_stream)
            for row in reader:
                try:
                    if row.get("INSTRUMENT") != "OPTIDX":
                        continue
                    underlying = (row.get("UNDERLYING_SYMBOL") or "").upper()
                    if underlying == sym:
                        usid_str = row.get("UNDERLYING_SECURITY_ID") or ""
                        return int(usid_str)
                except Exception:
                    continue
        return None

    def get_expiries(self, underlying_scrip: int, underlying_seg: str = "IDX_I"):
        url = f"{API_BASE}/optionchain/expirylist"
        payload = {"UnderlyingScrip": underlying_scrip, "UnderlyingSeg": underlying_seg}
        data = _request_json("POST", url, json_body=payload, timeout=15)
        return data.get("data", []) or []

    def get_option_chain(self, underlying_scrip: int, expiry: str, underlying_seg: str = "IDX_I") -> Dict[str, Any]:
        url = f"{API_BASE}/optionchain"
        payload = {"UnderlyingScrip": underlying_scrip, "UnderlyingSeg": underlying_seg, "Expiry": expiry}
        return _request_json("POST", url, json_body=payload, timeout=15)
