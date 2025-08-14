# integrations/dhan.py
# Dhan Option Chain client (low-memory, streaming CSV resolver)

import os
import io
import csv
import requests
from typing import Dict, Any, Optional

API_BASE = "https://api.dhan.co/v2"
INSTR_CSV_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"


def _hdrs() -> Dict[str, str]:
    """HTTP headers required by Dhan Data APIs"""
    return {
        "access-token": os.getenv("DHAN_ACCESS_TOKEN", ""),  # paste exact token (no spaces/newlines)
        "client-id": os.getenv("DHAN_CLIENT_ID", ""),        # your Dhan client id
        "Content-Type": "application/json",
    }


def _sym_norm(s: str) -> str:
    """Normalize common index name variations."""
    s = (s or "").strip().upper()
    if s in ("BANK NIFTY", "NIFTY BANK", "BANK-NIFTY", "BANKNIFTY"):
        return "BANKNIFTY"
    if s in ("FINNIFTY", "FININFTY", "FININFTI", "FININIFTY", "FININFTIY", "FININNIFTY"):
        return "FINNIFTY"
    if s in ("NIFTY50", "NIFTY 50"):
        return "NIFTY"
    return s


def _env_usid(sym: str) -> Optional[int]:
    """
    Optional env overrides to skip the big CSV entirely.
    - DHAN_USID_MAP e.g. "NIFTY=26000,BANKNIFTY=26009,FINNIFTY=26037"
    - or per symbol: DHAN_USID_NIFTY=26000, DHAN_USID_BANKNIFTY=26009 ...
    """
    # Map form
    m = os.getenv("DHAN_USID_MAP", "")
    if m:
        for pair in m.split(","):
            if "=" in pair:
                k, v = pair.split("=", 1)
                if _sym_norm(k) == sym:
                    try:
                        return int(v)
                    except:
                        pass
    # Per-symbol form
    v = os.getenv(f"DHAN_USID_{sym}", "")
    if v:
        try:
            return int(v)
        except:
            pass
    return None


class DhanClient:
    """
    Thin client for Dhan Data APIs used by our worker.
    - resolve_underlying_scrip: finds UNDERLYING_SECURITY_ID for an index by streaming the instrument CSV
    - get_expiries: POST /v2/optionchain/expirylist
    - get_option_chain: POST /v2/optionchain
    """

    def resolve_underlying_scrip(self, symbol: str) -> Optional[int]:
        """
        Returns Dhan UNDERLYING_SECURITY_ID for the given index symbol (NIFTY/BANKNIFTY/FINNIFTY).
        Memory-safe: streams CSV in text mode; does NOT load entire file into RAM.
        """
        sym = _sym_norm(symbol)

        # 0) Fast path: env overrides
        usid_env = _env_usid(sym)
        if usid_env:
            return usid_env

        # 1) Stream the CSV and stop at first match
        with requests.get(INSTR_CSV_URL, stream=True, timeout=20) as r:
            r.raise_for_status()
            # Wrap the raw bytes stream as text for csv.DictReader
            r.raw.decode_content = True
            text_stream = io.TextIOWrapper(r.raw, encoding="utf-8", newline="")
            reader = csv.DictReader(text_stream)  # streaming dict rows

            for row in reader:
                try:
                    if row.get("INSTRUMENT") != "OPTIDX":
                        continue
                    underlying = (row.get("UNDERLYING_SYMBOL") or "").upper()
                    if underlying == sym:
                        usid_str = row.get("UNDERLYING_SECURITY_ID") or ""
                        return int(usid_str)
                except Exception:
                    # skip malformed row
                    continue

        # Not found
        return None

    def get_expiries(self, underlying_scrip: int, underlying_seg: str = "IDX_I"):
        """POST /v2/optionchain/expirylist → returns list[str] of YYYY-MM-DD."""
        url = f"{API_BASE}/optionchain/expirylist"
        payload = {"UnderlyingScrip": underlying_scrip, "UnderlyingSeg": underlying_seg}
        resp = requests.post(url, headers=_hdrs(), json=payload, timeout=15)
        resp.raise_for_status()
        data = resp.json() or {}
        return data.get("data", []) or []

    def get_option_chain(self, underlying_scrip: int, expiry: str, underlying_seg: str = "IDX_I") -> Dict[str, Any]:
        """POST /v2/optionchain → returns OC JSON (includes 'data': {'last_price', 'oc': {...}})."""
        url = f"{API_BASE}/optionchain"
        payload = {"UnderlyingScrip": underlying_scrip, "UnderlyingSeg": underlying_seg, "Expiry": expiry}
        resp = requests.post(url, headers=_hdrs(), json=payload, timeout=15)
        resp.raise_for_status()
        return resp.json() or {}
