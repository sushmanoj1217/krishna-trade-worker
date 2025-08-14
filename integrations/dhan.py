# integrations/dhan.py
import os, csv, requests, io  
from typing import Dict, Any, Optional

API_BASE = "https://api.dhan.co/v2"
INSTR_CSV_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"

def _hdrs():
    return {
        "access-token": os.getenv("DHAN_ACCESS_TOKEN", ""),
        "client-id": os.getenv("DHAN_CLIENT_ID", ""),
        "Content-Type": "application/json",
    }

def _sym_norm(s: str) -> str:
    s = (s or "").strip().upper()
    if s in ("BANK NIFTY", "NIFTY BANK", "BANK-NIFTY", "BANKNIFTY"): return "BANKNIFTY"
    if s in ("FINNIFTY", "FININFTY", "FININFTI", "FININFTY", "FININFTIY", "FININNIFTY"): return "FINNIFTY"
    if s in ("NIFTY50", "NIFTY 50"): return "NIFTY"
    return s

def _env_usid(sym: str) -> Optional[int]:
    """Optional override via env to avoid CSV altogether.
       Examples:
       DHAN_USID_MAP=NIFTY=26000,BANKNIFTY=26009,FINNIFTY=26037
       or DHAN_USID_NIFTY=26000  (per-symbol)
    """
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

class DhanClient:
    def resolve_underlying_scrip(self, symbol: str) -> Optional[int]:
        """Memory-safe streaming resolve: reads CSV line-by-line and returns at first match."""
        sym = _sym_norm(symbol)

        # 1) Env override (fastest; zero memory)
        usid_env = _env_usid(sym)
        if usid_env:
            return usid_env

        # 2) Stream CSV; do NOT load entire file in memory
        with requests.get(INSTR_CSV_URL, stream=True, timeout=20) as r:
            r.raise_for_status()
            # Build a streaming CSV reader
            lines = r.iter_lines(decode_unicode=True)
            reader = csv.reader(lines)
            headers = next(reader)  # first line
            idx = {h: i for i, h in enumerate(headers)}

            # Required columns (defensive)
            need = ["INSTRUMENT", "UNDERLYING_SYMBOL", "UNDERLYING_SECURITY_ID", "SYMBOL_NAME", "DISPLAY_NAME"]
            for k in need:
                if k not in idx:
                    return None

            for row in reader:
                try:
                    instrument = row[idx["INSTRUMENT"]]
                    if instrument != "OPTIDX":
                        continue
                    underlying = (row[idx["UNDERLYING_SYMBOL"]] or "").upper()
                    if underlying == sym:
                        usid_str = row[idx["UNDERLYING_SECURITY_ID"]] or ""
                        return int(usid_str)
                except Exception:
                    # skip bad row
                    continue

        # Fallback not found
        return None

    def get_expiries(self, underlying_scrip: int, underlying_seg: str = "IDX_I"):
        url = f"{API_BASE}/optionchain/expirylist"
        j = {"UnderlyingScrip": underlying_scrip, "UnderlyingSeg": underlying_seg}
        r = requests.post(url, headers=_hdrs(), json=j, timeout=15)
        r.raise_for_status()
        data = r.json() or {}
        return data.get("data", []) or []

    def get_option_chain(self, underlying_scrip: int, expiry: str, underlying_seg: str = "IDX_I") -> Dict[str, Any]:
        url = f"{API_BASE}/optionchain"
        j = {"UnderlyingScrip": underlying_scrip, "UnderlyingSeg": underlying_seg, "Expiry": expiry}
        r = requests.post(url, headers=_hdrs(), json=j, timeout=15)
        r.raise_for_status()
        return r.json() or {}
