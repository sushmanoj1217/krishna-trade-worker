
import os, csv, io, requests
from typing import Dict, Any, List, Optional

API_BASE = "https://api.dhan.co/v2"
INSTR_CSV_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"

def _hdrs():
    return {"access-token": os.getenv("DHAN_ACCESS_TOKEN",""),
            "client-id": os.getenv("DHAN_CLIENT_ID",""),
            "Content-Type": "application/json"}

def _sym_norm(s: str) -> str:
    s = (s or "").strip().upper()
    if s in ("BANK NIFTY","NIFTY BANK","BANK-NIFTY","BANKNIFTY"): return "BANKNIFTY"
    if s in ("FINNIFTY","FININFTY","FININFTI","FININFTY","FININFTIY","FININNIFTY","FININFTY"): return "FINNIFTY"
    if s in ("NIFTY50","NIFTY 50"): return "NIFTY"
    return s

class DhanClient:
    def __init__(self):
        self._cache_instr: Optional[List[Dict[str,str]]] = None

    def _get_instruments(self) -> List[Dict[str,str]]:
        if self._cache_instr is not None: return self._cache_instr
        resp = requests.get(INSTR_CSV_URL, timeout=15); resp.raise_for_status()
        rows = list(csv.DictReader(io.StringIO(resp.text)))
        self._cache_instr = rows; return rows

    def resolve_underlying_scrip(self, symbol: str) -> Optional[int]:
        sym = _sym_norm(symbol)
        rows = self._get_instruments()
        for r in rows:
            if r.get("INSTRUMENT") == "OPTIDX" and r.get("UNDERLYING_SYMBOL","").upper() == sym:
                try: return int(r.get("UNDERLYING_SECURITY_ID"))
                except: pass
        for r in rows:
            if r.get("INSTRUMENT") == "INDEX" and sym in (r.get("SYMBOL_NAME","").upper(), r.get("DISPLAY_NAME","").upper()):
                try: return int(r.get("SECURITY_ID"))
                except: pass
        return None

    def get_expiries(self, underlying_scrip: int, underlying_seg: str = "IDX_I") -> List[str]:
        url = f"{API_BASE}/optionchain/expirylist"
        j = {"UnderlyingScrip": underlying_scrip, "UnderlyingSeg": underlying_seg}
        r = requests.post(url, headers=_hdrs(), json=j, timeout=15); r.raise_for_status()
        return (r.json() or {}).get("data", []) or []

    def get_option_chain(self, underlying_scrip: int, expiry: str, underlying_seg: str = "IDX_I") -> Dict[str, Any]:
        url = f"{API_BASE}/optionchain"
        j = {"UnderlyingScrip": underlying_scrip, "UnderlyingSeg": underlying_seg, "Expiry": expiry}
        r = requests.post(url, headers=_hdrs(), json=j, timeout=15); r.raise_for_status()
        return r.json() or {}
