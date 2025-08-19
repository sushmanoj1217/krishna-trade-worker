# analytics/market_context.py
import requests, time
from agents import logger

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

def compute_pcr_from_chain(oc_items) -> float | None:
    """
    oc_items: iterable of dicts with at least {'call_oi':..., 'put_oi':...}
    Returns PCR = sum(put_oi) / sum(call_oi) or None.
    """
    call_oi = 0
    put_oi = 0
    for it in oc_items or []:
        c = it.get("call_oi") or it.get("ce_oi")
        p = it.get("put_oi") or it.get("pe_oi")
        try:
            if c is not None: call_oi += float(c)
            if p is not None: put_oi += float(p)
        except Exception:
            continue
    if call_oi > 0:
        return put_oi / call_oi
    return None

def fetch_india_vix() -> float | None:
    url = "https://www.nseindia.com/api/allIndices?index=INDIA%20VIX"
    s = requests.Session()
    s.headers.update({"user-agent": UA, "accept": "application/json"})
    try:
        # Warm cookie
        s.get("https://www.nseindia.com", timeout=5)
        time.sleep(0.3)
        r = s.get(url, timeout=6)
        r.raise_for_status()
        js = r.json()
        for item in js.get("data", []):
            if item.get("indexSymbol") == "INDIA VIX":
                return float(item.get("last"))
    except Exception:
        return None
    return None

def write_context(oc_items=None):
    pcr = compute_pcr_from_chain(oc_items) if oc_items is not None else None
    vix = fetch_india_vix()
    logger.log_market_context(pcr=pcr, vix=vix)
    return {"PCR": pcr, "VIX": vix}
