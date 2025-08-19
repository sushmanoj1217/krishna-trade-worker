# integrations/option_chain_dhan.py
import os, requests, csv, io
from datetime import date
from tenacity import retry, stop_after_attempt, wait_fixed
from utils.logger import log

DHAN_BASE = os.getenv("DHAN_BASE", "https://api.dhan.co").rstrip("/")
DHAN_UNDERLYING_SCRIP = os.getenv("DHAN_UNDERLYING_SCRIP", "").strip()   # if empty, auto-resolve
DHAN_UNDERLYING_SEG = os.getenv("DHAN_UNDERLYING_SEG", "IDX_I").strip()  # indices: IDX_I
DHAN_EXPIRY_ENV = os.getenv("DHAN_EXPIRY", "").strip()                   # optional YYYY-MM-DD

MASTER_CSV_URL = os.getenv(
    "DHAN_MASTER_CSV",
    "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
)

def _headers():
    token = os.getenv("DHAN_ACCESS_TOKEN", "")
    cid = os.getenv("DHAN_CLIENT_ID", "")
    if not token or not cid:
        raise RuntimeError("DHAN credentials missing (DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN)")
    return {
        "access-token": token,
        "client-id": cid,
        "Content-Type": "application/json",
    }

def _post_json(url: str, json_payload: dict, timeout=12):
    r = requests.post(url, headers=_headers(), json=json_payload, timeout=timeout)
    if r.status_code >= 400:
        log.warning(f"Dhan {r.status_code}: {r.text[:300]} @ {url}")
    r.raise_for_status()
    return r.json()

@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
def get_expiry_list(underlying_scrip: int, underlying_seg: str) -> list[str]:
    url = f"{DHAN_BASE}/v2/optionchain/expirylist"
    payload = {"UnderlyingScrip": underlying_scrip, "UnderlyingSeg": underlying_seg}
    data = _post_json(url, payload)
    return (data.get("data") or []) if isinstance(data, dict) else []

def _pick_nearest_expiry(expiries: list[str]) -> str | None:
    if not expiries:
        return None
    today = date.today().isoformat()
    future = sorted([d for d in expiries if d >= today])
    return future[0] if future else sorted(expiries)[0]

@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
def get_option_chain_v2(underlying_scrip: int, underlying_seg: str, expiry: str) -> dict:
    url = f"{DHAN_BASE}/v2/optionchain"
    payload = {
        "UnderlyingScrip": underlying_scrip,
        "UnderlyingSeg": underlying_seg,
        "Expiry": expiry
    }
    return _post_json(url, payload)

def _guess_index_names(symbol: str) -> list[str]:
    s = (symbol or "").upper()
    if s == "NIFTY":
        return ["NIFTY 50", "NIFTY50"]
    if s == "BANKNIFTY":
        return ["NIFTY BANK", "BANK NIFTY", "BANKNIFTY"]
    if s == "FINNIFTY":
        return ["NIFTY FINANCIAL SERVICES", "FINNIFTY"]
    return [s]

@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
def _fetch_master_csv_text() -> str:
    r = requests.get(MASTER_CSV_URL, timeout=12)
    r.raise_for_status()
    r.encoding = "utf-8"
    return r.text

def _find_security_id_from_master(symbol: str) -> int | None:
    """
    Download Dhan scrip master CSV and locate Security ID for index underlyings.
    We scan common name columns and take the first match.
    """
    text = _fetch_master_csv_text()
    f = io.StringIO(text)
    reader = csv.DictReader(f)
    name_cols = [  # try in this order, case-insensitive contains
        "Scrip Name", "Name", "Instrument Name", "Trading Symbol", "Security Name"
    ]
    id_cols = ["Security ID", "SecurityID", "SecurityId", "Sec ID"]
    targets = [t.upper() for t in _guess_index_names(os.getenv("OC_SYMBOL", "NIFTY"))]

    for row in reader:
        row_up = {k: (v or "").strip() for k, v in row.items()}
        hay = " ".join([row_up.get(c, "") for c in name_cols]).upper()
        if any(t in hay for t in targets):
            # Segment gate (prefer index-type)
            seg = (row_up.get("Segment") or row_up.get("Exch Seg") or "").upper()
            if "IDX" not in seg and "INDEX" not in seg:
                continue
            # pull ID
            for idc in id_cols:
                val = row_up.get(idc)
                if val and val.isdigit():
                    sid = int(val)
                    log.info(f"Resolved SecurityID {sid} for {targets[0]} from scrip master")
                    return sid
    return None

def ensure_inputs() -> tuple[int, str, str | None]:
    if DHAN_UNDERLYING_SCRIP.isdigit():
        return int(DHAN_UNDERLYING_SCRIP), DHAN_UNDERLYING_SEG, (DHAN_EXPIRY_ENV or None)
    # auto-resolve from master
    sid = _find_security_id_from_master(os.getenv("OC_SYMBOL", "NIFTY"))
    if not sid:
        raise RuntimeError("DHAN_UNDERLYING_SCRIP missing/invalid and auto-resolve failed. "
                           "Set Security ID from Dhan scrip master CSV.")
    return sid, DHAN_UNDERLYING_SEG, (DHAN_EXPIRY_ENV or None)

def compute_levels_from_oc_v2(oc_json: dict, used_expiry: str) -> dict:
    data = oc_json.get("data") or {}
    oc = data.get("oc") or {}
    spot = float(data.get("last_price") or 0.0)

    rows = []
    pe_sum = 0
    ce_sum = 0
    for k, v in oc.items():
        try:
            strike = float(k)
        except Exception:
            continue
        ce_oi = int((v.get("ce") or {}).get("oi") or 0)
        pe_oi = int((v.get("pe") or {}).get("oi") or 0)
        rows.append((strike, ce_oi, pe_oi))
        ce_sum += ce_oi
        pe_sum += pe_oi

    if not rows:
        raise RuntimeError("Empty option chain data")

    top_pe = sorted(rows, key=lambda t: t[2], reverse=True)
    top_ce = sorted(rows, key=lambda t: t[1], reverse=True)
    s1, s2 = (top_pe[0][0], top_pe[1][0]) if len(top_pe) >= 2 else (None, None)
    r1, r2 = (top_ce[0][0], top_ce[1][0]) if len(top_ce) >= 2 else (None, None)
    pcr = round(pe_sum / ce_sum, 4) if ce_sum > 0 else None
    max_pain = max(rows, key=lambda t: (t[1] + t[2]))[0] if rows else None

    return {
        "spot": spot,
        "s1": s1, "s2": s2, "r1": r1, "r2": r2,
        "pcr": pcr,
        "max_pain": max_pain,
        "expiry": used_expiry,
    }

def fetch_levels() -> dict:
    u_scrip, u_seg, expiry_override = ensure_inputs()
    expiry = expiry_override
    if not expiry:
        expiries = get_expiry_list(u_scrip, u_seg)
        expiry = _pick_nearest_expiry(expiries)
        if not expiry:
            raise RuntimeError("No expiry available from Dhan")
    oc = get_option_chain_v2(u_scrip, u_seg, expiry)
    return compute_levels_from_oc_v2(oc, expiry)
