# analytics/oc_refresh.py
import os
from utils.logger import log
from utils.params import Params
from utils.cache import OCSnapshot, set_snapshot
from integrations import sheets as sh
from integrations.option_chain_dhan import fetch_levels  # NEW

SYMBOL = os.getenv("OC_SYMBOL", "NIFTY").upper()
MODE = os.getenv("OC_MODE", "sheet").lower()

BUFFERS = {"NIFTY": 12, "BANKNIFTY": 30, "FINNIFTY": 15}

def _buffer_for_symbol(sym: str, params: Params) -> int:
    return params.buffer_points or BUFFERS.get(sym.upper(), 12)

def refresh_once() -> OCSnapshot | None:
    params = Params()
    try:
        if MODE == "dhan":
            data = fetch_levels()  # uses env to talk to Dhan v2
        else:
            row = sh.last_row("OC_Live")
            if not row:
                log.warning("OC_Live empty in sheet")
                return None
            data = {
                "spot": float(row.get("spot", 0) or 0),
                "s1": float(row.get("s1", 0) or 0),
                "s2": float(row.get("s2", 0) or 0),
                "r1": float(row.get("r1", 0) or 0),
                "r2": float(row.get("r2", 0) or 0),
                "pcr": float(row.get("pcr", 0) or 0),
                "max_pain": float(row.get("max_pain", 0) or 0),
                "expiry": row.get("expiry", ""),
            }

        b = _buffer_for_symbol(SYMBOL, params)
        s1s = (data["s1"] - b) if data.get("s1") else None
        s2s = (data["s2"] - b) if data.get("s2") else None
        r1s = (data["r1"] + b) if data.get("r1") else None
        r2s = (data["r2"] + b) if data.get("r2") else None

        mp = data.get("max_pain")
        mpd = (data["spot"] - mp) if (mp and data.get("spot")) else None

        bias = None
        if mp and mpd is not None:
            if data["spot"] >= mp + params.mp_support_dist():
                bias = "mv_bull_mp"
            elif data["spot"] <= mp - params.mp_support_dist():
                bias = "mv_bear_mp"

        snap = OCSnapshot(
            spot=data["spot"],
            s1=data.get("s1"),
            s2=data.get("s2"),
            r1=data.get("r1"),
            r2=data.get("r2"),
            expiry=data.get("expiry", ""),
            vix=None,
            pcr=data.get("pcr"),
            max_pain=mp,
            max_pain_dist=mpd,
            bias_tag=bias,
            stale=False,
            extras={"s1s": s1s, "s2s": s2s, "r1s": r1s, "r2s": r2s, "buffer": b},
        )
        set_snapshot(snap)
        return snap

    except Exception as e:
        log.error(f"OC refresh failed: {e}")
        return None
