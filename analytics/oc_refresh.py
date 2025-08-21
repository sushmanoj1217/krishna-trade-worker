import os
import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from utils.logger import log
from utils.params import Params
from utils.cache import set_snapshot, get_snapshot as _get_snapshot
from integrations.option_chain_dhan import fetch_levels
from integrations import sheets as sh

@dataclass
class OCSnapshot:
    ts: datetime
    spot: float
    s1: float
    s2: float
    r1: float
    r2: float
    expiry: str
    vix: float | None
    pcr: float | None
    max_pain: float
    bias: str | None
    stale: bool = False

COOLDOWN = {"until": None}

def get_snapshot() -> OCSnapshot | None:
    return _get_snapshot()

async def day_oc_loop():
    """Refresh OC snapshot; write to OC_Live; respect rate-limit cooldown."""
    now = datetime.now(timezone.utc)
    until = COOLDOWN.get("until")
    if until and now < until:
        # don't spam; if no cache, log error
        if not _get_snapshot():
            raise RuntimeError("In cooldown and no OC cache available")
        await asyncio.sleep(1)
        return

    p = Params.from_env()
    try:
        oc = await fetch_levels(p)
        snap = OCSnapshot(
            ts=datetime.now(timezone.utc),
            spot=oc["spot"],
            s1=oc["s1"], s2=oc["s2"], r1=oc["r1"], r2=oc["r2"],
            expiry=oc["expiry"],
            vix=oc.get("vix"),
            pcr=oc.get("pcr"),
            max_pain=oc["max_pain"],
            bias=oc.get("bias_tag"),
            stale=False
        )
        set_snapshot(snap)
        # write to sheet (best-effort)
        try:
            await sh.log_oc_live(snap)
        except Exception as e:
            log.warning(f"OC Live write failed: {e}")
    except fetch_levels.TooManyRequests as e:
        log.warning(f"Dhan 429: {e} → cooldown 30s")
        COOLDOWN["until"] = datetime.now(timezone.utc) + timedelta(seconds=30)
        raise
    except Exception as e:
        log.error(f"OC refresh failed: {e}")
        if not _get_snapshot():
            raise
        # else keep old cache
        return
