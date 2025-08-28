# analytics/paper_exit.py
# Paper-mode exits: TP/SL + 15:15 hard close, underlying-spot पर आधारित।
# किसी broker order की ज़रूरत नहीं—सिर्फ Sheets की Trades rows अपडेट होती हैं।
from __future__ import annotations
import os
from dataclasses import dataclass
from typing import Optional, Dict, Any
from datetime import datetime, time

# ---- Defaults (env/Params_Override से भी आ सकते हैं) ----
DEF_TP = float(os.environ.get("TP_POINTS", "40"))
DEF_SL = float(os.environ.get("SL_POINTS", "20"))
DEF_TRAIL_TRIG = float(os.environ.get("TRAIL_TRIGGER_POINTS", "25"))
DEF_TRAIL_OFF = float(os.environ.get("TRAIL_OFFSET_POINTS", "15"))

# 15:15 IST पर flat
AUTO_FLAT_HH = int(os.environ.get("AUTO_FLAT_HOUR", "15"))
AUTO_FLAT_MM = int(os.environ.get("AUTO_FLAT_MIN", "15"))

def _now_ist_naive() -> datetime:
    # Render में tz aware नहीं मानते—IST समय ही ऑप्स में यूज़ हो रहा है
    # अगर सिस्टम UTC है तो भी cut-off intent यही है (15:15 local routine)
    return datetime.now()

@dataclass
class ExitParams:
    tp: float = DEF_TP
    sl: float = DEF_SL
    trail_trigger: float = DEF_TRAIL_TRIG
    trail_offset: float = DEF_TRAIL_OFF

@dataclass
class TradeRow:
    # लचीले parsing के लिए पहले से normalized values
    id: Optional[str]
    symbol: str
    side: str               # "CE" या "PE"
    status: str             # "OPEN" / "CLOSED"
    entry_level: float      # shifted trigger @ entry (S1*/S2*/R1*/R2*)
    entry_spot: float       # entry के समय का underlying spot (अगर खाली हो तो entry_level यूज)
    qty: float              # lot-size नहीं पता तो 1 treat
    trail_max: Optional[float] = None  # trailing के लिए हाई/लो बुक-कीप
    # raw row भी साथ रखें ताकि writer direct map कर सके
    raw: Dict[str, Any] = None

def _ceilnum(x: Optional[str | float]) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        if isinstance(x, (int, float)):
            return float(x)
        return float(str(x).replace(",", ""))
    except Exception:
        return None

def _norm_side(s: str) -> str:
    if not s: return ""
    s = s.strip().upper()
    if "CALL" in s: return "CE"
    if "PUT" in s: return "PE"
    if s in ("CE", "PE"): return s
    # fallbacks: BUY_CE/SELL_PE इत्यादि नहीं चाहिए—paper only BUY at triggers
    return s

def _auto_flat_due(now: datetime) -> bool:
    return (now.time() >= time(hour=AUTO_FLAT_HH, minute=AUTO_FLAT_MM))

def evaluate_exit(spot: float, trade: TradeRow, p: ExitParams) -> Dict[str, Any]:
    """
    Return: {"action": "HOLD"|"EXIT", "reason": str, "exit_spot": float, "pnl_points": float | None }
    PnL points underlying-move से approx; paper mode में यही ठीक है.
    """
    now = _now_ist_naive()

    # Hard auto-flat at 15:15
    if _auto_flat_due(now):
        pnl = (spot - trade.entry_level) if trade.side == "CE" else (trade.entry_level - spot)
        return {"action": "EXIT", "reason": "AUTO_FLAT_15_15", "exit_spot": spot, "pnl_points": pnl}

    base = trade.entry_level if (trade.entry_level is not None) else (trade.entry_spot or spot)
    if base is None:
        # safety: बिना base के कुछ नहीं करेंगे
        return {"action": "HOLD", "reason": "NO_BASE_LEVEL", "exit_spot": spot, "pnl_points": None}

    # Fixed TP/SL by underlying distance
    up = spot - base
    dn = base - spot

    if trade.side == "CE":
        if up >= p.tp:
            return {"action": "EXIT", "reason": f"TP_HIT(+{p.tp})", "exit_spot": spot, "pnl_points": up}
        if dn >= p.sl:
            return {"action": "EXIT", "reason": f"SL_HIT(-{p.sl})", "exit_spot": spot, "pnl_points": -dn}
        # Optional trailing (simple): trigger पर आए तो trailing stop = (current - offset)
        if p.trail_trigger > 0 and p.trail_offset > 0:
            # पिछला high ट्रैक करें (paper: trail_max)
            tmax = trade.trail_max or (trade.entry_spot or base)
            if spot > tmax:
                tmax = spot
            trail_stop = tmax - p.trail_offset
            # trigger तभी लगे जब (spot - base) >= trigger
            if (spot - base) >= p.trail_trigger and spot <= trail_stop:
                pnl = spot - base
                return {"action": "EXIT", "reason": f"TRAIL_STOP({p.trail_offset})", "exit_spot": spot, "pnl_points": pnl}
            # HOLD + updated tmax
            return {"action": "HOLD", "reason": "CE_TRAILING", "exit_spot": spot, "pnl_points": None, "trail_max": tmax}

        return {"action": "HOLD", "reason": "CE_HOLD", "exit_spot": spot, "pnl_points": None}

    elif trade.side == "PE":
        if dn >= p.tp:
            return {"action": "EXIT", "reason": f"TP_HIT(+{p.tp})", "exit_spot": spot, "pnl_points": dn}
        if up >= p.sl:
            return {"action": "EXIT", "reason": f"SL_HIT(-{p.sl})", "exit_spot": spot, "pnl_points": -up}
        if p.trail_trigger > 0 and p.trail_offset > 0:
            tmin = trade.trail_max  # reuse field name; store as "trail_min" semantically
            if tmin is None:
                tmin = trade.entry_spot or base
            if spot < tmin:
                tmin = spot
            trail_stop = tmin + p.trail_offset
            if (base - spot) >= p.trail_trigger and spot >= trail_stop:
                pnl = base - spot
                return {"action": "EXIT", "reason": f"TRAIL_STOP({p.trail_offset})", "exit_spot": spot, "pnl_points": pnl}
            return {"action": "HOLD", "reason": "PE_TRAILING", "exit_spot": spot, "pnl_points": None, "trail_max": tmin}

        return {"action": "HOLD", "reason": "PE_HOLD", "exit_spot": spot, "pnl_points": None}

    else:
        return {"action": "HOLD", "reason": "SIDE_UNKNOWN", "exit_spot": spot, "pnl_points": None}
