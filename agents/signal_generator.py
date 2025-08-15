# agents/signal_generator.py
# Adapter to use Option-Chain based rules (agents/oc_rules.py)
# Returns a normalized signal dict ready for the executor.

from typing import Optional, Dict, Any
from datetime import datetime
from agents.oc_rules import OCCtx, evaluate as evaluate_oc

def _to_float(x, default=None):
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default

def generate_signal_from_oc(oc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Input `oc` expected keys (scanner provides):
      symbol, spot, s1, s2, r1, r2, ce_oi_pct?, pe_oi_pct?, volume_low?
    Returns None or dict:
      {
        symbol, side("BUY_CE"/"BUY_PE"), reason, level_tag, level, view,
        target_pct, sl_pct, exit_by_ist, dedup_key, instrument_hint("ATM_CE"/"ATM_PE")
      }
    """
    ctx = OCCtx(
        symbol=str(oc.get("symbol") or oc.get("sym") or "NIFTY"),
        spot=_to_float(oc.get("spot"), 0.0),
        s1=_to_float(oc.get("s1"), 0.0),
        s2=_to_float(oc.get("s2"), 0.0),
        r1=_to_float(oc.get("r1"), 0.0),
        r2=_to_float(oc.get("r2"), 0.0),
        ce_oi_pct=_to_float(oc.get("ce_oi_pct")) if oc.get("ce_oi_pct") is not None else None,
        pe_oi_pct=_to_float(oc.get("pe_oi_pct")) if oc.get("pe_oi_pct") is not None else None,
        volume_low=bool(oc.get("volume_low")) if oc.get("volume_low") is not None else None,
        now=oc.get("now") if isinstance(oc.get("now"), datetime) else None,
    )

    sig = evaluate_oc(ctx)
    if not sig:
        return None

    # Suggest ATM instrument by side
    sig["instrument_hint"] = "ATM_CE" if sig.get("side") == "BUY_CE" else "ATM_PE"
    sig["symbol"] = ctx.symbol
    return sig
