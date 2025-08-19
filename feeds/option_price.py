# feeds/option_price.py
import os
from datetime import datetime
from typing import Optional, Literal, Dict

Mode = Literal["synthetic", "dhan"]

OPT_PER_SPOT_POINT = float(os.getenv("OPT_PER_SPOT_POINT", "1.0"))  # k factor
DEFAULT_ENTRY_OPT_PRICE = float(os.getenv("DEFAULT_ENTRY_OPT_PRICE", "100"))
PAPER_LTP_MODE: Mode = os.getenv("PAPER_LTP_MODE", "synthetic").lower()  # synthetic|dhan

def get_ltp(trade: Dict, spot: float) -> Optional[float]:
    """
    Return paper LTP for the option in `trade`. Trade dict needs:
      - side: "CE"|"PE"
      - fill_spot: float
      - fill_opt_price: float (fallback to DEFAULT_ENTRY_OPT_PRICE)
      - k_factor: float (optional; else OPT_PER_SPOT_POINT)
    """
    mode: Mode = PAPER_LTP_MODE if PAPER_LTP_MODE in ("synthetic", "dhan") else "synthetic"
    if mode == "dhan":
        # TODO: Implement using Dhan instrument (requires instrument id & /ltp endpoint).
        # Keep API specifics out of this patch; return None to fall back to synthetic.
        pass

    # synthetic
    base = float(trade.get("fill_opt_price") or DEFAULT_ENTRY_OPT_PRICE)
    k = float(trade.get("k_factor") or OPT_PER_SPOT_POINT)
    fill_spot = float(trade.get("fill_spot") or spot)
    if trade.get("side") == "CE":
        return max(0.0, base + (spot - fill_spot) * k)
    else:  # PE
        return max(0.0, base + (fill_spot - spot) * k)
