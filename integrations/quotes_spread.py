# integrations/quotes_spread.py
# -----------------------------------------------------------------------------
# Reads bid/ask for ATM CE/PE from the latest OC snapshot and computes spread.
# Designed to work with provider snapshots that include a 'chain' (list of rows).
#
# Expected chain row (tolerant to key variants):
#   {
#     "strike": 25000,
#     "ce_bid": 12.5, "ce_ask": 13.2, "ce_ltp": 12.9,   # (keys may vary)
#     "pe_bid": 15.7, "pe_ask": 16.4, "pe_ltp": 16.0
#   }
#
# Public:
#   def estimate_spread(sym: str, side: str, spot: float, expiry: str|None, limit_bp: float)
#       -> (ok: bool|None, reason: str)
#   - ok==True  => spread within limit
#   - ok==False => spread too wide
#   - ok==None  => quotes not available (non-blocking)
# -----------------------------------------------------------------------------

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple

# --------- tolerant getters ----------
def _g(row: Dict[str, Any], names: List[str]) -> Optional[float]:
    for n in names:
        if n in row and row[n] not in (None, "", "—"):
            try:
                return float(str(row[n]).replace(",", "").strip())
            except Exception:
                pass
    return None

def _strike_of(row: Dict[str, Any]) -> Optional[float]:
    return _g(row, ["strike", "Strike", "stk", "STRIKE"])

def _ce_bid(row): return _g(row, ["ce_bid","CE_bid","bid_ce","best_bid_ce","ceBestBid","CE_BID"])
def _ce_ask(row): return _g(row, ["ce_ask","CE_ask","ask_ce","best_ask_ce","ceBestAsk","CE_ASK"])
def _ce_ltp(row): return _g(row, ["ce_ltp","CE_ltp","ltp_ce","CE_LTP","ceLtp"])
def _pe_bid(row): return _g(row, ["pe_bid","PE_bid","bid_pe","best_bid_pe","peBestBid","PE_BID"])
def _pe_ask(row): return _g(row, ["pe_ask","PE_ask","ask_pe","best_ask_pe","peBestAsk","PE_ASK"])
def _pe_ltp(row): return _g(row, ["pe_ltp","PE_ltp","ltp_pe","PE_LTP","peLtp"])

def _round_to_step(x: float, step: float) -> float:
    if step <= 0: return x
    return round(x / step) * step

def _strike_step(sym: str) -> float:
    s = (sym or "").upper()
    # defaults; can be overridden by env via caller if needed
    if s == "BANKNIFTY": return 100.0
    if s == "FINNIFTY":  return 50.0
    return 50.0  # NIFTY

def _best_chain_row_for(side: str, spot: float, chain: List[Dict[str, Any]], step: float) -> Optional[Dict[str, Any]]:
    if not chain: return None
    target = _round_to_step(spot, step)
    # prefer exact strike match, else nearest by abs distance
    exact = None; best = None; best_d = None
    for r in chain:
        k = _strike_of(r)
        if k is None: continue
        if k == target:
            exact = r
            break
        d = abs(k - target)
        if best_d is None or d < best_d:
            best, best_d = r, d
    return exact or best

def _chain_from_snapshot(snap: Dict[str, Any]) -> List[Dict[str, Any]]:
    # tolerant keys: 'chain', 'rows', 'data', 'oc'
    for k in ("chain","rows","data","oc_chain","oc"):
        v = snap.get(k)
        if isinstance(v, list):
            return v
    return []

def _compute_bp(bid: Optional[float], ask: Optional[float], ltp: Optional[float]) -> Optional[float]:
    if bid is None or ask is None:
        return None
    mid = ltp if (ltp and ltp > 0) else ( (bid + ask)/2 if (bid>0 and ask>0) else None )
    if not mid or mid <= 0: 
        return None
    spread_pct = (ask - bid) / mid
    return spread_pct * 10000.0  # basis points

def estimate_spread(sym: str, side: str, spot: Optional[float], expiry: Optional[str], limit_bp: float) -> Tuple[Optional[bool], str]:
    """
    Returns (ok, reason).
      ok=True  : spread within limit
      ok=False : spread exceeds limit
      ok=None  : quotes not available (non-blocking)
    """
    if spot is None:
        return None, "quotes n/a (no spot)"

    try:
        # Late import to avoid hard dependency at module import time
        from analytics import oc_refresh  # type: ignore
    except Exception:
        return None, "quotes n/a (oc_refresh import failed)"

    snap = oc_refresh.get_snapshot() or {}
    chain = _chain_from_snapshot(snap)
    if not chain:
        return None, "quotes n/a (no chain)"

    step = _strike_step(sym)
    row = _best_chain_row_for(side, float(spot), chain, step)
    if not row:
        return None, "quotes n/a (no matching strike)"

    if (side or "").upper() == "CE":
        bid, ask, ltp = _ce_bid(row), _ce_ask(row), _ce_ltp(row)
    else:
        bid, ask, ltp = _pe_bid(row), _pe_ask(row), _pe_ltp(row)

    bp = _compute_bp(bid, ask, ltp)
    if bp is None:
        return None, "quotes n/a (bid/ask missing)"

    ok = bp <= float(limit_bp)
    reason = f"spread {bp:.0f}bp ≤ {float(limit_bp):.0f}bp" if ok else f"spread {bp:.0f}bp > {float(limit_bp):.0f}bp"
    return ok, reason
