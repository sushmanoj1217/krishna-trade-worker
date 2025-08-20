# agents/signal_emit.py
from __future__ import annotations

from typing import Any, Dict, List, Optional
from utils.logger import log

try:
    # our unified Sheets wrapper (handles throttling, 10M cap, etc.)
    from integrations import sheets as sh
except Exception:
    class _S:
        def write_signal_row(self, *a, **k): pass
        def tap_signal_row(self, *a, **k): pass
    sh = _S()  # type: ignore


# --- Column order expected by your master brief ---
_EXPECTED_SIGNAL_COLS: List[str] = [
    "signal_id", "ts", "side", "trigger",
    "c1", "c2", "c3", "c4", "c5", "c6",
    "eligible", "reason",
    "mv_pcr_ok", "mv_mp_ok", "mv_basis",
    "oc_bull_normal", "oc_bull_shortcover", "oc_bear_normal", "oc_bear_crash",
    "oc_pattern_basis", "near/cross", "notes",
]

def _bool_cell(x: Optional[bool]) -> str:
    if x is True:  return "TRUE"
    if x is False: return "FALSE"
    return ""

def _to_row_from_dict(sig: Dict[str, Any]) -> List[Any]:
    """Map a signal dict to the exact column order required by Signals sheet."""
    return [
        sig.get("signal_id", ""),
        sig.get("ts", ""),
        str(sig.get("side", "")).upper(),  # CE/PE
        sig.get("trigger", ""),             # S1*/S2*/R1*/R2*

        _bool_cell(sig.get("c1")),
        _bool_cell(sig.get("c2")),
        _bool_cell(sig.get("c3")),
        _bool_cell(sig.get("c4")),
        _bool_cell(sig.get("c5")),
        _bool_cell(sig.get("c6")),

        _bool_cell(sig.get("eligible")),
        sig.get("reason", "") or sig.get("eligible_reason", ""),

        _bool_cell(sig.get("mv_pcr_ok")),
        _bool_cell(sig.get("mv_mp_ok")),
        sig.get("mv_basis", ""),

        _bool_cell(sig.get("oc_bull_normal")),
        _bool_cell(sig.get("oc_bull_shortcover")),
        _bool_cell(sig.get("oc_bear_normal")),
        _bool_cell(sig.get("oc_bear_crash")),

        sig.get("oc_pattern_basis", ""),
        sig.get("near/cross", "") or sig.get("near_cross", ""),
        sig.get("notes", ""),
    ]


def emit_signal_row_from_dict(sig: Dict[str, Any]) -> List[Any]:
    """
    Build correct row order from dict, memory-tap it (so /oc_now works even if Sheets capped),
    then best-effort append to Signals sheet.
    """
    row = _to_row_from_dict(sig)
    try:
        sh.tap_signal_row(row)   # memory → /oc_now
    except Exception as e:
        log.warning(f"signal_emit: tap failed: {e}")

    try:
        sh.write_signal_row(row) # Sheets (throttled / cap-safe)
    except Exception as e:
        log.warning(f"signal_emit: write failed: {e}")
    return row


def emit_signal_row_from_list(row: List[Any]) -> List[Any]:
    """
    If your generator already produces row in the exact expected order,
    use this to tap + write with zero refactor.
    """
    try:
        sh.tap_signal_row(row)
    except Exception as e:
        log.warning(f"signal_emit: tap failed: {e}")

    try:
        sh.write_signal_row(row)
    except Exception as e:
        log.warning(f"signal_emit: write failed: {e}")
    return row
