# utils/state.py
from __future__ import annotations
import threading
from typing import Optional, Dict, Any, List

_lock = threading.RLock()

_oc_auto = True

_last_signal: Optional[Dict[str, Any]] = None
_last_signal_placed: bool = False

# approvals
_approvals_required: bool = False
_pending: Dict[str, Dict[str, Any]] = {}  # signal_id -> signal dict

def set_oc_auto(enabled: bool) -> None:
    global _oc_auto
    with _lock:
        _oc_auto = bool(enabled)

def is_oc_auto() -> bool:
    with _lock:
        return _oc_auto

def set_last_signal(sig: Dict[str, Any]) -> None:
    global _last_signal, _last_signal_placed
    with _lock:
        _last_signal = sig
        _last_signal_placed = False

def get_last_signal() -> Optional[Dict[str, Any]]:
    with _lock:
        return dict(_last_signal) if _last_signal else None

def mark_last_signal_placed() -> None:
    global _last_signal_placed
    with _lock:
        _last_signal_placed = True

def is_last_signal_placed() -> bool:
    with _lock:
        return _last_signal_placed

# approvals API
def set_approvals_required(on: bool) -> None:
    global _approvals_required
    with _lock:
        _approvals_required = bool(on)

def approvals_required() -> bool:
    with _lock:
        return _approvals_required

def queue_for_approval(sig: Dict[str, Any]) -> None:
    with _lock:
        _pending[sig["id"]] = sig

def list_pending() -> List[Dict[str, Any]]:
    with _lock:
        return list(_pending.values())

def approve(id_: str) -> bool:
    with _lock:
        sig = _pending.pop(id_, None)
        if not sig:
            return False
        # push to last signal pipe
        set_last_signal(sig)
        return True

def deny(id_: str) -> bool:
    with _lock:
        return _pending.pop(id_, None) is not None
