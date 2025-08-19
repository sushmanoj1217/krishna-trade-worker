from utils.logger import log
from housekeeping.auto_backup import run as backup_run
from agents.eod_tuner import run as tuner_run
from agents.backtest_runner import run as backtest_run
from integrations import sheets as sh

def ops_mem_backup():
    backup_run(); return "mem_backup: ok"

def ops_git_file_update():
    # placeholder (CI/CD)
    sh.append_row("Status", [sh.now_str(), "git_file_update", "noop"])
    return "git_file_update: noop"

def ops_render_restart():
    # cannot restart from code on Render; log intent
    sh.append_row("Status", [sh.now_str(), "render_restart", "requested"])
    return "render_restart: requested (manual)";

def ops_tick_speed(value: int):
    return f"tick speed change request to {value}s (set OC_REFRESH_SECS)"

def ops_diag_conflict():
    # simple diag dump
    return "diag: ok"

# Learning queue (placeholders)
_QUEUE = []

def ops_learn(payload: str):
    _QUEUE.append(("learn", payload)); return f"queued learn: {payload[:40]}..."

def ops_queue():
    return f"queue size: {len(_QUEUE)}"

def ops_approve(idx: int):
    if 0 <= idx < len(_QUEUE):
        item = _QUEUE.pop(idx); return f"approved: {item}"
    return "index out of range"

def ops_list():
    return "\n".join(f"{i}: {x}" for i,x in enumerate(_QUEUE)) or "empty"
