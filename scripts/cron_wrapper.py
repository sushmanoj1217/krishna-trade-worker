#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Render Cron friendly runner with Sheet markers, DRY_RUN, jitter & lock.

Usage (Cron Command examples):
  python scripts/cron_wrapper.py all
  python scripts/cron_wrapper.py tuner
  python scripts/cron_wrapper.py backtest
  python scripts/cron_wrapper.py backup

Recommended env on night jobs:
  DISABLE_TELEGRAM=1       # to avoid polling conflicts
  DRY_RUN=1 or 0           # 1 = test-only (no heavy jobs), 0 = real
  CRON_JITTER_SEC=0..60    # optional random delay before starting
  CRON_LOCKFILE=/tmp/ktw_night.lock   # optional path override
"""

# --- FIX: ensure project root on sys.path even if cron runs from another CWD ---
import os, sys, time, random
HERE = os.path.abspath(os.path.dirname(__file__))
ROOT = os.path.abspath(os.path.join(HERE, ".."))   # /opt/render/project/src
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
# --- end fix ---

from contextlib import contextmanager
from utils.logger import log
from integrations.sheets import append_row, now_str

DRY_RUN = os.getenv("DRY_RUN", "0").strip() == "1"
JITTER = int(os.getenv("CRON_JITTER_SEC", "0") or 0)
LOCKFILE = os.getenv("CRON_LOCKFILE", "/tmp/ktw_night.lock")

@contextmanager
def _lockfile(path: str = LOCKFILE):
    """
    Prevent overlapping runs. If the file exists, we skip gracefully.
    """
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o644)
        os.write(fd, str(os.getpid()).encode("utf-8"))
        os.close(fd)
        yield
    except FileExistsError:
        log.warning("cron: another run in progress; exiting")
        try:
            append_row("Status", [now_str(), "cron_skip", "lock_exists"])
        except Exception:
            pass
    finally:
        try:
            os.remove(path)
        except Exception:
            pass

def _mark(event: str, detail: str = ""):
    msg = f"{event}{(' ' + detail) if detail else ''}"
    log.info(f"cron: {msg}")
    try:
        append_row("Status", [now_str(), event, detail])
    except Exception:
        # Sheets unavailable? Ignore—cron should still succeed.
        pass

def do_tuner():
    _mark("tuner_start")
    if not DRY_RUN:
        from agents.eod_tuner import run as tuner_run
        try:
            tuner_run()
        finally:
            _mark("tuner_done")
    else:
        _mark("tuner_done")

def do_backtest():
    _mark("backtest_start")
    if not DRY_RUN:
        from agents.backtest_runner import run as backtest_run
        try:
            backtest_run()
        finally:
            _mark("backtest_done")
    else:
        _mark("backtest_done")

def do_backup():
    _mark("backup_start")
    if not DRY_RUN:
        try:
            from housekeeping.auto_backup import run as backup_run
            backup_run()
        except Exception as e:
            log.warning(f"backup skipped: {e}")
            _mark("backup_skip", str(e)[:140])
            return
        finally:
            _mark("backup_done")
    else:
        _mark("backup_done")

def main():
    if len(sys.argv) < 2:
        print("Usage: cron_wrapper.py all|tuner|backtest|backup", file=sys.stderr)
        sys.exit(2)

    cmd = (sys.argv[1] or "").lower()

    # optional jitter to de-conflict multiple jobs starting at same minute
    if JITTER > 0:
        s = random.randint(0, JITTER)
        log.info(f"cron: jitter sleep {s}s")
        time.sleep(s)

    with _lockfile():
        _mark("cron_start", cmd)
        if cmd == "all":
            do_tuner(); do_backtest(); do_backup()
        elif cmd == "tuner":
            do_tuner()
        elif cmd == "backtest":
            do_backtest()
        elif cmd == "backup":
            do_backup()
        else:
            print("unknown cmd", file=sys.stderr)
            sys.exit(2)
        _mark("cron_done", cmd)

if __name__ == "__main__":
    main()
