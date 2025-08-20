# scripts/cron_wrapper.py
"""
Render Cron friendly runner with Sheet markers, DRY_RUN, jitter & lock.

Examples:
  python scripts/cron_wrapper.py all
  python scripts/cron_wrapper.py tuner
  python scripts/cron_wrapper.py backtest
  python scripts/cron_wrapper.py backup
Env:
  DISABLE_TELEGRAM=1   # recommended on night jobs
  DRY_RUN=1            # just log, don't run heavy jobs
  CRON_JITTER_SEC=0..60  # optional random delay to de-conflict
"""
from __future__ import annotations
import sys, os, time, random
from contextlib import contextmanager
from utils.logger import log
from integrations.sheets import append_row, now_str

DRY_RUN = os.getenv("DRY_RUN", "0") == "1"
JITTER = int(os.getenv("CRON_JITTER_SEC", "0") or 0)

@contextmanager
def _lockfile(name: str = "/tmp/ktw_night.lock"):
    try:
        fd = os.open(name, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o644)
        os.write(fd, str(os.getpid()).encode("utf-8"))
        os.close(fd)
        yield
    except FileExistsError:
        log.warning("cron: another run in progress; exiting")
        try: append_row("Status", [now_str(), "cron_skip", "lock_exists"])
        except Exception: pass
    finally:
        try: os.remove(name)
        except Exception: pass

def _mark(event: str, detail: str = ""):
    msg = f"{event}{(' ' + detail) if detail else ''}"
    log.info(f"cron: {msg}")
    try: append_row("Status", [now_str(), event, detail])
    except Exception: pass

def do_tuner():
    _mark("tuner_start")
    if not DRY_RUN:
        from agents.eod_tuner import run as tuner_run
        tuner_run()
    _mark("tuner_done")

def do_backtest():
    _mark("backtest_start")
    if not DRY_RUN:
        from agents.backtest_runner import run as backtest_run
        backtest_run()
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
    _mark("backup_done")

def main():
    if len(sys.argv) < 2:
        print("Usage: cron_wrapper.py all|tuner|backtest|backup"); sys.exit(2)
    cmd = sys.argv[1].lower()

    # optional jitter
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
            print("unknown cmd"); sys.exit(2)
        _mark("cron_done", cmd)

if __name__ == "__main__":
    main()
