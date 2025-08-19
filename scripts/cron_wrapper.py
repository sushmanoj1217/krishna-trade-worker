# scripts/cron_wrapper.py
"""
Render Cron friendly runner.

Examples:
  python scripts/cron_wrapper.py all
  python scripts/cron_wrapper.py tuner
  python scripts/cron_wrapper.py backtest
  python scripts/cron_wrapper.py backup
"""
from __future__ import annotations
import sys, os, time
from utils.logger import log

def do_tuner():
    from agents.eod_tuner import run as tuner_run
    tuner_run()
    log.info("cron: tuner done")

def do_backtest():
    from agents.backtest_runner import run as backtest_run
    backtest_run()
    log.info("cron: backtest done")

def do_backup():
    try:
        from housekeeping.auto_backup import run as backup_run
        backup_run()
    except Exception as e:
        log.warning(f"backup skipped: {e}")
    log.info("cron: backup done")

def main():
    if len(sys.argv) < 2:
        print("Usage: cron_wrapper.py all|tuner|backtest|backup"); sys.exit(2)
    cmd = sys.argv[1].lower()
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

if __name__ == "__main__":
    main()
