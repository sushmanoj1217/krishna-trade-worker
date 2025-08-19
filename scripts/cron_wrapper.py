"""
Render Cron friendly runner for night jobs:
- eod_tuner
- backtest_runner
- auto_backup
"""
import sys
from utils.logger import log
from agents.eod_tuner import run as tuner_run
from agents.backtest_runner import run as backtest_run
from housekeeping.auto_backup import run as backup_run

def main():
    job = (sys.argv[1] if len(sys.argv)>1 else "all").lower()
    if job in ("tuner","all"):
        tuner_run()
    if job in ("backtest","all"):
        backtest_run()
    if job in ("backup","all"):
        backup_run()
    log.info(f"cron_wrapper done: {job}")

if __name__ == "__main__":
    main()
