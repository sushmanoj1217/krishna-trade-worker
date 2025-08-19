import sys
from housekeeping.auto_backup import run_backup
from agents.eod_tuner import run_nightly
from agents.backtest_runner import run_batch

# Usage: python scripts/cron_wrapper.py backup|tuner|backtest [days]

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "backup":
        run_backup()
    elif cmd == "tuner":
        run_nightly()
    elif cmd == "backtest":
        days = int(sys.argv[2]) if len(sys.argv) > 2 else 40
        run_batch(days)
    else:
        print("Usage: backup|tuner|backtest [days]")
