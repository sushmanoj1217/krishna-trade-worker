#!/usr/bin/env python
import os
import sys
from datetime import datetime, timezone

# Ensure src on path when Render cron runs from repo root
SRC = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from utils.logger import log
from integrations import sheets as sh

def main():
    job = sys.argv[1] if len(sys.argv) > 1 else "unknown"
    log.info(f"cron: cron_start {job}")
    try:
        if job == "archive":
            # pruning handled by sheets_admin.py; kept here for simple delegation
            log.info("cron: use scripts/sheets_admin.py archive")
        elif job == "backup":
            from housekeeping.auto_backup import nightly_backup
            nightly_backup()
        else:
            log.info(f"cron: {job}_start")
            log.info(f"cron: {job}_done")
    except Exception as e:
        log.error(f"cron: {job} failed: {e}")
    finally:
        log.info(f"cron: cron_done {job}")

if __name__ == "__main__":
    main()
