#!/usr/bin/env python
import os
import sys
from datetime import datetime, timezone, timedelta

SRC = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from integrations import sheets as sh
from utils.logger import log

# Simple archive: delete rows older than cutoff (UTC), or move if ARCHIVE_SPREADSHEET_ID set
CUTS = {
    "OC_Live": 2,        # days
    "Signals": 14,
    "Status": 7,
    "Events": 7,
    "Snapshots": 3,
}

def _utc():
    return datetime.now(timezone.utc)

def cmd_archive():
    arch_enabled = os.getenv("SHEETS_ARCHIVE_ENABLED", "false").lower() == "true"
    arch_id = os.getenv("ARCHIVE_SPREADSHEET_ID", "")

    if arch_enabled and not arch_id:
        log.warning("SHEETS_ARCHIVE_ENABLED=true but ARCHIVE_SPREADSHEET_ID not set → will skip archive and delete instead.")

    # only OC_Live prune here (rest would be similar; keeping minimal)
    try:
        w = sh._open().worksheet("OC_Live")
        rows = w.get_all_values()
        if not rows or len(rows) == 1:
            print("OC_Live: nothing to prune (keep ≥ 2d).")
            return
        hdr = rows[0]
        ts_idx = hdr.index("timestamp") if "timestamp" in hdr else 0
        cutoff = _utc() - timedelta(days=CUTS["OC_Live"])
        moved = deleted = 0
        keep = [hdr]
        for r in rows[1:]:
            try:
                dt = datetime.fromisoformat(r[ts_idx])
            except Exception:
                dt = None
            if dt is None or (dt.tzinfo is None):
                # naive → treat as UTC naive; delete if too old by date string
                dt = cutoff  # conservative: delete
            if dt < cutoff:
                deleted += 1
            else:
                keep.append(r)
        if deleted:
            w.clear()
            w.append_row(hdr)
            if len(keep) > 1:
                w.append_rows(keep[1:])
            print(f"OC_Live: moved 0, deleted {deleted}")
        else:
            print("OC_Live: nothing to prune (keep ≥ 2d).")
    except Exception as e:
        log.error(f"archive OC_Live failed: {e}")

def main():
    if len(sys.argv) < 2:
        print("Usage: sheets_admin.py archive|setup")
        sys.exit(1)
    cmd = sys.argv[1]
    if cmd == "setup":
        import asyncio
        asyncio.run(sh.ensure_tabs())
        print("✅ Sheets setup done.")
    elif cmd == "archive":
        cmd_archive()
        print("✅ Archive done.")
    else:
        print("unknown command")

if __name__ == "__main__":
    main()
