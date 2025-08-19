from utils.logger import log
from integrations import sheets as sh




def run_backup():
try:
sh.append_row("Snapshots", ["auto_backup", "ok"])
log.info("Backup snapshot written")
except Exception as e:
log.error(f"Backup failed: {e}")
