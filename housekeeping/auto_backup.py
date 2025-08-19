"""
Nightly auto-backup:
- Dumps Params_Override & last OC snapshot to 'Snapshots' tab
- Optionally pushes 'ops_mem_backup' row
"""
from utils.logger import log
from utils.cache import get_snapshot
from integrations import sheets as sh

def run():
    snap = get_snapshot()
    if snap:
        sh.append_row("Snapshots", [
            sh.now_str(), snap.spot, snap.s1, snap.s2, snap.r1, snap.r2,
            snap.pcr, snap.max_pain, snap.max_pain_dist, snap.expiry, snap.bias_tag
        ])
    # params overrides already persisted by tuner via Params_Override
    sh.append_row("Events", [sh.now_str(), "auto_backup", "ok"])
    log.info("Auto-backup complete")
