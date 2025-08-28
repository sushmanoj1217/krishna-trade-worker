# scripts/supervisor.py
# Runs: paper_entry_maker (entries), paper_exit_watcher (exits)
# + optional nightly eod_tuner at 18:10 Asia/Kolkata
# Single-file, no external deps. Python 3.11+ (zoneinfo ok).

from __future__ import annotations
import asyncio
import os
import sys
import signal
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

# ------------ Config helpers ------------

def _env_bool(name: str, default: bool = False) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}

def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.environ.get(name, default)).strip())
    except Exception:
        return default

def _now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")

# ------------ Processes we supervise ------------

@dataclass
class ProcSpec:
    name: str
    cmd: List[str]
    backoff_sec: int = 3
    backoff_max: int = 60

async def run_and_restart(spec: ProcSpec, env: Dict[str, str]) -> None:
    """Run a subprocess; on exit, restart with exponential backoff."""
    while True:
        print(f"{_now_ts()} [SUP] starting `{spec.name}`: {' '.join(spec.cmd)}", flush=True)
        try:
            proc = await asyncio.create_subprocess_exec(
                *spec.cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env=env,
            )
        except Exception as e:
            print(f"{_now_ts()} [SUP][{spec.name}] spawn failed: {e}", flush=True)
            await asyncio.sleep(min(spec.backoff_sec, spec.backoff_max))
            spec.backoff_sec = min(spec.backoff_sec * 2, spec.backoff_max)
            continue

        # stream logs
        assert proc.stdout is not None
        async for line in proc.stdout:
            try:
                txt = line.decode(errors="replace").rstrip("\n")
            except Exception:
                txt = str(line).rstrip("\n")
            print(f"{_now_ts()} [{spec.name}] {txt}", flush=True)

        rc = await proc.wait()
        print(f"{_now_ts()} [SUP][{spec.name}] exited rc={rc}", flush=True)
        # backoff then restart
        await asyncio.sleep(spec.backoff_sec)
        spec.backoff_sec = min(spec.backoff_sec * 2, spec.backoff_max)

# ------------ Nightly EOD tuner ------------

def _seconds_until_next_run_ist(hour: int, minute: int) -> int:
    if ZoneInfo is None:
        # Fallback: assume local time
        now = time.time()
        t = time.localtime(now)
        today_target = time.struct_time((
            t.tm_year, t.tm_mon, t.tm_mday, hour, minute, 0,
            t.tm_wday, t.tm_yday, t.tm_isdst
        ))
        target_ts = time.mktime(today_target)
        if target_ts <= now:
            target_ts += 86400
        return max(1, int(round(target_ts - now)))
    else:
        import datetime as dt
        tz = ZoneInfo("Asia/Kolkata")
        now = dt.datetime.now(tz)
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if target <= now:
            target = target + dt.timedelta(days=1)
        return int((target - now).total_seconds())

async def nightly_tuner(env: Dict[str, str]) -> None:
    enabled = _env_bool("EOD_TUNER_ENABLED", True)
    if not enabled:
        print(f"{_now_ts()} [TUNER] disabled via EOD_TUNER_ENABLED=0", flush=True)
        return

    hour = _env_int("EOD_TUNER_HOUR", 18)     # 18:10 IST default
    minute = _env_int("EOD_TUNER_MINUTE", 10)

    cmd = [sys.executable, "-m", "scripts.eod_tuner"]
    while True:
        wait_s = _seconds_until_next_run_ist(hour, minute)
        print(f"{_now_ts()} [TUNER] next run in ~{wait_s}s at {hour:02d}:{minute:02d} IST", flush=True)
        await asyncio.sleep(wait_s)
        print(f"{_now_ts()} [TUNER] running: {' '.join(cmd)}", flush=True)
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env=env,
            )
            assert proc.stdout is not None
            async for line in proc.stdout:
                try:
                    txt = line.decode(errors="replace").rstrip("\n")
                except Exception:
                    txt = str(line).rstrip("\n")
                print(f"{_now_ts()} [eod_tuner] {txt}", flush=True)
            rc = await proc.wait()
            print(f"{_now_ts()} [TUNER] finished rc={rc}", flush=True)
        except Exception as e:
            print(f"{_now_ts()} [TUNER] spawn failed: {e}", flush=True)
            # try again next day
            await asyncio.sleep(5)

# ------------ Main ------------

async def main() -> None:
    # Required envs should already be set from your existing setup
    # (DHAN_*, GOOGLE_SA_JSON, GSHEET_TRADES_SPREADSHEET_ID, OC_SYMBOL, etc.)
    loop_secs = _env_int("LOOP_SECS", 18)
    entry_loop = _env_int("ENTRY_LOOP_SECS", loop_secs)
    exit_loop  = _env_int("EXIT_LOOP_SECS",  loop_secs)

    # Respect your dry-run toggles if provided; Supervisor just passes env through.
    env = dict(os.environ)

    entry_cmd = [sys.executable, "-m", "scripts.paper_entry_maker", "--loop", str(entry_loop)]
    exit_cmd  = [sys.executable, "-m", "scripts.paper_exit_watcher", "--loop", str(exit_loop)]

    tasks = [
        asyncio.create_task(run_and_restart(ProcSpec("entry", entry_cmd), env)),
        asyncio.create_task(run_and_restart(ProcSpec("exit",  exit_cmd),  env)),
    ]

    # optional nightly tuner
    if _env_bool("EOD_TUNER_RUN_IN_SUPERVISOR", True):
        tasks.append(asyncio.create_task(nightly_tuner(env)))
    else:
        print(f"{_now_ts()} [SUP] nightly tuner disabled (EOD_TUNER_RUN_IN_SUPERVISOR=0)", flush=True)

    # Handle SIGTERM/SIGINT for clean shutdown
    stop = asyncio.Event()
    def _signal_handler(*_):
        print(f"{_now_ts()} [SUP] signal received, shutting down...", flush=True)
        stop.set()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_running_loop().add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            pass  # Windows

    # heartbeat
    async def heartbeat():
        while not stop.is_set():
            print(f"{_now_ts()} [SUP] heartbeat ok", flush=True)
            await asyncio.sleep(60)

    tasks.append(asyncio.create_task(heartbeat()))
    await stop.wait()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    print(f"{_now_ts()} [SUP] bye", flush=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"{_now_ts()} [SUP] interrupted", flush=True)
