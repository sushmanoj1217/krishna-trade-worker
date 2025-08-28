#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Supervisor that runs:
  - scripts.paper_entry_maker  (entry loop)
  - scripts.paper_exit_watcher (exit loop)
  - scripts.eod_tuner          (nightly at ~18:10 IST, optional)

Key fixes:
  * Child processes launched with -u (unbuffered) + env PYTHONUNBUFFERED=1
  * UTF-8 output; stdout is streamed line-by-line immediately
  * Heartbeat logs
  * Auto-restart on crash

Env knobs (optional):
  LOOP_SECS                -> default 18 (used if ENTRY/EXIT specific not set)
  ENTRY_LOOP_SECS          -> override for entry loop
  EXIT_LOOP_SECS           -> override for exit loop
  EOD_TUNER_RUN_IN_SUPERVISOR -> 1/0 (default 1 = run)
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

# ---------- helpers ----------

def _now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")

def _env_int(name: str, default: int) -> int:
    v = os.environ.get(name)
    if not v:
        return default
    try:
        return int(v)
    except Exception:
        return default

def _env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    v = v.strip().lower()
    if v in ("1", "true", "yes", "y", "on"):
        return True
    if v in ("0", "false", "no", "n", "off"):
        return False
    return default

def _ist_now() -> datetime:
    # IST = UTC+5:30
    return datetime.now(timezone(timedelta(hours=5, minutes=30)))

def _sec_human(n: int) -> str:
    if n < 60:
        return f"{n}s"
    m, s = divmod(n, 60)
    if m < 60:
        return f"{m}m{s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m"

@dataclass
class ProcSpec:
    name: str
    cmd: list[str]

# ---------- core runners ----------

async def run_and_restart(spec: ProcSpec, env: dict) -> None:
    """Run a child process, stream logs, restart if it exits."""
    backoff = 1
    while True:
        print(f"{_now_ts()} [SUP] starting `{spec.name}`: {' '.join(spec.cmd)}", flush=True)
        try:
            proc = await asyncio.create_subprocess_exec(
                *spec.cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env=env,
                cwd=os.getcwd(),  # keep working dir consistent
            )
        except Exception as e:
            print(f"{_now_ts()} [SUP] spawn failed `{spec.name}`: {e}", flush=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
            continue

        backoff = 1  # reset on successful spawn

        assert proc.stdout is not None
        try:
            # Stream child's stdout line by line
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                try:
                    text = line.decode("utf-8", errors="replace").rstrip("\n\r")
                except Exception:
                    text = str(line).rstrip("\n\r")
                # Prefix with child name
                print(f"[{spec.name}] {text}", flush=True)
        except Exception as e:
            print(f"{_now_ts()} [SUP] stream error `{spec.name}`: {e}", flush=True)

        rc = await proc.wait()
        print(f"{_now_ts()} [SUP] `{spec.name}` exited with code {rc}. Restarting in 3s...", flush=True)
        await asyncio.sleep(3)

async def nightly_tuner(env: dict) -> None:
    """Run eod_tuner once daily at ~18:10 IST."""
    # Schedule next 18:10 IST
    while True:
        now = _ist_now()
        run_time = now.replace(hour=18, minute=10, second=0, microsecond=0)
        if run_time <= now:
            run_time = run_time + timedelta(days=1)
        wait_sec = int((run_time - now).total_seconds())
        print(f"{_now_ts()} [TUNER] next run in ~{_sec_human(wait_sec)} at {run_time.strftime('%H:%M')} IST", flush=True)
        await asyncio.sleep(wait_sec)

        # Run tuner
        cmd = [sys.executable, "-u", "-m", "scripts.eod_tuner"]
        print(f"{_now_ts()} [TUNER] running: {' '.join(cmd)}", flush=True)
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env=env,
                cwd=os.getcwd(),
            )
            assert proc.stdout is not None
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").rstrip("\n\r")
                print(f"[tuner] {text}", flush=True)
            rc = await proc.wait()
            print(f"{_now_ts()} [TUNER] exit code {rc}", flush=True)
        except Exception as e:
            print(f"{_now_ts()} [TUNER] failed to run: {e}", flush=True)

async def heartbeat() -> None:
    while True:
        print(f"{_now_ts()} [SUP] heartbeat ok", flush=True)
        await asyncio.sleep(60)

async def main() -> None:
    loop_secs = _env_int("LOOP_SECS", 18)
    entry_loop = _env_int("ENTRY_LOOP_SECS", loop_secs)
    exit_loop  = _env_int("EXIT_LOOP_SECS",  loop_secs)

    # Inherit env + force unbuffered I/O in children
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"

    # Build commands with -u (unbuffered)
    entry_cmd = [sys.executable, "-u", "-m", "scripts.paper_entry_maker", "--loop", str(entry_loop)]
    exit_cmd  = [sys.executable, "-u", "-m", "scripts.paper_exit_watcher", "--loop", str(exit_loop)]

    tasks = [
        asyncio.create_task(run_and_restart(ProcSpec("entry", entry_cmd), env)),
        asyncio.create_task(run_and_restart(ProcSpec("exit",  exit_cmd),  env)),
        asyncio.create_task(heartbeat()),
    ]

    if _env_bool("EOD_TUNER_RUN_IN_SUPERVISOR", True):
        tasks.append(asyncio.create_task(nightly_tuner(env)))
    else:
        print(f"{_now_ts()} [SUP] nightly tuner disabled (EOD_TUNER_RUN_IN_SUPERVISOR=0)", flush=True)

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"{_now_ts()} [SUP] interrupted", flush=True)
