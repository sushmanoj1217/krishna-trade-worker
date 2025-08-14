#!/usr/bin/env python3
# Krishna Trade Worker — Night Cron wrapper
# Purpose: Run your main command, then auto-stop after MAX_RUNTIME_MINS
# so Render Cron billing closes cleanly.

import os, time, signal, subprocess, sys

def getenv_int(name, default):
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

def send_signal_tree(proc, sig):
    try:
        if hasattr(os, "killpg") and hasattr(os, "getpgid"):
            os.killpg(os.getpgid(proc.pid), sig)
        else:
            proc.send_signal(sig)
    except Exception as e:
        print(f"[cron_wrapper] sending {sig} failed: {e}", flush=True)

def main():
    max_mins  = getenv_int("MAX_RUNTIME_MINS", 300)
    grace_secs = getenv_int("CRON_GRACE_SECS", 30)
    kill_secs  = getenv_int("CRON_KILL_SECS", 15)

    cmd = os.getenv("CRON_CMD")
    if not cmd:
        print("[cron_wrapper] ERROR: CRON_CMD is not set. Example: CRON_CMD='python -u krishna_main.py'", flush=True)
        sys.exit(2)

    # Night defaults (can be overridden per job env)
    os.environ.setdefault("SHIFT_MODE", "NIGHT")
    os.environ.setdefault("WORKER_ID", "NIGHT_A")
    os.environ.setdefault("AUTO_TRADE", "off")
    os.environ.setdefault("OC_MODE", "sheet")

    print(f"[cron_wrapper] Starting command: {cmd}", flush=True)
    print(f"[cron_wrapper] MAX_RUNTIME_MINS={max_mins}, GRACE={grace_secs}s, KILL={kill_secs}s", flush=True)

    start = time.time()
    max_secs = max_mins * 60

    preexec = os.setsid if hasattr(os, "setsid") else None
    if preexec:
        proc = subprocess.Popen(cmd, shell=True, preexec_fn=preexec)
    else:
        proc = subprocess.Popen(cmd, shell=True)

    try:
        while True:
            ret = proc.poll()
            if ret is not None:
                print(f"[cron_wrapper] Process exited with code {ret}", flush=True)
                sys.exit(ret)

            if time.time() - start >= max_secs:
                print("[cron_wrapper] Time limit reached. Graceful shutdown...", flush=True)
                send_signal_tree(proc, signal.SIGINT)

                t0 = time.time()
                while time.time() - t0 < grace_secs:
                    if proc.poll() is not None:
                        print("[cron_wrapper] Exited after SIGINT.", flush=True)
                        sys.exit(proc.returncode or 0)
                    time.sleep(1)

                print("[cron_wrapper] Forcing termination (SIGTERM)...", flush=True)
                send_signal_tree(proc, signal.SIGTERM)

                t1 = time.time()
                while time.time() - t1 < kill_secs:
                    if proc.poll() is not None:
                        print("[cron_wrapper] Exited after SIGTERM.", flush=True)
                        sys.exit(proc.returncode or 0)
                    time.sleep(1)

                print("[cron_wrapper] Killing (SIGKILL)...", flush=True)
                send_signal_tree(proc, signal.SIGKILL)
                sys.exit(0)

            time.sleep(1)

    except KeyboardInterrupt:
        print("[cron_wrapper] KeyboardInterrupt: exiting.", flush=True)
        sys.exit(0)

if __name__ == "__main__":
    main()
