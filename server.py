#!/usr/bin/env python3
"""Run Garden Friends and apply saved connection changes with a clean restart."""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent
RESTART_GRACE_SECONDS = 20


def marker_signature(path: Path):
    try:
        data = path.read_bytes()
        info = path.stat()
    except FileNotFoundError:
        return None
    if len(data) > 16384:
        raise RuntimeError('Unexpected restart marker size.')
    return info.st_mtime_ns, len(data), hashlib.sha256(data).digest()


def stop_child(child, grace=RESTART_GRACE_SECONDS):
    if child.poll() is not None:
        return child.returncode
    child.send_signal(signal.SIGTERM)
    try:
        return child.wait(timeout=grace)
    except subprocess.TimeoutExpired:
        child.kill()
        return child.wait(timeout=5)


def supervise(command=None, data_directory=None, poll_interval=0.25):
    """One child at a time; marker changes are coalesced before the next launch."""
    data = Path(data_directory or os.environ.get('GARDEN_FRIENDS_DATA_DIR', ROOT / 'data')).resolve()
    data.mkdir(mode=0o700, parents=True, exist_ok=True)
    request = data / 'restart.request'
    command = command or [sys.executable, '-u', str(ROOT / 'main.py'), *sys.argv[1:]]
    stopping = False
    child = None
    previous_handlers = {}

    def shutdown(signum, frame):
        nonlocal stopping
        stopping = True

    for signum in (signal.SIGTERM, signal.SIGINT):
        previous_handlers[signum] = signal.signal(signum, shutdown)
    try:
        while not stopping:
            seen = marker_signature(request)
            env = dict(os.environ, GARDEN_FRIENDS_DATA_DIR=str(data), PYTHONUNBUFFERED='1')
            child = subprocess.Popen(command, cwd=ROOT, env=env)
            while not stopping:
                code = child.poll()
                if code is not None:
                    return code
                time.sleep(poll_interval)
                current = marker_signature(request)
                if current != seen:
                    print('[Garden Friends] Applying saved connection settings.', flush=True)
                    stop_child(child)
                    child = None
                    break
        return 0
    finally:
        if child is not None:
            stop_child(child)
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


if __name__ == '__main__':
    try:
        sys.exit(supervise())
    except (OSError, RuntimeError) as exc:
        print('[Garden Friends] Supervisor stopped: ' + type(exc).__name__, file=sys.stderr)
        sys.exit(1)
