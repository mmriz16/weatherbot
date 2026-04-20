#!/usr/bin/env python3
"""Single-instance runtime guard for the weatherbot main loop."""

from __future__ import annotations

import fcntl
import os
from pathlib import Path


class AlreadyRunningError(RuntimeError):
    """Raised when another weatherbot runner already owns the lock."""



def acquire_lock(lock_path: str | Path):
    path = Path(lock_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+")

    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        handle.seek(0)
        owner = handle.read().strip() or "unknown"
        handle.close()
        raise AlreadyRunningError(f"another weatherbot runner already holds {path} (owner pid: {owner})") from exc

    handle.seek(0)
    handle.truncate()
    handle.write(str(os.getpid()))
    handle.flush()
    return handle



def release_lock(handle) -> None:
    if handle is None:
        return
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        handle.close()
