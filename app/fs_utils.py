from __future__ import annotations

import logging
import shutil
import tempfile
from pathlib import Path


def safe_unlink(path: Path | str | None, *, logger: logging.Logger | None = None) -> bool:
    if not path:
        return False
    target = Path(path)
    try:
        existed = target.exists()
        target.unlink(missing_ok=True)
        return existed
    except OSError:
        if logger is not None:
            logger.warning("Failed to remove file %s", target, exc_info=True)
        return False


def directory_is_writable(path: Path, *, logger: logging.Logger | None = None) -> bool:
    try:
        path.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=path, prefix=".writecheck-", delete=True):
            pass
        return True
    except OSError:
        if logger is not None:
            logger.warning("Directory is not writable: %s", path, exc_info=True)
        return False


def free_disk_bytes(path: Path) -> int | None:
    try:
        usage = shutil.disk_usage(path)
    except OSError:
        return None
    return int(usage.free)
