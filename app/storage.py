from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

logger = logging.getLogger(__name__)

from app.models import StorageConfig
from app.fs_utils import directory_is_writable, free_disk_bytes, safe_unlink


class StorageService:
    def __init__(self, config: StorageConfig):
        self.config = config

    def ensure_directories(self) -> None:
        for path in self._directories():
            path.mkdir(parents=True, exist_ok=True)

    def generate_image_id(self) -> str:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        return f"{stamp}_{uuid.uuid4().hex[:8]}"

    def original_path(self, image_id: str, extension: str = ".jpg") -> Path:
        return self.config.incoming_dir / f"{image_id}{extension}"

    def rendered_path(self, image_id: str, extension: str = ".png") -> Path:
        return self.config.rendered_dir / f"{image_id}{extension}"

    def healthcheck(self) -> bool:
        details = self.health_details()
        return bool(details["paths_exist"] and details["writable"])

    def health_details(self) -> dict[str, int | bool | None]:
        directories = list(self._directories())
        paths_exist = all(path.exists() for path in directories)
        writable = all(directory_is_writable(path, logger=logger) for path in directories)
        free_bytes = free_disk_bytes(self.config.current_payload_path.parent)
        return {
            "paths_exist": paths_exist,
            "writable": writable,
            "free_bytes": free_bytes,
        }

    def cleanup_rendered_cache(self) -> None:
        keep = max(self.config.keep_recent_rendered, 0)
        rendered_files = sorted(
            (path for path in self.config.rendered_dir.glob("*") if path.is_file()),
            key=lambda item: item.stat().st_mtime,
            reverse=True,
        )
        to_remove = [f for f in rendered_files[keep:] if f.name != ".gitkeep"]
        if to_remove:
            logger.info("Cleaning up %d old rendered file(s)", len(to_remove))
        for old_file in to_remove:
            safe_unlink(old_file, logger=logger)

    def _directories(self) -> Iterable[Path]:
        return (
            self.config.incoming_dir,
            self.config.rendered_dir,
            self.config.cache_dir,
            self.config.archive_dir,
            self.config.inkypi_payload_dir,
            self.config.current_payload_path.parent,
            self.config.current_image_path.parent,
        )
