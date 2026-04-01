from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

from app.config import load_config
from app.database import Database
from app.fs_utils import safe_unlink


@dataclass(slots=True)
class CleanupSummary:
    deleted_images: int
    deleted_files: int


def clear_all_images(
    database: Database,
    current_payload_path: Path,
    current_image_path: Path,
) -> CleanupSummary:
    records = database.get_images_excluding(None)

    deleted_files = 0
    deleted_paths: set[Path] = set()
    for record in records:
        for path_value in (record.local_original_path, record.local_rendered_path):
            if not path_value:
                continue
            path = Path(path_value)
            if path in deleted_paths:
                continue
            existed = safe_unlink(path)
            if existed:
                deleted_files += 1
                deleted_paths.add(path)
        database.delete_image(record.image_id)

    for path in (current_payload_path, current_image_path):
        if path in deleted_paths:
            continue
        existed = safe_unlink(path)
        if existed:
            deleted_files += 1
            deleted_paths.add(path)

    return CleanupSummary(
        deleted_images=len(records),
        deleted_files=deleted_files,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clear all stored images and local display payload files")
    parser.add_argument("--config", default=None, help="Path to config.yaml")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    database = Database(config.database.path)
    database.initialize()
    summary = clear_all_images(
        database,
        config.storage.current_payload_path,
        config.storage.current_image_path,
    )
    print(
        f"Cleared {summary.deleted_images} image record(s) and removed {summary.deleted_files} file(s)."
    )


if __name__ == "__main__":
    main()
