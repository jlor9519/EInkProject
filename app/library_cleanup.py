from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

from app.config import load_config
from app.database import Database


@dataclass(slots=True)
class CleanupSummary:
    preserved_image_id: str | None
    deleted_images: int
    deleted_files: int


def clear_non_current_images(database: Database, current_payload_path: Path) -> CleanupSummary:
    preserved_image_id = _read_current_image_id(current_payload_path)
    records = database.get_images_excluding(preserved_image_id)

    deleted_files = 0
    deleted_paths: set[Path] = set()
    for record in records:
        for path_value in (record.local_original_path, record.local_rendered_path):
            if not path_value:
                continue
            path = Path(path_value)
            if path in deleted_paths:
                continue
            existed = path.exists()
            path.unlink(missing_ok=True)
            if existed:
                deleted_files += 1
                deleted_paths.add(path)
        database.delete_image(record.image_id)

    return CleanupSummary(
        preserved_image_id=preserved_image_id,
        deleted_images=len(records),
        deleted_files=deleted_files,
    )


def _read_current_image_id(current_payload_path: Path) -> str | None:
    if not current_payload_path.exists():
        return None
    try:
        payload = json.loads(current_payload_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    image_id = payload.get("image_id")
    return str(image_id) if image_id else None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clear stored images while preserving the current display payload")
    parser.add_argument("--config", default=None, help="Path to config.yaml")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    database = Database(config.database.path)
    database.initialize()
    summary = clear_non_current_images(database, config.storage.current_payload_path)
    preserved_note = (
        f" Preserved current image: {summary.preserved_image_id}."
        if summary.preserved_image_id
        else ""
    )
    print(
        f"Cleared {summary.deleted_images} image record(s) and removed {summary.deleted_files} file(s)."
        f"{preserved_note}"
    )


if __name__ == "__main__":
    main()
