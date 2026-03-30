from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import load_config
from app.inkypi_adapter import InkyPiAdapter
from app.models import DisplayRequest
from app.storage import StorageService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manual InkyPi display hook")
    parser.add_argument("--config", default=None, help="Path to config.yaml")
    parser.add_argument("--image", help="Path to a composed bridge image")
    parser.add_argument("--original", help="Path to the original image", default="")
    parser.add_argument("--image-id", default="manual-trigger")
    parser.add_argument("--location", default="Manual")
    parser.add_argument("--taken-at", default="Now")
    parser.add_argument("--caption", default="Manual refresh")
    parser.add_argument("--refresh-only", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    storage = StorageService(config.storage)
    storage.ensure_directories()
    display = InkyPiAdapter(config.inkypi, config.storage, config.display)

    if args.refresh_only:
        result = display.refresh_only()
    else:
        if not args.image:
            raise SystemExit("--image is required unless --refresh-only is used")
        request = DisplayRequest(
            image_id=args.image_id,
            original_path=Path(args.original or args.image),
            composed_path=Path(args.image),
            location=args.location,
            taken_at=args.taken_at,
            caption=args.caption,
            created_at="manual",
            uploaded_by=0,
        )
        result = display.display(request)

    if not result.success:
        raise SystemExit(result.message)
    print(result.message)


if __name__ == "__main__":
    main()
