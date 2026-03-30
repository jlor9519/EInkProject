from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import load_config
from app.render import RenderService
from app.storage import StorageService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a local bridge image for visual inspection")
    parser.add_argument("--config", default=None, help="Path to config.yaml")
    parser.add_argument("--source", required=True, help="Path to the source image")
    parser.add_argument("--output", help="Optional output path")
    parser.add_argument("--location", default="Berlin")
    parser.add_argument("--taken-at", default="2026-03-18")
    parser.add_argument("--caption", default="A test image for the photo frame.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    storage = StorageService(config.storage)
    storage.ensure_directories()
    renderer = RenderService(config.display)
    image_id = storage.generate_image_id()
    output_path = Path(args.output) if args.output else storage.rendered_path(image_id)
    renderer.render(
        Path(args.source),
        output_path,
        location=args.location,
        taken_at=args.taken_at,
        caption=args.caption,
    )
    print(output_path)


if __name__ == "__main__":
    main()
