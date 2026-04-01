from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from telegram import Update

from app.auth import AuthService
from app.bot import build_application
from app.config import DEFAULT_CONFIG_PATH, load_config
from app.database import Database
from app.inkypi_adapter import InkyPiAdapter
from app.logging_setup import configure_logging
from app.models import AppServices
from app.render import RenderService
from app.storage import StorageService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Telegram to InkyPi photo frame")
    parser.add_argument("--config", default=None, help="Path to config.yaml")
    parser.add_argument("--log-level", default="INFO", help="Python logging level")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging(args.log_level)
    logger = logging.getLogger(__name__)

    config = load_config(args.config)
    storage = StorageService(config.storage)
    storage.ensure_directories()
    config_path = Path(
        args.config
        or os.getenv("PHOTO_FRAME_CONFIG")
        or os.getenv("CONFIG_FILE")
        or DEFAULT_CONFIG_PATH
    ).expanduser()

    database = Database(config.database.path)
    database.initialize()
    database.seed_admins(config.security.admin_user_ids)
    database.seed_whitelist(config.security.whitelisted_user_ids)

    services = AppServices(
        config_path=config_path,
        config=config,
        database=database,
        auth=AuthService(database),
        storage=storage,
        renderer=RenderService(config.display),
        display=InkyPiAdapter(config.inkypi, config.storage, config.display, database=database),
    )

    logger.info("Starting Telegram photo frame bot")
    logger.info("Pending Telegram updates will be preserved across restarts")
    application = build_application(services)
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
