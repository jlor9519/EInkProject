from __future__ import annotations

import argparse
import logging

from telegram import Update

from app.auth import AuthService
from app.bot import build_application
from app.config import load_config
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

    database = Database(config.database.path)
    database.initialize()
    database.seed_admins(config.security.admin_user_ids)
    database.seed_whitelist(config.security.whitelisted_user_ids)

    services = AppServices(
        config=config,
        database=database,
        auth=AuthService(database),
        storage=storage,
        renderer=RenderService(config.display),
        display=InkyPiAdapter(config.inkypi, config.storage, config.display),
    )

    logger.info("Starting Telegram photo frame bot")
    application = build_application(services)
    application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
