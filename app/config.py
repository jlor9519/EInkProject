from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

from app.models import (
    AppConfig,
    DatabaseConfig,
    DisplayConfig,
    InkyPiConfig,
    SecurityConfig,
    StorageConfig,
    TelegramConfig,
)
from app.inkypi_paths import DEFAULT_INSTALL_PATH


class ConfigError(ValueError):
    """Raised when the application configuration is invalid."""


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
DEFAULT_ENV_PATH = PROJECT_ROOT / ".env"
DEFAULT_UPDATE_METHOD = "http_update_now"
DEFAULT_UPDATE_NOW_URL = "http://127.0.0.1/update_now"
LEGACY_RESTART_REFRESH_COMMAND = "sudo systemctl restart inkypi.service"
LEGACY_CAPTION_HEIGHT = 132
LEGACY_CAPTION_FONT_SIZE = 28
LEGACY_MAX_CAPTION_LINES = 2
DEFAULT_CAPTION_HEIGHT = 44
DEFAULT_METADATA_FONT_SIZE = 14
DEFAULT_CAPTION_FONT_SIZE = 20
DEFAULT_CAPTION_CHARACTER_LIMIT = 72
DEFAULT_MAX_CAPTION_LINES = 1
DEFAULT_EMOJI_FONT_PATH = "/usr/share/fonts/truetype/noto/NotoColorEmoji.ttf"


def load_config(config_path: str | Path | None = None) -> AppConfig:
    env_path = Path(
        os.getenv("PHOTO_FRAME_ENV_FILE")
        or os.getenv("ENV_FILE")
        or DEFAULT_ENV_PATH
    )
    load_dotenv(env_path)
    path = Path(
        config_path
        or os.getenv("PHOTO_FRAME_CONFIG")
        or os.getenv("CONFIG_FILE")
        or DEFAULT_CONFIG_PATH
    )
    if not path.exists():
        raise ConfigError(
            f"Config file not found at {path}. Copy config/config.example.yaml to config/config.yaml first."
        )

    with path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}

    errors: list[str] = []

    telegram_section = raw.get("telegram", {})
    bot_token_env = str(telegram_section.get("bot_token_env", "TELEGRAM_BOT_TOKEN"))
    bot_token = os.getenv(bot_token_env, "").strip()
    if not bot_token:
        errors.append(f"Missing Telegram bot token in environment variable {bot_token_env}.")

    security_section = raw.get("security", {})
    admin_user_ids = _parse_int_list(security_section.get("admin_user_ids", []), "security.admin_user_ids", errors)
    whitelisted_user_ids = _parse_int_list(
        security_section.get("whitelisted_user_ids", []),
        "security.whitelisted_user_ids",
        errors,
    )

    database_section = raw.get("database", {})
    database_path = _resolve_path(database_section.get("path", "data/db/photo_frame.db"))

    storage_section = raw.get("storage", {})
    storage_config = StorageConfig(
        incoming_dir=_resolve_path(storage_section.get("incoming_dir", "data/incoming")),
        rendered_dir=_resolve_path(storage_section.get("rendered_dir", "data/rendered")),
        cache_dir=_resolve_path(storage_section.get("cache_dir", "data/cache")),
        archive_dir=_resolve_path(storage_section.get("archive_dir", "data/archive")),
        inkypi_payload_dir=_resolve_path(storage_section.get("inkypi_payload_dir", "data/inkypi")),
        current_payload_path=_resolve_path(storage_section.get("current_payload_path", "data/inkypi/current.json")),
        current_image_path=_resolve_path(storage_section.get("current_image_path", "data/inkypi/current.png")),
        keep_recent_rendered=_parse_positive_int(storage_section.get("keep_recent_rendered", 20), "storage.keep_recent_rendered", errors),
    )

    display_section = raw.get("display", {})
    caption_height_value = display_section.get("caption_height", DEFAULT_CAPTION_HEIGHT)
    metadata_font_size_value = display_section.get("metadata_font_size", DEFAULT_METADATA_FONT_SIZE)
    caption_font_size_value = display_section.get("caption_font_size", DEFAULT_CAPTION_FONT_SIZE)
    caption_character_limit_value = display_section.get(
        "caption_character_limit",
        DEFAULT_CAPTION_CHARACTER_LIMIT,
    )
    max_caption_lines_value = display_section.get("max_caption_lines", DEFAULT_MAX_CAPTION_LINES)

    if (
        caption_height_value == LEGACY_CAPTION_HEIGHT
        and caption_font_size_value == LEGACY_CAPTION_FONT_SIZE
        and max_caption_lines_value == LEGACY_MAX_CAPTION_LINES
    ):
        caption_height_value = DEFAULT_CAPTION_HEIGHT
        metadata_font_size_value = DEFAULT_METADATA_FONT_SIZE
        caption_font_size_value = DEFAULT_CAPTION_FONT_SIZE
        max_caption_lines_value = DEFAULT_MAX_CAPTION_LINES

    display_config = DisplayConfig(
        width=_parse_positive_int(display_section.get("width", 800), "display.width", errors),
        height=_parse_positive_int(display_section.get("height", 480), "display.height", errors),
        caption_height=_parse_positive_int(caption_height_value, "display.caption_height", errors),
        margin=_parse_positive_int(display_section.get("margin", 18), "display.margin", errors),
        metadata_font_size=_parse_positive_int(metadata_font_size_value, "display.metadata_font_size", errors),
        caption_font_size=_parse_positive_int(caption_font_size_value, "display.caption_font_size", errors),
        caption_character_limit=_parse_positive_int(
            caption_character_limit_value,
            "display.caption_character_limit",
            errors,
        ),
        max_caption_lines=_parse_positive_int(max_caption_lines_value, "display.max_caption_lines", errors),
        font_path=str(display_section.get("font_path", "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf")),
        background_color=str(display_section.get("background_color", "#F7F3EA")),
        text_color=str(display_section.get("text_color", "#111111")),
        divider_color=str(display_section.get("divider_color", "#3A3A3A")),
        emoji_font_path=str(display_section.get("emoji_font_path", DEFAULT_EMOJI_FONT_PATH)),
    )
    if display_config.caption_height >= display_config.height:
        errors.append("display.caption_height must be smaller than display.height.")

    inkypi_section = raw.get("inkypi", {})
    refresh_command = str(inkypi_section.get("refresh_command", "")).strip()
    raw_update_method = str(inkypi_section.get("update_method", "")).strip()
    if raw_update_method:
        update_method = raw_update_method
    elif not refresh_command or refresh_command == LEGACY_RESTART_REFRESH_COMMAND:
        update_method = DEFAULT_UPDATE_METHOD
    else:
        update_method = "command"

    if update_method not in {"http_update_now", "command"}:
        errors.append("inkypi.update_method must be 'http_update_now' or 'command'.")

    update_now_url = str(inkypi_section.get("update_now_url", DEFAULT_UPDATE_NOW_URL)).strip()
    if update_method == "http_update_now" and not update_now_url:
        errors.append("inkypi.update_now_url is required when inkypi.update_method is 'http_update_now'.")
    if update_method == "command" and not refresh_command:
        errors.append("inkypi.refresh_command is required when inkypi.update_method is 'command'.")
    inkypi_config = InkyPiConfig(
        repo_path=_resolve_path(inkypi_section.get("repo_path", "~/InkyPi")),
        install_path=_resolve_path(inkypi_section.get("install_path", str(DEFAULT_INSTALL_PATH))),
        validated_commit=str(inkypi_section.get("validated_commit", "main")),
        waveshare_model=str(inkypi_section.get("waveshare_model", "epd7in3e")),
        plugin_id=str(inkypi_section.get("plugin_id", "telegram_frame")),
        payload_dir=_resolve_path(inkypi_section.get("payload_dir", storage_config.inkypi_payload_dir)),
        update_method=update_method,
        update_now_url=update_now_url,
        refresh_command=refresh_command,
    )

    if errors:
        raise ConfigError("\n".join(errors))

    return AppConfig(
        telegram=TelegramConfig(bot_token=bot_token),
        security=SecurityConfig(
            admin_user_ids=sorted(set(admin_user_ids)),
            whitelisted_user_ids=sorted(set(whitelisted_user_ids)),
        ),
        database=DatabaseConfig(path=database_path),
        storage=storage_config,
        display=display_config,
        inkypi=inkypi_config,
    )


def _resolve_path(value: Any) -> Path:
    expanded = os.path.expandvars(os.path.expanduser(str(value)))
    path = Path(expanded)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def _parse_int_list(value: Any, field_name: str, errors: list[str]) -> list[int]:
    if value is None:
        return []
    if isinstance(value, str):
        raw_items = [item.strip() for item in value.split(",") if item.strip()]
    elif isinstance(value, list):
        raw_items = value
    else:
        errors.append(f"{field_name} must be a list of integers or a comma-separated string.")
        return []

    parsed: list[int] = []
    for item in raw_items:
        try:
            parsed.append(int(item))
        except (TypeError, ValueError):
            errors.append(f"{field_name} contains a non-numeric Telegram user ID: {item!r}.")
    return parsed


def _parse_positive_int(value: Any, field_name: str, errors: list[str]) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        errors.append(f"{field_name} must be an integer.")
        return 1
    if parsed <= 0:
        errors.append(f"{field_name} must be greater than zero.")
        return 1
    return parsed
