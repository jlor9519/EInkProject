from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


class PhotoFrameError(Exception):
    """Base exception for photo frame application errors."""


class RenderError(PhotoFrameError):
    """Raised when image rendering fails."""


class DisplayError(PhotoFrameError):
    """Raised when display update fails."""


@dataclass(slots=True)
class TelegramConfig:
    bot_token: str


@dataclass(slots=True)
class SecurityConfig:
    admin_user_ids: list[int]
    whitelisted_user_ids: list[int]


@dataclass(slots=True)
class DatabaseConfig:
    path: Path


@dataclass(slots=True)
class StorageConfig:
    incoming_dir: Path
    rendered_dir: Path
    cache_dir: Path
    archive_dir: Path
    inkypi_payload_dir: Path
    current_payload_path: Path
    current_image_path: Path
    keep_recent_rendered: int


@dataclass(slots=True)
class DisplayConfig:
    width: int
    height: int
    caption_height: int
    margin: int
    metadata_font_size: int
    caption_font_size: int
    caption_character_limit: int
    max_caption_lines: int
    font_path: str
    background_color: str
    text_color: str
    divider_color: str


@dataclass(slots=True)
class InkyPiConfig:
    repo_path: Path
    install_path: Path
    validated_commit: str
    waveshare_model: str
    plugin_id: str
    payload_dir: Path
    update_method: str
    update_now_url: str
    refresh_command: str


@dataclass(slots=True)
class AppConfig:
    telegram: TelegramConfig
    security: SecurityConfig
    database: DatabaseConfig
    storage: StorageConfig
    display: DisplayConfig
    inkypi: InkyPiConfig


@dataclass(slots=True)
class ImageRecord:
    image_id: str
    telegram_file_id: str
    local_original_path: str
    local_rendered_path: str | None
    location: str
    taken_at: str
    caption: str
    uploaded_by: int
    created_at: str
    status: str
    last_error: str | None = None


@dataclass(slots=True)
class DisplayRequest:
    image_id: str
    original_path: Path
    composed_path: Path
    location: str
    taken_at: str
    caption: str
    created_at: str
    uploaded_by: int
    show_caption: bool = True
    fit_mode: str = "fill"

    def to_payload(self) -> dict[str, Any]:
        return {
            "image_id": self.image_id,
            "original_path": str(self.original_path),
            "composed_path": str(self.composed_path),
            "location": self.location,
            "taken_at": self.taken_at,
            "caption": self.caption,
            "created_at": self.created_at,
            "uploaded_by": self.uploaded_by,
            "image_fit_mode": self.fit_mode,
        }


@dataclass(slots=True)
class DisplayResult:
    success: bool
    message: str
    payload_path: Path | None = None


@dataclass(slots=True)
class DeviceSettingsApplyResult:
    success: bool
    message: str
    confirmed_settings: dict[str, Any]
    device_config_path: Path | None = None
    saved: bool = False
    reloaded: bool = False
    refreshed: bool = False
    refresh_skipped: bool = False


@dataclass(slots=True)
class ProcessingReservation:
    owner_user_id: int | None = None
    image_id: str | None = None


@dataclass(slots=True)
class AppServices:
    config: AppConfig
    database: Any
    auth: Any
    storage: Any
    renderer: Any
    display: Any
