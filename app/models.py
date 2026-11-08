from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DISPLAY_VERIFICATION_VERIFIED = "verified"
# Only set by older versions that drove the panel through InkyPi; may still be stored as the
# state of the current image.
DISPLAY_VERIFICATION_ASSUMED_AFTER_RECOVERY = "assumed_after_recovery"
DISPLAY_VERIFICATION_FAILED = "failed"


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
    font_path: str
    text_color: str
    # "waveshare" drives the e-paper panel; "file" writes the panel image to panel_output_path.
    panel: str = "waveshare"
    waveshare_model: str = "epd7in3e"
    driver_dir: Path = PROJECT_ROOT / "vendor" / "waveshare_epd"
    # Wipe the panel to white before drawing, which avoids ghosting but doubles the refresh time.
    clear_before_display: bool = True
    refresh_timeout_seconds: int = 300
    panel_output_path: Path = PROJECT_ROOT / "data" / "panel.png"


@dataclass(slots=True)
class AppConfig:
    telegram: TelegramConfig
    security: SecurityConfig
    database: DatabaseConfig
    storage: StorageConfig
    display: DisplayConfig
    # device.json files of an old InkyPi install; their settings are imported once.
    legacy_inkypi_device_paths: tuple[Path, ...] = ()


@dataclass(slots=True)
class ImageRecord:
    image_id: str
    telegram_file_id: str
    telegram_chat_id: int | None
    local_original_path: str
    local_rendered_path: str | None
    location: str
    taken_at: str
    caption: str
    uploaded_by: int
    created_at: str
    status: str
    last_error: str | None = None
    orientation_bucket: str = "shared"
    display_attempts: int = 0


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
    verification_state: str | None = None
    verification_detail: str | None = None

    def __post_init__(self) -> None:
        if self.verification_state is None:
            self.verification_state = (
                DISPLAY_VERIFICATION_VERIFIED if self.success else DISPLAY_VERIFICATION_FAILED
            )


@dataclass(slots=True)
class DeviceSettingsApplyResult:
    success: bool
    message: str
    confirmed_settings: dict[str, Any]


@dataclass(slots=True)
class AppServices:
    config_path: Path | None
    config: AppConfig
    database: Any
    auth: Any
    storage: Any
    renderer: Any
    display: Any
