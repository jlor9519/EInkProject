"""Shows images on the frame and keeps the frame's own settings.

The service draws the frame image (app.compose), turns it into what the panel expects
(app.panel.prepare_panel_image) and hands it to the panel. Only after the panel accepted the image
does it become the current image: the prepared photo is kept under `committed/`, copied to
`current.png`, and its details are written to `current.json` so the frame can be redrawn later,
for example after a settings change.

Frame settings (orientation, image tuning, slideshow interval, quiet hours) live in the database.
On first start they are imported once from an old InkyPi device.json, if there is one.
"""

from __future__ import annotations

import hashlib
import json
import logging
import shutil
import tempfile
import threading
from pathlib import Path
from typing import Any

from PIL import Image

from app.compose import compose_frame, compose_placeholder, frame_size
from app.models import (
    DeviceSettingsApplyResult,
    DisplayConfig,
    DisplayRequest,
    DisplayResult,
    StorageConfig,
)
from app.panel import Panel, PanelError, prepare_panel_image

logger = logging.getLogger(__name__)

COMMITTED_ARTIFACT_DIR_NAME = "committed"
COMMITTED_ARTIFACT_RETENTION = 5
DEFAULT_SLIDESHOW_INTERVAL = 86400
ORIENTATIONS = ("horizontal", "vertical")

DEFAULT_IMAGE_SETTINGS: dict[str, float] = {
    "saturation": 1.4,
    "contrast": 1.4,
    "sharpness": 1.2,
    "brightness": 1.1,
}
DEFAULT_ORIENTATION = "vertical"

ORIENTATION_KEY = "display_orientation"
INVERTED_IMAGE_KEY = "display_inverted_image"
IMAGE_SETTINGS_KEY = "display_image_settings"
SLIDESHOW_INTERVAL_KEY = "display_slideshow_interval"
SLEEP_SCHEDULE_KEY = "display_sleep_schedule"
# Unset until changed in /settings; until then display.clear_before_display from the config applies.
CLEAR_BEFORE_DISPLAY_KEY = "display_clear_before_display"
SETTINGS_INITIALIZED_KEY = "display_settings_initialized"

# Written by older versions that kept these settings in InkyPi's device.json.
LEGACY_CACHE_ORIENTATION_KEY = "runtime_cache_orientation"
LEGACY_CACHE_INTERVAL_KEY = "runtime_cache_slideshow_interval"
LEGACY_CACHE_SLEEP_SCHEDULE_KEY = "runtime_cache_sleep_schedule"
LEGACY_PLUGIN_ID = "telegram_frame"


class FrameDisplay:
    def __init__(
        self,
        panel: Panel,
        storage: StorageConfig,
        display: DisplayConfig,
        database: Any,
        *,
        legacy_inkypi_device_paths: tuple[Path, ...] = (),
    ):
        self.panel = panel
        self.storage = storage
        self.display_config = display
        self.database = database
        self.legacy_inkypi_device_paths = legacy_inkypi_device_paths
        self._lock = threading.RLock()
        self._settings_ready = False

    # ------------------------------------------------------------------
    # Showing images
    # ------------------------------------------------------------------

    def display(self, request: DisplayRequest) -> DisplayResult:
        logger.info("Showing image %s", request.image_id)
        with self._lock:
            payload = self._build_payload(request, prepared_image_path=request.composed_path)
            result = self._show_payload(payload)
            if result.success:
                self._commit_display_state(request)
                result.payload_path = self.storage.current_payload_path
            logger.info("Display result for %s: success=%s", request.image_id, result.success)
            return result

    def show_placeholder(self) -> DisplayResult:
        """Show the "no images yet" screen, e.g. after the last image was deleted.

        The current image only changes once the panel accepted the placeholder, so a failed
        refresh leaves it untouched.
        """
        logger.info("Showing the no-images placeholder")
        with self._lock:
            payload = self._placeholder_payload()
            result = self._show_payload(payload)
            if result.success:
                _write_json_atomic(self.storage.current_payload_path, payload)
                _safe_unlink(self.storage.current_image_path)
                result.payload_path = self.storage.current_payload_path
            return result

    def refresh_only(self) -> DisplayResult:
        """Draw the current image again, e.g. after its settings changed."""
        with self._lock:
            try:
                payload = _load_json(self.storage.current_payload_path)
            except (OSError, json.JSONDecodeError) as exc:
                return DisplayResult(False, f"Aktuelle Bilddaten sind nicht lesbar: {exc}")
            if payload is None:
                return DisplayResult(False, "Es gibt noch kein aktuelles Bild.")
            return self._show_payload(payload)

    def payload_exists(self) -> bool:
        return self.storage.current_payload_path.exists()

    def panel_status(self) -> tuple[bool, str]:
        return self.panel.status()

    def _show_payload(self, payload: dict[str, Any]) -> DisplayResult:
        try:
            frame = self._compose_payload(payload)
        except (OSError, ValueError) as exc:
            logger.warning("Could not compose the frame image: %s", exc)
            return DisplayResult(False, f"Bild konnte nicht vorbereitet werden: {exc}")

        settings = self.read_device_settings()
        panel_image = prepare_panel_image(
            frame,
            panel_size=(self.display_config.width, self.display_config.height),
            orientation=settings["orientation"],
            inverted=settings["inverted_image"],
            image_settings=settings["image_settings"],
        )
        try:
            self.panel.show(panel_image, clear=settings["clear_before_display"])
        except PanelError as exc:
            logger.warning("Panel refresh failed: %s", exc)
            return DisplayResult(False, str(exc))
        except Exception as exc:
            logger.exception("Panel refresh failed unexpectedly")
            return DisplayResult(False, str(exc) or type(exc).__name__)
        return DisplayResult(True, "Anzeige aktualisiert.")

    def _compose_payload(self, payload: dict[str, Any]) -> Image.Image:
        config = self.display_config
        width, height = frame_size(config.width, config.height, self.current_orientation())
        if payload.get("placeholder"):
            return compose_placeholder(width, height, str(payload.get("font_path") or config.font_path))

        image_path = Path(
            str(
                payload.get("prepared_image_path")
                or payload.get("bridge_image_path")
                or payload.get("composed_path")
                or ""
            )
        )
        if not image_path.is_file():
            raise FileNotFoundError(f"Bilddatei fehlt: {image_path}")

        with Image.open(image_path) as prepared:
            return compose_frame(
                prepared,
                width=width,
                height=height,
                caption_bar_height=_int(payload.get("caption_bar_height"), config.caption_height),
                caption=str(payload.get("caption") or ""),
                taken_at=str(payload.get("taken_at") or ""),
                location=str(payload.get("location") or ""),
                caption_font_size=_int(payload.get("caption_font_size"), config.caption_font_size),
                metadata_font_size=_int(payload.get("metadata_font_size"), config.metadata_font_size),
                caption_character_limit=_int(payload.get("caption_character_limit"), config.caption_character_limit),
                caption_margin=_int(payload.get("caption_margin"), config.margin),
                font_path=str(payload.get("font_path") or config.font_path),
                text_color=str(payload.get("caption_text_color") or config.text_color),
                fit_mode=str(payload.get("image_fit_mode") or "fill"),
            )

    def _build_payload(
        self,
        request: DisplayRequest,
        *,
        prepared_image_path: Path,
        bridge_image_path: Path | None = None,
    ) -> dict[str, Any]:
        config = self.display_config
        payload = request.to_payload()
        payload["prepared_image_path"] = str(prepared_image_path)
        payload["bridge_image_path"] = str(bridge_image_path or prepared_image_path)
        payload["payload_path"] = str(self.storage.current_payload_path)
        payload["caption_bar_height"] = config.caption_height if request.show_caption else 0
        payload["caption_font_size"] = config.caption_font_size
        payload["caption_character_limit"] = config.caption_character_limit
        payload["caption_margin"] = config.margin
        payload["metadata_font_size"] = config.metadata_font_size
        payload["caption_text_color"] = config.text_color
        payload["font_path"] = config.font_path
        payload["image_fit_mode"] = request.fit_mode
        payload["revision"] = _revision_hash(payload)
        return payload

    def _placeholder_payload(self) -> dict[str, Any]:
        return {
            "placeholder": True,
            "font_path": self.display_config.font_path,
            "payload_path": str(self.storage.current_payload_path),
        }

    def _commit_display_state(self, request: DisplayRequest) -> None:
        committed_image_path = self._committed_images_dir() / (
            f"{request.image_id}_{_revision_hash(request.to_payload())}.png"
        )
        committed_image_path.parent.mkdir(parents=True, exist_ok=True)
        _copy_atomic(request.composed_path, committed_image_path)
        _copy_atomic(committed_image_path, self.storage.current_image_path)
        payload = self._build_payload(
            request,
            prepared_image_path=committed_image_path,
            bridge_image_path=self.storage.current_image_path,
        )
        _write_json_atomic(self.storage.current_payload_path, payload)
        self._cleanup_old_committed_images(keep_path=committed_image_path)

    def _committed_images_dir(self) -> Path:
        return self.storage.inkypi_payload_dir / COMMITTED_ARTIFACT_DIR_NAME

    def _cleanup_old_committed_images(self, *, keep_path: Path) -> None:
        committed_dir = self._committed_images_dir()
        if not committed_dir.exists():
            return
        candidates = sorted(
            (path for path in committed_dir.glob("*.png") if path.is_file()),
            key=lambda item: item.stat().st_mtime,
            reverse=True,
        )
        retained = 0
        for candidate in candidates:
            if candidate == keep_path:
                retained += 1
                continue
            if retained < COMMITTED_ARTIFACT_RETENTION:
                retained += 1
                continue
            _safe_unlink(candidate)

    # ------------------------------------------------------------------
    # Frame settings
    # ------------------------------------------------------------------

    def read_device_settings(self) -> dict[str, Any]:
        self._ensure_settings()
        orientation = self.database.get_setting(ORIENTATION_KEY) or DEFAULT_ORIENTATION
        if orientation not in ORIENTATIONS:
            orientation = DEFAULT_ORIENTATION
        inverted = (self.database.get_setting(INVERTED_IMAGE_KEY) or "0") == "1"
        image_settings = dict(DEFAULT_IMAGE_SETTINGS)
        stored = _parse_json_dict(self.database.get_setting(IMAGE_SETTINGS_KEY))
        image_settings.update({key: value for key, value in stored.items() if _is_number(value)})
        stored_clear = self.database.get_setting(CLEAR_BEFORE_DISPLAY_KEY)
        clear_before_display = (
            self.display_config.clear_before_display if stored_clear is None else stored_clear == "1"
        )
        return {
            "orientation": orientation,
            "inverted_image": inverted,
            "image_settings": image_settings,
            "clear_before_display": clear_before_display,
        }

    def current_orientation(self) -> str:
        return self.read_device_settings()["orientation"]

    def apply_device_settings(
        self,
        updates: dict[str, Any],
        *,
        refresh_current: bool = True,
    ) -> DeviceSettingsApplyResult:
        with self._lock:
            current = self.read_device_settings()
            try:
                self._save_device_settings(current, updates)
            except Exception as exc:
                logger.warning("Saving frame settings failed: %s", exc)
                return DeviceSettingsApplyResult(
                    success=False,
                    message=f"Einstellungen konnten nicht gespeichert werden: {exc}",
                    confirmed_settings={},
                )
            confirmed = self.read_device_settings()

            if not refresh_current:
                return DeviceSettingsApplyResult(success=True, message="", confirmed_settings=confirmed)
            if not self.payload_exists():
                return DeviceSettingsApplyResult(
                    success=True,
                    message="Es gibt noch kein aktuelles Bild, daher wurde die Anzeige nicht aktualisiert.",
                    confirmed_settings=confirmed,
                )
            refresh_result = self.refresh_only()
            if not refresh_result.success:
                return DeviceSettingsApplyResult(
                    success=False,
                    message=(
                        "Einstellungen wurden gespeichert, aber die Anzeige-Aktualisierung ist "
                        f"fehlgeschlagen: {refresh_result.message}"
                    ),
                    confirmed_settings=confirmed,
                )
            return DeviceSettingsApplyResult(
                success=True,
                message="Die Anzeige wurde aktualisiert.",
                confirmed_settings=confirmed,
            )

    def _save_device_settings(self, current: dict[str, Any], updates: dict[str, Any]) -> None:
        values: dict[str, str | None] = {}
        if "orientation" in updates:
            orientation = str(updates["orientation"]).strip().lower()
            if orientation not in ORIENTATIONS:
                raise ValueError(f"Ungültige Ausrichtung: {updates['orientation']!r}")
            values[ORIENTATION_KEY] = orientation
        if "inverted_image" in updates:
            values[INVERTED_IMAGE_KEY] = "1" if updates["inverted_image"] else "0"
        if "clear_before_display" in updates:
            values[CLEAR_BEFORE_DISPLAY_KEY] = "1" if updates["clear_before_display"] else "0"
        if "image_settings" in updates:
            image_settings = dict(current["image_settings"])
            for key, value in dict(updates["image_settings"]).items():
                image_settings[key] = float(value)
            values[IMAGE_SETTINGS_KEY] = json.dumps(image_settings, sort_keys=True)
        if values:
            self.database.set_settings(values)

    def get_slideshow_interval(self) -> int:
        self._ensure_settings()
        try:
            interval = int(self.database.get_setting(SLIDESHOW_INTERVAL_KEY) or DEFAULT_SLIDESHOW_INTERVAL)
        except (TypeError, ValueError):
            return DEFAULT_SLIDESHOW_INTERVAL
        return interval if interval > 0 else DEFAULT_SLIDESHOW_INTERVAL

    def set_slideshow_interval(self, seconds: int) -> DeviceSettingsApplyResult:
        self._ensure_settings()
        self.database.set_setting(SLIDESHOW_INTERVAL_KEY, str(max(1, int(seconds))))
        return DeviceSettingsApplyResult(success=True, message="", confirmed_settings=self.read_device_settings())

    def get_sleep_schedule(self) -> tuple[str, str] | None:
        """Return (sleep_start, wake_up) as 'HH:MM' strings, or None if quiet hours are off."""
        self._ensure_settings()
        return _decode_sleep_schedule(self.database.get_setting(SLEEP_SCHEDULE_KEY))

    def set_sleep_schedule(self, sleep_start: str | None, wake_up: str | None) -> DeviceSettingsApplyResult:
        """Set or clear quiet hours. Pass None/None to disable."""
        self._ensure_settings()
        schedule = None if sleep_start is None or wake_up is None else (sleep_start, wake_up)
        self.database.set_setting(SLEEP_SCHEDULE_KEY, _encode_sleep_schedule(schedule))
        return DeviceSettingsApplyResult(success=True, message="", confirmed_settings=self.read_device_settings())

    # ------------------------------------------------------------------
    # First start: defaults, or the settings of an old InkyPi install
    # ------------------------------------------------------------------

    def _ensure_settings(self) -> None:
        if self._settings_ready:
            return
        if self.database.get_setting(SETTINGS_INITIALIZED_KEY) != "1":
            # Only fill in what is missing; never overwrite a setting that is already stored.
            values = {
                key: value
                for key, value in self._initial_settings().items()
                if self.database.get_setting(key) is None
            }
            values[SETTINGS_INITIALIZED_KEY] = "1"
            self.database.set_settings(values)
        self._settings_ready = True

    def _initial_settings(self) -> dict[str, str | None]:
        device_data, source = self._read_legacy_device_json()
        if device_data is not None:
            logger.info("Importing frame settings from the old InkyPi config %s", source)
            orientation = str(device_data.get("orientation") or "horizontal").strip().lower()
            image_settings = dict(DEFAULT_IMAGE_SETTINGS)
            legacy_image_settings = device_data.get("image_settings")
            if isinstance(legacy_image_settings, dict):
                image_settings.update(
                    {key: float(value) for key, value in legacy_image_settings.items() if _is_number(value)}
                )
            return {
                ORIENTATION_KEY: orientation if orientation in ORIENTATIONS else "horizontal",
                INVERTED_IMAGE_KEY: "1" if device_data.get("inverted_image") else "0",
                IMAGE_SETTINGS_KEY: json.dumps(image_settings, sort_keys=True),
                SLIDESHOW_INTERVAL_KEY: str(_legacy_plugin_refresh_interval(device_data)),
                SLEEP_SCHEDULE_KEY: _encode_sleep_schedule(_legacy_sleep_schedule(device_data)),
            }

        values: dict[str, str | None] = {
            ORIENTATION_KEY: DEFAULT_ORIENTATION,
            INVERTED_IMAGE_KEY: "1",
            IMAGE_SETTINGS_KEY: json.dumps(DEFAULT_IMAGE_SETTINGS, sort_keys=True),
            SLIDESHOW_INTERVAL_KEY: str(DEFAULT_SLIDESHOW_INTERVAL),
            SLEEP_SCHEDULE_KEY: "",
        }
        # Older versions cached a few InkyPi settings in the database; prefer them to defaults.
        cached_orientation = (self.database.get_setting(LEGACY_CACHE_ORIENTATION_KEY) or "").strip().lower()
        if cached_orientation in ORIENTATIONS:
            logger.info("Importing frame settings cached by an older version")
            values[ORIENTATION_KEY] = cached_orientation
            values[INVERTED_IMAGE_KEY] = "1" if cached_orientation == "vertical" else "0"
            cached_interval = self.database.get_setting(LEGACY_CACHE_INTERVAL_KEY)
            if cached_interval and cached_interval.isdigit() and int(cached_interval) > 0:
                values[SLIDESHOW_INTERVAL_KEY] = cached_interval
            cached_schedule = _decode_sleep_schedule(self.database.get_setting(LEGACY_CACHE_SLEEP_SCHEDULE_KEY))
            values[SLEEP_SCHEDULE_KEY] = _encode_sleep_schedule(cached_schedule)
        return values

    def _read_legacy_device_json(self) -> tuple[dict[str, Any] | None, Path | None]:
        for path in self.legacy_inkypi_device_paths:
            try:
                data = _load_json(path)
            except (OSError, json.JSONDecodeError) as exc:
                logger.warning("Could not read the old InkyPi config %s: %s", path, exc)
                continue
            if isinstance(data, dict):
                return data, path
        return None, None


# ----------------------------------------------------------------------
# InkyPi device.json parsing (for the one-time import)
# ----------------------------------------------------------------------


def _legacy_plugin_refresh_interval(data: dict[str, Any]) -> int:
    playlist_config = data.get("playlist_config")
    if not isinstance(playlist_config, dict):
        return DEFAULT_SLIDESHOW_INTERVAL
    for playlist in playlist_config.get("playlists", []):
        if not isinstance(playlist, dict):
            continue
        for plugin in playlist.get("plugins", []):
            if not isinstance(plugin, dict) or plugin.get("plugin_id") != LEGACY_PLUGIN_ID:
                continue
            refresh = plugin.get("refresh")
            interval = refresh.get("interval") if isinstance(refresh, dict) else None
            if isinstance(interval, (int, float)) and interval > 0:
                return int(interval)
    return DEFAULT_SLIDESHOW_INTERVAL


def _legacy_sleep_schedule(data: dict[str, Any]) -> tuple[str, str] | None:
    """InkyPi stored the active window (wake-up to sleep); quiet hours are the inverse."""
    playlist_config = data.get("playlist_config")
    if not isinstance(playlist_config, dict):
        return None
    for playlist in playlist_config.get("playlists", []):
        if not isinstance(playlist, dict):
            continue
        start = str(playlist.get("start_time", "00:00"))
        end = str(playlist.get("end_time", "24:00"))
        if start == "00:00" and end in ("24:00", "23:59"):
            return None
        # InkyPi uses "24:00" for end of day; as a quiet-hours time that is midnight.
        quiet_start, wake_up = ("00:00" if value == "24:00" else value for value in (end, start))
        return quiet_start, wake_up
    return None


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _encode_sleep_schedule(schedule: tuple[str, str] | None) -> str:
    if schedule is None:
        return ""
    return f"{schedule[0]}|{schedule[1]}"


def _decode_sleep_schedule(raw: str | None) -> tuple[str, str] | None:
    if not raw:
        return None
    parts = raw.split("|", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        return None
    return parts[0], parts[1]


def _parse_json_dict(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _revision_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:16]


def _load_json(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json_atomic(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        json.dump(data, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temp_path = Path(handle.name)
    temp_path.replace(path)


def _copy_atomic(source: Path, target: Path) -> None:
    if source.resolve() == target.resolve():
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=target.parent, suffix=target.suffix, delete=False) as handle:
        temp_path = Path(handle.name)
    try:
        shutil.copy2(source, temp_path)
        temp_path.replace(target)
    except OSError:
        _safe_unlink(temp_path)
        raise


def _safe_unlink(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return
    except OSError:
        logger.warning("Failed to remove %s", path, exc_info=True)
