from __future__ import annotations

import hashlib
import json
import logging
import socket
import shlex
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any
from urllib import error, parse, request

logger = logging.getLogger(__name__)

from app.inkypi_paths import resolve_inkypi_layout
from app.models import (
    DeviceSettingsApplyResult,
    DisplayConfig,
    DisplayRequest,
    DisplayResult,
    InkyPiConfig,
    StorageConfig,
)


INKYPI_SERVICE_NAME = "inkypi.service"
INKYPI_RESTART_TIMEOUT_SECONDS = 45
INKYPI_HTTP_READY_TIMEOUT_SECONDS = 30
HTTP_DEGRADED_COOLDOWN_SECONDS = 300
DEFAULT_TELEGRAM_FRAME_INSTANCE_NAME = "Telegram Frame"
COMMITTED_ARTIFACT_DIR_NAME = "committed"
COMMITTED_ARTIFACT_RETENTION = 5
RUNTIME_CACHE_ORIENTATION_KEY = "runtime_cache_orientation"
RUNTIME_CACHE_INTERVAL_KEY = "runtime_cache_slideshow_interval"
RUNTIME_CACHE_SLEEP_SCHEDULE_KEY = "runtime_cache_sleep_schedule"


def _write_device_json(path: Path, updates: dict[str, object]) -> None:
    data: dict[str, object] = {}
    if path.exists():
        data = json.loads(path.read_text(encoding="utf-8"))
    data = _merge_device_settings(data, updates)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        json.dump(data, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temp_path = Path(handle.name)
    temp_path.replace(path)


def _merge_device_settings(existing: dict[str, object], updates: dict[str, object]) -> dict[str, object]:
    merged = dict(existing)
    for key, value in updates.items():
        if (
            key == "image_settings"
            and isinstance(value, dict)
            and isinstance(merged.get("image_settings"), dict)
        ):
            nested = dict(merged["image_settings"])
            nested.update(value)
            merged[key] = nested
        else:
            merged[key] = value
    return merged


class InkyPiAdapter:
    def __init__(
        self,
        config: InkyPiConfig,
        storage: StorageConfig,
        display: DisplayConfig,
        database: Any | None = None,
    ):
        self.config = config
        self.storage = storage
        self.display_config = display
        self.database = database
        self.layout = resolve_inkypi_layout(config.repo_path, config.install_path)
        self._systemctl_bin = shutil.which("systemctl") or "/usr/bin/systemctl"
        self._http_degraded_until_monotonic = 0.0
        self._http_degraded_reason = ""

    def display(self, request: DisplayRequest) -> DisplayResult:
        logger.info("Preparing staged bridge payload for image %s", request.image_id)
        staged_payload_path: Path | None = None
        staged_image_path: Path | None = None
        try:
            staged_payload_path, staged_image_path = self._prepare_staged_display(request)
            result = self._trigger_display_update(
                staged_payload_path,
                image_path=staged_image_path,
                plugin_payload_path=self.storage.current_payload_path,
            )
            if result.success:
                try:
                    self._commit_display_state(request, staged_image_path)
                    staged_image_path = None
                    result.payload_path = self.storage.current_payload_path
                finally:
                    restore_result = self._sync_active_plugin_instance(self.storage.current_payload_path)
                    if restore_result is not None:
                        logger.warning(
                            "Failed to restore Telegram Frame plugin payload to %s after successful display: %s",
                            self.storage.current_payload_path,
                            restore_result.message,
                        )
            else:
                result.payload_path = staged_payload_path
            logger.info("Display result for %s: success=%s", request.image_id, result.success)
            return result
        finally:
            if staged_payload_path is not None:
                self._safe_unlink(staged_payload_path)
            if staged_image_path is not None:
                self._safe_unlink(staged_image_path)

    def read_device_settings(self) -> dict[str, object]:
        path = self._device_config_path()
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    def runtime_settings_diagnostics(self) -> dict[str, str | bool]:
        data, error_text = self._read_device_settings_with_status()
        if data is not None:
            return {"degraded": False, "message": ""}
        cache_available = any(
            self._read_runtime_cache(key)
            for key in (
                RUNTIME_CACHE_ORIENTATION_KEY,
                RUNTIME_CACHE_INTERVAL_KEY,
                RUNTIME_CACHE_SLEEP_SCHEDULE_KEY,
            )
        )
        if cache_available:
            return {
                "degraded": True,
                "message": f"Geräteeinstellungen aus Cache aktiv ({error_text or 'device.json nicht lesbar'})",
            }
        return {
            "degraded": True,
            "message": f"Geräteeinstellungen verwenden Standardwerte ({error_text or 'device.json fehlt'})",
        }

    def backend_diagnostics(self) -> dict[str, str | bool]:
        if self.config.update_method != "http_update_now" or not self._is_http_backend_degraded():
            return {"degraded": False, "message": ""}
        remaining = max(1, int(self._http_degraded_until_monotonic - time.monotonic()))
        return {
            "degraded": True,
            "message": (
                "InkyPi-HTTP gestört, Befehl-Fallback aktiv "
                f"(noch ca. {self._format_duration_brief(remaining)}): {self._http_degraded_reason}"
            ),
        }

    def apply_device_settings(
        self,
        updates: dict[str, object],
        *,
        refresh_current: bool = True,
    ) -> DeviceSettingsApplyResult:
        device_config_path = self._device_config_path()
        try:
            current = self.read_device_settings()
            merged = _merge_device_settings(current, updates)
            _write_device_json(device_config_path, merged)
            confirmed = self.read_device_settings()
        except PermissionError as exc:
            logger.warning("Permission denied saving device settings: %s", exc)
            return DeviceSettingsApplyResult(
                success=False,
                message=f"Einstellungen konnten nicht gespeichert werden: {exc}",
                confirmed_settings={},
                device_config_path=device_config_path,
            )
        except OSError as exc:
            logger.warning("OS error saving device settings: %s", exc)
            return DeviceSettingsApplyResult(
                success=False,
                message=f"Einstellungen konnten nicht gespeichert werden: {exc}",
                confirmed_settings={},
                device_config_path=device_config_path,
            )
        except json.JSONDecodeError as exc:
            logger.warning("Invalid JSON in device settings file: %s", exc)
            return DeviceSettingsApplyResult(
                success=False,
                message=f"device.json ist ungültiges JSON: {exc}",
                confirmed_settings={},
                device_config_path=device_config_path,
            )

        self._refresh_runtime_cache_from_data(confirmed)

        restart_error = self._restart_inkypi_service()
        if restart_error is not None:
            return DeviceSettingsApplyResult(
                success=False,
                message=(
                    "Einstellungen wurden gespeichert, aber InkyPi konnte nicht neu geladen werden: "
                    f"{restart_error}"
                ),
                confirmed_settings=confirmed,
                device_config_path=device_config_path,
                saved=True,
            )

        # Re-assert our settings in case InkyPi overwrote them during startup.
        # InkyPi may write to device.json on startup (e.g. updating latest_refresh_time from
        # its in-memory state), which can reset image_settings to stale values.
        # We exclude playlist_config so we don't undo changes from _sync_active_plugin_instance.
        settings_to_preserve = {k: v for k, v in merged.items() if k != "playlist_config"}
        if settings_to_preserve:
            try:
                _write_device_json(device_config_path, settings_to_preserve)
            except Exception:
                logger.warning("Failed to re-assert settings after InkyPi restart", exc_info=True)

        if not refresh_current:
            return DeviceSettingsApplyResult(
                success=True,
                message="Einstellungen wurden gespeichert und InkyPi wurde neu geladen.",
                confirmed_settings=confirmed,
                device_config_path=device_config_path,
                saved=True,
                reloaded=True,
                refresh_skipped=True,
            )

        if not self.storage.current_payload_path.exists():
            return DeviceSettingsApplyResult(
                success=True,
                message=(
                    "Einstellungen wurden gespeichert und InkyPi wurde neu geladen. "
                    "Es gibt noch kein aktuelles Bild, daher wurde keine Live-Aktualisierung ausgelost."
                ),
                confirmed_settings=confirmed,
                device_config_path=device_config_path,
                saved=True,
                reloaded=True,
                refresh_skipped=True,
            )

        http_ready_error = self._wait_for_inkypi_http_ready()
        if http_ready_error is not None:
            if self._has_refresh_command():
                self._mark_http_backend_degraded(http_ready_error)
                logger.warning(
                    "InkyPi HTTP backend is not ready after reload; using command fallback for the live refresh: %s",
                    http_ready_error,
                )
            else:
                return DeviceSettingsApplyResult(
                    success=False,
                    message=(
                        "Einstellungen wurden gespeichert und InkyPi wurde neu geladen, "
                        f"aber der InkyPi-Webserver war noch nicht erreichbar: {http_ready_error}"
                    ),
                    confirmed_settings=confirmed,
                    device_config_path=device_config_path,
                    saved=True,
                    reloaded=True,
                )
        else:
            self._clear_http_backend_degraded()

        refresh_result = self._trigger_display_update(self.storage.current_payload_path)

        # Re-assert again after display trigger: _sync_active_plugin_instance reads device.json
        # fresh and writes it back, which may have picked up InkyPi-reset values.
        if settings_to_preserve:
            try:
                _write_device_json(device_config_path, settings_to_preserve)
            except Exception:
                logger.warning("Failed to re-assert settings after display refresh", exc_info=True)

        if not refresh_result.success:
            return DeviceSettingsApplyResult(
                success=False,
                message=(
                    "Einstellungen wurden gespeichert und InkyPi wurde neu geladen, "
                    f"aber die Anzeige-Aktualisierung ist fehlgeschlagen: {refresh_result.message}"
                ),
                confirmed_settings=confirmed,
                device_config_path=device_config_path,
                saved=True,
                reloaded=True,
            )

        return DeviceSettingsApplyResult(
            success=True,
            message="Einstellungen wurden gespeichert, InkyPi wurde neu geladen und die Anzeige aktualisiert.",
            confirmed_settings=confirmed,
            device_config_path=device_config_path,
            saved=True,
            reloaded=True,
            refreshed=True,
        )

    def refresh_only(self) -> DisplayResult:
        return self._trigger_display_update(self.storage.current_payload_path)

    def wait_until_ready(self) -> str | None:
        if self.config.update_method == "command":
            return None
        service_error = self._wait_for_inkypi_service_active()
        if service_error is not None:
            return service_error
        http_error = self._wait_for_inkypi_http_ready()
        if http_error is None:
            self._clear_http_backend_degraded()
            return None
        if self._has_refresh_command():
            self._mark_http_backend_degraded(http_error)
            logger.warning(
                "InkyPi HTTP backend is not ready; continuing startup with command fallback enabled: %s",
                http_error,
            )
            return None
        return http_error

    def _trigger_display_update(
        self,
        payload_path: Path,
        *,
        image_path: Path | None = None,
        plugin_payload_path: Path | None = None,
    ) -> DisplayResult:
        try:
            payload = self._load_payload(payload_path)
        except json.JSONDecodeError as exc:
            return DisplayResult(False, f"InkyPi payload is not valid JSON: {exc}")

        if payload is None:
            return DisplayResult(False, f"InkyPi payload does not exist: {payload_path}")

        command_image_path = image_path or self.storage.current_image_path
        primary_plugin_payload_path = plugin_payload_path or payload_path

        if self.config.update_method == "http_update_now":
            plugin_sync_result = self._sync_active_plugin_instance(primary_plugin_payload_path)
            if plugin_sync_result is not None:
                return plugin_sync_result

            if self._is_http_backend_degraded() and self._has_refresh_command():
                logger.warning(
                    "Skipping InkyPi update_now because the HTTP backend is in degraded mode; using command fallback"
                )
                command_result = self._run_refresh_command_with_payload_support(
                    payload_path,
                    command_image_path,
                    primary_plugin_payload_path=primary_plugin_payload_path,
                )
                if command_result.success:
                    logger.info("Display refresh succeeded via command fallback while HTTP backend was degraded")
                    retry_result = self._retry_http_post_after_fallback(payload_path)
                    if retry_result and retry_result.success:
                        self._clear_http_backend_degraded()
                    return DisplayResult(
                        True,
                        f"command fallback used while HTTP backend was degraded: {command_result.message}",
                    )
                return DisplayResult(
                    False,
                    (
                        "InkyPi HTTP backend ist derzeit gestört "
                        f"({self._http_degraded_reason}); command fallback failed: {command_result.message}"
                    ),
                )

            logger.info("Triggering display via HTTP POST to %s", self.config.update_now_url)
            http_result, fallback_eligible = self._post_update_now(payload_path)
            if http_result.success:
                self._clear_http_backend_degraded()
                logger.info("Display refresh succeeded via HTTP update_now")
                return http_result
            if fallback_eligible and self._has_refresh_command():
                self._mark_http_backend_degraded(http_result.message)
                logger.warning("HTTP refresh failed; trying command fallback: %s", http_result.message)
                command_result = self._run_refresh_command_with_payload_support(
                    payload_path,
                    command_image_path,
                    primary_plugin_payload_path=primary_plugin_payload_path,
                )
                if command_result.success:
                    logger.warning(
                        "Display refresh recovered via command fallback after HTTP failure: %s",
                        http_result.message,
                    )
                    self._retry_http_post_after_fallback(payload_path)
                    return DisplayResult(
                        True,
                        f"HTTP refresh failed ({http_result.message}); command fallback succeeded: {command_result.message}",
                    )
                logger.warning(
                    "Display refresh failed on both HTTP and command fallback: %s | %s",
                    http_result.message,
                    command_result.message,
                )
                return DisplayResult(
                    False,
                    f"HTTP refresh failed: {http_result.message}; command fallback failed: {command_result.message}",
                )
            return http_result

        plugin_sync_result = self._sync_active_plugin_instance(primary_plugin_payload_path)
        if plugin_sync_result is not None:
            return plugin_sync_result
        return self._run_refresh_command_with_payload_support(
            payload_path,
            command_image_path,
            primary_plugin_payload_path=primary_plugin_payload_path,
        )

    def _run_refresh_command_with_payload_support(
        self,
        payload_path: Path,
        image_path: Path,
        *,
        primary_plugin_payload_path: Path,
    ) -> DisplayResult:
        restore_plugin_payload = (
            primary_plugin_payload_path
            if self._command_needs_temporary_plugin_payload(payload_path, primary_plugin_payload_path)
            else None
        )
        if restore_plugin_payload is not None:
            sync_result = self._sync_active_plugin_instance(payload_path)
            if sync_result is not None:
                return sync_result

        command = self._format_refresh_command(payload_path, image_path)
        logger.info("Triggering display via command: %s", command)
        result = self._execute_refresh_command(command)
        if restore_plugin_payload is not None and not result.success:
            restore_result = self._sync_active_plugin_instance(restore_plugin_payload)
            if restore_result is not None:
                logger.warning(
                    "Failed to restore Telegram Frame plugin payload to %s after command failure: %s",
                    restore_plugin_payload,
                    restore_result.message,
                )
        return result

    def _execute_refresh_command(self, command: list[str]) -> DisplayResult:
        try:
            completed = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )
        except subprocess.TimeoutExpired:
            logger.warning("Refresh command timed out after 60s")
            return DisplayResult(False, "InkyPi refresh command timed out after 60 seconds")
        if completed.returncode != 0:
            stderr = completed.stderr.strip() or completed.stdout.strip() or "unknown refresh error"
            return DisplayResult(False, f"InkyPi refresh failed: {stderr}")
        return DisplayResult(True, completed.stdout.strip() or "refresh command completed successfully")

    def _post_update_now(self, payload_path: Path) -> tuple[DisplayResult, bool]:
        form = parse.urlencode(
            {
                "plugin_id": self.config.plugin_id,
                "payload_path": str(payload_path),
            }
        ).encode("utf-8")
        http_request = request.Request(
            self.config.update_now_url,
            data=form,
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

        try:
            with request.urlopen(http_request, timeout=30) as response:
                body = response.read().decode("utf-8", errors="replace")
                return self._parse_http_response(body, response.status), False
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            parsed = self._parse_http_response(body, exc.code)
            if parsed.success:
                return DisplayResult(False, f"InkyPi update_now returned HTTP {exc.code}"), False
            return parsed, False
        except (TimeoutError, socket.timeout):
            return DisplayResult(False, "InkyPi update_now request timed out after 30 seconds"), True
        except error.URLError as exc:
            return DisplayResult(False, f"InkyPi update_now request failed: {exc.reason}"), True
        except OSError as exc:
            return DisplayResult(False, f"InkyPi update_now request failed: {exc}"), True

    def _retry_http_post_after_fallback(self, payload_path: Path) -> DisplayResult | None:
        """Sleep briefly after a command fallback restart, then retry the HTTP POST.

        Returns the retry DisplayResult on success, or None if the retry failed.
        This ensures InkyPi actually picks up the new payload after a service restart.
        """
        logger.info("Waiting 5s for InkyPi to come back up before retrying HTTP POST")
        time.sleep(5)
        retry_result, _ = self._post_update_now(payload_path)
        if retry_result.success:
            logger.info("HTTP POST succeeded after command fallback restart")
            return retry_result
        logger.warning("HTTP POST retry after command fallback also failed: %s", retry_result.message)
        return None

    def _parse_http_response(self, body: str, status_code: int) -> DisplayResult:
        text = body.strip()
        parsed_json: dict[str, object] | None = None

        if text:
            try:
                candidate = json.loads(text)
            except json.JSONDecodeError:
                candidate = None
            if isinstance(candidate, dict):
                parsed_json = candidate

        if status_code < 200 or status_code >= 300:
            if parsed_json and parsed_json.get("error"):
                return DisplayResult(False, f"InkyPi update_now failed: {parsed_json['error']}")
            return DisplayResult(False, f"InkyPi update_now failed with HTTP {status_code}: {text or 'no response body'}")

        if parsed_json and parsed_json.get("error"):
            return DisplayResult(False, f"InkyPi update_now failed: {parsed_json['error']}")
        if parsed_json and parsed_json.get("message"):
            return DisplayResult(True, str(parsed_json["message"]))
        if text:
            return DisplayResult(True, text)
        return DisplayResult(True, "InkyPi update_now completed successfully")

    def _prepare_staged_display(self, request: DisplayRequest) -> tuple[Path, Path]:
        staged_image_path = self._make_temp_path(self.storage.current_image_path)
        staged_payload_path = self._make_temp_path(self.storage.current_payload_path)
        staged_image_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(request.composed_path, staged_image_path)
        self._write_bridge_payload(
            request,
            payload_path=staged_payload_path,
            image_path=staged_image_path,
        )
        return staged_payload_path, staged_image_path

    def _commit_display_state(self, request: DisplayRequest, staged_image_path: Path) -> None:
        committed_image_path = self._committed_image_path(request)
        committed_image_path.parent.mkdir(parents=True, exist_ok=True)
        staged_image_path.replace(committed_image_path)
        self.storage.current_image_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(committed_image_path, self.storage.current_image_path)
        self._write_bridge_payload(
            request,
            payload_path=self.storage.current_payload_path,
            image_path=committed_image_path,
            bridge_image_path=self.storage.current_image_path,
        )
        self._cleanup_old_committed_images(keep_path=committed_image_path)

    def _write_bridge_payload(
        self,
        request: DisplayRequest,
        *,
        payload_path: Path,
        image_path: Path,
        bridge_image_path: Path | None = None,
    ) -> Path:
        orientation_hint = self.current_orientation()

        payload = request.to_payload()
        payload["prepared_image_path"] = str(image_path)
        payload["bridge_image_path"] = str(bridge_image_path or image_path)
        payload["payload_path"] = str(payload_path)
        payload["plugin_id"] = self.config.plugin_id
        payload["orientation_hint"] = orientation_hint
        payload["caption_bar_height"] = self.display_config.caption_height if request.show_caption else 0
        payload["caption_font_size"] = self.display_config.caption_font_size
        payload["caption_character_limit"] = self.display_config.caption_character_limit
        payload["caption_margin"] = self.display_config.margin
        payload["caption_max_lines"] = self.display_config.max_caption_lines
        payload["metadata_font_size"] = self.display_config.metadata_font_size
        payload["caption_text_color"] = self.display_config.text_color
        payload["caption_background_color"] = "#FFFFFF"
        payload["font_path"] = self.display_config.font_path
        payload["image_fit_mode"] = request.fit_mode
        payload["revision"] = self._revision_hash(payload)

        payload_path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=payload_path.parent,
            delete=False,
        ) as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            temp_path = Path(handle.name)
        temp_path.replace(payload_path)
        return payload_path

    def _revision_hash(self, payload: dict[str, object]) -> str:
        encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()[:16]

    def _load_payload(self, payload_path: Path) -> dict[str, object] | None:
        if not payload_path.exists():
            return None
        return json.loads(payload_path.read_text(encoding="utf-8"))

    def _device_config_path(self) -> Path:
        return self.layout.device_config_path.resolve(strict=False)

    def payload_exists(self) -> bool:
        return self.storage.current_payload_path.exists()

    def current_orientation(self) -> str:
        data, error_text = self._read_device_settings_with_status()
        if data is not None:
            orientation = str(data.get("orientation") or "horizontal").strip().lower()
            if orientation in {"horizontal", "vertical"}:
                self._write_runtime_cache(RUNTIME_CACHE_ORIENTATION_KEY, orientation)
                return orientation
            logger.warning("Invalid current InkyPi orientation in device.json: %s", orientation)

        cached = (self._read_runtime_cache(RUNTIME_CACHE_ORIENTATION_KEY) or "").strip().lower()
        if cached in {"horizontal", "vertical"}:
            if error_text:
                logger.warning(
                    "Failed to read current InkyPi orientation, using cached value %s: %s",
                    cached,
                    error_text,
                )
            return cached

        if error_text:
            logger.warning("Failed to read current InkyPi orientation, defaulting to horizontal: %s", error_text)
        return "horizontal"

    def get_slideshow_interval(self) -> int:
        """Return the slideshow refresh interval in seconds from device.json, default 86400."""
        data, error_text = self._read_device_settings_with_status()
        if data is not None:
            interval = self._read_plugin_refresh_interval(data)
            self._write_runtime_cache(RUNTIME_CACHE_INTERVAL_KEY, str(interval))
            return interval

        cached = self._read_runtime_cache(RUNTIME_CACHE_INTERVAL_KEY)
        if cached:
            try:
                interval = max(1, int(cached))
            except (TypeError, ValueError):
                interval = None
            else:
                if error_text:
                    logger.warning(
                        "Failed to read slideshow interval from device.json, using cached value %s: %s",
                        interval,
                        error_text,
                    )
                return interval
        return 86400

    def set_slideshow_interval(self, seconds: int) -> DeviceSettingsApplyResult:
        """Update the Telegram Frame plugin refresh interval in device.json and restart InkyPi."""
        device_config_path = self._device_config_path()
        try:
            data = self.read_device_settings()
            playlist_config = data.get("playlist_config")
            if not isinstance(playlist_config, dict):
                return DeviceSettingsApplyResult(
                    success=False,
                    message="playlist_config nicht in device.json gefunden. Bitte führe die Einrichtung erneut aus.",
                    confirmed_settings={},
                )
            updated = False
            for playlist in playlist_config.get("playlists", []):
                if not isinstance(playlist, dict):
                    continue
                for plugin in playlist.get("plugins", []):
                    if not isinstance(plugin, dict):
                        continue
                    if plugin.get("plugin_id") == self.config.plugin_id:
                        plugin.setdefault("refresh", {})["interval"] = int(seconds)
                        updated = True
            if not updated:
                return DeviceSettingsApplyResult(
                    success=False,
                    message="Plugin-Instanz nicht gefunden. Bitte führe die Einrichtung erneut aus.",
                    confirmed_settings={},
                )
            data["playlist_config"] = playlist_config
            _write_device_json(device_config_path, data)
        except Exception as exc:
            return DeviceSettingsApplyResult(
                success=False,
                message=f"Fehler beim Speichern: {exc}",
                confirmed_settings={},
            )
        return self.apply_device_settings({}, refresh_current=True)

    def get_sleep_schedule(self) -> tuple[str, str] | None:
        """Return (sleep_start, wake_up) as 'HH:MM' strings, or None if quiet hours are off."""
        data, error_text = self._read_device_settings_with_status()
        if data is not None:
            schedule = self._extract_sleep_schedule(data)
            self._write_runtime_cache(
                RUNTIME_CACHE_SLEEP_SCHEDULE_KEY,
                self._encode_sleep_schedule(schedule),
            )
            return schedule

        cached = self._decode_sleep_schedule(self._read_runtime_cache(RUNTIME_CACHE_SLEEP_SCHEDULE_KEY))
        if cached is not None:
            if error_text:
                logger.warning(
                    "Failed to read quiet-hours schedule from device.json, using cached value %s-%s: %s",
                    cached[0],
                    cached[1],
                    error_text,
                )
            return cached
        return None

    def set_sleep_schedule(self, sleep_start: str | None, wake_up: str | None) -> DeviceSettingsApplyResult:
        """Set or clear quiet hours. Pass None/None to disable."""
        device_config_path = self._device_config_path()
        if sleep_start is None or wake_up is None:
            active_start, active_end = "00:00", "24:00"
        else:
            active_start, active_end = wake_up, sleep_start
        try:
            data = self.read_device_settings()
            playlist_config = data.get("playlist_config")
            if not isinstance(playlist_config, dict):
                return DeviceSettingsApplyResult(
                    success=False,
                    message="playlist_config nicht in device.json gefunden. Bitte führe die Einrichtung erneut aus.",
                    confirmed_settings={},
                )
            updated = False
            for playlist in playlist_config.get("playlists", []):
                if not isinstance(playlist, dict):
                    continue
                playlist["start_time"] = active_start
                playlist["end_time"] = active_end
                updated = True
            if not updated:
                return DeviceSettingsApplyResult(
                    success=False,
                    message="Keine Playlist in device.json gefunden. Bitte führe die Einrichtung erneut aus.",
                    confirmed_settings={},
                )
            data["playlist_config"] = playlist_config
            _write_device_json(device_config_path, data)
        except Exception as exc:
            return DeviceSettingsApplyResult(
                success=False,
                message=f"Fehler beim Speichern: {exc}",
                confirmed_settings={},
            )
        return self.apply_device_settings({}, refresh_current=True)

    def ping_inkypi(self) -> bool | None:
        """Return True=reachable, False=unreachable, None=not applicable (non-HTTP mode)."""
        if self.config.update_method != "http_update_now":
            return None
        if self._is_http_backend_degraded():
            return False
        update_parts = parse.urlsplit(self.config.update_now_url)
        probe_url = parse.urlunsplit((update_parts.scheme, update_parts.netloc, "/", "", ""))
        try:
            with request.urlopen(probe_url, timeout=5) as response:
                response.read(1)
                return True
        except error.HTTPError:
            return True  # any HTTP response means InkyPi is up
        except Exception:
            return False

    def _read_plugin_refresh_interval(self, data: dict) -> int:
        playlist_config = data.get("playlist_config")
        if not isinstance(playlist_config, dict):
            return 86400
        for playlist in playlist_config.get("playlists", []):
            if not isinstance(playlist, dict):
                continue
            for plugin in playlist.get("plugins", []):
                if not isinstance(plugin, dict):
                    continue
                if plugin.get("plugin_id") == self.config.plugin_id:
                    interval = plugin.get("refresh", {}).get("interval")
                    if isinstance(interval, (int, float)) and interval > 0:
                        return int(interval)
        return 86400

    def _extract_sleep_schedule(self, data: dict[str, object]) -> tuple[str, str] | None:
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
            return (end, start)
        return None

    def _sync_active_plugin_instance(self, payload_path: Path) -> DisplayResult | None:
        device_config_path = self._device_config_path()
        try:
            data = json.loads(device_config_path.read_text(encoding="utf-8")) if device_config_path.exists() else {}
        except PermissionError as exc:
            logger.warning("Permission denied reading device config for plugin sync: %s", exc)
            return DisplayResult(False, f"Failed to read InkyPi device config: {exc}")
        except OSError as exc:
            logger.warning("OS error reading device config for plugin sync: %s", exc)
            return DisplayResult(False, f"Failed to read InkyPi device config: {exc}")
        except json.JSONDecodeError as exc:
            logger.warning("Invalid JSON in device config during plugin sync: %s", exc)
            return DisplayResult(False, f"InkyPi device config is invalid JSON: {exc}")

        playlist_config = data.get("playlist_config")
        if not isinstance(playlist_config, dict):
            return None

        playlists = playlist_config.get("playlists")
        if not isinstance(playlists, list):
            return None

        target_playlist: dict[str, object] | None = None
        target_instance: dict[str, object] | None = None
        target_index: int | None = None

        for playlist in playlists:
            if not isinstance(playlist, dict):
                continue
            plugins = playlist.get("plugins")
            if not isinstance(plugins, list):
                continue

            named_match: tuple[dict[str, object], int] | None = None
            fallback_match: tuple[dict[str, object], int] | None = None
            for index, plugin in enumerate(plugins):
                if not isinstance(plugin, dict):
                    continue
                if plugin.get("plugin_id") != self.config.plugin_id:
                    continue
                if fallback_match is None:
                    fallback_match = (plugin, index)
                if plugin.get("name") == DEFAULT_TELEGRAM_FRAME_INSTANCE_NAME:
                    named_match = (plugin, index)
                    break

            match = named_match or fallback_match
            if match is not None:
                target_playlist = playlist
                target_instance, target_index = match
                break

        if target_playlist is None or target_instance is None or target_index is None:
            logger.debug("No matching %s plugin instance found in playlist_config; skipping plugin sync.", self.config.plugin_id)
            return None

        changed = False
        payload_text = str(payload_path.resolve(strict=False))

        plugin_settings = target_instance.get("plugin_settings")
        if not isinstance(plugin_settings, dict):
            plugin_settings = {}
            target_instance["plugin_settings"] = plugin_settings
            changed = True
        if plugin_settings.get("payload_path") != payload_text:
            plugin_settings["payload_path"] = payload_text
            changed = True

        if target_playlist.get("current_plugin_index") != target_index:
            target_playlist["current_plugin_index"] = target_index
            changed = True

        playlist_name = target_playlist.get("name")
        if isinstance(playlist_name, str) and playlist_name:
            if playlist_config.get("active_playlist") != playlist_name:
                playlist_config["active_playlist"] = playlist_name
                changed = True

        if not changed:
            return None

        try:
            device_config_path.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                "w",
                encoding="utf-8",
                dir=device_config_path.parent,
                delete=False,
            ) as handle:
                json.dump(data, handle, indent=2, sort_keys=True)
                handle.write("\n")
                temp_path = Path(handle.name)
            temp_path.replace(device_config_path)
            return None
        except PermissionError as exc:
            logger.warning("Permission denied syncing Telegram Frame plugin instance: %s", exc)
            return DisplayResult(False, f"Failed to update InkyPi plugin settings: {exc}")
        except OSError as exc:
            logger.warning("OS error syncing Telegram Frame plugin instance: %s", exc)
            return DisplayResult(False, f"Failed to update InkyPi plugin settings: {exc}")

    def _read_device_settings_with_status(self) -> tuple[dict[str, object] | None, str | None]:
        path = self._device_config_path()
        if not path.exists():
            return None, "device.json fehlt"
        try:
            return json.loads(path.read_text(encoding="utf-8")), None
        except (OSError, json.JSONDecodeError) as exc:
            return None, str(exc)

    def _refresh_runtime_cache_from_data(self, data: dict[str, object]) -> None:
        orientation = str(data.get("orientation") or "horizontal").strip().lower()
        if orientation not in {"horizontal", "vertical"}:
            orientation = "horizontal"
        self._write_runtime_cache(RUNTIME_CACHE_ORIENTATION_KEY, orientation)
        self._write_runtime_cache(
            RUNTIME_CACHE_INTERVAL_KEY,
            str(self._read_plugin_refresh_interval(data)),
        )
        self._write_runtime_cache(
            RUNTIME_CACHE_SLEEP_SCHEDULE_KEY,
            self._encode_sleep_schedule(self._extract_sleep_schedule(data)),
        )

    def _write_runtime_cache(self, key: str, value: str) -> None:
        if self.database is None:
            return
        self.database.set_setting(key, value)

    def _read_runtime_cache(self, key: str) -> str | None:
        if self.database is None:
            return None
        return self.database.get_setting(key)

    @staticmethod
    def _encode_sleep_schedule(schedule: tuple[str, str] | None) -> str:
        if schedule is None:
            return ""
        return f"{schedule[0]}|{schedule[1]}"

    @staticmethod
    def _decode_sleep_schedule(raw: str | None) -> tuple[str, str] | None:
        if not raw:
            return None
        parts = raw.split("|", 1)
        if len(parts) != 2 or not parts[0] or not parts[1]:
            return None
        return parts[0], parts[1]

    def _format_refresh_command(self, payload_path: Path, image_path: Path) -> list[str]:
        command = self.config.refresh_command.format(
            payload_path=payload_path,
            image_path=image_path,
            repo_path=self.config.repo_path,
            install_path=self.config.install_path,
            plugin_id=self.config.plugin_id,
        )
        return shlex.split(command)

    def _has_refresh_command(self) -> bool:
        return bool(self.config.refresh_command.strip())

    def _refresh_command_uses_explicit_paths(self) -> bool:
        raw = self.config.refresh_command
        return "{payload_path}" in raw or "{image_path}" in raw

    def _command_needs_temporary_plugin_payload(
        self,
        payload_path: Path,
        primary_plugin_payload_path: Path,
    ) -> bool:
        return (
            not self._refresh_command_uses_explicit_paths()
            and payload_path != primary_plugin_payload_path
        )

    def _mark_http_backend_degraded(self, reason: str) -> None:
        self._http_degraded_until_monotonic = time.monotonic() + HTTP_DEGRADED_COOLDOWN_SECONDS
        self._http_degraded_reason = reason

    def _clear_http_backend_degraded(self) -> None:
        self._http_degraded_until_monotonic = 0.0
        self._http_degraded_reason = ""

    def _is_http_backend_degraded(self) -> bool:
        return self._http_degraded_until_monotonic > time.monotonic()

    @staticmethod
    def _format_duration_brief(seconds: int) -> str:
        if seconds >= 3600:
            hours, remainder = divmod(seconds, 3600)
            minutes = remainder // 60
            return f"{hours} Std. {minutes} Min." if minutes else f"{hours} Std."
        if seconds >= 60:
            minutes, remainder = divmod(seconds, 60)
            return f"{minutes} Min. {remainder} Sek." if remainder else f"{minutes} Min."
        return f"{seconds} Sek."

    def _committed_image_path(self, request: DisplayRequest) -> Path:
        return self._committed_images_dir() / f"{request.image_id}_{self._revision_hash(request.to_payload())}.png"

    def _committed_images_dir(self) -> Path:
        return self.storage.inkypi_payload_dir / COMMITTED_ARTIFACT_DIR_NAME

    def _cleanup_old_committed_images(self, *, keep_path: Path) -> None:
        committed_dir = self._committed_images_dir()
        if not committed_dir.exists():
            return
        current_prepared = self._read_current_prepared_image_path()
        candidates = sorted(
            (path for path in committed_dir.glob("*.png") if path.is_file()),
            key=lambda item: item.stat().st_mtime,
            reverse=True,
        )
        retained = 0
        for candidate in candidates:
            if candidate == keep_path or candidate == current_prepared:
                retained += 1
                continue
            if retained < COMMITTED_ARTIFACT_RETENTION:
                retained += 1
                continue
            self._safe_unlink(candidate)

    def _read_current_prepared_image_path(self) -> Path | None:
        try:
            payload = self._load_payload(self.storage.current_payload_path)
        except (json.JSONDecodeError, OSError):
            logger.warning(
                "Could not read current payload while cleaning committed display artifacts",
                exc_info=True,
            )
            return None
        if payload is None:
            return None
        prepared = payload.get("prepared_image_path")
        if not prepared:
            return None
        return Path(str(prepared))

    @staticmethod
    def _make_temp_path(target_path: Path) -> Path:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=target_path.parent, suffix=target_path.suffix, delete=False) as handle:
            return Path(handle.name)

    @staticmethod
    def _safe_unlink(path: Path) -> None:
        try:
            path.unlink()
        except FileNotFoundError:
            return
        except OSError:
            logger.warning("Failed to clean up temporary file %s", path, exc_info=True)

    def _restart_inkypi_service(self) -> str | None:
        sudo_bin = shutil.which("sudo")
        if sudo_bin is None:
            return "sudo ist nicht verfügbar."

        restart_command = [
            sudo_bin,
            "-n",
            self._systemctl_bin,
            "restart",
            INKYPI_SERVICE_NAME,
        ]
        try:
            restart_completed = subprocess.run(
                restart_command,
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )
        except subprocess.TimeoutExpired:
            return "Neustart von inkypi.service hat das Zeitlimit überschritten."

        if restart_completed.returncode != 0:
            stderr = restart_completed.stderr.strip() or restart_completed.stdout.strip() or "unbekannter Fehler"
            if "password is required" in stderr.lower() or "a password is required" in stderr.lower():
                return (
                    "nicht-interaktive sudo-Rechte für inkypi.service fehlen. "
                    "Führe scripts/setup_inkypi.sh erneut aus."
                )
            return stderr

        return self._wait_for_inkypi_service_active()

    def _wait_for_inkypi_service_active(self) -> str | None:
        sudo_bin = shutil.which("sudo")
        if sudo_bin is None:
            return "sudo ist nicht verfügbar."
        status_command = [
            sudo_bin,
            "-n",
            self._systemctl_bin,
            "is-active",
            INKYPI_SERVICE_NAME,
        ]
        deadline = time.monotonic() + INKYPI_RESTART_TIMEOUT_SECONDS
        last_status = "inkypi.service ist nicht aktiv geworden."
        while time.monotonic() < deadline:
            try:
                status_completed = subprocess.run(
                    status_command,
                    capture_output=True,
                    text=True,
                    timeout=15,
                    check=False,
                )
            except subprocess.TimeoutExpired:
                last_status = "Abfrage von inkypi.service hat das Zeitlimit überschritten."
                time.sleep(1)
                continue
            if status_completed.returncode == 0 and status_completed.stdout.strip() == "active":
                return None
            last_status = status_completed.stderr.strip() or status_completed.stdout.strip() or last_status
            time.sleep(1)

        return last_status

    def _wait_for_inkypi_http_ready(self) -> str | None:
        if self.config.update_method != "http_update_now":
            return None

        update_parts = parse.urlsplit(self.config.update_now_url)
        probe_url = parse.urlunsplit((update_parts.scheme, update_parts.netloc, "/", "", ""))
        deadline = time.monotonic() + INKYPI_HTTP_READY_TIMEOUT_SECONDS
        last_error = f"InkyPi war unter {probe_url} nicht erreichbar."

        while time.monotonic() < deadline:
            try:
                with request.urlopen(probe_url, timeout=5) as response:
                    response.read(1)
                    return None
            except error.HTTPError:
                return None
            except error.URLError as exc:
                last_error = str(exc.reason)
            except Exception as exc:  # pragma: no cover - defensive runtime guard
                last_error = str(exc)
            time.sleep(1)

        return last_error
