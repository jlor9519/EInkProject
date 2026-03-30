from __future__ import annotations

import hashlib
import json
import logging
import shlex
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
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
DEFAULT_TELEGRAM_FRAME_INSTANCE_NAME = "Telegram Frame"


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
    def __init__(self, config: InkyPiConfig, storage: StorageConfig, display: DisplayConfig):
        self.config = config
        self.storage = storage
        self.display_config = display
        self.layout = resolve_inkypi_layout(config.repo_path, config.install_path)
        self._systemctl_bin = shutil.which("systemctl") or "/usr/bin/systemctl"

    def display(self, request: DisplayRequest) -> DisplayResult:
        logger.info("Writing bridge payload for image %s", request.image_id)
        payload_path = self._write_bridge_payload(request)
        result = self._trigger_display_update(payload_path)
        result.payload_path = payload_path
        logger.info("Display result for %s: success=%s", request.image_id, result.success)
        return result

    def read_device_settings(self) -> dict[str, object]:
        path = self._device_config_path()
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

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

    def _trigger_display_update(self, payload_path: Path) -> DisplayResult:
        try:
            payload = self._load_payload(payload_path)
        except json.JSONDecodeError as exc:
            return DisplayResult(False, f"InkyPi payload is not valid JSON: {exc}")

        if payload is None:
            return DisplayResult(False, f"InkyPi payload does not exist: {payload_path}")

        plugin_sync_result = self._sync_active_plugin_instance(payload_path)
        if plugin_sync_result is not None:
            return plugin_sync_result

        if self.config.update_method == "http_update_now":
            logger.info("Triggering display via HTTP POST to %s", self.config.update_now_url)
            return self._post_update_now(payload_path)

        command = self._format_refresh_command(payload_path, self.storage.current_image_path)
        logger.info("Triggering display via command: %s", command)
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

    def _post_update_now(self, payload_path: Path) -> DisplayResult:
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
            with request.urlopen(http_request, timeout=60) as response:
                body = response.read().decode("utf-8", errors="replace")
                return self._parse_http_response(body, response.status)
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            parsed = self._parse_http_response(body, exc.code)
            if parsed.success:
                return DisplayResult(False, f"InkyPi update_now returned HTTP {exc.code}")
            return parsed
        except error.URLError as exc:
            return DisplayResult(False, f"InkyPi update_now request failed: {exc.reason}")

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

    def _write_bridge_payload(self, request: DisplayRequest) -> Path:
        self.storage.inkypi_payload_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(request.composed_path, self.storage.current_image_path)
        orientation_hint = self.current_orientation()

        payload = request.to_payload()
        payload["prepared_image_path"] = str(self.storage.current_image_path)
        payload["bridge_image_path"] = str(self.storage.current_image_path)
        payload["payload_path"] = str(self.storage.current_payload_path)
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

        self.storage.current_payload_path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=self.storage.current_payload_path.parent,
            delete=False,
        ) as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            temp_path = Path(handle.name)
        temp_path.replace(self.storage.current_payload_path)
        return self.storage.current_payload_path

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
        try:
            settings = self.read_device_settings()
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("Failed to read current InkyPi orientation, defaulting to horizontal: %s", exc)
            return "horizontal"

        orientation = str(settings.get("orientation") or "horizontal").strip().lower()
        if orientation not in {"horizontal", "vertical"}:
            return "horizontal"
        return orientation

    def get_slideshow_interval(self) -> int:
        """Return the slideshow refresh interval in seconds from device.json, default 86400."""
        try:
            data = self.read_device_settings()
        except (OSError, json.JSONDecodeError):
            return 86400
        return self._read_plugin_refresh_interval(data)

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
        try:
            data = self.read_device_settings()
        except (OSError, json.JSONDecodeError):
            return None
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
            # start_time = wake_up, end_time = sleep_start (device.json is "active" window)
            return (end, start)
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

    def _format_refresh_command(self, payload_path: Path, image_path: Path) -> list[str]:
        command = self.config.refresh_command.format(
            payload_path=payload_path,
            image_path=image_path,
            repo_path=self.config.repo_path,
            install_path=self.config.install_path,
            plugin_id=self.config.plugin_id,
        )
        return shlex.split(command)

    def _restart_inkypi_service(self) -> str | None:
        sudo_bin = shutil.which("sudo")
        if sudo_bin is None:
            return "sudo ist nicht verfugbar."

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
            return "Neustart von inkypi.service hat das Zeitlimit uberschritten."

        if restart_completed.returncode != 0:
            stderr = restart_completed.stderr.strip() or restart_completed.stdout.strip() or "unbekannter Fehler"
            if "password is required" in stderr.lower() or "a password is required" in stderr.lower():
                return (
                    "nicht-interaktive sudo-Rechte fur inkypi.service fehlen. "
                    "Fuhre scripts/setup_inkypi.sh erneut aus."
                )
            return stderr

        deadline = time.monotonic() + INKYPI_RESTART_TIMEOUT_SECONDS
        status_command = [
            sudo_bin,
            "-n",
            self._systemctl_bin,
            "is-active",
            INKYPI_SERVICE_NAME,
        ]
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
                last_status = "Abfrage von inkypi.service hat das Zeitlimit uberschritten."
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
