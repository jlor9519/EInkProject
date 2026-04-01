from __future__ import annotations

import importlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_PLAYLIST_NAME = "Default"
DEFAULT_PLUGIN_INSTANCE_NAME = "Telegram Frame"
DEFAULT_PLUGIN_REFRESH_INTERVAL = 86400

DEVICE_DEFAULTS: dict[str, Any] = {
    "orientation": "vertical",
    "inverted_image": True,
    "timezone": "Europe/Berlin",
    "time_format": "24h",
    "image_settings": {
        "saturation": 1.4,
        "contrast": 1.4,
        "sharpness": 1.2,
        "brightness": 1.1,
    },
}


@dataclass(slots=True)
class DashboardSeedResult:
    applied: bool
    message: str
    playlist_name: str | None = None
    instance_name: str | None = None


def seed_dashboard_plugin_instance(
    device_config_path: str | Path,
    plugin_id: str,
    payload_path: str | Path,
    *,
    instance_name: str = DEFAULT_PLUGIN_INSTANCE_NAME,
    refresh_interval: int = DEFAULT_PLUGIN_REFRESH_INTERVAL,
) -> DashboardSeedResult:
    device_path = Path(device_config_path)
    payload_text = str(Path(payload_path).expanduser().resolve(strict=False))

    data = _load_json(device_path)
    removed_legacy_keys = _remove_legacy_telegram_frame_keys(data, plugin_id)
    playlist_config = data.get("playlist_config")
    if playlist_config is None:
        playlist_config = _new_playlist_config()
        data["playlist_config"] = playlist_config
        should_seed = True
    elif not isinstance(playlist_config, dict):
        raise RuntimeError(f"device.json has an invalid playlist_config value at {device_path}")
    else:
        should_seed = _is_fresh_playlist_config(playlist_config)

    if should_seed:
        playlist = _ensure_default_playlist(playlist_config)
        plugin_instance = _build_plugin_instance(
            plugin_id=plugin_id,
            instance_name=instance_name,
            payload_path=payload_text,
            refresh_interval=refresh_interval,
        )

        plugins = playlist.setdefault("plugins", [])
        existing = _find_plugin_instance(plugins, plugin_id, instance_name)
        if existing is None:
            plugins.append(plugin_instance)
            message = f"Seeded Default playlist with plugin instance '{instance_name}'."
        else:
            existing.update(plugin_instance)
            message = f"Updated existing plugin instance '{instance_name}' in Default playlist."

        playlist.setdefault("current_plugin_index", None)
        playlist_config.setdefault("active_playlist", None)
        data["playlist_config"] = playlist_config
        _write_json(device_path, data)
        return DashboardSeedResult(True, message, DEFAULT_PLAYLIST_NAME, instance_name)

    if removed_legacy_keys:
        _write_json(device_path, data)
        return DashboardSeedResult(
            False,
            "Skipped dashboard seed because existing playlist data already contains user-managed content. Removed legacy Telegram Frame config keys.",
        )

    return DashboardSeedResult(
        False,
        "Skipped dashboard seed because existing playlist data already contains user-managed content.",
    )


def seed_device_defaults(device_config_path: str | Path) -> None:
    """Apply preferred device defaults to device.json (called during setup/reinstall)."""
    device_path = Path(device_config_path)
    data = _load_json(device_path)
    for key, value in DEVICE_DEFAULTS.items():
        if key == "image_settings":
            existing = data.setdefault("image_settings", {})
            if not isinstance(existing, dict):
                existing = {}
                data["image_settings"] = existing
            for nested_key, nested_value in value.items():
                existing.setdefault(nested_key, nested_value)
        else:
            data.setdefault(key, value)
    _write_json(device_path, data)


def verify_seeded_plugin_instance(
    device_config_path: str | Path,
    plugin_id: str,
    payload_path: str | Path,
    *,
    instance_name: str = DEFAULT_PLUGIN_INSTANCE_NAME,
) -> None:
    device_path = Path(device_config_path)
    payload_text = str(Path(payload_path).expanduser().resolve(strict=False))
    data = _load_json(device_path)

    playlist_config = data.get("playlist_config")
    if not isinstance(playlist_config, dict):
        raise RuntimeError(f"playlist_config is missing from {device_path}")

    playlists = playlist_config.get("playlists")
    if not isinstance(playlists, list):
        raise RuntimeError(f"playlist_config.playlists is missing from {device_path}")

    default_playlist = next(
        (playlist for playlist in playlists if isinstance(playlist, dict) and playlist.get("name") == DEFAULT_PLAYLIST_NAME),
        None,
    )
    if default_playlist is None:
        raise RuntimeError("Default playlist is missing from playlist_config.")

    plugins = default_playlist.get("plugins", [])
    plugin_instance = _find_plugin_instance(plugins, plugin_id, instance_name)
    if plugin_instance is None:
        raise RuntimeError(f"Plugin instance '{instance_name}' for plugin '{plugin_id}' is missing.")

    settings = plugin_instance.get("plugin_settings", {})
    if settings.get("payload_path") != payload_text:
        raise RuntimeError(
            f"Plugin instance payload_path is {settings.get('payload_path')!r}, expected {payload_text!r}."
        )


def verify_plugin_module_import(source_root: str | Path, plugin_id: str, class_name: str) -> None:
    root = Path(source_root).resolve(strict=False)
    sys.path.insert(0, str(root))
    importlib.invalidate_caches()
    try:
        module = importlib.import_module(f"plugins.{plugin_id}.{plugin_id}")
    except Exception as exc:  # pragma: no cover - exercised through integration tests
        raise RuntimeError(f"Failed to import plugins.{plugin_id}.{plugin_id}: {exc}") from exc
    finally:
        try:
            sys.path.remove(str(root))
        except ValueError:
            pass

    if not hasattr(module, class_name):
        raise RuntimeError(f"Plugin module does not expose class {class_name!r}.")


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _remove_legacy_telegram_frame_keys(data: dict[str, Any], plugin_id: str) -> bool:
    removed = False
    legacy_playlists = data.get("playlists")
    if isinstance(legacy_playlists, dict) and all(isinstance(value, list) for value in legacy_playlists.values()):
        data.pop("playlists", None)
        removed = True

    legacy_plugin_settings = data.get(plugin_id)
    if isinstance(legacy_plugin_settings, dict) and "payload_path" in legacy_plugin_settings:
        data.pop(plugin_id, None)
        removed = True

    return removed


def _write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _new_playlist_config() -> dict[str, Any]:
    return {
        "playlists": [
            {
                "name": DEFAULT_PLAYLIST_NAME,
                "start_time": "00:00",
                "end_time": "24:00",
                "plugins": [],
                "current_plugin_index": None,
            }
        ],
        "active_playlist": None,
    }


def _is_fresh_playlist_config(playlist_config: dict[str, Any]) -> bool:
    playlists = playlist_config.get("playlists")
    if playlists is None:
        return True
    if not isinstance(playlists, list):
        raise RuntimeError("playlist_config.playlists must be a list.")
    if not playlists:
        return True
    if len(playlists) != 1:
        return False

    playlist = playlists[0]
    if not isinstance(playlist, dict):
        raise RuntimeError("playlist_config.playlists contains an invalid playlist entry.")
    if playlist.get("name") != DEFAULT_PLAYLIST_NAME:
        return False
    plugins = playlist.get("plugins", [])
    if not isinstance(plugins, list):
        raise RuntimeError("playlist_config Default playlist has an invalid plugins value.")
    return len(plugins) == 0


def _ensure_default_playlist(playlist_config: dict[str, Any]) -> dict[str, Any]:
    playlists = playlist_config.setdefault("playlists", [])
    if not isinstance(playlists, list):
        raise RuntimeError("playlist_config.playlists must be a list.")

    for playlist in playlists:
        if isinstance(playlist, dict) and playlist.get("name") == DEFAULT_PLAYLIST_NAME:
            playlist.setdefault("plugins", [])
            playlist.setdefault("start_time", "00:00")
            playlist.setdefault("end_time", "24:00")
            playlist.setdefault("current_plugin_index", None)
            return playlist

    playlist = {
        "name": DEFAULT_PLAYLIST_NAME,
        "start_time": "00:00",
        "end_time": "24:00",
        "plugins": [],
        "current_plugin_index": None,
    }
    playlists.append(playlist)
    return playlist


def _build_plugin_instance(
    *,
    plugin_id: str,
    instance_name: str,
    payload_path: str,
    refresh_interval: int,
) -> dict[str, Any]:
    return {
        "plugin_id": plugin_id,
        "name": instance_name,
        "plugin_settings": {
            "payload_path": payload_path,
        },
        "refresh": {
            "interval": int(refresh_interval),
        },
        "latest_refresh_time": None,
    }


def _find_plugin_instance(plugins: list[Any], plugin_id: str, instance_name: str) -> dict[str, Any] | None:
    for plugin_instance in plugins:
        if not isinstance(plugin_instance, dict):
            continue
        if plugin_instance.get("plugin_id") == plugin_id and plugin_instance.get("name") == instance_name:
            return plugin_instance
    return None
