from __future__ import annotations

import importlib
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

from app.inkypi_setup import (
    DEFAULT_PLUGIN_INSTANCE_NAME,
    seed_dashboard_plugin_instance,
    seed_device_defaults,
    verify_plugin_module_import,
    verify_seeded_plugin_instance,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class InkyPiSetupTests(unittest.TestCase):
    def test_seed_dashboard_plugin_creates_default_playlist_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            device_path = Path(tmpdir) / "device.json"

            result = seed_dashboard_plugin_instance(device_path, "telegram_frame", "/tmp/current.json")

            self.assertTrue(result.applied)
            verify_seeded_plugin_instance(device_path, "telegram_frame", "/tmp/current.json")
            data = json.loads(device_path.read_text(encoding="utf-8"))
            self.assertIn("playlist_config", data)

    def test_seed_dashboard_plugin_seeds_empty_default_playlist(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            device_path = Path(tmpdir) / "device.json"
            device_path.write_text(
                json.dumps(
                    {
                        "playlists": {"Default": ["telegram_frame"]},
                        "telegram_frame": {"payload_path": "/tmp/old.json"},
                        "playlist_config": {
                            "playlists": [
                                {
                                    "name": "Default",
                                    "start_time": "00:00",
                                    "end_time": "24:00",
                                    "plugins": [],
                                    "current_plugin_index": None,
                                }
                            ],
                            "active_playlist": None,
                        }
                    }
                ),
                encoding="utf-8",
            )

            result = seed_dashboard_plugin_instance(device_path, "telegram_frame", "/tmp/current.json")

            self.assertTrue(result.applied)
            verify_seeded_plugin_instance(device_path, "telegram_frame", "/tmp/current.json")
            data = json.loads(device_path.read_text(encoding="utf-8"))
            self.assertNotIn("playlists", data)
            self.assertNotIn("telegram_frame", data)

    def test_seed_dashboard_plugin_skips_existing_nonempty_playlists(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            device_path = Path(tmpdir) / "device.json"
            original = {
                "playlist_config": {
                    "playlists": [
                        {
                            "name": "Default",
                            "start_time": "00:00",
                            "end_time": "24:00",
                            "plugins": [
                                {
                                    "plugin_id": "weather",
                                    "name": "Weather",
                                    "plugin_settings": {},
                                    "refresh": {"interval": 600},
                                    "latest_refresh_time": None,
                                }
                            ],
                            "current_plugin_index": None,
                        }
                    ],
                    "active_playlist": None,
                }
            }
            device_path.write_text(json.dumps(original), encoding="utf-8")

            result = seed_dashboard_plugin_instance(device_path, "telegram_frame", "/tmp/current.json")

            self.assertFalse(result.applied)
            data = json.loads(device_path.read_text(encoding="utf-8"))
            self.assertEqual(data, original)

    def test_seed_device_defaults_preserves_existing_orientation_and_image_settings(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            device_path = Path(tmpdir) / "device.json"
            device_path.write_text(
                json.dumps(
                    {
                        "orientation": "horizontal",
                        "timezone": "America/New_York",
                        "image_settings": {
                            "saturation": 1.9,
                            "contrast": 1.1,
                            "sharpness": 1.7,
                            "brightness": 0.8,
                        },
                    }
                ),
                encoding="utf-8",
            )

            seed_device_defaults(device_path)

            data = json.loads(device_path.read_text(encoding="utf-8"))
            self.assertEqual(data["orientation"], "horizontal")
            self.assertEqual(data["timezone"], "America/New_York")
            self.assertEqual(
                data["image_settings"],
                {
                    "saturation": 1.9,
                    "contrast": 1.1,
                    "sharpness": 1.7,
                    "brightness": 0.8,
                },
            )
            self.assertTrue(data["inverted_image"])

    def test_seed_device_defaults_only_fills_missing_nested_image_settings(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            device_path = Path(tmpdir) / "device.json"
            device_path.write_text(
                json.dumps(
                    {
                        "image_settings": {
                            "saturation": 1.9,
                        },
                    }
                ),
                encoding="utf-8",
            )

            seed_device_defaults(device_path)

            data = json.loads(device_path.read_text(encoding="utf-8"))
            self.assertEqual(data["image_settings"]["saturation"], 1.9)
            self.assertEqual(data["image_settings"]["contrast"], 1.4)
            self.assertEqual(data["image_settings"]["sharpness"], 1.2)
            self.assertEqual(data["image_settings"]["brightness"], 1.1)

    def test_verify_plugin_module_import_smoke(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            source_root = tmpdir_path / "src"
            self._prepare_plugin_import_tree(source_root)

            verify_plugin_module_import(source_root, "telegram_frame", "TelegramFrame")

            plugin_module = __import__("plugins.telegram_frame.telegram_frame", fromlist=["TelegramFrame"])
            plugin_class = getattr(plugin_module, "TelegramFrame")
            self.assertEqual(plugin_class.__name__, "TelegramFrame")
            self.assertEqual(DEFAULT_PLUGIN_INSTANCE_NAME, "Telegram Frame")

    def test_plugin_generates_horizontal_canvas_with_white_caption_bar(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            source_root = tmpdir_path / "src"
            self._prepare_plugin_import_tree(source_root)

            image_path = tmpdir_path / "prepared.png"
            payload_path = tmpdir_path / "payload.json"
            from PIL import Image

            Image.new("RGB", (1600, 600), (210, 120, 70)).save(image_path)
            payload_path.write_text(
                json.dumps(
                    {
                        "prepared_image_path": str(image_path),
                        "caption": "A very long caption that should still end up in a tiny white bar.",
                        "taken_at": "2026-03-18",
                        "location": "Berlin, Germany",
                        "caption_bar_height": 44,
                        "caption_font_size": 20,
                        "metadata_font_size": 14,
                        "caption_character_limit": 72,
                        "caption_margin": 12,
                        "caption_text_color": "#111111",
                        "caption_background_color": "#FFFFFF",
                    }
                ),
                encoding="utf-8",
            )

            plugin_class = self._import_plugin_class(source_root)
            plugin = plugin_class()
            generated = plugin.generate_image(
                {"payload_path": str(payload_path)},
                _FakeDeviceConfig("horizontal", (800, 480)),
            )

            self.assertEqual(generated.size, (800, 480))
            self.assertNotEqual(generated.getpixel((10, 10)), (255, 255, 255))
            self.assertEqual(generated.getpixel((2, 470)), (255, 255, 255))
            self.assertEqual(generated.getpixel((798, 470)), (255, 255, 255))
            self.assertGreater(self._count_nonwhite_pixels(generated, (12, 438, 280, 478)), 0)
            self.assertGreater(self._count_nonwhite_pixels(generated, (560, 438, 788, 478)), 0)

    def test_plugin_generates_vertical_canvas_for_portrait_images(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            source_root = tmpdir_path / "src"
            self._prepare_plugin_import_tree(source_root)

            image_path = tmpdir_path / "prepared.png"
            payload_path = tmpdir_path / "payload.json"
            from PIL import Image

            Image.new("RGB", (600, 1600), (70, 120, 210)).save(image_path)
            payload_path.write_text(
                json.dumps(
                    {
                        "prepared_image_path": str(image_path),
                        "caption": "Portrait test",
                        "taken_at": "2026-03-18",
                        "location": "Berlin",
                        "caption_bar_height": 44,
                        "caption_font_size": 20,
                        "metadata_font_size": 14,
                        "caption_character_limit": 72,
                        "caption_margin": 12,
                        "caption_text_color": "#111111",
                        "caption_background_color": "#FFFFFF",
                    }
                ),
                encoding="utf-8",
            )

            plugin_class = self._import_plugin_class(source_root)
            plugin = plugin_class()
            generated = plugin.generate_image(
                {"payload_path": str(payload_path)},
                _FakeDeviceConfig("vertical", (800, 480)),
            )

            self.assertEqual(generated.size, (480, 800))
            self.assertNotEqual(generated.getpixel((10, 10)), (255, 255, 255))
            self.assertEqual(generated.getpixel((2, 790)), (255, 255, 255))
            self.assertEqual(generated.getpixel((478, 790)), (255, 255, 255))
            self.assertGreater(self._count_nonwhite_pixels(generated, (12, 758, 170, 798)), 0)
            self.assertGreater(self._count_nonwhite_pixels(generated, (300, 758, 468, 798)), 0)

    def test_plugin_uses_device_orientation_even_if_payload_hint_differs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            source_root = tmpdir_path / "src"
            self._prepare_plugin_import_tree(source_root)

            image_path = tmpdir_path / "prepared.png"
            payload_path = tmpdir_path / "payload.json"
            from PIL import Image

            Image.new("RGB", (600, 1600), (70, 120, 210)).save(image_path)
            payload_path.write_text(
                json.dumps(
                    {
                        "prepared_image_path": str(image_path),
                        "orientation_hint": "vertical",
                        "caption": "Should still render horizontal",
                        "caption_bar_height": 44,
                    }
                ),
                encoding="utf-8",
            )

            plugin_class = self._import_plugin_class(source_root)
            plugin = plugin_class()
            generated = plugin.generate_image(
                {"payload_path": str(payload_path)},
                _FakeDeviceConfig("horizontal", (800, 480)),
            )

            self.assertEqual(generated.size, (800, 480))

    def test_plugin_preserves_emoji_in_text_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            source_root = tmpdir_path / "src"
            self._prepare_plugin_import_tree(source_root)

            plugin_class = self._import_plugin_class(source_root)
            plugin = plugin_class()
            text_font = object()
            emoji_font = object()

            runs = plugin._split_text_runs("Urlaub 😊 in Rom", text_font, emoji_font)

            self.assertEqual([run["text"] for run in runs], ["Urlaub ", "😊", " in Rom"])
            self.assertEqual([run["font"] for run in runs], [text_font, emoji_font, text_font])
            self.assertEqual([run["embedded_color"] for run in runs], [False, True, False])

    def test_plugin_generate_image_does_not_crash_when_emoji_font_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            source_root = tmpdir_path / "src"
            self._prepare_plugin_import_tree(source_root)

            image_path = tmpdir_path / "prepared.png"
            payload_path = tmpdir_path / "payload.json"
            from PIL import Image

            Image.new("RGB", (1600, 600), (210, 120, 70)).save(image_path)
            payload_path.write_text(
                json.dumps(
                    {
                        "prepared_image_path": str(image_path),
                        "caption": "Urlaub 😊 in Rom",
                        "taken_at": "2026-03-18",
                        "location": "Berlin",
                        "caption_bar_height": 44,
                        "caption_font_size": 20,
                        "metadata_font_size": 14,
                        "caption_character_limit": 72,
                        "caption_margin": 12,
                        "font_path": "/tmp/does-not-exist.ttf",
                        "emoji_font_path": "/tmp/missing-emoji-font.ttf",
                        "caption_text_color": "#111111",
                        "caption_background_color": "#FFFFFF",
                    }
                ),
                encoding="utf-8",
            )

            plugin_class = self._import_plugin_class(source_root)
            plugin = plugin_class()
            generated = plugin.generate_image(
                {"payload_path": str(payload_path)},
                _FakeDeviceConfig("horizontal", (800, 480)),
            )

            self.assertEqual(generated.size, (800, 480))
            self.assertGreater(self._count_nonwhite_pixels(generated, (12, 438, 320, 478)), 0)

    def _prepare_plugin_import_tree(self, source_root: Path) -> None:
        plugin_root = source_root / "plugins"
        base_plugin_dir = plugin_root / "base_plugin"
        telegram_plugin_dir = plugin_root / "telegram_frame"

        base_plugin_dir.mkdir(parents=True)
        telegram_plugin_dir.parent.mkdir(parents=True, exist_ok=True)
        (plugin_root / "__init__.py").write_text("", encoding="utf-8")
        (base_plugin_dir / "__init__.py").write_text("", encoding="utf-8")
        (base_plugin_dir / "base_plugin.py").write_text(
            "class BasePlugin:\n    def __init__(self, config=None, **dependencies):\n        self.config = config or {}\n",
            encoding="utf-8",
        )
        shutil.copytree(PROJECT_ROOT / "integrations" / "inkypi_plugin" / "telegram_frame", telegram_plugin_dir)

    def _import_plugin_class(self, source_root: Path):
        sys.path.insert(0, str(source_root))
        try:
            importlib.invalidate_caches()
            for module_name in list(sys.modules):
                if module_name == "plugins" or module_name.startswith("plugins."):
                    sys.modules.pop(module_name, None)
            module = importlib.import_module("plugins.telegram_frame.telegram_frame")
            return getattr(module, "TelegramFrame")
        finally:
            try:
                sys.path.remove(str(source_root))
            except ValueError:
                pass

    def _count_nonwhite_pixels(self, image, box: tuple[int, int, int, int]) -> int:
        crop = image.crop(box)
        return sum(1 for pixel in crop.getdata() if pixel != (255, 255, 255))


class _FakeDeviceConfig:
    def __init__(self, orientation: str, resolution: tuple[int, int]):
        self._orientation = orientation
        self._resolution = resolution

    def get_config(self, key: str):
        if key == "orientation":
            return self._orientation
        return None

    def get_resolution(self) -> tuple[int, int]:
        return self._resolution


if __name__ == "__main__":
    unittest.main()
