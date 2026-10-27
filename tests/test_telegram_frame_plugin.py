from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path

PLUGIN_PATH = Path(__file__).resolve().parents[1] / "integrations" / "inkypi_plugin" / "telegram_frame" / "telegram_frame.py"


def _load_plugin_class():
    """Import the InkyPi plugin with a stub for InkyPi's BasePlugin, which only exists on the Pi."""
    stub_module = types.ModuleType("plugins.base_plugin.base_plugin")
    stub_module.BasePlugin = type("BasePlugin", (), {})
    for name in ("plugins", "plugins.base_plugin"):
        sys.modules.setdefault(name, types.ModuleType(name))
    sys.modules["plugins.base_plugin.base_plugin"] = stub_module
    spec = importlib.util.spec_from_file_location("telegram_frame_under_test", PLUGIN_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.TelegramFrame


class _FakeDeviceConfig:
    def __init__(self, orientation: str) -> None:
        self.orientation = orientation

    def get_config(self, key: str):
        return self.orientation if key == "orientation" else None

    def get_resolution(self) -> tuple[int, int]:
        return 800, 480


class TelegramFramePlaceholderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.plugin = _load_plugin_class()()

    def test_missing_payload_shows_placeholder_in_both_orientations(self) -> None:
        for orientation, size in (("horizontal", (800, 480)), ("vertical", (480, 800))):
            with self.subTest(orientation=orientation), tempfile.TemporaryDirectory() as tmpdir:
                missing = Path(tmpdir) / "current.json"

                image = self.plugin.generate_image({"payload_path": str(missing)}, _FakeDeviceConfig(orientation))

                self.assertEqual(image.size, size)
                self._assert_has_dark_text(image)

    def test_placeholder_payload_shows_placeholder(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            payload = Path(tmpdir) / "current.json"
            payload.write_text(json.dumps({"placeholder": True}), encoding="utf-8")

            image = self.plugin.generate_image({"payload_path": str(payload)}, _FakeDeviceConfig("horizontal"))

            self.assertEqual(image.size, (800, 480))
            self._assert_has_dark_text(image)

    def test_placeholder_text_is_shrunk_to_fit_the_width(self) -> None:
        from PIL import Image, ImageDraw

        draw = ImageDraw.Draw(Image.new("RGB", (10, 10)))
        font = self.plugin._fitting_font(draw, "Noch keine Bilder", "", 200, 300)

        self.assertLessEqual(draw.textlength("Noch keine Bilder", font=font), 300)
        self.assertLess(font.size, 200)

    def test_broken_payload_still_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            payload = Path(tmpdir) / "current.json"
            payload.write_text("{not json", encoding="utf-8")

            with self.assertRaises(RuntimeError):
                self.plugin.generate_image({"payload_path": str(payload)}, _FakeDeviceConfig("horizontal"))

    def _assert_has_dark_text(self, image) -> None:
        darkest = min(image.convert("L").getdata())
        self.assertLess(darkest, 100)
        # The text must fit: the left and right edge columns stay white (no clipped letters).
        gray = image.convert("L")
        for x in (0, 1, image.width - 2, image.width - 1):
            self.assertEqual(min(gray.getpixel((x, y)) for y in range(image.height)), 255)


if __name__ == "__main__":
    unittest.main()
