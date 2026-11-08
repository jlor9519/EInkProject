from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from PIL import Image

from app.database import Database
from app.frame_display import FrameDisplay
from app.models import DisplayConfig, DisplayRequest, StorageConfig
from app.panel import PanelError


class _RecordingPanel:
    def __init__(self) -> None:
        self.error: str | None = None
        self.shown: list[Image.Image] = []

    def status(self) -> tuple[bool, str]:
        return True, "Testpanel"

    def show(self, image: Image.Image) -> None:
        if self.error:
            raise PanelError(self.error)
        self.shown.append(image)


class FrameDisplayTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.base = Path(self._tmp.name)
        self.database = Database(self.base / "frame.db")
        self.database.initialize()
        self.storage = StorageConfig(
            incoming_dir=self.base / "incoming",
            rendered_dir=self.base / "rendered",
            inkypi_payload_dir=self.base / "payload",
            current_payload_path=self.base / "payload" / "current.json",
            current_image_path=self.base / "payload" / "current.png",
            keep_recent_rendered=5,
        )
        self.display_config = DisplayConfig(
            width=800,
            height=480,
            caption_height=44,
            margin=18,
            metadata_font_size=14,
            caption_font_size=20,
            caption_character_limit=72,
            font_path="",
            text_color="#111111",
        )
        self.panel = _RecordingPanel()
        self.legacy_device_json = self.base / "InkyPi" / "src" / "config" / "device.json"

    def tearDown(self) -> None:
        self.database.close()
        self._tmp.cleanup()

    def _display(self) -> FrameDisplay:
        return FrameDisplay(
            self.panel,
            self.storage,
            self.display_config,
            self.database,
            legacy_inkypi_device_paths=(self.legacy_device_json,),
        )

    def _request(self, image_id: str, color=(200, 40, 40)) -> DisplayRequest:
        rendered = self.base / "rendered" / f"{image_id}.png"
        rendered.parent.mkdir(parents=True, exist_ok=True)
        Image.new("RGB", (1200, 900), color).save(rendered)
        return DisplayRequest(
            image_id=image_id,
            original_path=rendered,
            composed_path=rendered,
            location="Berlin",
            taken_at="2026-03-18",
            caption="Am Meer",
            created_at="2026-03-18T12:00:00+00:00",
            uploaded_by=1,
        )

    def _payload(self) -> dict:
        return json.loads(self.storage.current_payload_path.read_text(encoding="utf-8"))


class DisplayTests(FrameDisplayTestCase):
    def test_success_shows_the_image_and_makes_it_current(self) -> None:
        display = self._display()

        result = display.display(self._request("img-1"))

        self.assertTrue(result.success)
        self.assertEqual(len(self.panel.shown), 1)
        self.assertEqual(self.panel.shown[0].size, (800, 480))
        payload = self._payload()
        self.assertEqual(payload["image_id"], "img-1")
        committed = Path(payload["prepared_image_path"])
        self.assertTrue(committed.is_file())
        self.assertEqual(committed.parent, self.storage.inkypi_payload_dir / "committed")
        self.assertTrue(self.storage.current_image_path.is_file())

    def test_the_current_image_survives_removal_of_the_rendered_file(self) -> None:
        display = self._display()
        request = self._request("img-1")
        display.display(request)
        request.composed_path.unlink()

        result = display.refresh_only()

        self.assertTrue(result.success)
        self.assertEqual(len(self.panel.shown), 2)

    def test_a_panel_failure_leaves_the_current_image_untouched(self) -> None:
        display = self._display()
        display.display(self._request("img-1"))
        self.panel.error = "Das Display hat nach 300 Sekunden nicht reagiert."

        result = display.display(self._request("img-2"))

        self.assertFalse(result.success)
        self.assertEqual(result.message, "Das Display hat nach 300 Sekunden nicht reagiert.")
        self.assertEqual(self._payload()["image_id"], "img-1")

    def test_a_missing_image_file_fails_without_touching_the_panel(self) -> None:
        display = self._display()
        request = self._request("img-1")
        request.composed_path.unlink()

        result = display.display(request)

        self.assertFalse(result.success)
        self.assertIn("Bilddatei fehlt", result.message)
        self.assertEqual(self.panel.shown, [])
        self.assertFalse(self.storage.current_payload_path.exists())

    def test_refresh_without_a_current_image_fails(self) -> None:
        result = self._display().refresh_only()

        self.assertFalse(result.success)
        self.assertEqual(self.panel.shown, [])

    def test_refresh_redraws_a_payload_written_by_the_old_inkypi_version(self) -> None:
        prepared = self.base / "payload" / "committed" / "img-old_abc.png"
        prepared.parent.mkdir(parents=True, exist_ok=True)
        Image.new("RGB", (900, 600), (10, 20, 30)).save(prepared)
        self.storage.current_payload_path.write_text(
            json.dumps(
                {
                    "image_id": "img-old",
                    "plugin_id": "telegram_frame",
                    "orientation_hint": "horizontal",
                    "prepared_image_path": str(prepared),
                    "caption": "Alt",
                    "caption_bar_height": 44,
                }
            ),
            encoding="utf-8",
        )

        result = self._display().refresh_only()

        self.assertTrue(result.success)
        self.assertEqual(len(self.panel.shown), 1)


class PlaceholderTests(FrameDisplayTestCase):
    def test_placeholder_replaces_the_current_image(self) -> None:
        display = self._display()
        display.display(self._request("img-1"))

        result = display.show_placeholder()

        self.assertTrue(result.success)
        self.assertTrue(self._payload()["placeholder"])
        self.assertFalse(self.storage.current_image_path.exists())
        # A later refresh draws the placeholder again.
        self.assertTrue(display.refresh_only().success)

    def test_a_failed_placeholder_keeps_the_current_image(self) -> None:
        display = self._display()
        display.display(self._request("img-1"))
        self.panel.error = "SPI nicht verfügbar"

        result = display.show_placeholder()

        self.assertFalse(result.success)
        self.assertEqual(self._payload()["image_id"], "img-1")
        self.assertTrue(self.storage.current_image_path.exists())


class SettingsTests(FrameDisplayTestCase):
    def test_fresh_install_uses_the_defaults(self) -> None:
        display = self._display()

        settings = display.read_device_settings()

        self.assertEqual(settings["orientation"], "vertical")
        self.assertTrue(settings["inverted_image"])
        self.assertEqual(
            settings["image_settings"],
            {"saturation": 1.4, "contrast": 1.4, "sharpness": 1.2, "brightness": 1.1},
        )
        self.assertEqual(display.get_slideshow_interval(), 86400)
        self.assertIsNone(display.get_sleep_schedule())

    def test_image_tuning_merges_and_redraws_the_current_image(self) -> None:
        display = self._display()
        display.display(self._request("img-1"))

        result = display.apply_device_settings({"image_settings": {"saturation": 2.0}}, refresh_current=True)

        self.assertTrue(result.success)
        self.assertEqual(result.message, "Die Anzeige wurde aktualisiert.")
        self.assertEqual(result.confirmed_settings["image_settings"]["saturation"], 2.0)
        self.assertEqual(result.confirmed_settings["image_settings"]["contrast"], 1.4)
        self.assertEqual(len(self.panel.shown), 2)

    def test_a_failed_redraw_still_saves_the_setting(self) -> None:
        display = self._display()
        display.display(self._request("img-1"))
        self.panel.error = "SPI nicht verfügbar"

        result = display.apply_device_settings({"image_settings": {"contrast": 1.0}}, refresh_current=True)

        self.assertFalse(result.success)
        self.assertIn("SPI nicht verfügbar", result.message)
        self.assertEqual(display.read_device_settings()["image_settings"]["contrast"], 1.0)

    def test_orientation_change_without_refresh_does_not_touch_the_panel(self) -> None:
        display = self._display()

        result = display.apply_device_settings(
            {"orientation": "horizontal", "inverted_image": False},
            refresh_current=False,
        )

        self.assertTrue(result.success)
        self.assertEqual(result.message, "")
        self.assertEqual(display.current_orientation(), "horizontal")
        self.assertFalse(display.read_device_settings()["inverted_image"])
        self.assertEqual(self.panel.shown, [])

    def test_invalid_orientation_is_rejected(self) -> None:
        result = self._display().apply_device_settings({"orientation": "diagonal"}, refresh_current=False)

        self.assertFalse(result.success)

    def test_interval_and_quiet_hours_round_trip(self) -> None:
        display = self._display()

        display.set_slideshow_interval(3600)
        display.set_sleep_schedule("22:00", "07:00")
        self.assertEqual(display.get_slideshow_interval(), 3600)
        self.assertEqual(display.get_sleep_schedule(), ("22:00", "07:00"))

        display.set_sleep_schedule(None, None)
        self.assertIsNone(display.get_sleep_schedule())
        self.assertEqual(self.panel.shown, [])


class LegacyImportTests(FrameDisplayTestCase):
    def _write_device_json(self, data: dict) -> None:
        self.legacy_device_json.parent.mkdir(parents=True, exist_ok=True)
        self.legacy_device_json.write_text(json.dumps(data), encoding="utf-8")

    def test_settings_are_imported_once_from_inkypi(self) -> None:
        self._write_device_json(
            {
                "orientation": "horizontal",
                "inverted_image": False,
                "image_settings": {"saturation": 1.8, "contrast": 1.2},
                "playlist_config": {
                    "playlists": [
                        {
                            "name": "Default",
                            "start_time": "07:00",
                            "end_time": "22:00",
                            "plugins": [
                                {"plugin_id": "telegram_frame", "refresh": {"interval": 3600}},
                            ],
                        }
                    ]
                },
            }
        )

        display = self._display()
        settings = display.read_device_settings()

        self.assertEqual(settings["orientation"], "horizontal")
        self.assertFalse(settings["inverted_image"])
        self.assertEqual(settings["image_settings"]["saturation"], 1.8)
        self.assertEqual(settings["image_settings"]["contrast"], 1.2)
        self.assertEqual(settings["image_settings"]["sharpness"], 1.2)  # default for missing keys
        self.assertEqual(display.get_slideshow_interval(), 3600)
        self.assertEqual(display.get_sleep_schedule(), ("22:00", "07:00"))

        # Later changes to device.json are ignored; the import only happens once.
        self._write_device_json({"orientation": "vertical", "inverted_image": True})
        self.assertEqual(self._display().current_orientation(), "horizontal")

    def test_without_device_json_the_old_database_cache_is_used(self) -> None:
        self.database.set_settings(
            {
                "runtime_cache_orientation": "horizontal",
                "runtime_cache_slideshow_interval": "7200",
                "runtime_cache_sleep_schedule": "23:00|06:00",
            }
        )

        display = self._display()

        self.assertEqual(display.current_orientation(), "horizontal")
        self.assertFalse(display.read_device_settings()["inverted_image"])
        self.assertEqual(display.get_slideshow_interval(), 7200)
        self.assertEqual(display.get_sleep_schedule(), ("23:00", "06:00"))

    def test_settings_that_already_exist_are_not_overwritten(self) -> None:
        self.database.set_setting("display_slideshow_interval", "1800")
        self._write_device_json({"orientation": "horizontal"})

        display = self._display()

        self.assertEqual(display.get_slideshow_interval(), 1800)
        self.assertEqual(display.current_orientation(), "horizontal")

    def test_unreadable_device_json_falls_back_to_defaults(self) -> None:
        self.legacy_device_json.parent.mkdir(parents=True, exist_ok=True)
        self.legacy_device_json.write_text("{not json", encoding="utf-8")

        self.assertEqual(self._display().current_orientation(), "vertical")


if __name__ == "__main__":
    unittest.main()
