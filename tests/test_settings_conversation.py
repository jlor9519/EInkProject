from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from telegram.ext import ConversationHandler

from app.database import Database
from app.models import DeviceSettingsApplyResult, DisplayResult, ImageRecord
from app.settings_conversation import (
    PENDING_SETTINGS_KEY,
    WAITING_FOR_SETTINGS_CHOICE,
    WAITING_FOR_SETTINGS_VALUE,
    receive_settings_value,
    settings_entry,
)


class SettingsConversationTests(unittest.IsolatedAsyncioTestCase):
    async def test_settings_entry_rejects_non_admin(self) -> None:
        services = _FakeServices(is_admin=False)
        update = _FakeUpdate("/settings", user_id=11)
        context = _FakeContext(services)

        result = await settings_entry(update, context)

        self.assertIsNone(result)
        self.assertEqual(update.effective_message.replies, ["Dieser Befehl ist nur für Admins verfügbar."])

    async def test_settings_entry_shows_image_tuning_and_orientation_options(self) -> None:
        services = _FakeServices(is_admin=True)
        update = _FakeUpdate("/settings", user_id=11)
        context = _FakeContext(services)

        result = await settings_entry(update, context)

        self.assertEqual(result, WAITING_FOR_SETTINGS_CHOICE)
        self.assertEqual(len(update.effective_message.replies), 1)
        reply = update.effective_message.replies[0]
        self.assertIn("1. Sättigung", reply)
        self.assertIn("2. Kontrast", reply)
        self.assertIn("3. Schärfe", reply)
        self.assertIn("4. Helligkeit", reply)
        self.assertIn("5. Ausrichtung: Hochformat", reply)
        self.assertIn("6. Bildanpassung", reply)

    async def test_receive_settings_value_applies_and_confirms_value(self) -> None:
        services = _FakeServices(is_admin=True)
        services.display.apply_result = DeviceSettingsApplyResult(
            success=True,
            message="Einstellungen wurden gespeichert, InkyPi wurde neu geladen und die Anzeige aktualisiert.",
            confirmed_settings={"image_settings": {"saturation": 1.8}},
            device_config_path=services.display.device_config_path,
            saved=True,
            reloaded=True,
            refreshed=True,
        )
        update = _FakeUpdate("1.8", user_id=11)
        context = _FakeContext(services)
        context.user_data[PENDING_SETTINGS_KEY] = 0

        result = await receive_settings_value(update, context)

        self.assertEqual(result, ConversationHandler.END)
        self.assertEqual(
            services.display.last_updates,
            {"image_settings": {"saturation": 1.8}},
        )
        self.assertEqual(services.display.last_refresh_current, True)
        self.assertEqual(len(update.effective_message.replies), 1)
        reply = update.effective_message.replies[0]
        self.assertIn("Sättigung ist jetzt 1.8", reply)
        self.assertIn("Anzeige aktualisiert", reply)

    async def test_receive_settings_value_rejects_invalid_float(self) -> None:
        services = _FakeServices(is_admin=True)
        update = _FakeUpdate("abc", user_id=11)
        context = _FakeContext(services)
        context.user_data[PENDING_SETTINGS_KEY] = 0

        result = await receive_settings_value(update, context)

        self.assertEqual(result, WAITING_FOR_SETTINGS_VALUE)
        self.assertEqual(context.user_data[PENDING_SETTINGS_KEY], 0)
        self.assertEqual(len(update.effective_message.replies), 1)
        self.assertIn("Ungültiger Wert", update.effective_message.replies[0])
        self.assertIsNone(services.display.last_updates)

    async def test_receive_settings_value_applies_orientation_and_inverted_image(self) -> None:
        services = _FakeServices(is_admin=True)
        services.display.apply_result = DeviceSettingsApplyResult(
            success=True,
            message="Einstellungen wurden gespeichert und InkyPi wurde neu geladen.",
            confirmed_settings={
                "orientation": "vertical",
                "inverted_image": True,
                "image_settings": services.display.settings["image_settings"],
            },
            device_config_path=services.display.device_config_path,
            saved=True,
            reloaded=True,
            refreshed=True,
        )
        update = _FakeUpdate("vertical", user_id=11)
        context = _FakeContext(services)
        context.user_data[PENDING_SETTINGS_KEY] = 4

        with patch("app.settings_conversation._switch_orientation_library", return_value=(False, "Es gibt noch keine Bilder für Hochformat.")):
            result = await receive_settings_value(update, context)

        self.assertEqual(result, ConversationHandler.END)
        self.assertEqual(
            services.display.last_updates,
            {"orientation": "vertical", "inverted_image": True},
        )
        self.assertEqual(services.display.last_refresh_current, False)
        reply = update.effective_message.replies[0]
        self.assertIn("Ausrichtung ist jetzt Hochformat", reply)

    async def test_receive_settings_value_accepts_querformat_alias(self) -> None:
        services = _FakeServices(is_admin=True)
        services.display.apply_result = DeviceSettingsApplyResult(
            success=True,
            message="ok",
            confirmed_settings={"orientation": "horizontal"},
            device_config_path=services.display.device_config_path,
            saved=True,
            reloaded=True,
            refreshed=False,
        )
        update = _FakeUpdate("querformat", user_id=11)
        context = _FakeContext(services)
        context.user_data[PENDING_SETTINGS_KEY] = 4

        with patch("app.settings_conversation._switch_orientation_library", return_value=(True, "Zeige jetzt Querformat-Bild img-2.")):
            result = await receive_settings_value(update, context)

        self.assertEqual(result, ConversationHandler.END)
        self.assertEqual(
            services.display.last_updates,
            {"orientation": "horizontal", "inverted_image": False},
        )
        self.assertIn("Ausrichtung ist jetzt Querformat", update.effective_message.replies[0])


class OrientationSwitchTests(unittest.IsolatedAsyncioTestCase):
    async def test_orientation_switch_promotes_rendered_image_from_matching_library(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _RealishServices(tmpdir_path)
            original = tmpdir_path / "incoming" / "img-rendered.jpg"
            original.parent.mkdir(parents=True, exist_ok=True)
            original.write_bytes(b"original")
            services.database.upsert_image(
                ImageRecord(
                    image_id="img-rendered",
                    telegram_file_id="file-rendered",
                    telegram_chat_id=11,
                    local_original_path=str(original),
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="Rendered",
                    uploaded_by=11,
                    created_at="2026-03-18T12:20:00+00:00",
                    status="rendered",
                    last_error=None,
                    orientation_bucket="horizontal",
                )
            )
            update = _FakeUpdate("querformat", user_id=11)
            context = _RealishContext(services)
            context.user_data[PENDING_SETTINGS_KEY] = 4

            result = await receive_settings_value(update, context)

            self.assertEqual(result, ConversationHandler.END)
            self.assertEqual(services.display.display_calls, ["img-rendered"])
            self.assertEqual(services.database.get_image_by_id("img-rendered").status, "displayed")
            self.assertIsNotNone(services.database.get_setting("last_new_image_displayed_at"))
            self.assertIn("Zeige jetzt Querformat-Bild img-rendered.", update.effective_message.replies[0])

    async def test_orientation_switch_with_no_matching_images_keeps_current_display(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _RealishServices(tmpdir_path)
            update = _FakeUpdate("querformat", user_id=11)
            context = _RealishContext(services)
            context.user_data[PENDING_SETTINGS_KEY] = 4

            result = await receive_settings_value(update, context)

            self.assertEqual(result, ConversationHandler.END)
            self.assertEqual(services.display.display_calls, [])
            self.assertIn("Es gibt noch keine Bilder für Querformat.", update.effective_message.replies[0])


class _FakeAuth:
    def __init__(self, *, is_admin: bool):
        self._is_admin = is_admin
        self.synced_user_ids: list[int] = []

    def sync_user(self, user) -> None:
        self.synced_user_ids.append(user.id)

    def is_admin(self, user_id: int) -> bool:
        return self._is_admin


class _FakeDisplay:
    def __init__(self) -> None:
        self.device_config_path = "/tmp/device.json"
        self.settings = {
            "orientation": "vertical",
            "inverted_image": True,
            "image_settings": {
                "saturation": 1.4,
                "contrast": 1.4,
                "sharpness": 1.2,
                "brightness": 1.1,
            }
        }
        self.apply_result = DeviceSettingsApplyResult(
            success=True,
            message="ok",
            confirmed_settings=self.settings,
            device_config_path=self.device_config_path,
            saved=True,
            reloaded=True,
            refreshed=True,
        )
        self.last_updates: dict[str, object] | None = None
        self.last_refresh_current: bool | None = None

    def read_device_settings(self) -> dict[str, object]:
        return self.settings

    def get_slideshow_interval(self) -> int:
        return 86400

    def get_sleep_schedule(self) -> tuple[str, str] | None:
        return None

    def apply_device_settings(
        self,
        updates: dict[str, object],
        *,
        refresh_current: bool = True,
    ) -> DeviceSettingsApplyResult:
        self.last_updates = updates
        self.last_refresh_current = refresh_current
        return self.apply_result


class _FakeDatabase:
    def __init__(self) -> None:
        self._settings: dict[str, str] = {}

    def get_setting(self, key: str) -> str | None:
        return self._settings.get(key)

    def set_setting(self, key: str, value: str) -> None:
        self._settings[key] = value


class _FakeServices:
    def __init__(self, *, is_admin: bool):
        self.auth = _FakeAuth(is_admin=is_admin)
        self.display = _FakeDisplay()
        self.database = _FakeDatabase()
        self.config = _FakeConfig()


class _FakeConfig:
    def __init__(self) -> None:
        self.inkypi = SimpleNamespace(update_method="http_update_now")


class _FakeMessage:
    def __init__(self, text: str):
        self.text = text
        self.replies: list[str] = []

    async def reply_text(self, text: str) -> None:
        self.replies.append(text)


class _FakeUser:
    def __init__(self, user_id: int):
        self.id = user_id


class _FakeUpdate:
    def __init__(self, text: str, *, user_id: int):
        self.effective_user = _FakeUser(user_id)
        self.effective_message = _FakeMessage(text)


class _FakeContext:
    def __init__(self, services: _FakeServices):
        self.application = SimpleNamespace(bot_data={"services": services, "display_lock": asyncio.Lock()})
        self.user_data: dict[str, object] = {}


class _RealishDisplay:
    def __init__(self) -> None:
        self.device_config_path = "/tmp/device.json"
        self.orientation = "vertical"
        self.display_calls: list[str] = []

    def read_device_settings(self) -> dict[str, object]:
        return {"orientation": self.orientation, "image_settings": {}}

    def get_slideshow_interval(self) -> int:
        return 86400

    def get_sleep_schedule(self) -> tuple[str, str] | None:
        return None

    def apply_device_settings(
        self,
        updates: dict[str, object],
        *,
        refresh_current: bool = True,
    ) -> DeviceSettingsApplyResult:
        orientation = str(updates.get("orientation", self.orientation))
        self.orientation = orientation
        return DeviceSettingsApplyResult(
            success=True,
            message="Einstellungen wurden gespeichert und InkyPi wurde neu geladen.",
            confirmed_settings={"orientation": orientation},
            device_config_path=self.device_config_path,
            saved=True,
            reloaded=True,
            refreshed=False,
            refresh_skipped=True,
        )

    def display(self, request) -> DisplayResult:
        self.display_calls.append(request.image_id)
        return DisplayResult(True, f"displayed {request.image_id}")


class _RealishRenderer:
    def render(self, original_path: Path, output_path: Path, **_: object) -> Path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(Path(original_path).read_bytes())
        return output_path


class _RealishStorage:
    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir

    def rendered_path(self, image_id: str, extension: str = ".png") -> Path:
        return self.base_dir / "rendered" / f"{image_id}{extension}"

    def cleanup_rendered_cache(self) -> None:
        return None


class _RealishServices:
    def __init__(self, base_dir: Path) -> None:
        self.auth = _FakeAuth(is_admin=True)
        self.display = _RealishDisplay()
        self.database = Database(base_dir / "settings.db")
        self.database.initialize()
        self.storage = _RealishStorage(base_dir)
        self.renderer = _RealishRenderer()
        self.config = _FakeConfig()


class _RealishContext:
    def __init__(self, services: _RealishServices):
        self.application = SimpleNamespace(
            bot_data={"services": services, "display_lock": asyncio.Lock()},
            job_queue=None,
        )
        self.user_data: dict[str, object] = {}


if __name__ == "__main__":
    unittest.main()
