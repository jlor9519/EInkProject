from __future__ import annotations

import asyncio
import tempfile
import threading
import unittest
import warnings
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from telegram import CallbackQuery, Chat, Message, PhotoSize, Update, User
from telegram.ext import ConversationHandler

from app.conversations import (
    PENDING_SUBMISSION_KEY,
    PHOTO_CONVERSATION_KEY,
    WAITING_FOR_TEXT_CHOICE,
    build_photo_conversation,
)
from app.database import Database
from app.models import DeviceSettingsApplyResult, DisplayResult, ImageRecord
from app.settings_conversation import (
    PENDING_IMAGE_TUNING_FIELD_KEY,
    PENDING_SETTINGS_KEY,
    WAITING_FOR_SETTINGS_CHOICE,
    WAITING_FOR_SETTINGS_VALUE,
    _SETTINGS_BUSY_TEXT,
    build_settings_conversation,
    receive_settings_value,
    settings_callback,
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
        self.assertEqual(reply, "Einstellungen")
        markup = update.effective_message.reply_markups[0]
        labels = [button.text for row in markup.inline_keyboard for button in row]
        self.assertIn("Bildoptimierung", labels)
        self.assertIn("Ausrichtung: Hochformat", labels)
        self.assertIn("Bildanpassung: Zuschneiden", labels)
        self.assertIn("Täglicher Wechsel: Deaktiviert", labels)
        self.assertIn("Bilder in Rotation: 100", labels)
        self.assertIn("Abbrechen", labels)
        self.assertEqual(
            [[button.text for button in row] for row in markup.inline_keyboard],
            [
                ["Bildoptimierung", "Ausrichtung: Hochformat"],
                ["Bildanpassung: Zuschneiden", "Anzeigedauer: 1 Tag"],
                ["Ruhezeit: Keine", "Neue Bilder: 1 Stunde"],
                ["Täglicher Wechsel: Deaktiviert", "Bilder in Rotation: 100"],
                ["Abbrechen"],
            ],
        )

    async def test_settings_callback_opens_prompt_from_button(self) -> None:
        services = _FakeServices(is_admin=True)
        update = _FakeCallbackUpdate("settings|select|1", user_id=11)
        context = _FakeContext(services)

        result = await settings_callback(update, context)

        self.assertEqual(result, WAITING_FOR_SETTINGS_VALUE)
        self.assertEqual(context.user_data[PENDING_SETTINGS_KEY], 1)
        self.assertIn("Wähle den neuen Wert per Button.", update.callback_query.text_edits[0])
        labels = [button.text for row in update.callback_query.text_edit_markups[0].inline_keyboard for button in row]
        self.assertIn("Hochformat", labels)
        self.assertIn("Querformat", labels)
        self.assertIn("Zurück", labels)
        self.assertIn("Abbrechen", labels)

    async def test_settings_callback_opens_image_tuning_submenu(self) -> None:
        services = _FakeServices(is_admin=True)
        update = _FakeCallbackUpdate("settings|select|0", user_id=11)
        context = _FakeContext(services)

        result = await settings_callback(update, context)

        self.assertEqual(result, WAITING_FOR_SETTINGS_CHOICE)
        self.assertEqual(update.callback_query.text_edits[0], "Bildoptimierung")
        labels = [button.text for row in update.callback_query.text_edit_markups[0].inline_keyboard for button in row]
        self.assertIn("Sättigung: 1.4", labels)
        self.assertIn("Kontrast: 1.4", labels)
        self.assertIn("Schärfe: 1.2", labels)
        self.assertIn("Helligkeit: 1.1", labels)
        self.assertIn("Speichern", labels)
        self.assertIn("Zurück", labels)
        self.assertIn("Abbrechen", labels)
        self.assertEqual(
            [[button.text for button in row] for row in update.callback_query.text_edit_markups[0].inline_keyboard],
            [
                ["Sättigung: 1.4"],
                ["Kontrast: 1.4"],
                ["Schärfe: 1.2"],
                ["Helligkeit: 1.1"],
                ["Speichern"],
                ["Zurück", "Abbrechen"],
            ],
        )

    async def test_settings_callback_back_returns_to_menu(self) -> None:
        services = _FakeServices(is_admin=True)
        update = _FakeCallbackUpdate("settings|back", user_id=11)
        context = _FakeContext(services)
        context.user_data[PENDING_SETTINGS_KEY] = 1

        result = await settings_callback(update, context)

        self.assertEqual(result, WAITING_FOR_SETTINGS_CHOICE)
        self.assertNotIn(PENDING_SETTINGS_KEY, context.user_data)
        self.assertEqual(update.callback_query.text_edits[0], "Einstellungen")

    async def test_settings_callback_close_ends_conversation(self) -> None:
        services = _FakeServices(is_admin=True)
        update = _FakeCallbackUpdate("settings|close", user_id=11)
        context = _FakeContext(services)
        context.user_data[PENDING_SETTINGS_KEY] = 1

        result = await settings_callback(update, context)

        self.assertEqual(result, ConversationHandler.END)
        self.assertNotIn(PENDING_SETTINGS_KEY, context.user_data)
        self.assertEqual(context.bot.deleted_messages, [(111, 222)])
        self.assertEqual(update.callback_query.text_edits, [])

    async def test_scheduled_time_setting_can_be_set_and_cleared(self) -> None:
        services = _FakeServices(is_admin=True)
        context = _FakeContext(services)

        # Set a scheduled time
        update = _FakeUpdate("08:00", user_id=11)
        context.user_data[PENDING_SETTINGS_KEY] = 6

        result = await receive_settings_value(update, context)

        self.assertEqual(result, WAITING_FOR_SETTINGS_CHOICE)
        self.assertEqual(services.database.get_setting("scheduled_change_time"), "08:00")
        self.assertIn("08:00", update.effective_message.replies[0])

    async def test_scheduled_time_setting_cleared_with_keine(self) -> None:
        services = _FakeServices(is_admin=True)
        services.database.set_setting("scheduled_change_time", "08:00")
        context = _FakeContext(services)

        update = _FakeUpdate("keine", user_id=11)
        context.user_data[PENDING_SETTINGS_KEY] = 6

        result = await receive_settings_value(update, context)

        self.assertEqual(result, WAITING_FOR_SETTINGS_CHOICE)
        self.assertEqual(services.database.get_setting("scheduled_change_time"), "")
        self.assertIn("deaktiviert", update.effective_message.replies[0].lower())

    async def test_scheduled_time_setting_rejects_invalid_time(self) -> None:
        services = _FakeServices(is_admin=True)
        context = _FakeContext(services)

        update = _FakeUpdate("25:00", user_id=11)
        context.user_data[PENDING_SETTINGS_KEY] = 6

        result = await receive_settings_value(update, context)

        self.assertEqual(result, WAITING_FOR_SETTINGS_VALUE)
        self.assertIn("Ungültige Uhrzeit", update.effective_message.replies[0])
        self.assertIsNone(services.database.get_setting("scheduled_change_time"))

    async def test_settings_entry_shows_scheduled_time_when_active(self) -> None:
        services = _FakeServices(is_admin=True)
        services.database.set_setting("scheduled_change_time", "08:30")
        update = _FakeUpdate("/settings", user_id=11)
        context = _FakeContext(services)

        result = await settings_entry(update, context)

        self.assertEqual(result, WAITING_FOR_SETTINGS_CHOICE)
        reply = update.effective_message.replies[0]
        self.assertEqual(reply, "Einstellungen")
        labels = [button.text for row in update.effective_message.reply_markups[0].inline_keyboard for button in row]
        self.assertIn("Täglicher Wechsel: 08:30", labels)

    async def test_settings_entry_shows_unlimited_rotation_label(self) -> None:
        services = _FakeServices(is_admin=True)
        services.database.set_setting("rotation_limit", "0")
        update = _FakeUpdate("/settings", user_id=11)
        context = _FakeContext(services)

        result = await settings_entry(update, context)

        self.assertEqual(result, WAITING_FOR_SETTINGS_CHOICE)
        labels = [button.text for row in update.effective_message.reply_markups[0].inline_keyboard for button in row]
        self.assertIn("Bilder in Rotation: Unbegrenzt", labels)

    async def test_interval_change_resets_timer_from_now(self) -> None:
        services = _FakeServices(is_admin=True)
        update = _FakeUpdate("2h", user_id=11)
        context = _FakeContext(services, with_job_queue=True)
        context.user_data[PENDING_SETTINGS_KEY] = 3

        result = await receive_settings_value(update, context)

        self.assertEqual(result, WAITING_FOR_SETTINGS_CHOICE)
        self.assertIn("Timer wurde ab jetzt", update.effective_message.replies[0])
        self.assertEqual(context.application.job_queue.calls[-1]["interval"], 7200)
        self.assertEqual(context.application.job_queue.calls[-1]["first"], 7200)

    async def test_interval_change_mentions_scheduled_override_when_active(self) -> None:
        services = _FakeServices(is_admin=True)
        services.database.set_setting("scheduled_change_time", "09:00")
        update = _FakeUpdate("2h", user_id=11)
        context = _FakeContext(services, with_job_queue=True)
        context.user_data[PENDING_SETTINGS_KEY] = 3

        result = await receive_settings_value(update, context)

        self.assertEqual(result, WAITING_FOR_SETTINGS_CHOICE)
        self.assertIn("überschreibt diese Anzeigedauer", update.effective_message.replies[0])

    async def test_image_tuning_field_edit_updates_draft_only(self) -> None:
        services = _FakeServices(is_admin=True)
        update = _FakeUpdate("1.8", user_id=11)
        context = _FakeContext(services)
        context.user_data[PENDING_IMAGE_TUNING_FIELD_KEY] = "saturation"
        context.user_data["image_tuning_draft"] = {
            "saturation": 1.4,
            "contrast": 1.4,
            "sharpness": 1.2,
            "brightness": 1.1,
        }
        context.user_data["image_tuning_base"] = dict(context.user_data["image_tuning_draft"])

        result = await receive_settings_value(update, context)

        self.assertEqual(result, WAITING_FOR_SETTINGS_CHOICE)
        self.assertIsNone(services.display.last_updates)
        self.assertEqual(context.user_data["image_tuning_draft"]["saturation"], 1.8)
        self.assertNotIn(PENDING_IMAGE_TUNING_FIELD_KEY, context.user_data)
        reply = update.effective_message.replies[0]
        self.assertIn("Sättigung ist jetzt 1.8", reply)
        self.assertIn("Bildoptimierung", reply)

    async def test_image_tuning_field_rejects_invalid_float(self) -> None:
        services = _FakeServices(is_admin=True)
        update = _FakeUpdate("abc", user_id=11)
        context = _FakeContext(services)
        context.user_data[PENDING_IMAGE_TUNING_FIELD_KEY] = "saturation"
        context.user_data["image_tuning_draft"] = {
            "saturation": 1.4,
            "contrast": 1.4,
            "sharpness": 1.2,
            "brightness": 1.1,
        }
        context.user_data["image_tuning_base"] = dict(context.user_data["image_tuning_draft"])

        result = await receive_settings_value(update, context)

        self.assertEqual(result, WAITING_FOR_SETTINGS_VALUE)
        self.assertEqual(context.user_data[PENDING_IMAGE_TUNING_FIELD_KEY], "saturation")
        self.assertEqual(len(update.effective_message.replies), 1)
        self.assertIn("Ungültiger Wert", update.effective_message.replies[0])
        self.assertNotIn("/cancel", update.effective_message.replies[0])
        self.assertIsNone(services.display.last_updates)

    async def test_image_tuning_save_applies_multiple_values_in_one_call(self) -> None:
        services = _FakeServices(is_admin=True)
        services.display.apply_result = DeviceSettingsApplyResult(
            success=True,
            message="Einstellungen wurden gespeichert, InkyPi wurde neu geladen und die Anzeige aktualisiert.",
            confirmed_settings={
                "image_settings": {
                    "saturation": 1.8,
                    "contrast": 1.6,
                    "sharpness": 1.2,
                    "brightness": 1.1,
                }
            },
            device_config_path=services.display.device_config_path,
        )
        update = _FakeCallbackUpdate("settings|tuning_save", user_id=11)
        context = _FakeContext(services)
        context.user_data["image_tuning_draft"] = {
            "saturation": 1.8,
            "contrast": 1.6,
            "sharpness": 1.2,
            "brightness": 1.1,
        }
        context.user_data["image_tuning_base"] = {
            "saturation": 1.4,
            "contrast": 1.4,
            "sharpness": 1.2,
            "brightness": 1.1,
        }

        result = await settings_callback(update, context)

        self.assertEqual(result, WAITING_FOR_SETTINGS_CHOICE)
        self.assertEqual(
            services.display.last_updates,
            {"image_settings": {"saturation": 1.8, "contrast": 1.6}},
        )
        self.assertEqual(services.display.last_refresh_current, True)
        self.assertNotIn("image_tuning_draft", context.user_data)
        self.assertIn("Bildoptimierung wurde gespeichert", update.callback_query.text_edits[0])
        self.assertIn("Einstellungen", update.callback_query.text_edits[0])

    async def test_image_tuning_back_discards_draft(self) -> None:
        services = _FakeServices(is_admin=True)
        update = _FakeCallbackUpdate("settings|tuning_back", user_id=11)
        context = _FakeContext(services)
        context.user_data["image_tuning_draft"] = {
            "saturation": 1.8,
            "contrast": 1.4,
            "sharpness": 1.2,
            "brightness": 1.1,
        }
        context.user_data["image_tuning_base"] = {
            "saturation": 1.4,
            "contrast": 1.4,
            "sharpness": 1.2,
            "brightness": 1.1,
        }

        result = await settings_callback(update, context)

        self.assertEqual(result, WAITING_FOR_SETTINGS_CHOICE)
        self.assertNotIn("image_tuning_draft", context.user_data)
        self.assertEqual(update.callback_query.text_edits[0], "Einstellungen")

    async def test_rotation_limit_setting_can_be_saved(self) -> None:
        services = _FakeServices(is_admin=True)
        update = _FakeUpdate("250", user_id=11)
        context = _FakeContext(services)
        context.user_data[PENDING_SETTINGS_KEY] = 7

        result = await receive_settings_value(update, context)

        self.assertEqual(result, WAITING_FOR_SETTINGS_CHOICE)
        self.assertEqual(services.database.get_setting("rotation_limit"), "250")
        self.assertIn("Bilder in Rotation ist jetzt 250", update.effective_message.replies[0])

    async def test_rotation_limit_can_be_set_to_unlimited(self) -> None:
        services = _FakeServices(is_admin=True)
        update = _FakeCallbackUpdate("settings|apply|7|unlimited", user_id=11)
        context = _FakeContext(services)

        result = await settings_callback(update, context)

        self.assertEqual(result, WAITING_FOR_SETTINGS_CHOICE)
        self.assertEqual(services.database.get_setting("rotation_limit"), "0")
        self.assertIn("Unbegrenzt", update.callback_query.text_edits[0])

    async def test_rotation_limit_setting_rejects_invalid_values(self) -> None:
        services = _FakeServices(is_admin=True)
        update = _FakeUpdate("1001", user_id=11)
        context = _FakeContext(services)
        context.user_data[PENDING_SETTINGS_KEY] = 7

        result = await receive_settings_value(update, context)

        self.assertEqual(result, WAITING_FOR_SETTINGS_VALUE)
        self.assertEqual(context.user_data[PENDING_SETTINGS_KEY], 7)
        self.assertIn("zwischen 1 und 1000", update.effective_message.replies[0])
        self.assertNotIn("/cancel", update.effective_message.replies[0])


    async def test_device_changes_run_off_event_loop_under_display_lock(self) -> None:
        for idx, text, expected_call in (
            (3, "2h", "set_slideshow_interval"),
            (4, "22:00-07:00", "set_sleep_schedule"),
            (4, "keine", "set_sleep_schedule"),
            (1, "horizontal", "apply_device_settings"),
        ):
            with self.subTest(text=text):
                services = _FakeServices(is_admin=True)
                context = _FakeContext(services, with_job_queue=True)
                services.display.lock = context.application.bot_data["display_lock"]
                context.user_data[PENDING_SETTINGS_KEY] = idx

                with patch("app.settings_conversation._switch_orientation_library", return_value=(True, "ok")):
                    await receive_settings_value(_FakeUpdate(text, user_id=11), context)

                self.assertEqual(services.display.change_calls, [(expected_call, True, True)])
                self.assertFalse(context.application.bot_data["display_lock"].locked())

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
        )
        update = _FakeUpdate("vertical", user_id=11)
        context = _FakeContext(services)
        context.user_data[PENDING_SETTINGS_KEY] = 1

        with patch("app.settings_conversation._switch_orientation_library", return_value=(False, "Es gibt noch keine Bilder für Hochformat.")):
            result = await receive_settings_value(update, context)

        self.assertEqual(result, WAITING_FOR_SETTINGS_CHOICE)
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
        )
        update = _FakeUpdate("querformat", user_id=11)
        context = _FakeContext(services)
        context.user_data[PENDING_SETTINGS_KEY] = 1

        with patch("app.settings_conversation._switch_orientation_library", return_value=(True, "Zeige jetzt Querformat-Bild img-2.")):
            result = await receive_settings_value(update, context)

        self.assertEqual(result, WAITING_FOR_SETTINGS_CHOICE)
        self.assertEqual(
            services.display.last_updates,
            {"orientation": "horizontal", "inverted_image": False},
        )
        self.assertIn("Ausrichtung ist jetzt Querformat", update.effective_message.replies[0])

    async def test_settings_callback_applies_orientation_choice_and_returns_to_menu(self) -> None:
        services = _FakeServices(is_admin=True)
        services.display.apply_result = DeviceSettingsApplyResult(
            success=True,
            message="ok",
            confirmed_settings={"orientation": "horizontal"},
            device_config_path=services.display.device_config_path,
        )
        update = _FakeCallbackUpdate("settings|apply|1|horizontal", user_id=11)
        context = _FakeContext(services)

        with patch("app.settings_conversation._switch_orientation_library", return_value=(True, "Zeige jetzt Querformat-Bild img-2.")):
            result = await settings_callback(update, context)

        self.assertEqual(result, WAITING_FOR_SETTINGS_CHOICE)
        self.assertEqual(services.display.last_updates, {"orientation": "horizontal", "inverted_image": False})
        self.assertIn("Ausrichtung ist jetzt Querformat", update.callback_query.text_edits[0])
        self.assertIn("\n\nEinstellungen", update.callback_query.text_edits[0])
        labels = [button.text for row in update.callback_query.text_edit_markups[0].inline_keyboard for button in row]
        self.assertIn("Abbrechen", labels)


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
            context.user_data[PENDING_SETTINGS_KEY] = 1

            result = await receive_settings_value(update, context)

            self.assertEqual(result, WAITING_FOR_SETTINGS_CHOICE)
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
            context.user_data[PENDING_SETTINGS_KEY] = 1

            result = await receive_settings_value(update, context)

            self.assertEqual(result, WAITING_FOR_SETTINGS_CHOICE)
            self.assertEqual(services.display.display_calls, [])
            self.assertIn("Es gibt noch keine Bilder für Querformat.", update.effective_message.replies[0])


    async def test_orientation_setting_switches_library_without_deadlock(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _RealishServices(tmpdir_path)
            original = tmpdir_path / "incoming" / "img-h.jpg"
            original.parent.mkdir(parents=True, exist_ok=True)
            original.write_bytes(b"original")
            services.database.upsert_image(
                ImageRecord(
                    image_id="img-h",
                    telegram_file_id="file-h",
                    telegram_chat_id=11,
                    local_original_path=str(original),
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="",
                    uploaded_by=11,
                    created_at="2026-03-18T12:20:00+00:00",
                    status="displayed",
                    last_error=None,
                    orientation_bucket="horizontal",
                )
            )
            context = _RealishContext(services)
            context.user_data[PENDING_SETTINGS_KEY] = 1

            await asyncio.wait_for(
                receive_settings_value(_FakeUpdate("horizontal", user_id=11), context),
                timeout=5,
            )

            self.assertEqual(services.display.display_calls, ["img-h"])

class SettingsAndUploadRoutingTests(unittest.IsolatedAsyncioTestCase):
    """Drive the real ConversationHandlers the way bot.py registers them (settings first)."""

    def setUp(self) -> None:
        self.services = _FakeServices(is_admin=True)
        self.services.auth.is_whitelisted = lambda user_id: True
        self.release_apply = threading.Event()
        self.services.display.set_slideshow_interval = self._slow_set_interval
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")  # PTB's per_message hint, same as for the real app
            self.settings = build_settings_conversation()
            self.photo = build_photo_conversation()
        self.application = SimpleNamespace(
            bot_data={
                "services": self.services,
                "display_lock": asyncio.Lock(),
                "upload_queue": asyncio.Queue(),
                PHOTO_CONVERSATION_KEY: self.photo,
            },
            bot=None,
            job_queue=None,
            create_task=lambda coroutine, update=None, name=None: asyncio.create_task(coroutine),
        )
        self.context = SimpleNamespace(application=self.application, user_data={}, bot=None)
        self.user = User(11, "Admin", False)
        self.chat = Chat(11, "private")
        self.key = (11, 11)
        self.replies: list[str] = []
        self.answers: list[str | None] = []
        patches = [
            patch.object(Message, "reply_text", new=self._record_reply),
            patch.object(CallbackQuery, "answer", new=self._record_answer),
            patch("app.conversations._download_image", new=self._fake_download),
            warnings.catch_warnings(),
        ]
        for item in patches:
            item.__enter__()
            self.addCleanup(item.__exit__, None, None, None)
        warnings.simplefilter("ignore")

    def _slow_set_interval(self, seconds: int) -> DeviceSettingsApplyResult:
        self.release_apply.wait(5)
        return DeviceSettingsApplyResult(True, "ok", {}, None)

    async def _record_reply(self, text, *args, **kwargs):
        self.replies.append(text)

    async def _record_answer(self, text=None, *args, **kwargs):
        self.answers.append(text)

    async def _fake_download(self, services, message, user_id):
        token = message.photo[-1].file_id
        return {"image_id": token, "telegram_file_id": token, "original_path": f"/tmp/{token}.jpg"}, None

    def _message(self, update_id: int, **kwargs) -> Update:
        message = Message(update_id, datetime.now(timezone.utc), self.chat, from_user=self.user, **kwargs)
        return Update(update_id, message=message)

    def _photo(self, update_id: int, token: str) -> Update:
        return self._message(update_id, photo=[PhotoSize(token, token, 10, 10)])

    def _tap(self, update_id: int, data: str) -> Update:
        message = Message(update_id, datetime.now(timezone.utc), self.chat, from_user=self.user, text="menu")
        return Update(update_id, callback_query=CallbackQuery(str(update_id), self.user, "inst", data=data, message=message))

    async def _dispatch(self, update: Update) -> str | None:
        """Hand the update to the first matching handler, like the Application does within a group."""
        for name, handler in (("settings", self.settings), ("photo", self.photo)):
            check = handler.check_update(update)
            if check is not None and check is not False:
                await handler.handle_update(update, self.application, check, self.context)
                return name
        return None

    async def test_photo_while_settings_menu_is_open_starts_upload(self) -> None:
        self.settings._conversations[self.key] = WAITING_FOR_SETTINGS_CHOICE

        await self._dispatch(self._photo(1, "one"))

        self.assertNotIn(self.key, self.settings._conversations)
        self.assertEqual(self.photo._conversations[self.key], WAITING_FOR_TEXT_CHOICE)
        self.assertEqual(self.context.user_data[PENDING_SUBMISSION_KEY]["image_id"], "one")
        self.assertIn("Möchtest du Text hinzufügen", self.replies[-1])

        # Typed answers now reach the upload, not the closed settings menu.
        self.assertEqual(await self._dispatch(self._message(2, text="Ja")), "photo")

    async def test_input_while_a_setting_is_applied_is_answered_and_not_misrouted(self) -> None:
        self.settings._conversations[self.key] = WAITING_FOR_SETTINGS_VALUE
        self.context.user_data[PENDING_SETTINGS_KEY] = 3  # display time
        await self._dispatch(self._message(1, text="2h"))  # starts the slow, non-blocking apply

        self.assertEqual(await self._dispatch(self._message(2, text="3h")), "settings")
        self.assertEqual(self.replies[-1], _SETTINGS_BUSY_TEXT)
        self.assertEqual(await self._dispatch(self._tap(3, "settings|menu")), "settings")
        self.assertEqual(self.answers[-1], _SETTINGS_BUSY_TEXT)

        # A photo meanwhile starts the upload, and the menu closes once the setting is applied.
        await self._dispatch(self._photo(4, "one"))
        self.assertEqual(self.photo._conversations[self.key], WAITING_FOR_TEXT_CHOICE)
        self.release_apply.set()
        for _ in range(50):
            await asyncio.sleep(0.01)
            self.settings.check_update(self._message(5, text="x"))  # resolves the finished apply
            if self.key not in self.settings._conversations:
                break
        self.assertNotIn(self.key, self.settings._conversations)
        self.assertIn("Anzeigedauer ist jetzt", self.replies[-1])
        self.assertEqual(await self._dispatch(self._message(6, text="Ja")), "photo")


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
        self.orientation = "vertical"
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
        )
        self.last_updates: dict[str, object] | None = None
        self.last_refresh_current: bool | None = None
        self.interval_seconds = 86400
        self.lock: asyncio.Lock | None = None
        self.change_calls: list[tuple[str, bool, bool]] = []

    def _record_change(self, name: str) -> None:
        locked = self.lock is not None and self.lock.locked()
        off_loop = threading.current_thread() is not threading.main_thread()
        self.change_calls.append((name, locked, off_loop))

    def read_device_settings(self) -> dict[str, object]:
        return self.settings

    def current_orientation(self) -> str:
        return self.orientation

    def get_slideshow_interval(self) -> int:
        return self.interval_seconds

    def get_sleep_schedule(self) -> tuple[str, str] | None:
        return None

    def apply_device_settings(
        self,
        updates: dict[str, object],
        *,
        refresh_current: bool = True,
    ) -> DeviceSettingsApplyResult:
        self._record_change("apply_device_settings")
        self.last_updates = updates
        self.last_refresh_current = refresh_current
        merged_settings = {
            **self.settings,
            "image_settings": dict(self.settings.get("image_settings", {})),
        }
        if "image_settings" in updates and isinstance(updates["image_settings"], dict):
            merged_settings["image_settings"].update(updates["image_settings"])
        for key, value in updates.items():
            if key != "image_settings":
                merged_settings[key] = value
        self.settings = merged_settings
        self.apply_result = DeviceSettingsApplyResult(
            success=self.apply_result.success,
            message=self.apply_result.message,
            confirmed_settings=merged_settings,
            device_config_path=self.apply_result.device_config_path,
        )
        return self.apply_result

    def set_slideshow_interval(self, seconds: int) -> DeviceSettingsApplyResult:
        self._record_change("set_slideshow_interval")
        self.interval_seconds = seconds
        return DeviceSettingsApplyResult(
            success=True,
            message="ok",
            confirmed_settings={"slideshow_interval": seconds},
            device_config_path=self.device_config_path,
        )

    def set_sleep_schedule(self, start: str | None, end: str | None) -> DeviceSettingsApplyResult:
        self._record_change("set_sleep_schedule")
        return DeviceSettingsApplyResult(
            success=True,
            message="ok",
            confirmed_settings={"sleep_schedule": (start, end)},
            device_config_path=self.device_config_path,
        )


class _FakeDatabase:
    def __init__(self) -> None:
        self._settings: dict[str, str] = {}

    def get_setting(self, key: str) -> str | None:
        return self._settings.get(key)

    def set_setting(self, key: str, value: str) -> None:
        self._settings[key] = value

    def count_rendered_images(self, active_orientation: str | None = None) -> int:
        return 0


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
        self.reply_markups: list[object | None] = []
        self.chat_id = 111
        self.message_id = 222

    async def reply_text(self, text: str, **kwargs) -> None:
        self.replies.append(text)
        self.reply_markups.append(kwargs.get("reply_markup"))


class _FakeUser:
    def __init__(self, user_id: int):
        self.id = user_id


class _FakeUpdate:
    def __init__(self, text: str, *, user_id: int):
        self.effective_user = _FakeUser(user_id)
        self.effective_message = _FakeMessage(text)
        self.callback_query = None


class _FakeCallbackQuery:
    def __init__(self, data: str):
        self.data = data
        self.message = _FakeMessage("")
        self.text_edits: list[str] = []
        self.text_edit_markups: list[object | None] = []

    async def answer(self) -> None:
        return None

    async def edit_message_text(self, text: str, reply_markup=None) -> None:
        self.text_edits.append(text)
        self.text_edit_markups.append(reply_markup)

    async def edit_message_caption(self, *, caption: str, reply_markup=None) -> None:
        self.text_edits.append(caption)
        self.text_edit_markups.append(reply_markup)


class _FakeCallbackUpdate:
    def __init__(self, data: str, *, user_id: int):
        self.effective_user = _FakeUser(user_id)
        self.callback_query = _FakeCallbackQuery(data)
        self.effective_message = self.callback_query.message


class _FakeContext:
    def __init__(self, services: _FakeServices, *, with_job_queue: bool = False):
        self.bot = _FakeBot()
        self.application = SimpleNamespace(
            bot_data={"services": services, "display_lock": asyncio.Lock()},
            bot=self.bot,
            job_queue=_FakeJobQueue() if with_job_queue else None,
        )
        self.user_data: dict[str, object] = {}


class _FakeJob:
    def schedule_removal(self) -> None:
        return None


class _FakeJobQueue:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def get_jobs_by_name(self, name: str):
        return []

    def run_repeating(self, callback, *, interval: int, first: int, name: str) -> _FakeJob:
        self.calls.append({"interval": interval, "first": first, "name": name})
        return _FakeJob()


class _FakeBot:
    def __init__(self) -> None:
        self.deleted_messages: list[tuple[int | None, int | None]] = []

    async def delete_message(self, chat_id=None, message_id=None) -> None:
        self.deleted_messages.append((chat_id, message_id))


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
