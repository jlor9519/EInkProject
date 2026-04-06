from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from app.commands import (
    _delete_cancel_callback,
    _delete_confirm_callback,
    command_action_callback,
    delete_command,
    help_command,
    list_command,
    next_command,
    prev_command,
    refresh_command,
    status_command,
)
from app.database import Database
from app.inkypi_adapter import InkyPiAdapter
from app.models import DisplayConfig, DisplayResult, ImageRecord, InkyPiConfig, StorageConfig


class DeleteCommandTests(unittest.IsolatedAsyncioTestCase):
    async def test_delete_command_shows_paginated_list(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path)
            services.config.storage.current_payload_path.parent.mkdir(parents=True, exist_ok=True)
            services.config.storage.current_payload_path.write_text(
                json.dumps({"image_id": "img-1"}),
                encoding="utf-8",
            )
            services.database.upsert_image(
                ImageRecord(
                    image_id="img-1",
                    telegram_file_id="file-1",
                    telegram_chat_id=111,
                    local_original_path=str(tmpdir_path / "incoming" / "img-1.jpg"),
                    local_rendered_path=None,
                    location="Berlin",
                    taken_at="2026-03-18",
                    caption="First",
                    uploaded_by=1,
                    created_at="2026-03-18T12:00:00+00:00",
                    status="displayed",
                    last_error=None,
                )
            )
            services.database.upsert_image(
                ImageRecord(
                    image_id="img-2",
                    telegram_file_id="file-2",
                    telegram_chat_id=111,
                    local_original_path=str(tmpdir_path / "incoming" / "img-2.jpg"),
                    local_rendered_path=None,
                    location="Munich",
                    taken_at="2026-03-19",
                    caption="Second",
                    uploaded_by=1,
                    created_at="2026-03-18T13:00:00+00:00",
                    status="displayed",
                    last_error=None,
                )
            )

            update = _MessageUpdate()
            context = _FakeContext(services)

            await delete_command(update, context)

            reply = update.effective_message.replies[0]
            self.assertIn("First", reply)
            self.assertIn("Second", reply)
            self.assertIn("▶", reply)  # current image marker

    async def test_delete_confirm_blocks_last_image(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path)
            services.config.storage.current_payload_path.parent.mkdir(parents=True, exist_ok=True)
            services.config.storage.current_payload_path.write_text(
                json.dumps({"image_id": "img-1"}),
                encoding="utf-8",
            )
            services.database.upsert_image(
                ImageRecord(
                    image_id="img-1",
                    telegram_file_id="file-1",
                    telegram_chat_id=111,
                    local_original_path=str(tmpdir_path / "incoming" / "img-1.jpg"),
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="",
                    uploaded_by=1,
                    created_at="2026-03-18T12:00:00+00:00",
                    status="displayed",
                    last_error=None,
                )
            )

            update = _FakeUpdate(data="del|y|img-1", has_media=False)
            context = _FakeContext(services)

            await _delete_confirm_callback(update, context)

            self.assertEqual(update.callback_query.text_edits, [
                "Das letzte Bild kann nicht gelöscht werden. Lade zuerst ein neues Bild hoch."
            ])

    async def test_delete_cancel_callback_uses_caption_edit_for_media_messages(self) -> None:
        services = _build_services(Path(tempfile.mkdtemp()))
        update = _FakeUpdate(data="del|c", has_media=True)
        context = _FakeContext(services)

        await _delete_cancel_callback(update, context)

        self.assertEqual(context.bot.deleted_messages, [(111, 222)])
        self.assertEqual(update.callback_query.caption_edits, [])
        self.assertEqual(update.callback_query.text_edits, [])

    async def test_delete_current_image_keeps_original_row_when_replacement_display_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path)
            current_original = tmpdir_path / "incoming" / "img-1.jpg"
            current_original.parent.mkdir(parents=True, exist_ok=True)
            current_original.write_bytes(b"current")
            services.config.storage.current_payload_path.parent.mkdir(parents=True, exist_ok=True)
            services.config.storage.current_payload_path.write_text(
                json.dumps({"image_id": "img-1"}),
                encoding="utf-8",
            )
            services.database.upsert_image(
                ImageRecord(
                    image_id="img-1",
                    telegram_file_id="file-1",
                    telegram_chat_id=111,
                    local_original_path=str(current_original),
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="Current",
                    uploaded_by=1,
                    created_at="2026-03-18T12:00:00+00:00",
                    status="displayed",
                    last_error=None,
                )
            )
            services.database.upsert_image(
                ImageRecord(
                    image_id="img-2",
                    telegram_file_id="file-2",
                    telegram_chat_id=111,
                    local_original_path=str(tmpdir_path / "incoming" / "missing-img-2.jpg"),
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="Replacement",
                    uploaded_by=1,
                    created_at="2026-03-18T12:10:00+00:00",
                    status="displayed",
                    last_error=None,
                )
            )

            update = _FakeUpdate(data="del|y|img-1", has_media=False)
            context = _FakeContext(services)

            await _delete_confirm_callback(update, context)

            self.assertIsNotNone(services.database.get_image_by_id("img-1"))
            self.assertIsNotNone(services.database.get_image_by_id("img-2"))
            self.assertTrue(current_original.exists())
            self.assertEqual(
                update.callback_query.text_edits,
                ["Aktuelles Bild konnte nicht ersetzt werden. Anzeige fehlgeschlagen: Bilddatei für img-2 nicht mehr vorhanden."],
            )

    async def test_status_and_list_use_active_orientation_library(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path)
            services.database.set_setting("slideshow_next_fire_at", "2099-01-01T00:00:00+00:00")
            services.database.upsert_image(
                ImageRecord(
                    image_id="img-shared",
                    telegram_file_id="file-shared",
                    telegram_chat_id=111,
                    local_original_path=str(tmpdir_path / "incoming" / "img-shared.jpg"),
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="Shared",
                    uploaded_by=1,
                    created_at="2026-03-18T12:00:00+00:00",
                    status="displayed",
                    last_error=None,
                    orientation_bucket="shared",
                )
            )
            services.database.upsert_image(
                ImageRecord(
                    image_id="img-vertical",
                    telegram_file_id="file-vertical",
                    telegram_chat_id=111,
                    local_original_path=str(tmpdir_path / "incoming" / "img-vertical.jpg"),
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="Vertical",
                    uploaded_by=1,
                    created_at="2026-03-18T12:10:00+00:00",
                    status="displayed",
                    last_error=None,
                    orientation_bucket="vertical",
                )
            )
            services.database.upsert_image(
                ImageRecord(
                    image_id="img-horizontal",
                    telegram_file_id="file-horizontal",
                    telegram_chat_id=111,
                    local_original_path=str(tmpdir_path / "incoming" / "img-horizontal.jpg"),
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="Horizontal",
                    uploaded_by=1,
                    created_at="2026-03-18T12:20:00+00:00",
                    status="displayed",
                    last_error=None,
                    orientation_bucket="horizontal",
                )
            )
            services.database.upsert_image(
                ImageRecord(
                    image_id="img-vertical-rendered",
                    telegram_file_id="file-rendered",
                    telegram_chat_id=111,
                    local_original_path=str(tmpdir_path / "incoming" / "img-vertical-rendered.jpg"),
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="Rendered",
                    uploaded_by=1,
                    created_at="2026-03-18T12:30:00+00:00",
                    status="rendered",
                    last_error=None,
                    orientation_bucket="vertical",
                )
            )
            services.config.storage.current_payload_path.parent.mkdir(parents=True, exist_ok=True)
            services.config.storage.current_payload_path.write_text(
                json.dumps({"image_id": "img-horizontal"}),
                encoding="utf-8",
            )
            services.display.orientation = "vertical"

            update_status = _MessageUpdate()
            update_list = _MessageUpdate()
            context = _FakeContext(services)

            await status_command(update_status, context)
            await list_command(update_list, context)

            self.assertIn("Bibliothek: Hochformat", update_status.effective_message.replies[0])
            self.assertIn("In Rotation: 3 Bilder", update_status.effective_message.replies[0])
            self.assertIn("Neue Bilder in Warteliste: 0", update_status.effective_message.replies[0])
            self.assertIn("Warteschlange: 1 neues Bild wartend", update_status.effective_message.replies[0])
            self.assertIn("Neue Bilder in Warteliste: 0", update_list.effective_message.replies[0])
            self.assertIn("Bilderliste Hochformat (2 gesamt)", update_list.effective_message.replies[0])
            self.assertIn("Horizontal", update_list.effective_message.replies[0])
            self.assertIn("nicht Teil der aktuellen Bibliothek", update_list.effective_message.replies[0])

    async def test_status_and_list_show_hidden_images_outside_rotation_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path)
            services.database.set_setting("rotation_limit", "2")
            services.database.set_setting("slideshow_next_fire_at", "2099-01-01T00:00:00+00:00")
            for index, image_id in enumerate(("img-old", "img-mid", "img-new")):
                services.database.upsert_image(
                    ImageRecord(
                        image_id=image_id,
                        telegram_file_id=f"file-{image_id}",
                        telegram_chat_id=111,
                        local_original_path=str(tmpdir_path / "incoming" / f"{image_id}.jpg"),
                        local_rendered_path=None,
                        location="",
                        taken_at="",
                        caption=image_id,
                        uploaded_by=1,
                        created_at=f"2026-03-18T12:{index:02d}:00+00:00",
                        status="displayed",
                        last_error=None,
                        orientation_bucket="horizontal",
                    )
                )
            services.config.storage.current_payload_path.parent.mkdir(parents=True, exist_ok=True)
            services.config.storage.current_payload_path.write_text(
                json.dumps({"image_id": "img-old"}),
                encoding="utf-8",
            )

            update_status = _MessageUpdate()
            update_list = _MessageUpdate()
            context = _FakeContext(services)

            await status_command(update_status, context)
            await list_command(update_list, context)

            self.assertIn("In Rotation: 2 Bilder", update_status.effective_message.replies[0])
            self.assertIn("Neue Bilder in Warteliste: 1", update_status.effective_message.replies[0])
            self.assertIn("img-old", update_list.effective_message.replies[0])
            self.assertIn("nicht Teil der aktuellen Bibliothek", update_list.effective_message.replies[0])
            self.assertIn("Neue Bilder in Warteliste: 1", update_list.effective_message.replies[0])
            self.assertIn('"img-mid"', update_list.effective_message.replies[0])
            self.assertIn('"img-new"', update_list.effective_message.replies[0])

    async def test_status_and_list_show_no_hidden_images_when_rotation_is_unlimited(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path)
            services.database.set_setting("rotation_limit", "0")
            services.database.set_setting("slideshow_next_fire_at", "2099-01-01T00:00:00+00:00")
            for index, image_id in enumerate(("img-1", "img-2", "img-3")):
                services.database.upsert_image(
                    ImageRecord(
                        image_id=image_id,
                        telegram_file_id=f"file-{image_id}",
                        telegram_chat_id=111,
                        local_original_path=str(tmpdir_path / "incoming" / f"{image_id}.jpg"),
                        local_rendered_path=None,
                        location="",
                        taken_at="",
                        caption=image_id,
                        uploaded_by=1,
                        created_at=f"2026-03-18T12:{index:02d}:00+00:00",
                        status="displayed",
                        last_error=None,
                        orientation_bucket="horizontal",
                    )
                )
            services.config.storage.current_payload_path.parent.mkdir(parents=True, exist_ok=True)
            services.config.storage.current_payload_path.write_text(
                json.dumps({"image_id": "img-1"}),
                encoding="utf-8",
            )

            update_status = _MessageUpdate()
            update_list = _MessageUpdate()
            context = _FakeContext(services)

            await status_command(update_status, context)
            await list_command(update_list, context)

            self.assertIn("In Rotation: 3 Bilder", update_status.effective_message.replies[0])
            self.assertIn("Neue Bilder in Warteliste: 0", update_status.effective_message.replies[0])
            self.assertIn("Neue Bilder in Warteliste: 0", update_list.effective_message.replies[0])

    async def test_list_self_heals_stale_interval_timer_and_shows_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path)
            services.database.set_setting("slideshow_next_fire_at", "2000-01-01T00:00:00+00:00")
            services.config.storage.current_payload_path.parent.mkdir(parents=True, exist_ok=True)
            services.config.storage.current_payload_path.write_text(
                json.dumps({"image_id": "img-1"}),
                encoding="utf-8",
            )
            services.database.upsert_image(
                ImageRecord(
                    image_id="img-1",
                    telegram_file_id="file-1",
                    telegram_chat_id=111,
                    local_original_path=str(tmpdir_path / "incoming" / "img-1.jpg"),
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="Current",
                    uploaded_by=1,
                    created_at="2026-03-18T12:00:00+00:00",
                    status="displayed",
                    last_error=None,
                    orientation_bucket="horizontal",
                )
            )
            update = _MessageUpdate()
            context = _FakeContext(services)

            await list_command(update, context)

            reply = update.effective_message.replies[0]
            self.assertIn("Modus: Intervall", reply)
            self.assertNotIn("Wechsel in ca. weniger als 1 Minute", reply)
            self.assertNotEqual(services.database.get_setting("slideshow_next_fire_at"), "2000-01-01T00:00:00+00:00")

    async def test_status_shows_runtime_settings_degraded_warning(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            services = _build_services(Path(tmpdir))
            services.display.runtime_settings_diagnostics = lambda: {
                "degraded": True,
                "message": "Geräteeinstellungen aus Cache aktiv",
            }
            update = _MessageUpdate()
            context = _FakeContext(services)

            await status_command(update, context)

            reply = update.effective_message.replies[0]
            self.assertIn("Warnungen:", reply)
            self.assertIn("Geräteeinstellungen aus Cache aktiv", reply)

    async def test_status_shows_backend_fallback_warning(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            services = _build_services(Path(tmpdir))
            services.display.backend_diagnostics = lambda: {
                "degraded": True,
                "message": "InkyPi-HTTP gestört, Befehl-Fallback aktiv",
            }
            update = _MessageUpdate()
            context = _FakeContext(services)

            await status_command(update, context)

            reply = update.effective_message.replies[0]
            self.assertIn("Warnungen:", reply)
            self.assertIn("Befehl-Fallback aktiv", reply)

    async def test_list_shows_cooldown_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path)
            services.database.set_setting("new_image_cooldown", "3600")
            services.database.set_setting("last_new_image_displayed_at", "2099-01-01T00:00:00+00:00")
            services.config.storage.current_payload_path.parent.mkdir(parents=True, exist_ok=True)
            services.config.storage.current_payload_path.write_text(
                json.dumps({"image_id": "img-1"}),
                encoding="utf-8",
            )
            services.database.upsert_image(
                ImageRecord(
                    image_id="img-1",
                    telegram_file_id="file-1",
                    telegram_chat_id=111,
                    local_original_path=str(tmpdir_path / "incoming" / "img-1.jpg"),
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="Current",
                    uploaded_by=1,
                    created_at="2026-03-18T12:00:00+00:00",
                    status="displayed",
                    last_error=None,
                    orientation_bucket="horizontal",
                )
            )
            services.database.upsert_image(
                ImageRecord(
                    image_id="img-2",
                    telegram_file_id="file-2",
                    telegram_chat_id=111,
                    local_original_path=str(tmpdir_path / "incoming" / "img-2.jpg"),
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="Queued",
                    uploaded_by=1,
                    created_at="2026-03-18T12:05:00+00:00",
                    status="rendered",
                    last_error=None,
                    orientation_bucket="horizontal",
                )
            )
            update = _MessageUpdate()
            context = _FakeContext(services)

            await list_command(update, context)

            self.assertIn("Modus: Warteschlange / Cooldown", update.effective_message.replies[0])

    async def test_list_shows_quiet_hours_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path)
            services.display.sleep_schedule = ("00:00", "23:59")
            services.config.storage.current_payload_path.parent.mkdir(parents=True, exist_ok=True)
            services.config.storage.current_payload_path.write_text(
                json.dumps({"image_id": "img-1"}),
                encoding="utf-8",
            )
            services.database.upsert_image(
                ImageRecord(
                    image_id="img-1",
                    telegram_file_id="file-1",
                    telegram_chat_id=111,
                    local_original_path=str(tmpdir_path / "incoming" / "img-1.jpg"),
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="Current",
                    uploaded_by=1,
                    created_at="2026-03-18T12:00:00+00:00",
                    status="displayed",
                    last_error=None,
                    orientation_bucket="horizontal",
                )
            )
            services.database.set_setting("slideshow_next_fire_at", "2000-01-01T00:00:00+00:00")
            update = _MessageUpdate()
            context = _FakeContext(services)

            await list_command(update, context)

            self.assertIn("Modus: Ruhezeit bis 23:59", update.effective_message.replies[0])

    async def test_list_future_etas_skip_upcoming_quiet_hours(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path)
            services.display.interval_seconds = 3600
            now_local = datetime.now(timezone.utc).astimezone()
            sleep_start = now_local + timedelta(hours=2)
            wake_up = sleep_start + timedelta(hours=8)
            services.display.sleep_schedule = (
                sleep_start.strftime("%H:%M"),
                wake_up.strftime("%H:%M"),
            )
            services.config.storage.current_payload_path.parent.mkdir(parents=True, exist_ok=True)
            services.config.storage.current_payload_path.write_text(
                json.dumps({"image_id": "img-1"}),
                encoding="utf-8",
            )
            for index, image_id in enumerate(("img-1", "img-2", "img-3", "img-4")):
                services.database.upsert_image(
                    ImageRecord(
                        image_id=image_id,
                        telegram_file_id=f"file-{image_id}",
                        telegram_chat_id=111,
                        local_original_path=str(tmpdir_path / "incoming" / f"{image_id}.jpg"),
                        local_rendered_path=None,
                        location="",
                        taken_at="",
                        caption=image_id,
                        uploaded_by=1,
                        created_at=f"2026-03-18T12:{index:02d}:00+00:00",
                        status="displayed",
                        last_error=None,
                        orientation_bucket="horizontal",
                    )
                )
            services.database.set_setting(
                "slideshow_next_fire_at",
                (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
            )
            services.database.set_setting("slideshow_next_fire_mode", "interval")
            services.database.set_setting("slideshow_next_fire_detail", "3600")
            update = _MessageUpdate()
            context = _FakeContext(services)

            await list_command(update, context)

            reply = update.effective_message.replies[0]
            self.assertIn("Nächste Bilder:", reply)
            self.assertNotIn("In ca. 3 Std.", reply)

    async def test_next_promotes_oldest_rendered_image_before_displayed_rotation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path)
            services.display.orientation = "horizontal"
            services.config.storage.current_payload_path.parent.mkdir(parents=True, exist_ok=True)
            services.config.storage.current_payload_path.write_text(
                json.dumps({"image_id": "img-current"}),
                encoding="utf-8",
            )
            for image_id in ("img-current", "img-next"):
                original = tmpdir_path / "incoming" / f"{image_id}.jpg"
                original.parent.mkdir(parents=True, exist_ok=True)
                original.write_bytes(image_id.encode("utf-8"))
            services.database.upsert_image(
                ImageRecord(
                    image_id="img-current",
                    telegram_file_id="file-current",
                    telegram_chat_id=111,
                    local_original_path=str(tmpdir_path / "incoming" / "img-current.jpg"),
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="Current",
                    uploaded_by=1,
                    created_at="2026-03-18T12:00:00+00:00",
                    status="displayed",
                    last_error=None,
                    orientation_bucket="horizontal",
                )
            )
            services.database.upsert_image(
                ImageRecord(
                    image_id="img-next",
                    telegram_file_id="file-next",
                    telegram_chat_id=111,
                    local_original_path=str(tmpdir_path / "incoming" / "img-next.jpg"),
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="Queued",
                    uploaded_by=1,
                    created_at="2026-03-18T12:05:00+00:00",
                    status="rendered",
                    last_error="queued",
                    orientation_bucket="horizontal",
                )
            )
            update = _MessageUpdate()
            context = _FakeContext(services, with_job_queue=True)

            await next_command(update, context)

            promoted = services.database.get_image_by_id("img-next")
            self.assertEqual(promoted.status, "displayed")
            self.assertIsNone(promoted.last_error)
            self.assertEqual(services.display.display_calls[-1], "img-next")
            self.assertIsNotNone(services.database.get_setting("last_new_image_displayed_at"))
            self.assertIsNotNone(services.database.get_setting("slideshow_next_fire_at"))
            self.assertEqual(update.effective_message.replies[-1], "Bild 2 von 2: img-next")

    async def test_next_ignores_displayed_images_hidden_by_rotation_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path)
            services.database.set_setting("rotation_limit", "2")
            services.config.storage.current_payload_path.parent.mkdir(parents=True, exist_ok=True)
            services.config.storage.current_payload_path.write_text(
                json.dumps({"image_id": "img-old"}),
                encoding="utf-8",
            )
            for image_id in ("img-mid", "img-new"):
                original = tmpdir_path / "incoming" / f"{image_id}.jpg"
                original.parent.mkdir(parents=True, exist_ok=True)
                original.write_bytes(image_id.encode("utf-8"))
            services.database.upsert_image(
                ImageRecord(
                    image_id="img-old",
                    telegram_file_id="file-old",
                    telegram_chat_id=111,
                    local_original_path=str(tmpdir_path / "incoming" / "img-old.jpg"),
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="Old",
                    uploaded_by=1,
                    created_at="2026-03-18T12:00:00+00:00",
                    status="displayed",
                    last_error=None,
                    orientation_bucket="horizontal",
                )
            )
            services.database.upsert_image(
                ImageRecord(
                    image_id="img-mid",
                    telegram_file_id="file-mid",
                    telegram_chat_id=111,
                    local_original_path=str(tmpdir_path / "incoming" / "img-mid.jpg"),
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="Mid",
                    uploaded_by=1,
                    created_at="2026-03-18T12:05:00+00:00",
                    status="displayed",
                    last_error=None,
                    orientation_bucket="horizontal",
                )
            )
            services.database.upsert_image(
                ImageRecord(
                    image_id="img-new",
                    telegram_file_id="file-new",
                    telegram_chat_id=111,
                    local_original_path=str(tmpdir_path / "incoming" / "img-new.jpg"),
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="New",
                    uploaded_by=1,
                    created_at="2026-03-18T12:10:00+00:00",
                    status="displayed",
                    last_error=None,
                    orientation_bucket="horizontal",
                )
            )
            update = _MessageUpdate()
            context = _FakeContext(services, with_job_queue=True)

            await next_command(update, context)

            self.assertEqual(services.display.display_calls[-1], "img-mid")
            self.assertEqual(update.effective_message.replies[-1], "Bild 1 von 2: img-mid")

    async def test_prev_ignores_rendered_waiting_images(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path)
            services.config.storage.current_payload_path.parent.mkdir(parents=True, exist_ok=True)
            services.config.storage.current_payload_path.write_text(
                json.dumps({"image_id": "img-current"}),
                encoding="utf-8",
            )
            for image_id in ("img-previous", "img-current"):
                original = tmpdir_path / "incoming" / f"{image_id}.jpg"
                original.parent.mkdir(parents=True, exist_ok=True)
                original.write_bytes(image_id.encode("utf-8"))
            services.database.upsert_image(
                ImageRecord(
                    image_id="img-previous",
                    telegram_file_id="file-previous",
                    telegram_chat_id=111,
                    local_original_path=str(tmpdir_path / "incoming" / "img-previous.jpg"),
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="Previous",
                    uploaded_by=1,
                    created_at="2026-03-18T11:55:00+00:00",
                    status="displayed",
                    last_error=None,
                    orientation_bucket="horizontal",
                )
            )
            services.database.upsert_image(
                ImageRecord(
                    image_id="img-current",
                    telegram_file_id="file-current",
                    telegram_chat_id=111,
                    local_original_path=str(tmpdir_path / "incoming" / "img-current.jpg"),
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="Current",
                    uploaded_by=1,
                    created_at="2026-03-18T12:00:00+00:00",
                    status="displayed",
                    last_error=None,
                    orientation_bucket="horizontal",
                )
            )
            services.database.upsert_image(
                ImageRecord(
                    image_id="img-rendered",
                    telegram_file_id="file-rendered",
                    telegram_chat_id=111,
                    local_original_path=str(tmpdir_path / "incoming" / "img-rendered.jpg"),
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="Rendered",
                    uploaded_by=1,
                    created_at="2026-03-18T12:02:00+00:00",
                    status="rendered",
                    last_error=None,
                    orientation_bucket="horizontal",
                )
            )
            update = _MessageUpdate()
            context = _FakeContext(services)

            await prev_command(update, context)

            self.assertEqual(services.display.display_calls[-1], "img-previous")
            self.assertEqual(update.effective_message.replies[-1], "Bild 1 von 2: img-previous")

    async def test_next_uses_command_fallback_after_http_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services_with_real_adapter(tmpdir_path)
            _seed_real_adapter_image(tmpdir_path, services, "img-current", created_at="2026-03-18T12:00:00+00:00")
            _seed_real_adapter_image(tmpdir_path, services, "img-next", created_at="2026-03-18T12:05:00+00:00")
            services.config.storage.current_payload_path.parent.mkdir(parents=True, exist_ok=True)
            services.config.storage.current_payload_path.write_text(
                json.dumps({"image_id": "img-current"}),
                encoding="utf-8",
            )
            services.config.storage.current_image_path.write_bytes(b"img-current")

            update_next = _MessageUpdate()
            context = _FakeContext(services, with_job_queue=True)

            with patch(
                "app.inkypi_adapter.request.urlopen",
                side_effect=TimeoutError("timed out"),
            ), patch(
                "app.inkypi_adapter.subprocess.run",
                return_value=SimpleNamespace(returncode=0, stdout="refresh ok", stderr=""),
            ):
                await next_command(update_next, context)

            self.assertEqual(update_next.effective_message.replies[-1], "Bild 2 von 2: img-next")
            payload = json.loads(services.config.storage.current_payload_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["image_id"], "img-next")

            update_list = _MessageUpdate()
            await list_command(update_list, context)
            self.assertIn('"img-next"', update_list.effective_message.replies[0])

    async def test_prev_uses_command_fallback_after_http_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services_with_real_adapter(tmpdir_path)
            _seed_real_adapter_image(tmpdir_path, services, "img-prev", created_at="2026-03-18T11:55:00+00:00")
            _seed_real_adapter_image(tmpdir_path, services, "img-current", created_at="2026-03-18T12:00:00+00:00")
            services.config.storage.current_payload_path.parent.mkdir(parents=True, exist_ok=True)
            services.config.storage.current_payload_path.write_text(
                json.dumps({"image_id": "img-current"}),
                encoding="utf-8",
            )
            services.config.storage.current_image_path.write_bytes(b"img-current")

            update = _MessageUpdate()
            context = _FakeContext(services, with_job_queue=True)

            with patch(
                "app.inkypi_adapter.request.urlopen",
                side_effect=TimeoutError("timed out"),
            ), patch(
                "app.inkypi_adapter.subprocess.run",
                return_value=SimpleNamespace(returncode=0, stdout="refresh ok", stderr=""),
            ):
                await prev_command(update, context)

            self.assertEqual(update.effective_message.replies[-1], "Bild 1 von 2: img-prev")
            payload = json.loads(services.config.storage.current_payload_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["image_id"], "img-prev")

    async def test_next_keeps_current_image_when_restart_recovery_cannot_verify_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services_with_real_adapter(
                tmpdir_path,
                refresh_command="sudo systemctl restart inkypi.service",
            )
            _seed_real_adapter_image(tmpdir_path, services, "img-current", created_at="2026-03-18T12:00:00+00:00")
            _seed_real_adapter_image(tmpdir_path, services, "img-next", created_at="2026-03-18T12:05:00+00:00")
            services.config.storage.current_payload_path.parent.mkdir(parents=True, exist_ok=True)
            services.config.storage.current_payload_path.write_text(
                json.dumps({"image_id": "img-current"}),
                encoding="utf-8",
            )
            services.config.storage.current_image_path.write_bytes(b"img-current")

            update_next = _MessageUpdate()
            context = _FakeContext(services, with_job_queue=True)

            with patch(
                "app.inkypi_adapter.request.urlopen",
                side_effect=TimeoutError("timed out"),
            ), patch(
                "app.inkypi_adapter.subprocess.run",
                return_value=SimpleNamespace(returncode=0, stdout="active", stderr=""),
            ), patch.object(
                services.display,
                "_wait_for_inkypi_http_ready",
                return_value="connection refused",
            ):
                await next_command(update_next, context)

            self.assertEqual(
                update_next.effective_message.replies[-1],
                "Display nicht erreichbar. Bitte prüfe die Verbindung zum Pi.",
            )
            payload = json.loads(services.config.storage.current_payload_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["image_id"], "img-current")

            update_list = _MessageUpdate()
            await list_command(update_list, context)
            self.assertIn('"img-current"', update_list.effective_message.replies[0])

    async def test_prev_keeps_current_image_when_restart_recovery_cannot_verify_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services_with_real_adapter(
                tmpdir_path,
                refresh_command="sudo systemctl restart inkypi.service",
            )
            _seed_real_adapter_image(tmpdir_path, services, "img-prev", created_at="2026-03-18T11:55:00+00:00")
            _seed_real_adapter_image(tmpdir_path, services, "img-current", created_at="2026-03-18T12:00:00+00:00")
            services.config.storage.current_payload_path.parent.mkdir(parents=True, exist_ok=True)
            services.config.storage.current_payload_path.write_text(
                json.dumps({"image_id": "img-current"}),
                encoding="utf-8",
            )
            services.config.storage.current_image_path.write_bytes(b"img-current")

            update = _MessageUpdate()
            context = _FakeContext(services, with_job_queue=True)

            with patch(
                "app.inkypi_adapter.request.urlopen",
                side_effect=TimeoutError("timed out"),
            ), patch(
                "app.inkypi_adapter.subprocess.run",
                return_value=SimpleNamespace(returncode=0, stdout="active", stderr=""),
            ), patch.object(
                services.display,
                "_wait_for_inkypi_http_ready",
                return_value="connection refused",
            ):
                await prev_command(update, context)

            self.assertEqual(
                update.effective_message.replies[-1],
                "Display nicht erreichbar. Bitte prüfe die Verbindung zum Pi.",
            )
            payload = json.loads(services.config.storage.current_payload_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["image_id"], "img-current")

    async def test_refresh_uses_command_fallback_after_http_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services_with_real_adapter(tmpdir_path)
            services.config.storage.current_payload_path.parent.mkdir(parents=True, exist_ok=True)
            services.config.storage.current_payload_path.write_text(
                json.dumps({"image_id": "img-current", "orientation_hint": "horizontal"}),
                encoding="utf-8",
            )
            update = _MessageUpdate()
            context = _FakeContext(services)

            with patch(
                "app.inkypi_adapter.request.urlopen",
                side_effect=TimeoutError("timed out"),
            ), patch(
                "app.inkypi_adapter.subprocess.run",
                return_value=SimpleNamespace(returncode=0, stdout="refresh ok", stderr=""),
            ):
                await refresh_command(update, context)

            self.assertEqual(update.effective_message.replies[-1], "Aktualisierung ausgelöst.")
            status_update = _MessageUpdate()
            await status_command(status_update, context)
            self.assertIn("Befehl-Fallback aktiv", status_update.effective_message.replies[0])

    async def test_refresh_reports_failure_when_restart_recovery_cannot_verify_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services_with_real_adapter(
                tmpdir_path,
                refresh_command="sudo systemctl restart inkypi.service",
            )
            services.config.storage.current_payload_path.parent.mkdir(parents=True, exist_ok=True)
            services.config.storage.current_payload_path.write_text(
                json.dumps({"image_id": "img-current", "orientation_hint": "horizontal"}),
                encoding="utf-8",
            )
            update = _MessageUpdate()
            context = _FakeContext(services)

            with patch(
                "app.inkypi_adapter.request.urlopen",
                side_effect=TimeoutError("timed out"),
            ), patch(
                "app.inkypi_adapter.subprocess.run",
                return_value=SimpleNamespace(returncode=0, stdout="active", stderr=""),
            ), patch.object(
                services.display,
                "_wait_for_inkypi_http_ready",
                return_value="connection refused",
            ):
                await refresh_command(update, context)

            self.assertEqual(
                update.effective_message.replies[-1],
                "Display nicht erreichbar. Bitte prüfe die Verbindung zum Pi.",
            )

    async def test_help_command_adds_quick_actions_and_admin_settings_button(self) -> None:
        services = _build_services(Path(tempfile.mkdtemp()), is_admin=True)
        update = _MessageUpdate(user_id=5)
        context = _FakeContext(services)

        await help_command(update, context)

        markup = update.effective_message.reply_markups[0]
        self.assertIsNotNone(markup)
        labels = [button.text for row in markup.inline_keyboard for button in row]
        self.assertIn("Status", labels)
        self.assertIn("Liste", labels)
        self.assertIn("Löschen", labels)
        self.assertIn("Einstellungen", labels)
        self.assertIn("Abbrechen", labels)
        reply = update.effective_message.replies[0]
        self.assertIn("/settings - Anzeigeeinstellungen anzeigen/ändern", reply)
        self.assertNotIn("/cancel", reply)
        self.assertNotIn("/restart", reply)
        self.assertNotIn("/update", reply)

    async def test_status_shows_recent_errors_when_present(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            services = _build_services(Path(tmpdir))
            services.database.log_error("display", "InkyPi update_now request timed out after 30 seconds")
            services.database.log_error("telegram", "httpx.ReadError")
            update = _MessageUpdate(user_id=7)
            context = _FakeContext(services)

            await status_command(update, context)

            text = update.effective_message.replies[0]
            self.assertIn("Letzte Fehler:", text)
            self.assertIn("display:", text)
            self.assertIn("telegram:", text)

    async def test_status_omits_errors_section_when_log_is_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            services = _build_services(Path(tmpdir))
            update = _MessageUpdate(user_id=7)
            context = _FakeContext(services)

            await status_command(update, context)

            text = update.effective_message.replies[0]
            self.assertNotIn("Letzte Fehler:", text)

    async def test_status_command_adds_admin_quick_actions(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            services = _build_services(Path(tmpdir), is_admin=True)
            update = _MessageUpdate(user_id=7)
            context = _FakeContext(services)

            await status_command(update, context)

            markup = update.effective_message.reply_markups[0]
            labels = [button.text for row in markup.inline_keyboard for button in row]
            self.assertIn("Liste", labels)
            self.assertIn("Vorheriges", labels)
            self.assertIn("Nächstes", labels)
            self.assertIn("Löschen", labels)
            self.assertIn("Abbrechen", labels)
            self.assertIn("Einstellungen", labels)

    async def test_list_command_adds_quick_actions(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path)
            services.config.storage.current_payload_path.parent.mkdir(parents=True, exist_ok=True)
            services.config.storage.current_payload_path.write_text(
                json.dumps({"image_id": "img-1"}),
                encoding="utf-8",
            )
            services.database.upsert_image(
                ImageRecord(
                    image_id="img-1",
                    telegram_file_id="file-1",
                    telegram_chat_id=111,
                    local_original_path=str(tmpdir_path / "incoming" / "img-1.jpg"),
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="Current",
                    uploaded_by=1,
                    created_at="2026-03-18T12:00:00+00:00",
                    status="displayed",
                    last_error=None,
                    orientation_bucket="horizontal",
                )
            )
            update = _MessageUpdate()
            context = _FakeContext(services)

            await list_command(update, context)

            markup = update.effective_message.reply_markups[0]
            labels = [button.text for row in markup.inline_keyboard for button in row]
            self.assertIn("Vorheriges", labels)
            self.assertIn("Nächstes", labels)
            self.assertIn("Löschen", labels)
            self.assertIn("Abbrechen", labels)
            self.assertNotIn("Neu laden", labels)

    async def test_command_action_callback_edits_status_message(self) -> None:
        services = _build_services(Path(tempfile.mkdtemp()), is_admin=True)
        update = _FakeUpdate(data="cmd|status", has_media=False, user_id=5)
        context = _FakeContext(services)

        await command_action_callback(update, context)

        self.assertIn("Fotorahmen-Status", update.callback_query.text_edits[0])
        markup = update.callback_query.text_edit_markups[0]
        self.assertIsNotNone(markup)
        labels = [button.text for row in markup.inline_keyboard for button in row]
        self.assertIn("Einstellungen", labels)

    async def test_command_action_callback_edits_list_message(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path)
            services.config.storage.current_payload_path.parent.mkdir(parents=True, exist_ok=True)
            services.config.storage.current_payload_path.write_text(
                json.dumps({"image_id": "img-1"}),
                encoding="utf-8",
            )
            services.database.upsert_image(
                ImageRecord(
                    image_id="img-1",
                    telegram_file_id="file-1",
                    telegram_chat_id=111,
                    local_original_path=str(tmpdir_path / "incoming" / "img-1.jpg"),
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="Current",
                    uploaded_by=1,
                    created_at="2026-03-18T12:00:00+00:00",
                    status="displayed",
                    last_error=None,
                    orientation_bucket="horizontal",
                )
            )
            update = _FakeUpdate(data="cmd|list", has_media=False)
            context = _FakeContext(services)

            await command_action_callback(update, context)

            self.assertIn("Bilderliste", update.callback_query.text_edits[0])
            labels = [button.text for row in update.callback_query.text_edit_markups[0].inline_keyboard for button in row]
            self.assertNotIn("Neu laden", labels)

    async def test_command_action_callback_runs_next_and_delete_actions(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path)
            services.config.storage.current_payload_path.parent.mkdir(parents=True, exist_ok=True)
            services.config.storage.current_payload_path.write_text(
                json.dumps({"image_id": "img-current"}),
                encoding="utf-8",
            )
            for image_id in ("img-current", "img-next"):
                original = tmpdir_path / "incoming" / f"{image_id}.jpg"
                original.parent.mkdir(parents=True, exist_ok=True)
                original.write_bytes(image_id.encode("utf-8"))
            services.database.upsert_image(
                ImageRecord(
                    image_id="img-current",
                    telegram_file_id="file-current",
                    telegram_chat_id=111,
                    local_original_path=str(tmpdir_path / "incoming" / "img-current.jpg"),
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="Current",
                    uploaded_by=1,
                    created_at="2026-03-18T12:00:00+00:00",
                    status="displayed",
                    last_error=None,
                    orientation_bucket="horizontal",
                )
            )
            services.database.upsert_image(
                ImageRecord(
                    image_id="img-next",
                    telegram_file_id="file-next",
                    telegram_chat_id=111,
                    local_original_path=str(tmpdir_path / "incoming" / "img-next.jpg"),
                    local_rendered_path=None,
                    location="",
                    taken_at="",
                    caption="Queued",
                    uploaded_by=1,
                    created_at="2026-03-18T12:05:00+00:00",
                    status="rendered",
                    last_error=None,
                    orientation_bucket="horizontal",
                )
            )
            next_update = _FakeUpdate(data="cmd|next", has_media=False)
            delete_update = _FakeUpdate(data="cmd|delete", has_media=False)
            context = _FakeContext(services, with_job_queue=True)

            await command_action_callback(next_update, context)
            await command_action_callback(delete_update, context)

            self.assertEqual(services.display.display_calls[-1], "img-next")
            self.assertEqual(next_update.callback_query.message.replies[-1], "Bild 2 von 2: img-next")
            self.assertIn("Bilder zum Löschen", delete_update.callback_query.message.replies[-1])

    async def test_command_action_callback_close_deletes_message(self) -> None:
        services = _build_services(Path(tempfile.mkdtemp()))
        update = _FakeUpdate(data="cmd|close", has_media=False)
        context = _FakeContext(services)

        await command_action_callback(update, context)

        self.assertEqual(context.bot.deleted_messages, [(111, 222)])
        self.assertEqual(update.callback_query.text_edits, [])


class _FakeQueryMessage:
    def __init__(self, *, has_media: bool) -> None:
        self.photo = [object()] if has_media else []
        self.document = None
        self.animation = None
        self.video = None
        self.replies: list[str] = []
        self.reply_markups: list[object | None] = []
        self.chat_id = 111
        self.message_id = 222

    async def reply_text(self, text: str, **kwargs) -> None:
        self.replies.append(text)
        self.reply_markups.append(kwargs.get("reply_markup"))


class _FakeCallbackQuery:
    def __init__(self, *, data: str, has_media: bool) -> None:
        self.data = data
        self.message = _FakeQueryMessage(has_media=has_media)
        self.text_edits: list[str] = []
        self.caption_edits: list[str] = []
        self.text_edit_markups: list[object | None] = []
        self.caption_edit_markups: list[object | None] = []

    async def answer(self) -> None:
        return None

    async def edit_message_text(self, text: str, reply_markup=None) -> None:
        self.text_edits.append(text)
        self.text_edit_markups.append(reply_markup)

    async def edit_message_caption(self, *, caption: str, reply_markup=None) -> None:
        self.caption_edits.append(caption)
        self.caption_edit_markups.append(reply_markup)


class _FakeUpdate:
    def __init__(self, *, data: str, has_media: bool, user_id: int = 1) -> None:
        self.callback_query = _FakeCallbackQuery(data=data, has_media=has_media)
        self.effective_message = self.callback_query.message
        self.effective_user = SimpleNamespace(id=user_id)


class _FakeMessageOnly:
    def __init__(self) -> None:
        self.replies: list[str] = []
        self.reply_markups: list[object | None] = []

    async def reply_text(self, text: str, **kwargs) -> None:
        self.replies.append(text)
        self.reply_markups.append(kwargs.get("reply_markup"))


class _MessageUpdate:
    def __init__(self, *, user_id: int = 1) -> None:
        self.effective_message = _FakeMessageOnly()
        self.effective_user = SimpleNamespace(id=user_id)


class _FakeContext:
    def __init__(self, services, *, with_job_queue: bool = False) -> None:
        self.bot = _FakeBot()
        self.application = SimpleNamespace(
            bot_data={"services": services, "display_lock": asyncio.Lock()},
            bot=self.bot,
            job_queue=_FakeJobQueue() if with_job_queue else None,
        )
        self.user_data: dict[str, object] = {}


class _FakeDisplay:
    def __init__(self) -> None:
        self.orientation = "horizontal"
        self.display_calls: list[str] = []
        self.sleep_schedule = None

    def current_orientation(self) -> str:
        return self.orientation

    def display(self, request) -> DisplayResult:
        self.display_calls.append(request.image_id)
        return DisplayResult(True, f"displayed {request.image_id}")

    def payload_exists(self) -> bool:
        return True

    def ping_inkypi(self):
        return None

    def get_slideshow_interval(self) -> int:
        return 86400

    def get_sleep_schedule(self):
        return self.sleep_schedule


class _FakeAuth:
    def __init__(self, *, is_admin: bool = False) -> None:
        self._is_admin = is_admin

    def sync_user(self, user) -> None:
        return None

    def is_whitelisted(self, user_id: int) -> bool:
        return True

    def is_admin(self, user_id: int) -> bool:
        return self._is_admin


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


def _build_services(base_dir: Path, *, is_admin: bool = False):
    database = Database(base_dir / "photo_frame.db")
    database.initialize()
    return SimpleNamespace(
        auth=_FakeAuth(is_admin=is_admin),
        database=database,
        display=_FakeDisplay(),
        storage=SimpleNamespace(
            rendered_path=lambda image_id: base_dir / "rendered" / f"{image_id}.png",
            healthcheck=lambda: True,
        ),
        renderer=SimpleNamespace(render=lambda *args, **kwargs: None),
        config=SimpleNamespace(
            storage=SimpleNamespace(
                current_payload_path=base_dir / "inkypi" / "current.json",
                current_image_path=base_dir / "inkypi" / "current.png",
            )
        ),
    )


class _CopyingRenderer:
    def render(self, original_path: Path, output_path: Path, **_: object) -> Path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(Path(original_path).read_bytes())
        return output_path


def _build_services_with_real_adapter(base_dir: Path, *, refresh_command: str = "echo refresh"):
    database = Database(base_dir / "photo_frame.db")
    database.initialize()
    storage_config = StorageConfig(
        incoming_dir=base_dir / "incoming",
        rendered_dir=base_dir / "rendered",
        cache_dir=base_dir / "cache",
        archive_dir=base_dir / "archive",
        inkypi_payload_dir=base_dir / "inkypi",
        current_payload_path=base_dir / "inkypi" / "current.json",
        current_image_path=base_dir / "inkypi" / "current.png",
        keep_recent_rendered=5,
    )
    display_config = DisplayConfig(
        width=800,
        height=480,
        caption_height=44,
        margin=18,
        metadata_font_size=14,
        caption_font_size=20,
        caption_character_limit=72,
        max_caption_lines=1,
        font_path="/tmp/does-not-exist.ttf",
        background_color="#F7F3EA",
        text_color="#111111",
        divider_color="#3A3A3A",
    )
    inkypi_config = InkyPiConfig(
        repo_path=base_dir / "InkyPi",
        install_path=base_dir / "usr" / "local" / "inkypi",
        validated_commit="main",
        waveshare_model="epd7in3e",
        plugin_id="telegram_frame",
        payload_dir=base_dir / "inkypi",
        update_method="http_update_now",
        update_now_url="http://127.0.0.1/update_now",
        refresh_command=refresh_command,
    )
    device_config_path = base_dir / "InkyPi" / "src" / "config" / "device.json"
    device_config_path.parent.mkdir(parents=True, exist_ok=True)
    device_config_path.write_text(json.dumps({"orientation": "horizontal"}), encoding="utf-8")
    return SimpleNamespace(
        auth=_FakeAuth(),
        database=database,
        display=InkyPiAdapter(inkypi_config, storage_config, display_config, database=database),
        storage=SimpleNamespace(
            rendered_path=lambda image_id: base_dir / "rendered" / f"{image_id}.png",
            healthcheck=lambda: True,
        ),
        renderer=_CopyingRenderer(),
        config=SimpleNamespace(storage=storage_config),
    )


def _seed_real_adapter_image(base_dir: Path, services, image_id: str, *, created_at: str) -> None:
    original = base_dir / "incoming" / f"{image_id}.jpg"
    original.parent.mkdir(parents=True, exist_ok=True)
    original.write_bytes(image_id.encode("utf-8"))
    services.database.upsert_image(
        ImageRecord(
            image_id=image_id,
            telegram_file_id=f"file-{image_id}",
            telegram_chat_id=111,
            local_original_path=str(original),
            local_rendered_path=None,
            location="",
            taken_at="",
            caption=image_id,
            uploaded_by=1,
            created_at=created_at,
            status="displayed",
            last_error=None,
            orientation_bucket="horizontal",
        )
    )


if __name__ == "__main__":
    unittest.main()
