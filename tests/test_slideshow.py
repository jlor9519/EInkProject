from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

from app.commands import list_command
from app.database import Database
from app.inkypi_adapter import InkyPiAdapter
from app.models import DisplayConfig, DisplayResult, ImageRecord, InkyPiConfig, StorageConfig
from app.slideshow import _advance_slideshow, compute_next_fire_decision, project_display_change_offsets


class SlideshowScheduledModeTests(unittest.IsolatedAsyncioTestCase):
    async def test_scheduled_auto_advance_rewrites_next_fire_for_next_day(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path, interval_seconds=7200)
            _seed_displayed_images(tmpdir_path, services, ["img-1", "img-2"])
            services.database.set_setting("scheduled_change_time", datetime.now().strftime("%H:%M"))
            context = _JobContext(services)

            await _advance_slideshow(context)

            self.assertEqual(services.display.display_calls[-1], "img-2")
            next_fire_raw = services.database.get_setting("slideshow_next_fire_at")
            self.assertIsNotNone(next_fire_raw)
            next_fire = datetime.fromisoformat(next_fire_raw)
            if next_fire.tzinfo is None:
                next_fire = next_fire.replace(tzinfo=timezone.utc)
            self.assertGreater(next_fire, datetime.now(timezone.utc) + timedelta(hours=23))

    async def test_list_after_scheduled_advance_shows_future_countdown(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path, interval_seconds=7200)
            _seed_displayed_images(tmpdir_path, services, ["img-1", "img-2"])
            services.database.set_setting("scheduled_change_time", datetime.now().strftime("%H:%M"))
            job_context = _JobContext(services)

            await _advance_slideshow(job_context)

            update = _MessageUpdate()
            command_context = _CommandContext(services)
            await list_command(update, command_context)

            reply = update.effective_message.replies[0]
            self.assertIn('"img-2"', reply)
            self.assertNotIn("Wechsel in ca. weniger als 1 Minute", reply)

    async def test_list_uses_daily_eta_spacing_in_scheduled_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path, interval_seconds=7200)
            _seed_displayed_images(tmpdir_path, services, ["img-1", "img-2", "img-3"])
            services.database.set_setting("scheduled_change_time", "09:00")
            services.database.set_setting(
                "slideshow_next_fire_at",
                (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
            )

            update = _MessageUpdate()
            context = _CommandContext(services)
            await list_command(update, context)

            reply = update.effective_message.replies[0]
            self.assertIn("Modus: täglicher Wechsel um 09:00", reply)
            self.assertIn("1 Tag", reply)
            self.assertNotIn("In ca. 3 Std.", reply)

    async def test_busy_auto_advance_reschedules_future_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            services = _build_services(Path(tmpdir), interval_seconds=7200)
            context = _JobContext(services)
            await context.application.bot_data["display_lock"].acquire()
            try:
                await _advance_slideshow(context)
            finally:
                context.application.bot_data["display_lock"].release()

            self.assert_future_timestamp(services.database.get_setting("slideshow_next_fire_at"))
            self.assertEqual(services.database.get_setting("slideshow_next_fire_mode"), "retry_busy")

    async def test_single_image_auto_advance_reschedules_without_stale_timer(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path, interval_seconds=7200)
            _seed_displayed_images(tmpdir_path, services, ["img-1"])
            context = _JobContext(services)

            await _advance_slideshow(context)

            self.assert_future_timestamp(services.database.get_setting("slideshow_next_fire_at"))
            self.assertEqual(services.database.get_setting("slideshow_next_fire_mode"), "single_image")

    async def test_payload_missing_auto_advance_reschedules_future_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            services = _build_services(Path(tmpdir), interval_seconds=7200)
            context = _JobContext(services)

            await _advance_slideshow(context)

            self.assert_future_timestamp(services.database.get_setting("slideshow_next_fire_at"))
            self.assertEqual(services.database.get_setting("slideshow_next_fire_mode"), "payload_missing")

    async def test_display_error_auto_advance_reschedules_future_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path, interval_seconds=7200)
            _seed_displayed_images(tmpdir_path, services, ["img-1", "img-2"])
            services.display.fail_next = True
            context = _JobContext(services)

            await _advance_slideshow(context)

            self.assert_future_timestamp(services.database.get_setting("slideshow_next_fire_at"))
            self.assertEqual(services.database.get_setting("slideshow_next_fire_mode"), "display_error")

    async def test_auto_advance_uses_command_fallback_while_http_backend_is_degraded(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services_with_real_adapter(tmpdir_path, interval_seconds=7200)
            _seed_displayed_images(tmpdir_path, services, ["img-1", "img-2", "img-3"])
            context = _JobContext(services)

            with patch(
                "app.inkypi_adapter.request.urlopen",
                side_effect=TimeoutError("timed out"),
            ) as mock_http, patch(
                "app.inkypi_adapter.subprocess.run",
                return_value=SimpleNamespace(returncode=0, stdout="refresh ok", stderr=""),
            ) as mock_command:
                await _advance_slideshow(context)
                await _advance_slideshow(context)

            payload = json.loads(services.config.storage.current_payload_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["image_id"], "img-3")
            self.assertEqual(mock_http.call_count, 1)
            self.assertEqual(mock_command.call_count, 2)
            self.assertNotEqual(services.database.get_setting("slideshow_next_fire_mode"), "display_error")

            update = _MessageUpdate()
            command_context = _CommandContext(services)
            await list_command(update, command_context)
            self.assertIn('"img-3"', update.effective_message.replies[0])

    async def test_auto_advance_ignores_images_hidden_by_rotation_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path, interval_seconds=7200)
            services.database.set_setting("rotation_limit", "2")
            _seed_displayed_images(tmpdir_path, services, ["img-1", "img-2", "img-3"])
            context = _JobContext(services)

            await _advance_slideshow(context)

            self.assertEqual(services.display.display_calls[-1], "img-2")
            payload = json.loads(services.config.storage.current_payload_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["image_id"], "img-2")

    async def test_auto_advance_uses_all_images_when_rotation_is_unlimited(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path, interval_seconds=7200)
            services.database.set_setting("rotation_limit", "0")
            _seed_displayed_images(tmpdir_path, services, ["img-1", "img-2", "img-3"])
            context = _JobContext(services)

            await _advance_slideshow(context)

            self.assertEqual(services.display.display_calls[-1], "img-2")
            payload = json.loads(services.config.storage.current_payload_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["image_id"], "img-2")

    async def test_interval_next_fire_skips_upcoming_quiet_hours(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            services = _build_services(Path(tmpdir), interval_seconds=3600)
            services.display.get_sleep_schedule = lambda: ("22:00", "08:00")
            berlin = ZoneInfo("Europe/Berlin")
            now = datetime(2026, 4, 1, 21, 30, tzinfo=berlin)

            with patch("app.slideshow.local_now", return_value=now), patch(
                "app.time_utils.local_now",
                return_value=now,
            ):
                decision = compute_next_fire_decision(services)

            self.assertEqual(decision.mode, "interval")
            self.assertEqual(decision.seconds, 10 * 3600 + 30 * 60)

    async def test_scheduled_next_fire_skips_upcoming_quiet_hours(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            services = _build_services(Path(tmpdir), interval_seconds=3600)
            services.display.get_sleep_schedule = lambda: ("22:00", "08:00")
            services.database.set_setting("scheduled_change_time", "23:00")
            berlin = ZoneInfo("Europe/Berlin")
            now = datetime(2026, 4, 1, 21, 0, tzinfo=berlin)

            with patch("app.slideshow.local_now", return_value=now), patch(
                "app.time_utils.local_now",
                return_value=now,
            ):
                decision = compute_next_fire_decision(services)

            self.assertEqual(decision.mode, "scheduled_daily")
            self.assertEqual(decision.detail, "23:00")
            self.assertEqual(decision.seconds, 11 * 3600)

    async def test_project_display_offsets_skip_future_quiet_hours(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            services = _build_services(Path(tmpdir), interval_seconds=3600)
            services.display.get_sleep_schedule = lambda: ("22:00", "08:00")
            berlin = ZoneInfo("Europe/Berlin")
            now = datetime(2026, 4, 1, 20, 0, tzinfo=berlin)
            first_fire_at = now.astimezone(timezone.utc) + timedelta(hours=1)

            with patch("app.slideshow.local_now", return_value=now):
                offsets = project_display_change_offsets(services, 3, first_fire_at=first_fire_at)

            self.assertEqual(offsets, [3600, 12 * 3600, 13 * 3600])

    def assert_future_timestamp(self, raw: str | None) -> None:
        self.assertIsNotNone(raw)
        target = datetime.fromisoformat(raw)
        if target.tzinfo is None:
            target = target.replace(tzinfo=timezone.utc)
        self.assertGreater(target, datetime.now(timezone.utc))


class _FakeDisplay:
    def __init__(self, *, payload_path: Path, current_image_path: Path, interval_seconds: int) -> None:
        self.orientation = "horizontal"
        self._payload_path = payload_path
        self._current_image_path = current_image_path
        self._interval_seconds = interval_seconds
        self.display_calls: list[str] = []
        self.fail_next = False

    def current_orientation(self) -> str:
        return self.orientation

    def display(self, request) -> DisplayResult:
        if self.fail_next:
            self.fail_next = False
            return DisplayResult(False, "simulated display failure")
        self.display_calls.append(request.image_id)
        self._payload_path.parent.mkdir(parents=True, exist_ok=True)
        self._payload_path.write_text(json.dumps({"image_id": request.image_id}), encoding="utf-8")
        self._current_image_path.parent.mkdir(parents=True, exist_ok=True)
        self._current_image_path.write_bytes(request.image_id.encode("utf-8"))
        return DisplayResult(True, f"displayed {request.image_id}")

    def get_slideshow_interval(self) -> int:
        return self._interval_seconds

    def get_sleep_schedule(self):
        return None


class _FakeAuth:
    def sync_user(self, user) -> None:
        return None

    def is_whitelisted(self, user_id: int) -> bool:
        return True


class _FakeRenderer:
    def render(self, original_path: Path, output_path: Path, **_: object) -> Path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(Path(original_path).read_bytes())
        return output_path


class _FakeStorage:
    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir

    def rendered_path(self, image_id: str, extension: str = ".png") -> Path:
        return self.base_dir / "rendered" / f"{image_id}{extension}"

    def cleanup_rendered_cache(self) -> None:
        return None


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


class _FakeMessage:
    def __init__(self) -> None:
        self.replies: list[str] = []

    async def reply_text(self, text: str, **kwargs) -> None:
        self.replies.append(text)


class _MessageUpdate:
    def __init__(self) -> None:
        self.effective_message = _FakeMessage()
        self.effective_user = SimpleNamespace(id=1)


class _CommandContext:
    def __init__(self, services) -> None:
        self.application = SimpleNamespace(
            bot_data={"services": services, "display_lock": asyncio.Lock()},
            job_queue=_FakeJobQueue(),
        )
        self.user_data: dict[str, object] = {}


class _JobContext:
    def __init__(self, services) -> None:
        self.application = SimpleNamespace(
            bot_data={"services": services, "display_lock": asyncio.Lock()},
            job_queue=_FakeJobQueue(),
        )


def _build_services(base_dir: Path, *, interval_seconds: int):
    database = Database(base_dir / "photo_frame.db")
    database.initialize()
    payload_path = base_dir / "inkypi" / "current.json"
    current_image_path = base_dir / "inkypi" / "current.png"
    return SimpleNamespace(
        auth=_FakeAuth(),
        database=database,
        display=_FakeDisplay(
            payload_path=payload_path,
            current_image_path=current_image_path,
            interval_seconds=interval_seconds,
        ),
        storage=_FakeStorage(base_dir),
        renderer=_FakeRenderer(),
        config=SimpleNamespace(
            storage=SimpleNamespace(
                current_payload_path=payload_path,
                current_image_path=current_image_path,
            )
        ),
    )


def _build_services_with_real_adapter(base_dir: Path, *, interval_seconds: int):
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
        refresh_command="echo refresh",
    )
    device_config_path = base_dir / "InkyPi" / "src" / "config" / "device.json"
    device_config_path.parent.mkdir(parents=True, exist_ok=True)
    device_config_path.write_text(json.dumps({"orientation": "horizontal"}), encoding="utf-8")
    return SimpleNamespace(
        auth=_FakeAuth(),
        database=database,
        display=InkyPiAdapter(inkypi_config, storage_config, display_config, database=database),
        storage=_FakeStorage(base_dir),
        renderer=_FakeRenderer(),
        config=SimpleNamespace(
            storage=storage_config,
        ),
    )


def _seed_displayed_images(base_dir: Path, services, image_ids: list[str]) -> None:
    for index, image_id in enumerate(image_ids):
        original = base_dir / "incoming" / f"{image_id}.jpg"
        original.parent.mkdir(parents=True, exist_ok=True)
        original.write_bytes(image_id.encode("utf-8"))
        services.database.upsert_image(
            ImageRecord(
                image_id=image_id,
                telegram_file_id=f"file-{image_id}",
                telegram_chat_id=1,
                local_original_path=str(original),
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
        json.dumps({"image_id": image_ids[0]}),
        encoding="utf-8",
    )


if __name__ == "__main__":
    unittest.main()
