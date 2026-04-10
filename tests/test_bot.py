from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

from telegram.error import NetworkError, TimedOut

from app.bot import (
    LAST_HANDLED_BOOT_ID_KEY,
    UPLOAD_QUEUE_KEY,
    _application_error_handler,
    _maybe_advance_after_boot,
    _post_init,
)
from app.database import Database
from app.models import DisplayResult, ImageRecord


class BootAdvanceTests(unittest.IsolatedAsyncioTestCase):
    async def test_post_init_advances_once_after_new_boot_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path, interval_seconds=3600)
            _seed_displayed_images(tmpdir_path, services, ["img-1", "img-2"])
            application = _build_application(services)

            with patch("app.bot._read_current_boot_id", return_value="boot-a"), patch(
                "app.bot._upload_worker",
                new=AsyncMock(return_value=None),
            ):
                await _post_init(application)

            self.assertEqual(services.display.display_calls, [])
            self.assertIsNone(services.database.get_setting(LAST_HANDLED_BOOT_ID_KEY))
            payload = json.loads(services.config.storage.current_payload_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["image_id"], "img-1")

    async def test_post_init_does_not_advance_again_on_same_boot(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path, interval_seconds=3600)
            _seed_displayed_images(tmpdir_path, services, ["img-1", "img-2"])
            application = _build_application(services)

            with patch("app.bot._read_current_boot_id", return_value="boot-a"), patch(
                "app.bot._upload_worker",
                new=AsyncMock(return_value=None),
            ):
                await _post_init(application)
                services.display.display_calls.clear()
                await _post_init(application)

            self.assertEqual(services.display.display_calls, [])

    async def test_post_init_skips_boot_advance_when_boot_id_is_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path, interval_seconds=3600)
            _seed_displayed_images(tmpdir_path, services, ["img-1", "img-2"])
            application = _build_application(services)

            with patch("app.bot._read_current_boot_id", return_value=None), patch(
                "app.bot._upload_worker",
                new=AsyncMock(return_value=None),
            ):
                await _post_init(application)

            self.assertEqual(services.display.display_calls, [])
            self.assertIsNone(services.database.get_setting(LAST_HANDLED_BOOT_ID_KEY))

    async def test_boot_advance_promotes_waiting_rendered_image_first(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path, interval_seconds=3600)
            _seed_displayed_images(tmpdir_path, services, ["img-1", "img-2"])
            queued = _seed_image(
                tmpdir_path,
                services,
                image_id="img-rendered",
                created_at="2026-03-18T12:30:00+00:00",
                status="rendered",
            )
            application = _build_application(services)

            with patch("app.bot._read_current_boot_id", return_value="boot-a"):
                await _maybe_advance_after_boot(application, "img-1")

            self.assertEqual(services.display.display_calls[-1], "img-rendered")
            self.assertEqual(services.database.get_image_by_id("img-rendered").status, "displayed")
            self.assertIsNotNone(services.database.get_setting("last_new_image_displayed_at"))
            self.assertTrue(Path(queued.local_rendered_path or "").exists())

    async def test_boot_advance_uses_next_displayed_image_when_no_rendered_image_waits(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path, interval_seconds=3600)
            _seed_displayed_images(tmpdir_path, services, ["img-1", "img-2"])
            application = _build_application(services)

            with patch("app.bot._read_current_boot_id", return_value="boot-a"):
                await _maybe_advance_after_boot(application, "img-1")

            self.assertEqual(services.display.display_calls[-1], "img-2")
            self.assertIsNone(services.database.get_setting("last_new_image_displayed_at"))

    async def test_boot_advance_is_noop_when_only_one_image_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path, interval_seconds=3600)
            _seed_displayed_images(tmpdir_path, services, ["img-1"])
            application = _build_application(services)

            with patch("app.bot._read_current_boot_id", return_value="boot-a"):
                await _maybe_advance_after_boot(application, "img-1")

            self.assertEqual(services.display.display_calls, [])
            self.assertEqual(services.database.get_setting(LAST_HANDLED_BOOT_ID_KEY), "boot-a")

    async def test_boot_advance_marks_boot_handled_even_without_current_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path, interval_seconds=3600)
            application = _build_application(services)

            with patch("app.bot._read_current_boot_id", return_value="boot-a"):
                await _maybe_advance_after_boot(application, None)

            self.assertEqual(services.display.display_calls, [])
            self.assertEqual(services.database.get_setting(LAST_HANDLED_BOOT_ID_KEY), "boot-a")

    async def test_readiness_failure_skips_boot_advance_without_retrying_same_boot(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path, interval_seconds=3600)
            _seed_displayed_images(tmpdir_path, services, ["img-1", "img-2"])
            services.display.readiness_error = "backend not ready"
            application = _build_application(services)

            with patch("app.bot._read_current_boot_id", return_value="boot-a"), patch(
                "app.bot._upload_worker",
                new=AsyncMock(return_value=None),
            ):
                await _post_init(application)
                services.display.readiness_error = None
                await _post_init(application)

            self.assertEqual(services.display.display_calls, [])
            self.assertIsNone(services.database.get_setting(LAST_HANDLED_BOOT_ID_KEY))

    async def test_boot_advance_ignores_quiet_hours_but_startup_schedule_still_respects_them(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path, interval_seconds=3600)
            _seed_displayed_images(tmpdir_path, services, ["img-1", "img-2"])
            services.display.sleep_schedule = ("00:00", "08:00")
            application = _build_application(services)
            berlin = ZoneInfo("Europe/Berlin")
            now = datetime(2026, 4, 1, 1, 0, tzinfo=berlin)

            with patch("app.bot._read_current_boot_id", return_value="boot-a"), patch(
                "app.bot._upload_worker",
                new=AsyncMock(return_value=None),
            ), patch("app.slideshow.local_now", return_value=now):
                await _post_init(application)

            self.assertEqual(services.display.display_calls, [])
            self.assertGreaterEqual(application.job_queue.calls[-1]["first"], 6 * 3600 + 50 * 60)
            self.assertLessEqual(application.job_queue.calls[-1]["first"], 7 * 3600)

    async def test_post_init_advances_after_new_boot_with_command_mode_display(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(
                tmpdir_path,
                interval_seconds=3600,
                display_update_method="command",
            )
            _seed_displayed_images(tmpdir_path, services, ["img-1", "img-2"])
            services.display.readiness_error = "inkypi.service ist nicht aktiv geworden."
            application = _build_application(services)

            with patch("app.bot._read_current_boot_id", return_value="boot-command"), patch(
                "app.bot._upload_worker",
                new=AsyncMock(return_value=None),
            ):
                await _post_init(application)

            self.assertEqual(services.display.display_calls, [])
            self.assertIsNone(services.database.get_setting(LAST_HANDLED_BOOT_ID_KEY))


class ErrorHandlerTests(unittest.IsolatedAsyncioTestCase):
    async def test_application_error_handler_logs_polling_network_errors_as_warning(self) -> None:
        context = SimpleNamespace(error=NetworkError("httpx.ReadError: boom"))

        with patch("app.bot.logger") as mock_logger:
            await _application_error_handler(None, context)

        mock_logger.warning.assert_called_once()
        mock_logger.error.assert_not_called()

    async def test_application_error_handler_downgrades_network_errors_during_update_processing(self) -> None:
        context = SimpleNamespace(error=TimedOut())
        update = SimpleNamespace(update_id=456)

        with patch("app.bot.logger") as mock_logger:
            await _application_error_handler(update, context)

        mock_logger.warning.assert_called_once()
        mock_logger.error.assert_not_called()

    async def test_application_error_handler_logs_non_network_errors_with_traceback(self) -> None:
        error = RuntimeError("boom")
        context = SimpleNamespace(error=error)
        update = SimpleNamespace(update_id=123)

        with patch("app.bot.logger") as mock_logger:
            await _application_error_handler(update, context)

        mock_logger.error.assert_called_once()
        mock_logger.warning.assert_not_called()


class _FakeDisplay:
    def __init__(
        self,
        *,
        payload_path: Path,
        current_image_path: Path,
        interval_seconds: int,
        update_method: str = "http_update_now",
    ) -> None:
        self.orientation = "horizontal"
        self._payload_path = payload_path
        self._current_image_path = current_image_path
        self._interval_seconds = interval_seconds
        self._update_method = update_method
        self.display_calls: list[str] = []
        self.sleep_schedule = None
        self.readiness_error: str | None = None

    def current_orientation(self) -> str:
        return self.orientation

    def display(self, request) -> DisplayResult:
        self.display_calls.append(request.image_id)
        self._payload_path.parent.mkdir(parents=True, exist_ok=True)
        self._payload_path.write_text(json.dumps({"image_id": request.image_id}), encoding="utf-8")
        self._current_image_path.parent.mkdir(parents=True, exist_ok=True)
        self._current_image_path.write_bytes(request.image_id.encode("utf-8"))
        return DisplayResult(True, f"displayed {request.image_id}")

    def wait_until_ready(self) -> str | None:
        if self._update_method == "command":
            return None
        return self.readiness_error

    def get_slideshow_interval(self) -> int:
        return self._interval_seconds

    def get_sleep_schedule(self):
        return self.sleep_schedule


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


def _build_application(services):
    return SimpleNamespace(
        bot_data={
            "services": services,
            "display_lock": asyncio.Lock(),
            UPLOAD_QUEUE_KEY: asyncio.Queue(),
        },
        job_queue=_FakeJobQueue(),
    )


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


def _build_services(base_dir: Path, *, interval_seconds: int, display_update_method: str = "http_update_now"):
    database = Database(base_dir / "photo_frame.db")
    database.initialize()
    payload_path = base_dir / "inkypi" / "current.json"
    current_image_path = base_dir / "inkypi" / "current.png"
    return SimpleNamespace(
        database=database,
        display=_FakeDisplay(
            payload_path=payload_path,
            current_image_path=current_image_path,
            interval_seconds=interval_seconds,
            update_method=display_update_method,
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


def _seed_displayed_images(base_dir: Path, services, image_ids: list[str]) -> None:
    for index, image_id in enumerate(image_ids):
        _seed_image(
            base_dir,
            services,
            image_id=image_id,
            created_at=f"2026-03-18T12:{index:02d}:00+00:00",
            status="displayed",
        )
    services.config.storage.current_payload_path.parent.mkdir(parents=True, exist_ok=True)
    services.config.storage.current_payload_path.write_text(
        json.dumps({"image_id": image_ids[0]}),
        encoding="utf-8",
    )


def _seed_image(
    base_dir: Path,
    services,
    *,
    image_id: str,
    created_at: str,
    status: str,
) -> ImageRecord:
    original = base_dir / "incoming" / f"{image_id}.jpg"
    original.parent.mkdir(parents=True, exist_ok=True)
    original.write_bytes(image_id.encode("utf-8"))
    record = ImageRecord(
        image_id=image_id,
        telegram_file_id=f"file-{image_id}",
        telegram_chat_id=1,
        local_original_path=str(original),
        local_rendered_path=None,
        location="",
        taken_at="",
        caption=image_id,
        uploaded_by=1,
        created_at=created_at,
        status=status,
        last_error=None,
        orientation_bucket="horizontal",
    )
    services.database.upsert_image(record)
    return record


if __name__ == "__main__":
    unittest.main()
