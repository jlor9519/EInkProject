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
    UPLOAD_QUEUE_KEY,
    _application_error_handler,
    _post_init,
)
from app.database import Database
from app.models import DisplayResult, ImageRecord


class StartupTests(unittest.IsolatedAsyncioTestCase):
    """The Pi reboots nightly; a restart must never rotate the displayed image."""

    async def test_post_init_keeps_current_image_after_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path, interval_seconds=3600)
            _seed_displayed_images(tmpdir_path, services, ["img-1", "img-2"])
            application = _build_application(services)

            with patch("app.bot._upload_worker", new=AsyncMock(return_value=None)):
                await _post_init(application)
                await _post_init(application)

            self.assertEqual(services.display.display_calls, [])
            payload = json.loads(services.config.storage.current_payload_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["image_id"], "img-1")

    async def test_startup_schedule_respects_quiet_hours(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            services = _build_services(tmpdir_path, interval_seconds=3600)
            _seed_displayed_images(tmpdir_path, services, ["img-1", "img-2"])
            services.display.sleep_schedule = ("00:00", "08:00")
            application = _build_application(services)
            berlin = ZoneInfo("Europe/Berlin")
            now = datetime(2026, 4, 1, 1, 0, tzinfo=berlin)

            with patch(
                "app.bot._upload_worker",
                new=AsyncMock(return_value=None),
            ), patch("app.slideshow.local_now", return_value=now):
                await _post_init(application)

            self.assertEqual(services.display.display_calls, [])
            self.assertGreaterEqual(application.job_queue.calls[-1]["first"], 6 * 3600 + 50 * 60)
            self.assertLessEqual(application.job_queue.calls[-1]["first"], 7 * 3600)


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
    ) -> None:
        self.orientation = "horizontal"
        self._payload_path = payload_path
        self._current_image_path = current_image_path
        self._interval_seconds = interval_seconds
        self.display_calls: list[str] = []
        self.sleep_schedule = None

    def current_orientation(self) -> str:
        return self.orientation

    def display(self, request) -> DisplayResult:
        self.display_calls.append(request.image_id)
        self._payload_path.parent.mkdir(parents=True, exist_ok=True)
        self._payload_path.write_text(json.dumps({"image_id": request.image_id}), encoding="utf-8")
        self._current_image_path.parent.mkdir(parents=True, exist_ok=True)
        self._current_image_path.write_bytes(request.image_id.encode("utf-8"))
        return DisplayResult(True, f"displayed {request.image_id}")

    def get_slideshow_interval(self) -> int:
        return self._interval_seconds

    def get_sleep_schedule(self):
        return self.sleep_schedule


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


def _build_services(base_dir: Path, *, interval_seconds: int):
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
        ),
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
