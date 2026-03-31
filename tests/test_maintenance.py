from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app.database import Database
from app.maintenance import (
    MAINTENANCE_LOCK_KEY,
    _launch_maintenance_job_runner,
    maintenance_confirm_callback,
    notify_maintenance_updates,
    restart_command,
    update_command,
)


class MaintenanceCommandTests(unittest.IsolatedAsyncioTestCase):
    async def test_restart_and_update_commands_show_confirmation(self) -> None:
        services = _build_services(Path(tempfile.mkdtemp()))
        context = _FakeContext(services)

        restart_update = _MessageUpdate(user_id=1)
        update_update = _MessageUpdate(user_id=1)

        await restart_command(restart_update, context)
        await update_command(update_update, context)

        self.assertIn("Raspberry Pi jetzt neu starten?", restart_update.effective_message.replies[0]["text"])
        self.assertIn("Projekt jetzt aktualisieren?", update_update.effective_message.replies[0]["text"])

    async def test_confirm_callback_creates_update_job_and_starts_runner(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            services = _build_services(Path(tmpdir))
            context = _FakeContext(services)
            update = _CallbackUpdate(data="maintenance_confirm:update", user_id=1, chat_id=999)

            with patch("app.maintenance._launch_maintenance_job_runner", return_value=None):
                await maintenance_confirm_callback(update, context)

            job = services.database.get_active_maintenance_job()
            self.assertIsNotNone(job)
            self.assertEqual(job.kind, "update")
            self.assertEqual(job.telegram_chat_id, 999)
            self.assertEqual(update.callback_query.text_edits, ["Update wurde gestartet. Ich schicke nach dem Neustart das Ergebnis."])

    async def test_notify_maintenance_updates_reports_reboot_completion(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            services = _build_services(Path(tmpdir))
            services.database.create_maintenance_job(
                job_id="job-restart",
                kind="restart",
                requested_by_user_id=1,
                telegram_chat_id=333,
                log_path=str(Path(tmpdir) / "logs" / "restart.log"),
                unit_name="photo-frame-restart-job",
                status="rebooting",
            )
            application = _FakeApplication(services)

            await notify_maintenance_updates(application)

            self.assertEqual(application.bot.messages, [(333, "Der Raspberry Pi ist wieder online.")])
            self.assertIsNotNone(services.database.get_maintenance_job("job-restart").notified_at)

    def test_launch_runner_uses_systemd_run_with_detached_runner(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            services = _build_services(Path(tmpdir))
            job = services.database.create_maintenance_job(
                job_id="job-update",
                kind="update",
                requested_by_user_id=1,
                telegram_chat_id=2,
                log_path=str(Path(tmpdir) / "logs" / "update.log"),
                unit_name="photo-frame-update-job",
            )

            with patch("app.maintenance.shutil.which", return_value="/usr/bin/systemd-run"), patch(
                "app.maintenance.subprocess.run"
            ) as mock_run:
                mock_run.return_value = SimpleNamespace(returncode=0, stdout="", stderr="")
                error = _launch_maintenance_job_runner(services, job)

            self.assertIsNone(error)
            launched = mock_run.call_args.args[0]
            self.assertEqual(launched[:3], ["sudo", "-n", "/usr/bin/systemd-run"])
            self.assertIn("-m", launched)
            self.assertIn("app.maintenance_runner", launched)
            self.assertIn("--job-id", launched)
            self.assertIn("job-update", launched)


class _FakeAuth:
    def sync_user(self, user) -> None:
        return None

    def is_admin(self, user_id: int) -> bool:
        return True


class _FakeMessage:
    def __init__(self) -> None:
        self.replies: list[dict[str, object]] = []

    async def reply_text(self, text: str, **kwargs) -> None:
        self.replies.append({"text": text, "kwargs": kwargs})


class _MessageUpdate:
    def __init__(self, *, user_id: int) -> None:
        self.effective_user = SimpleNamespace(id=user_id)
        self.effective_message = _FakeMessage()


class _FakeQueryMessage:
    def __init__(self) -> None:
        self.photo = []
        self.document = None
        self.animation = None
        self.video = None


class _FakeCallbackQuery:
    def __init__(self, *, data: str) -> None:
        self.data = data
        self.message = _FakeQueryMessage()
        self.text_edits: list[str] = []

    async def answer(self) -> None:
        return None

    async def edit_message_text(self, text: str) -> None:
        self.text_edits.append(text)

    async def edit_message_caption(self, *, caption: str) -> None:
        self.text_edits.append(caption)


class _CallbackUpdate:
    def __init__(self, *, data: str, user_id: int, chat_id: int) -> None:
        self.callback_query = _FakeCallbackQuery(data=data)
        self.effective_user = SimpleNamespace(id=user_id)
        self.effective_chat = SimpleNamespace(id=chat_id)


class _FakeBot:
    def __init__(self) -> None:
        self.messages: list[tuple[int, str]] = []

    async def send_message(self, chat_id: int, text: str) -> None:
        self.messages.append((chat_id, text))


class _FakeApplication:
    def __init__(self, services) -> None:
        self.bot = _FakeBot()
        self.bot_data = {"services": services}


class _FakeContext:
    def __init__(self, services) -> None:
        self.application = SimpleNamespace(
            bot_data={
                "services": services,
                MAINTENANCE_LOCK_KEY: asyncio.Lock(),
            }
        )
        self.user_data: dict[str, object] = {}


def _build_services(base_dir: Path):
    database = Database(base_dir / "photo_frame.db")
    database.initialize()
    return SimpleNamespace(
        config_path=base_dir / "config.yaml",
        database=database,
        auth=_FakeAuth(),
    )


if __name__ == "__main__":
    unittest.main()
