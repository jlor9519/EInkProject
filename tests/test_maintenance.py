from __future__ import annotations

import asyncio
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app.database import Database
from app.maintenance import (
    MAINTENANCE_LOCK_KEY,
    _launch_maintenance_job_runner,
    maintenance_confirm_callback,
    maintenance_cancel_callback,
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
        restart_labels = [button.text for row in restart_update.effective_message.replies[0]["kwargs"]["reply_markup"].inline_keyboard for button in row]
        update_labels = [button.text for row in update_update.effective_message.replies[0]["kwargs"]["reply_markup"].inline_keyboard for button in row]
        self.assertIn("Abbrechen", restart_labels)
        self.assertIn("Abbrechen", update_labels)

    async def test_maintenance_cancel_callback_deletes_message(self) -> None:
        services = _build_services(Path(tempfile.mkdtemp()))
        context = _FakeContext(services)
        update = _CallbackUpdate(data="maintenance_cancel:update", user_id=1, chat_id=999)

        await maintenance_cancel_callback(update, context)

        self.assertEqual(context.application.bot.deleted_messages, [(999, 222)])
        self.assertEqual(update.callback_query.text_edits, [])

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

    async def test_notify_maintenance_updates_reports_stale_update_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "logs" / "update.log"
            log_path.parent.mkdir(parents=True, exist_ok=True)
            log_path.write_text("line 1\nline 2\n", encoding="utf-8")
            services = _build_services(Path(tmpdir))
            services.database.create_maintenance_job(
                job_id="job-update-stale",
                kind="update",
                requested_by_user_id=1,
                telegram_chat_id=444,
                log_path=str(log_path),
                unit_name="photo-frame-update-job",
                status="queued",
            )
            application = _FakeApplication(services)

            await notify_maintenance_updates(application)

            self.assertEqual(len(application.bot.messages), 1)
            chat_id, text = application.bot.messages[0]
            self.assertEqual(chat_id, 444)
            self.assertIn("Update fehlgeschlagen.", text)
            self.assertIn("stale maintenance job recovered after restart", text)
            self.assertIsNotNone(services.database.get_maintenance_job("job-update-stale").notified_at)

    async def test_confirm_callback_recovers_stale_queued_update_and_starts_new_one(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            services = _build_services(Path(tmpdir))
            services.database.create_maintenance_job(
                job_id="job-stale-queued",
                kind="update",
                requested_by_user_id=1,
                telegram_chat_id=444,
                log_path=str(Path(tmpdir) / "logs" / "stale-queued.log"),
                unit_name="photo-frame-update-job-old",
            )
            old_created_at = (datetime.now(timezone.utc) - timedelta(seconds=61)).isoformat()
            services.database._connection.execute(  # noqa: SLF001 - test setup
                "UPDATE maintenance_jobs SET created_at = ? WHERE job_id = ?",
                (old_created_at, "job-stale-queued"),
            )
            services.database._connection.commit()  # noqa: SLF001 - test setup
            context = _FakeContext(services)
            update = _CallbackUpdate(data="maintenance_confirm:update", user_id=1, chat_id=999)

            with patch("app.maintenance._launch_maintenance_job_runner", return_value=None):
                await maintenance_confirm_callback(update, context)

            stale_job = services.database.get_maintenance_job("job-stale-queued")
            self.assertEqual(stale_job.status, "failed")
            self.assertEqual(stale_job.last_error, "stale maintenance job recovered before new maintenance request")
            self.assertIsNotNone(stale_job.notified_at)
            self.assertEqual(context.application.bot.messages[0][0], 444)
            self.assertIn("stale maintenance job recovered before new maintenance request", context.application.bot.messages[0][1])

            active_job = services.database.get_active_maintenance_job()
            self.assertIsNotNone(active_job)
            self.assertNotEqual(active_job.job_id, "job-stale-queued")
            self.assertEqual(active_job.kind, "update")
            self.assertEqual(update.callback_query.text_edits[-1], "Update wurde gestartet. Ich schicke nach dem Neustart das Ergebnis.")

    async def test_confirm_callback_recovers_stale_running_update_and_starts_new_one(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            services = _build_services(Path(tmpdir))
            services.database.create_maintenance_job(
                job_id="job-stale-running",
                kind="update",
                requested_by_user_id=1,
                telegram_chat_id=444,
                log_path=str(Path(tmpdir) / "logs" / "stale-running.log"),
                unit_name="photo-frame-update-job-old",
            )
            services.database.mark_maintenance_job_running("job-stale-running")
            old_started_at = (datetime.now(timezone.utc) - timedelta(minutes=11)).isoformat()
            services.database._connection.execute(  # noqa: SLF001 - test setup
                "UPDATE maintenance_jobs SET started_at = ? WHERE job_id = ?",
                (old_started_at, "job-stale-running"),
            )
            services.database._connection.commit()  # noqa: SLF001 - test setup
            context = _FakeContext(services)
            update = _CallbackUpdate(data="maintenance_confirm:update", user_id=1, chat_id=999)

            with patch("app.maintenance._launch_maintenance_job_runner", return_value=None):
                await maintenance_confirm_callback(update, context)

            stale_job = services.database.get_maintenance_job("job-stale-running")
            self.assertEqual(stale_job.status, "failed")
            self.assertEqual(stale_job.last_error, "stale maintenance job recovered before new maintenance request")
            self.assertIsNotNone(stale_job.notified_at)
            self.assertEqual(context.application.bot.messages[0][0], 444)
            self.assertIn("stale maintenance job recovered before new maintenance request", context.application.bot.messages[0][1])

            active_job = services.database.get_active_maintenance_job()
            self.assertIsNotNone(active_job)
            self.assertNotEqual(active_job.job_id, "job-stale-running")
            self.assertEqual(active_job.kind, "update")

    async def test_confirm_callback_blocks_when_running_update_is_fresh(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            services = _build_services(Path(tmpdir))
            services.database.create_maintenance_job(
                job_id="job-fresh-running",
                kind="update",
                requested_by_user_id=1,
                telegram_chat_id=444,
                log_path=str(Path(tmpdir) / "logs" / "fresh-running.log"),
                unit_name="photo-frame-update-job-old",
            )
            services.database.mark_maintenance_job_running("job-fresh-running")
            context = _FakeContext(services)
            update = _CallbackUpdate(data="maintenance_confirm:update", user_id=1, chat_id=999)

            with patch("app.maintenance._launch_maintenance_job_runner", return_value=None):
                await maintenance_confirm_callback(update, context)

            self.assertEqual(
                update.callback_query.text_edits,
                ["Es läuft bereits ein Wartungsvorgang: `/update` (Status: `running`)."],
            )
            self.assertEqual(len(context.application.bot.messages), 0)

    async def test_confirm_callback_does_not_recover_rebooting_restart_job(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            services = _build_services(Path(tmpdir))
            services.database.create_maintenance_job(
                job_id="job-restart",
                kind="restart",
                requested_by_user_id=1,
                telegram_chat_id=444,
                log_path=str(Path(tmpdir) / "logs" / "restart.log"),
                unit_name="photo-frame-restart-job",
                status="rebooting",
            )
            context = _FakeContext(services)
            update = _CallbackUpdate(data="maintenance_confirm:update", user_id=1, chat_id=999)

            with patch("app.maintenance._launch_maintenance_job_runner", return_value=None):
                await maintenance_confirm_callback(update, context)

            self.assertEqual(
                update.callback_query.text_edits,
                ["Es läuft bereits ein Wartungsvorgang: `/restart` (Status: `rebooting`)."],
            )
            self.assertEqual(services.database.get_maintenance_job("job-restart").status, "rebooting")

    async def test_confirm_callback_recovers_stale_queued_restart_and_starts_new_update(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            services = _build_services(Path(tmpdir))
            services.database.create_maintenance_job(
                job_id="job-stale-restart-queued",
                kind="restart",
                requested_by_user_id=1,
                telegram_chat_id=444,
                log_path=str(Path(tmpdir) / "logs" / "stale-restart-queued.log"),
                unit_name="photo-frame-restart-job-old",
            )
            old_created_at = (datetime.now(timezone.utc) - timedelta(seconds=61)).isoformat()
            services.database._connection.execute(  # noqa: SLF001 - test setup
                "UPDATE maintenance_jobs SET created_at = ? WHERE job_id = ?",
                (old_created_at, "job-stale-restart-queued"),
            )
            services.database._connection.commit()  # noqa: SLF001 - test setup
            context = _FakeContext(services)
            update = _CallbackUpdate(data="maintenance_confirm:update", user_id=1, chat_id=999)

            with patch("app.maintenance._launch_maintenance_job_runner", return_value=None):
                await maintenance_confirm_callback(update, context)

            stale_job = services.database.get_maintenance_job("job-stale-restart-queued")
            self.assertEqual(stale_job.status, "failed")
            self.assertEqual(stale_job.last_error, "stale maintenance job recovered before new maintenance request")
            self.assertIsNotNone(stale_job.notified_at)

            active_job = services.database.get_active_maintenance_job()
            self.assertIsNotNone(active_job)
            self.assertEqual(active_job.kind, "update")

    async def test_confirm_callback_recovers_stale_running_restart_and_starts_new_update(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            services = _build_services(Path(tmpdir))
            services.database.create_maintenance_job(
                job_id="job-stale-restart-running",
                kind="restart",
                requested_by_user_id=1,
                telegram_chat_id=444,
                log_path=str(Path(tmpdir) / "logs" / "stale-restart-running.log"),
                unit_name="photo-frame-restart-job-old",
            )
            services.database.mark_maintenance_job_running("job-stale-restart-running")
            old_started_at = (datetime.now(timezone.utc) - timedelta(minutes=3)).isoformat()
            services.database._connection.execute(  # noqa: SLF001 - test setup
                "UPDATE maintenance_jobs SET started_at = ? WHERE job_id = ?",
                (old_started_at, "job-stale-restart-running"),
            )
            services.database._connection.commit()  # noqa: SLF001 - test setup
            context = _FakeContext(services)
            update = _CallbackUpdate(data="maintenance_confirm:update", user_id=1, chat_id=999)

            with patch("app.maintenance._launch_maintenance_job_runner", return_value=None):
                await maintenance_confirm_callback(update, context)

            stale_job = services.database.get_maintenance_job("job-stale-restart-running")
            self.assertEqual(stale_job.status, "failed")
            self.assertEqual(stale_job.last_error, "stale maintenance job recovered before new maintenance request")
            self.assertIsNotNone(stale_job.notified_at)

            active_job = services.database.get_active_maintenance_job()
            self.assertIsNotNone(active_job)
            self.assertEqual(active_job.kind, "update")

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
        self.chat_id = 999
        self.message_id = 222


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
        self.deleted_messages: list[tuple[int | None, int | None]] = []

    async def send_message(self, chat_id: int, text: str) -> None:
        self.messages.append((chat_id, text))

    async def delete_message(self, chat_id=None, message_id=None) -> None:
        self.deleted_messages.append((chat_id, message_id))


class _FakeApplication:
    def __init__(self, services) -> None:
        self.bot = _FakeBot()
        self.bot_data = {"services": services}


class _FakeContext:
    def __init__(self, services) -> None:
        self.application = SimpleNamespace(
            bot=_FakeBot(),
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
