from __future__ import annotations

import asyncio
import getpass
import logging
import os
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, ContextTypes

from app.auth import require_admin
from app.models import AppServices, MaintenanceJobRecord

logger = logging.getLogger(__name__)

MAINTENANCE_LOCK_KEY = "maintenance_lock"
MAINTENANCE_KINDS = {"restart", "update"}
PROJECT_ROOT = Path(__file__).resolve().parents[1]


def get_maintenance_lock(context: ContextTypes.DEFAULT_TYPE) -> asyncio.Lock:
    return context.application.bot_data[MAINTENANCE_LOCK_KEY]


def _maintenance_log_dir() -> Path:
    return PROJECT_ROOT / "logs" / "maintenance"


def _edit_message_text(job_kind: str) -> str:
    if job_kind == "restart":
        return (
            "Raspberry Pi jetzt neu starten?\n"
            "Laufende Uploads und Bildwechsel werden dabei unterbrochen."
        )
    return (
        "Projekt jetzt aktualisieren?\n"
        "Ich führe `bash scripts/update.sh` aus. Der Bot startet dabei neu."
    )


def _confirmation_markup(job_kind: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Ja, ausführen", callback_data=f"maintenance_confirm:{job_kind}"),
                InlineKeyboardButton("Abbrechen", callback_data=f"maintenance_cancel:{job_kind}"),
            ]
        ]
    )


@require_admin
async def restart_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if message is None:
        return
    await message.reply_text(
        _edit_message_text("restart"),
        reply_markup=_confirmation_markup("restart"),
        parse_mode="Markdown",
    )


@require_admin
async def update_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if message is None:
        return
    await message.reply_text(
        _edit_message_text("update"),
        reply_markup=_confirmation_markup("update"),
        parse_mode="Markdown",
    )


async def maintenance_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return
    await query.answer()
    services = context.application.bot_data["services"]
    user = update.effective_user
    if user is None:
        return
    services.auth.sync_user(user)
    if not services.auth.is_admin(user.id):
        await _edit_query_message(query, "Dieser Befehl ist nur für Admins verfügbar.")
        return

    _, _, job_kind = (query.data or "").partition(":")
    if job_kind not in MAINTENANCE_KINDS:
        await _edit_query_message(query, "Unbekannter Wartungsbefehl.")
        return

    lock = get_maintenance_lock(context)
    async with lock:
        active_job = services.database.get_active_maintenance_job()
        if active_job is not None:
            await _edit_query_message(
                query,
                f"Es läuft bereits ein Wartungsvorgang: `{_job_label(active_job.kind)}`.",
            )
            return

        chat = update.effective_chat
        if chat is None:
            await _edit_query_message(query, "Telegram-Chat konnte nicht erkannt werden.")
            return

        log_dir = _maintenance_log_dir()
        log_dir.mkdir(parents=True, exist_ok=True)
        job_id = uuid.uuid4().hex
        unit_name = f"photo-frame-{job_kind}-{job_id[:8]}"
        log_path = log_dir / f"{job_id}.log"
        job = services.database.create_maintenance_job(
            job_id=job_id,
            kind=job_kind,
            requested_by_user_id=user.id,
            telegram_chat_id=chat.id,
            log_path=str(log_path),
            unit_name=unit_name,
        )

        error_message = await asyncio.to_thread(_launch_maintenance_job_runner, services, job)
        if error_message is not None:
            services.database.mark_maintenance_job_finished(job.job_id, status="failed", last_error=error_message)
            await _edit_query_message(query, f"Start fehlgeschlagen: {error_message}")
            return

        if job_kind == "restart":
            await _edit_query_message(
                query,
                "Neustart wurde ausgelöst. Ich melde mich wieder, sobald der Pi zurück ist.",
            )
        else:
            await _edit_query_message(
                query,
                "Update wurde gestartet. Ich schicke nach dem Neustart das Ergebnis.",
            )


async def maintenance_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return
    await query.answer()
    await _edit_query_message(query, "Wartung abgebrochen.")


async def notify_maintenance_updates(application: Application) -> None:
    services: AppServices = application.bot_data["services"]

    # A pending reboot means the machine came back successfully.
    reboot_jobs = services.database.complete_rebooting_maintenance_jobs()
    for job in reboot_jobs:
        await _notify_maintenance_job(application, job)

    finished_jobs = services.database.get_unnotified_finished_maintenance_jobs()
    for job in finished_jobs:
        await _notify_maintenance_job(application, job)


async def _notify_maintenance_job(application: Application, job: MaintenanceJobRecord) -> None:
    text = _notification_text(job)
    try:
        await application.bot.send_message(job.telegram_chat_id, text)
    except Exception:
        logger.exception("Failed to send maintenance notification for %s", job.job_id)
        return
    application.bot_data["services"].database.mark_maintenance_job_notified(job.job_id)


def _notification_text(job: MaintenanceJobRecord) -> str:
    if job.kind == "restart":
        if job.status == "failed":
            details = _log_tail_summary(Path(job.log_path))
            if job.last_error and job.last_error not in details:
                details = f"{job.last_error}\n{details}".strip()
            return f"Neustart fehlgeschlagen.\n\n{details}".strip()
        return "Der Raspberry Pi ist wieder online."

    action = "Update erfolgreich abgeschlossen." if job.status == "succeeded" else "Update fehlgeschlagen."
    details = _log_tail_summary(Path(job.log_path))
    if job.last_error and job.last_error not in details:
        details = f"{job.last_error}\n{details}".strip()
    if details:
        return f"{action}\n\nLetzte Ausgaben:\n{details}"
    return action


def _log_tail_summary(log_path: Path, *, max_lines: int = 8, max_chars: int = 1200) -> str:
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return ""
    if not lines:
        return ""
    tail = "\n".join(lines[-max_lines:])
    if len(tail) > max_chars:
        tail = tail[-max_chars:]
    return tail.strip()


def _job_label(kind: str) -> str:
    return "/restart" if kind == "restart" else "/update"


def _launch_maintenance_job_runner(services: AppServices, job: MaintenanceJobRecord) -> str | None:
    systemd_run_bin = shutil.which("systemd-run")
    if not systemd_run_bin:
        return "systemd-run wurde auf dem Raspberry Pi nicht gefunden."

    config_path = str((services.config_path or (PROJECT_ROOT / "config" / "config.yaml")).resolve())
    command = [
        "sudo",
        "-n",
        systemd_run_bin,
        "--quiet",
        "--collect",
        f"--unit={job.unit_name}",
        f"--uid={getpass.getuser()}",
        f"--working-directory={PROJECT_ROOT}",
        f"--setenv=PHOTO_FRAME_CONFIG={config_path}",
        "--setenv=PYTHONUNBUFFERED=1",
        str(Path(sys.executable).resolve()),
        "-m",
        "app.maintenance_runner",
        "--job-id",
        job.job_id,
    ]

    env_file = os.getenv("PHOTO_FRAME_ENV_FILE") or os.getenv("ENV_FILE")
    if env_file:
        command.insert(8, f"--setenv=PHOTO_FRAME_ENV_FILE={env_file}")

    try:
        completed = subprocess.run(command, capture_output=True, text=True, check=False)
    except OSError as exc:
        logger.exception("Failed to launch maintenance runner")
        return str(exc)

    if completed.returncode == 0:
        return None

    stderr = (completed.stderr or completed.stdout or "").strip()
    return stderr or f"systemd-run beendete sich mit Code {completed.returncode}."


async def _edit_query_message(query, text: str) -> None:
    message = query.message
    has_media = bool(
        getattr(message, "photo", None)
        or getattr(message, "document", None)
        or getattr(message, "animation", None)
        or getattr(message, "video", None)
    )
    if has_media:
        try:
            await query.edit_message_caption(caption=text)
            return
        except Exception:
            pass
    try:
        await query.edit_message_text(text)
    except Exception:
        try:
            if not has_media:
                await query.edit_message_caption(caption=text)
        except Exception:
            logger.warning("Could not edit maintenance callback message")
