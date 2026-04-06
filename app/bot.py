from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from telegram.error import NetworkError, TimedOut
from telegram.ext import Application, ApplicationBuilder, CallbackQueryHandler, CommandHandler, MessageHandler, filters

from app.slideshow import schedule_slideshow_job
from app.commands import (
    _advance_once_to_next_target,
    _delete_back_callback,
    _delete_cancel_callback,
    _delete_confirm_callback,
    _delete_page_callback,
    _delete_select_callback,
    command_action_callback,
    delete_command,
    help_command,
    list_command,
    myid_command,
    next_command,
    prev_command,
    refresh_command,
    status_command,
    stray_text_handler,
    unwhitelist_command,
    users_command,
    whitelist_command,
)
from app.conversations import build_photo_conversation, process_queued_upload
from app.display_state import DISPLAY_TRANSITION_KEYS, read_current_payload_image_id
from app.settings_conversation import build_settings_conversation
from app.models import AppServices

logger = logging.getLogger(__name__)

UPLOAD_QUEUE_KEY = "upload_queue"
UPLOAD_WORKER_TASK_KEY = "upload_worker_task"
LAST_HANDLED_BOOT_ID_KEY = "last_handled_boot_id"
BOOT_ID_PATH = Path("/proc/sys/kernel/random/boot_id")


def _exc_info_tuple(error: BaseException) -> tuple[type[BaseException], BaseException, object]:
    return (type(error), error, error.__traceback__)


async def _application_error_handler(update: object, context) -> None:
    error = context.error
    if isinstance(error, (NetworkError, TimedOut)) and update is None:
        logger.warning("Telegram polling network error; retrying: %s", error)
        return

    if isinstance(error, (NetworkError, TimedOut)):
        update_id = getattr(update, "update_id", None)
        logger.warning(
            "Telegram network error while processing update %s; user may need to retry: %s",
            update_id,
            error,
        )
        return

    if error is None:
        logger.error("Unhandled Telegram application error without exception context")
        return

    application = getattr(context, "application", None)
    if application is not None:
        services = application.bot_data.get("services")
        if services is not None:
            services.database.log_error("telegram", str(error))

    update_id = getattr(update, "update_id", None)
    if update_id is None:
        logger.error(
            "Unhandled Telegram application error without update context",
            exc_info=_exc_info_tuple(error),
        )
        return

    logger.error(
        "Unhandled Telegram application error while processing update %s",
        update_id,
        exc_info=_exc_info_tuple(error),
    )


def _read_current_boot_id(path: Path = BOOT_ID_PATH) -> str | None:
    try:
        boot_id = path.read_text(encoding="utf-8").strip()
    except OSError:
        logger.warning("Could not read Linux boot id from %s; skipping boot-time image advance", path)
        return None
    return boot_id or None


async def _maybe_advance_after_boot(application: Application, current_image_id: str | None) -> None:
    services = application.bot_data["services"]
    current_boot_id = _read_current_boot_id()
    if current_boot_id is None:
        return

    last_handled_boot_id = services.database.get_setting(LAST_HANDLED_BOOT_ID_KEY)
    if current_boot_id == last_handled_boot_id:
        logger.debug("Boot-time image advance already handled for boot id %s", current_boot_id)
        return

    try:
        wait_until_ready = getattr(services.display, "wait_until_ready", None)
        if callable(wait_until_ready):
            readiness_error = await asyncio.to_thread(wait_until_ready)
            if readiness_error is not None:
                logger.warning("Skipping boot-time image advance because display backend is not ready: %s", readiness_error)
                return

        if not current_image_id:
            logger.info("Skipping boot-time image advance because no current payload image exists")
            return

        active_orientation = services.display.current_orientation()
        lock: asyncio.Lock = application.bot_data["display_lock"]
        async with lock:
            target, result, _ = await _advance_once_to_next_target(
                services,
                current_image_id=current_image_id,
                active_orientation=active_orientation,
                transition_kind="boot_advance",
                allow_rendered_without_current=False,
            )

        if target is None:
            logger.info("Boot-time image advance skipped because no eligible next image was found")
            return
        if result is None:
            logger.info("Boot-time image advance skipped before display attempt")
            return
        if result.success:
            logger.info("Boot-time image advance displayed %s", target.image_id)
            return

        logger.warning("Boot-time image advance failed for %s: %s", target.image_id, result.message)
    finally:
        services.database.set_setting(LAST_HANDLED_BOOT_ID_KEY, current_boot_id)


async def _post_init(application: Application) -> None:
    services = application.bot_data["services"]
    current_image_id = read_current_payload_image_id(services.config.storage.current_payload_path)
    pending = services.database.reconcile_runtime_state(
        current_image_id,
        transition_keys=DISPLAY_TRANSITION_KEYS,
    )
    schedule_slideshow_job(application)
    application.bot_data[UPLOAD_WORKER_TASK_KEY] = asyncio.create_task(
        _upload_worker(application),
        name="photo-upload-worker",
    )
    queue: asyncio.Queue[str] = application.bot_data[UPLOAD_QUEUE_KEY]
    for record in pending:
        await queue.put(record.image_id)
    if pending:
        logger.info("Re-enqueued %d pending upload(s) on startup", len(pending))


async def _post_shutdown(application: Application) -> None:
    task: asyncio.Task[None] | None = application.bot_data.get(UPLOAD_WORKER_TASK_KEY)
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def _upload_worker(application: Application) -> None:
    queue: asyncio.Queue[str] = application.bot_data[UPLOAD_QUEUE_KEY]
    while True:
        image_id = await queue.get()
        try:
            await process_queued_upload(application, image_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Upload worker crashed while processing %s", image_id)
        finally:
            queue.task_done()


def build_application(services: AppServices) -> Application:
    application = ApplicationBuilder().token(services.config.telegram.bot_token).build()
    application.bot_data["services"] = services
    application.bot_data["display_lock"] = asyncio.Lock()
    application.bot_data[UPLOAD_QUEUE_KEY] = asyncio.Queue()
    application.post_init = _post_init
    application.post_shutdown = _post_shutdown
    application.add_error_handler(_application_error_handler)

    application.add_handler(build_photo_conversation())
    application.add_handler(build_settings_conversation())
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("myid", myid_command))
    application.add_handler(CommandHandler("whitelist", whitelist_command))
    application.add_handler(CommandHandler("next", next_command))
    application.add_handler(CommandHandler("prev", prev_command))
    application.add_handler(CommandHandler("list", list_command))
    application.add_handler(CommandHandler("delete", delete_command))
    application.add_handler(CallbackQueryHandler(_delete_page_callback, pattern=r"^del\|p\|"))
    application.add_handler(CallbackQueryHandler(_delete_select_callback, pattern=r"^del\|s\|"))
    application.add_handler(CallbackQueryHandler(_delete_confirm_callback, pattern=r"^del\|y\|"))
    application.add_handler(CallbackQueryHandler(_delete_back_callback, pattern=r"^del\|b\|"))
    application.add_handler(CallbackQueryHandler(_delete_cancel_callback, pattern=r"^del\|c$"))
    application.add_handler(CallbackQueryHandler(command_action_callback, pattern=r"^cmd\|"))
    application.add_handler(CommandHandler("refresh", refresh_command))
    application.add_handler(CommandHandler("users", users_command))
    application.add_handler(CommandHandler("unwhitelist", unwhitelist_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, stray_text_handler))
    return application
