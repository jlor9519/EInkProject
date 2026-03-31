from __future__ import annotations

import asyncio
import logging

from telegram.ext import Application, ApplicationBuilder, CallbackQueryHandler, CommandHandler, MessageHandler, filters

from app.slideshow import schedule_slideshow_job
from app.commands import (
    cancel_command,
    delete_cancel_callback,
    delete_command,
    delete_confirm_callback,
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
from app.settings_conversation import build_settings_conversation
from app.models import AppServices

logger = logging.getLogger(__name__)

UPLOAD_QUEUE_KEY = "upload_queue"
UPLOAD_WORKER_TASK_KEY = "upload_worker_task"


async def _post_init(application: Application) -> None:
    schedule_slideshow_job(application)
    application.bot_data[UPLOAD_WORKER_TASK_KEY] = asyncio.create_task(
        _upload_worker(application),
        name="photo-upload-worker",
    )
    services = application.bot_data["services"]
    pending = services.database.reconcile_pending_images()
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
    application.add_handler(CallbackQueryHandler(delete_confirm_callback, pattern=r"^delete_confirm:"))
    application.add_handler(CallbackQueryHandler(delete_cancel_callback, pattern=r"^delete_cancel$"))
    application.add_handler(CommandHandler("refresh", refresh_command))
    application.add_handler(CommandHandler("users", users_command))
    application.add_handler(CommandHandler("unwhitelist", unwhitelist_command))
    application.add_handler(CommandHandler("cancel", cancel_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, stray_text_handler))
    return application
