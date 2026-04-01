from __future__ import annotations

import asyncio
import logging
from io import BytesIO
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import TimedOut
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from app.auth import require_whitelist
from app.commands import _delete_query_message, get_display_lock, get_services
from app.database import utcnow_iso
from app.display_state import (
    DISPLAY_TRANSITION_KEYS,
    begin_display_transition,
    commit_display_success,
)
from app.fs_utils import safe_unlink
from app.models import DisplayRequest, ImageRecord
from app.orientation import format_orientation_label, orientation_matches
from app.time_utils import local_today_iso

(
    WAITING_FOR_TEXT_CHOICE,
    WAITING_FOR_LOCATION,
    WAITING_FOR_TAKEN_AT,
    WAITING_FOR_CAPTION,
    WAITING_FOR_PREVIEW_CONFIRM,
) = range(5)
PENDING_SUBMISSION_KEY = "pending_submission"
UPLOAD_QUEUE_KEY = "upload_queue"


def _discard_pending_submission(context: ContextTypes.DEFAULT_TYPE) -> None:
    pending = context.user_data.get(PENDING_SUBMISSION_KEY)
    if isinstance(pending, dict):
        original_path = pending.get("original_path")
        if original_path:
            safe_unlink(original_path, logger=logger)
    context.user_data.pop(PENDING_SUBMISSION_KEY, None)


def _location_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Überspringen", callback_data="photo_skip_location"),
            InlineKeyboardButton("Abbrechen", callback_data="photo_cancel"),
        ],
    ])


def _date_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Heute", callback_data="photo_date_today")],
        [
            InlineKeyboardButton("Überspringen", callback_data="photo_skip_date"),
            InlineKeyboardButton("Abbrechen", callback_data="photo_cancel"),
        ],
    ])


def _caption_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Überspringen", callback_data="photo_skip_caption"),
            InlineKeyboardButton("Abbrechen", callback_data="photo_cancel"),
        ],
    ])


def _caption_prompt(pending: dict[str, Any]) -> str:
    lines = [
        "Welche Bildunterschrift soll unter dem Foto angezeigt werden?",
        "",
        "Schreibe den Text in das Textfeld oder wähle eine Option.",
    ]
    suggested_caption = str(pending.get("caption") or "").strip()
    if suggested_caption:
        lines.extend(["", f"Aktueller Vorschlag: {suggested_caption}"])
    return "\n".join(lines)


@require_whitelist(conversation=True)
async def photo_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    services = get_services(context)
    user = update.effective_user
    message = update.effective_message
    if user is None or message is None or not message.photo:
        return ConversationHandler.END

    if context.user_data.get(PENDING_SUBMISSION_KEY):
        await message.reply_text("Du hast bereits einen Upload in Bearbeitung. Beantworte die Fragen oder nutze den Button „Abbrechen“.")
        return WAITING_FOR_TEXT_CHOICE

    photo = message.photo[-1]
    image_id = services.storage.generate_image_id()
    original_path = services.storage.original_path(image_id)

    try:
        logger.info("Downloading photo %s from user %d", image_id, user.id)
        telegram_file = await photo.get_file()
        await telegram_file.download_to_drive(custom_path=str(original_path))
    except Exception as exc:  # pragma: no cover - depends on Telegram runtime
        logger.exception("Failed to download photo %s from Telegram", image_id)
        await message.reply_text(f"Fehler beim Herunterladen des Fotos von Telegram: {exc}")
        return ConversationHandler.END

    context.user_data[PENDING_SUBMISSION_KEY] = {
        "image_id": image_id,
        "telegram_file_id": photo.file_id,
        "original_path": str(original_path),
        "caption": (message.caption or "").strip(),
        "orientation_bucket": services.display.current_orientation(),
    }

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Ja", callback_data="photo_text_yes"),
            InlineKeyboardButton("Nein", callback_data="photo_text_no"),
        ],
        [InlineKeyboardButton("Abbrechen", callback_data="photo_cancel")],
    ])
    await message.reply_text(
        "Möchtest du Text hinzufügen (Ort, Datum, Bildunterschrift)?",
        reply_markup=keyboard,
    )
    return WAITING_FOR_TEXT_CHOICE


async def receive_text_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    pending = context.user_data.get(PENDING_SUBMISSION_KEY)
    if update.effective_message is None or pending is None:
        return ConversationHandler.END
    text = (update.effective_message.text or "").strip().lower()
    if text in ("ja", "j"):
        await update.effective_message.reply_text(
            "Wo wurde dieses Foto aufgenommen?\n\nSchreibe den Ort in das Textfeld oder wähle eine Option.",
            reply_markup=_location_keyboard(),
        )
        return WAITING_FOR_LOCATION
    if text in ("nein", "n"):
        return await _submit_photo(update, context, show_caption=False)
    await update.effective_message.reply_text("Bitte antworte mit Ja/J oder Nein/N, oder nutze den Button „Abbrechen“.")
    return WAITING_FOR_TEXT_CHOICE


async def receive_location(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    pending = context.user_data.get(PENDING_SUBMISSION_KEY)
    if update.effective_message is None or pending is None:
        return ConversationHandler.END
    pending["location"] = (update.effective_message.text or "").strip()
    await update.effective_message.reply_text(
        "Wann wurde es aufgenommen?\n\nSchreibe das Datum in das Textfeld (z.B. 2026-03-15 oder Sommer 2025) oder wähle eine Option.",
        reply_markup=_date_keyboard(),
    )
    return WAITING_FOR_TAKEN_AT


async def receive_taken_at(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    pending = context.user_data.get(PENDING_SUBMISSION_KEY)
    if update.effective_message is None or pending is None:
        return ConversationHandler.END
    pending["taken_at"] = (update.effective_message.text or "").strip()
    await update.effective_message.reply_text(
        _caption_prompt(pending),
        reply_markup=_caption_keyboard(),
    )
    return WAITING_FOR_CAPTION


async def receive_caption(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    pending = context.user_data.get(PENDING_SUBMISSION_KEY)
    if update.effective_message is None or pending is None:
        return ConversationHandler.END
    pending["caption"] = (update.effective_message.text or "").strip()
    return await _show_preview(update.effective_message, context)


async def _send_preview_photo(
    message,
    preview_buf: BytesIO,
    *,
    caption: str,
    reply_markup: InlineKeyboardMarkup,
) -> None:
    preview_buf.name = "preview.jpg"
    try:
        preview_buf.seek(0)
        await message.reply_photo(
            photo=preview_buf,
            caption=caption,
            reply_markup=reply_markup,
            write_timeout=60,
        )
    except TimedOut:
        logger.warning("Preview send timed out; retrying once with the same rendered buffer")
        preview_buf.seek(0)
        await message.reply_photo(
            photo=preview_buf,
            caption=caption,
            reply_markup=reply_markup,
            write_timeout=60,
        )
        logger.info("Preview send retry succeeded")


async def _show_preview(message, context: ContextTypes.DEFAULT_TYPE) -> int:
    pending = context.user_data.get(PENDING_SUBMISSION_KEY)
    if pending is None:
        return ConversationHandler.END

    location = pending.get("location", "")
    taken_at = pending.get("taken_at", "")
    caption = pending.get("caption", "")

    lines = ["Vorschau:"]
    if location:
        lines.append(f"Ort: {location}")
    if taken_at:
        lines.append(f"Datum: {taken_at}")
    if caption:
        lines.append(f"Text: {caption}")
    if not any([location, taken_at, caption]):
        lines.append("(Kein Text)")
    preview_text = "\n".join(lines)

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Senden", callback_data="photo_confirm_send"),
            InlineKeyboardButton("Abbrechen", callback_data="photo_cancel"),
        ],
    ])

    original_path = Path(pending["original_path"])
    if original_path.exists():
        services = get_services(context)
        try:
            orientation = pending.get("orientation_bucket") or services.display.current_orientation()
            fit_mode = services.database.get_setting("image_fit_mode") or "fill"
            preview_buf = await asyncio.to_thread(
                services.renderer.compose_preview,
                original_path,
                location=location,
                taken_at=taken_at,
                caption=caption,
                orientation=orientation,
                fit_mode=fit_mode,
            )
        except Exception as exc:
            logger.exception("Preview render failed, falling back to original")
            preview_text += f"\n\n⚠ Vorschau-Rendering fehlgeschlagen: {type(exc).__name__}: {exc}"
            with open(original_path, "rb") as photo:
                await message.reply_photo(photo=photo, caption=preview_text, reply_markup=keyboard)
        else:
            try:
                await _send_preview_photo(
                    message,
                    preview_buf,
                    caption=preview_text,
                    reply_markup=keyboard,
                )
            except TimedOut as exc:
                logger.warning("Preview send retry failed with timeout")
                preview_text += f"\n\n⚠ Vorschau konnte nicht an Telegram gesendet werden: {type(exc).__name__}: {exc}"
                with open(original_path, "rb") as photo:
                    await message.reply_photo(photo=photo, caption=preview_text, reply_markup=keyboard)
            except Exception as exc:
                logger.exception("Preview send failed, falling back to original")
                preview_text += f"\n\n⚠ Vorschau konnte nicht an Telegram gesendet werden: {type(exc).__name__}: {exc}"
                with open(original_path, "rb") as photo:
                    await message.reply_photo(photo=photo, caption=preview_text, reply_markup=keyboard)
    else:
        await message.reply_text(preview_text, reply_markup=keyboard)
    return WAITING_FOR_PREVIEW_CONFIRM


async def photo_button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if query is None:
        return ConversationHandler.END
    await query.answer()

    pending = context.user_data.get(PENDING_SUBMISSION_KEY)
    if pending is None:
        await query.edit_message_text("Upload-Sitzung abgelaufen. Sende das Foto erneut.")
        return ConversationHandler.END

    data = query.data or ""

    if data == "photo_text_yes":
        await query.edit_message_text("Möchtest du Text hinzufügen? Ja")
        await query.message.reply_text(
            "Wo wurde dieses Foto aufgenommen?\n\nSchreibe den Ort in das Textfeld oder wähle eine Option.",
            reply_markup=_location_keyboard(),
        )
        return WAITING_FOR_LOCATION

    if data == "photo_text_no":
        await query.edit_message_text("Möchtest du Text hinzufügen? Nein")
        return await _submit_photo(update, context, show_caption=False)

    if data == "photo_skip_location":
        pending["location"] = ""
        await query.edit_message_text("Ort: übersprungen")
        await query.message.reply_text(
            "Wann wurde es aufgenommen?\n\nSchreibe das Datum in das Textfeld (z.B. 2026-03-15 oder Sommer 2025) oder wähle eine Option.",
            reply_markup=_date_keyboard(),
        )
        return WAITING_FOR_TAKEN_AT

    if data == "photo_date_today":
        pending["taken_at"] = local_today_iso()
        await query.edit_message_text(f"Datum: {pending['taken_at']}")
        await query.message.reply_text(
            _caption_prompt(pending),
            reply_markup=_caption_keyboard(),
        )
        return WAITING_FOR_CAPTION

    if data == "photo_skip_date":
        pending["taken_at"] = ""
        await query.edit_message_text("Datum: übersprungen")
        await query.message.reply_text(
            _caption_prompt(pending),
            reply_markup=_caption_keyboard(),
        )
        return WAITING_FOR_CAPTION

    if data == "photo_skip_caption":
        pending["caption"] = ""
        await query.edit_message_text("Bildunterschrift: übersprungen")
        show_caption = bool(pending.get("location") or pending.get("taken_at"))
        if not show_caption:
            return await _submit_photo(update, context, show_caption=False)
        return await _show_preview(query.message, context)

    if data == "photo_confirm_send":
        location = pending.get("location", "")
        taken_at = pending.get("taken_at", "")
        caption = pending.get("caption", "")
        show_caption = bool(location or taken_at or caption)
        try:
            await query.edit_message_caption(caption="Wird verarbeitet...")
        except Exception:
            try:
                await query.edit_message_text("Wird verarbeitet...")
            except Exception:
                pass
        return await _submit_photo(
            update,
            context,
            location=location,
            taken_at=taken_at,
            caption=caption,
            show_caption=show_caption,
        )

    if data == "photo_cancel":
        _discard_pending_submission(context)
        await _delete_query_message(context, query)
        return ConversationHandler.END

    return ConversationHandler.END


async def _submit_photo(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    location: str = "",
    taken_at: str = "",
    caption: str = "",
    show_caption: bool = True,
) -> int:
    services = get_services(context)
    user = update.effective_user
    pending = context.user_data.get(PENDING_SUBMISSION_KEY)
    if user is None or pending is None:
        return ConversationHandler.END

    # Find a message object to reply to
    message = update.effective_message
    if message is None and update.callback_query and update.callback_query.message:
        message = update.callback_query.message
    if message is None:
        return ConversationHandler.END

    chat = update.effective_chat or getattr(message, "chat", None)
    chat_id = getattr(chat, "id", None)

    record = ImageRecord(
        image_id=pending["image_id"],
        telegram_file_id=pending["telegram_file_id"],
        telegram_chat_id=chat_id,
        local_original_path=pending["original_path"],
        local_rendered_path=None,
        location=location,
        taken_at=taken_at,
        caption=caption,
        uploaded_by=user.id,
        created_at=utcnow_iso(),
        status="queued",
        last_error=None,
        orientation_bucket=str(pending.get("orientation_bucket") or "shared"),
    )
    try:
        services.database.upsert_image(record)
        queue = _get_upload_queue(context)
        await queue.put(record.image_id)
    except Exception as exc:
        logger.exception("Failed to enqueue image %s", record.image_id)
        record.status = "failed"
        record.last_error = str(exc)
        services.database.upsert_image(record)
        await _safe_reply_text(message, f"Verarbeitung fehlgeschlagen: {exc}")
        context.user_data.pop(PENDING_SUBMISSION_KEY, None)
        return ConversationHandler.END

    context.user_data.pop(PENDING_SUBMISSION_KEY, None)
    return ConversationHandler.END


def _get_upload_queue(context: ContextTypes.DEFAULT_TYPE) -> asyncio.Queue[str]:
    return context.application.bot_data[UPLOAD_QUEUE_KEY]


def _append_record_warning(record: ImageRecord, warning: str) -> ImageRecord:
    warning = warning.strip()
    if not warning:
        return record
    existing = [part.strip() for part in (record.last_error or "").split("|") if part.strip()]
    if warning not in existing:
        existing.append(warning)
    record.last_error = " | ".join(existing)
    if record.status == "displayed":
        record.status = "displayed_with_warnings"
    return record


async def _safe_reply_text(message, text: str) -> bool:
    try:
        await _send_reply_text_with_retry(message, text)
        return True
    except Exception:
        logger.warning("Failed to send Telegram reply to the active conversation", exc_info=True)
        return False


async def _send_reply_text_with_retry(message, text: str) -> None:
    try:
        await message.reply_text(text, write_timeout=60)
    except TimedOut:
        logger.warning("Telegram reply timed out; retrying once")
        await message.reply_text(text, write_timeout=60)


async def _send_chat_text_with_retry(application: Application, chat_id: int, text: str) -> None:
    try:
        await application.bot.send_message(chat_id=chat_id, text=text, write_timeout=60)
    except TimedOut:
        logger.warning("Telegram completion message timed out for chat %s; retrying once", chat_id)
        await application.bot.send_message(chat_id=chat_id, text=text, write_timeout=60)


async def _notify_completion(
    application: Application,
    record: ImageRecord,
    text: str,
) -> ImageRecord:
    if record.telegram_chat_id is None:
        logger.warning("Skipping completion message for %s because telegram_chat_id is missing", record.image_id)
        return record

    try:
        await _send_chat_text_with_retry(application, record.telegram_chat_id, text)
    except Exception as exc:
        logger.warning("Failed to notify chat %s for image %s", record.telegram_chat_id, record.image_id, exc_info=True)
        return _append_record_warning(record, f"Telegram-Benachrichtigung fehlgeschlagen: {exc}")
    return record


def _get_cooldown_seconds(services) -> int:
    """Read the new_image_cooldown setting. 0 means disabled."""
    raw = services.database.get_setting("new_image_cooldown")
    if raw is None:
        return 3600
    try:
        return max(0, int(raw))
    except (ValueError, TypeError):
        return 3600


def _cooldown_remaining(services, cooldown: int) -> int:
    """Seconds remaining before the next new image can be displayed. 0 = ready."""
    if cooldown <= 0:
        return 0
    raw = services.database.get_setting("last_new_image_displayed_at")
    if not raw:
        return 0
    from datetime import datetime, timezone
    try:
        last = datetime.fromisoformat(raw)
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        elapsed = (datetime.now(timezone.utc) - last).total_seconds()
        return max(0, int(cooldown - elapsed))
    except (ValueError, TypeError):
        return 0


async def process_queued_upload(application: Application, image_id: str) -> None:
    services = application.bot_data["services"]
    record = services.database.get_image_by_id(image_id)
    if record is None:
        logger.warning("Queued image %s no longer exists in the database", image_id)
        return
    if record.status not in {"queued", "processing"}:
        logger.info("Skipping queued image %s because it is already %s", image_id, record.status)
        return

    record.status = "processing"
    services.database.upsert_image(record)

    fit_mode = services.database.get_setting("image_fit_mode") or "fill"
    rendered_path = services.storage.rendered_path(record.image_id)
    show_caption = bool(record.location or record.taken_at or record.caption)
    display_state_committed = False

    try:
        # Always render the image first (no lock needed for rendering)
        record, warnings = await _render_image(services, record, rendered_path)
        if record.status == "failed":
            services.database.upsert_image(record)
            record = await _notify_completion(application, record, f"Verarbeitung fehlgeschlagen: {record.last_error}")
            services.database.upsert_image(record)
            return

        # Acquire lock before cooldown check to prevent two simultaneous uploads
        # from both passing the check and displaying at the same time
        lock = application.bot_data["display_lock"]
        async with lock:
            active_orientation = services.display.current_orientation()
            cooldown = _get_cooldown_seconds(services)
            remaining = _cooldown_remaining(services, cooldown)

            if not orientation_matches(record.orientation_bucket, active_orientation):
                record.status = "rendered"
                services.database.upsert_image(record)
                record = await _notify_completion(
                    application,
                    record,
                    f"Das Bild wartet in der Warteschlange, bis der Rahmen wieder im "
                    f"{format_orientation_label(record.orientation_bucket)} ist.",
                )
                services.database.upsert_image(record)
                return

            if remaining > 0:
                # Cooldown active — park as "rendered" and notify user with ETA
                record.status = "rendered"
                services.database.upsert_image(record)

                pending_count = services.database.count_rendered_images(active_orientation)
                from app.settings_conversation import _format_interval_label
                eta_seconds = remaining + (pending_count - 1) * cooldown
                eta_label = _format_interval_label(eta_seconds)
                record = await _notify_completion(
                    application, record,
                    f"Das Bild befindet sich in der Warteschlange (Position {pending_count}) "
                    f"und wird voraussichtlich in ca. {eta_label} angezeigt.",
                )
                services.database.upsert_image(record)

                # Reschedule slideshow to fire when cooldown expires
                from app.slideshow import reschedule_slideshow_job
                reschedule_slideshow_job(application, first_seconds=remaining)
                return

            # Cooldown expired or disabled — display immediately
            record, display_warnings = await _display_rendered_image(
                services, record, rendered_path, show_caption=show_caption, fit_mode=fit_mode,
            )
            warnings.extend(display_warnings)
            if record.status not in ("failed", "display_failed"):
                commit_display_success(
                    services.database,
                    record,
                    mark_new_image=True,
                )
                display_state_committed = True
                from app.slideshow import reschedule_slideshow_job
                try:
                    reschedule_slideshow_job(application)
                except Exception as exc:
                    logger.exception("Failed to reschedule slideshow after displaying %s", record.image_id)
                    warning = f"Slideshow-Neuplanung fehlgeschlagen: {exc}"
                    warnings.append(warning)
                    record = _append_record_warning(record, warning)
                    services.database.upsert_image(record)
            else:
                services.database.apply_image_and_settings(record, clear_keys=DISPLAY_TRANSITION_KEYS)
    except Exception as exc:
        if display_state_committed:
            logger.exception("Post-display processing failed for image %s", record.image_id)
            warning = f"Nachbearbeitung fehlgeschlagen: {exc}"
            warnings.append(warning)
            record = _append_record_warning(record, warning)
            services.database.upsert_image(record)
            record = await _notify_completion(application, record, _build_success_reply(record, warnings))
            services.database.upsert_image(record)
            return
        logger.exception("Processing failed for image %s", record.image_id)
        record.status = "failed"
        record.last_error = str(exc)
        record.local_rendered_path = str(rendered_path) if rendered_path.exists() else None
        services.database.apply_image_and_settings(record, clear_keys=DISPLAY_TRANSITION_KEYS)
        record = await _notify_completion(application, record, f"Verarbeitung fehlgeschlagen: {exc}")
        services.database.upsert_image(record)
        return

    record = await _notify_completion(application, record, _build_success_reply(record, warnings))
    services.database.upsert_image(record)


async def _render_image(
    services: Any, record: ImageRecord, rendered_path: Path,
) -> tuple[ImageRecord, list[str]]:
    """Render the image to disk. Does not display it."""
    warnings: list[str] = []

    logger.info("Rendering image %s", record.image_id)
    try:
        await asyncio.to_thread(
            services.renderer.render,
            Path(record.local_original_path),
            rendered_path,
            location=record.location,
            taken_at=record.taken_at,
            caption=record.caption,
        )
    except OSError as exc:
        record.status = "failed"
        record.last_error = f"Failed to render image: {exc}"
        return record, warnings
    record.local_rendered_path = str(rendered_path)
    return record, warnings


async def _display_rendered_image(
    services: Any, record: ImageRecord, rendered_path: Path, *, show_caption: bool = True, fit_mode: str = "fill",
) -> tuple[ImageRecord, list[str]]:
    """Send an already-rendered image to the display. Caller must hold display_lock."""
    warnings: list[str] = []

    display_request = DisplayRequest(
        image_id=record.image_id,
        original_path=Path(record.local_original_path),
        composed_path=rendered_path,
        location=record.location,
        taken_at=record.taken_at,
        caption=record.caption,
        created_at=record.created_at,
        uploaded_by=record.uploaded_by,
        show_caption=show_caption,
        fit_mode=fit_mode,
    )
    logger.info("Sending image %s to display", record.image_id)
    begin_display_transition(services.database, record.image_id, "upload")
    display_result = await asyncio.to_thread(services.display.display, display_request)

    if not display_result.success:
        logger.warning("Display failed for image %s: %s", record.image_id, display_result.message)
        record.status = "display_failed"
        record.last_error = display_result.message
        return record, warnings

    services.storage.cleanup_rendered_cache()

    record.status = "displayed_with_warnings" if warnings else "displayed"
    record.last_error = " | ".join(warnings) if warnings else None
    return record, warnings


def _build_success_reply(record: ImageRecord, warnings: list[str]) -> str:
    if record.status == "display_failed":
        return f"Foto gerendert, aber die Anzeige konnte nicht aktualisiert werden: {record.last_error}"
    lines = ["Das Bild wird jetzt angezeigt."]
    if warnings:
        lines.append("Warnungen:")
        lines.extend(f"- {w}" for w in warnings)
    return "\n".join(lines)


def _make_unexpected_handler(state: int):
    async def handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        if update.effective_message is not None:
            await update.effective_message.reply_text("Bitte beantworte die aktuelle Frage oder nutze den Button „Abbrechen“.")
        return state

    return handler


_unexpected_text_choice = _make_unexpected_handler(WAITING_FOR_TEXT_CHOICE)
_unexpected_location = _make_unexpected_handler(WAITING_FOR_LOCATION)
_unexpected_taken_at = _make_unexpected_handler(WAITING_FOR_TAKEN_AT)
_unexpected_caption = _make_unexpected_handler(WAITING_FOR_CAPTION)
_unexpected_preview = _make_unexpected_handler(WAITING_FOR_PREVIEW_CONFIRM)


async def _conversation_timeout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    _discard_pending_submission(context)
    if update and update.effective_message:
        await update.effective_message.reply_text(
            "Dein Upload ist nach 5 Minuten Inaktivität abgelaufen. "
            "Sende das Foto erneut, um neu zu starten."
        )
    return ConversationHandler.END


def build_photo_conversation() -> ConversationHandler:
    return ConversationHandler(
        entry_points=[MessageHandler(filters.PHOTO & ~filters.COMMAND, photo_entry)],
        states={
            WAITING_FOR_TEXT_CHOICE: [
                CallbackQueryHandler(photo_button_callback, pattern=r"^photo_"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_text_choice),
                MessageHandler(filters.ALL & ~filters.COMMAND, _unexpected_text_choice),
            ],
            WAITING_FOR_LOCATION: [
                CallbackQueryHandler(photo_button_callback, pattern=r"^photo_"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_location),
                MessageHandler(filters.ALL & ~filters.COMMAND, _unexpected_location),
            ],
            WAITING_FOR_TAKEN_AT: [
                CallbackQueryHandler(photo_button_callback, pattern=r"^photo_"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_taken_at),
                MessageHandler(filters.ALL & ~filters.COMMAND, _unexpected_taken_at),
            ],
            WAITING_FOR_CAPTION: [
                CallbackQueryHandler(photo_button_callback, pattern=r"^photo_"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_caption),
                MessageHandler(filters.ALL & ~filters.COMMAND, _unexpected_caption),
            ],
            WAITING_FOR_PREVIEW_CONFIRM: [
                CallbackQueryHandler(photo_button_callback, pattern=r"^photo_"),
                MessageHandler(filters.ALL & ~filters.COMMAND, _unexpected_preview),
            ],
            ConversationHandler.TIMEOUT: [
                MessageHandler(filters.ALL, _conversation_timeout),
            ],
        },
        allow_reentry=False,
        name="photo_upload",
        persistent=False,
        conversation_timeout=300,
    )
