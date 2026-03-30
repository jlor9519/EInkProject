from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import TimedOut
from telegram.ext import (
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from app.auth import require_whitelist
from app.commands import get_reservation, get_services
from app.database import utcnow_iso
from app.models import DisplayError, DisplayRequest, ImageRecord, RenderError

(
    WAITING_FOR_TEXT_CHOICE,
    WAITING_FOR_LOCATION,
    WAITING_FOR_TAKEN_AT,
    WAITING_FOR_CAPTION,
    WAITING_FOR_PREVIEW_CONFIRM,
) = range(5)
PENDING_SUBMISSION_KEY = "pending_submission"


def _discard_pending_submission(context: ContextTypes.DEFAULT_TYPE, *, user_id: int | None = None) -> None:
    pending = context.user_data.get(PENDING_SUBMISSION_KEY)
    if isinstance(pending, dict):
        original_path = pending.get("original_path")
        if original_path:
            Path(original_path).unlink(missing_ok=True)

    reservation = get_reservation(context)
    if user_id is None or reservation.owner_user_id == user_id:
        reservation.owner_user_id = None
        reservation.image_id = None
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


@require_whitelist(conversation=True)
async def photo_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    services = get_services(context)
    user = update.effective_user
    message = update.effective_message
    if user is None or message is None or not message.photo:
        return ConversationHandler.END

    reservation = get_reservation(context)
    if reservation.owner_user_id is not None and reservation.owner_user_id != user.id:
        await message.reply_text(
            "Ein anderes Foto wird gerade verarbeitet. Bitte warte einen Moment und sende dein Foto erneut."
        )
        return ConversationHandler.END

    if context.user_data.get(PENDING_SUBMISSION_KEY):
        await message.reply_text("Du hast bereits einen Upload in Bearbeitung. Beantworte die Fragen oder nutze /cancel.")
        return WAITING_FOR_TEXT_CHOICE

    photo = message.photo[-1]
    image_id = services.storage.generate_image_id()
    original_path = services.storage.original_path(image_id)
    reservation.owner_user_id = user.id
    reservation.image_id = image_id

    try:
        logger.info("Downloading photo %s from user %d", image_id, user.id)
        telegram_file = await photo.get_file()
        await telegram_file.download_to_drive(custom_path=str(original_path))
    except Exception as exc:  # pragma: no cover - depends on Telegram runtime
        logger.exception("Failed to download photo %s from Telegram", image_id)
        reservation.owner_user_id = None
        reservation.image_id = None
        await message.reply_text(f"Fehler beim Herunterladen des Fotos von Telegram: {exc}")
        return ConversationHandler.END

    context.user_data[PENDING_SUBMISSION_KEY] = {
        "image_id": image_id,
        "telegram_file_id": photo.file_id,
        "original_path": str(original_path),
    }

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Ja", callback_data="photo_text_yes"),
            InlineKeyboardButton("Nein", callback_data="photo_text_no"),
        ],
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
    await update.effective_message.reply_text("Bitte antworte mit Ja/J oder Nein/N, oder nutze /cancel.")
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
        "Welche Bildunterschrift soll unter dem Foto angezeigt werden?\n\nSchreibe den Text in das Textfeld oder wähle eine Option.",
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
            orientation = services.display.current_orientation()
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
        pending["taken_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        await query.edit_message_text(f"Datum: {pending['taken_at']}")
        await query.message.reply_text(
            "Welche Bildunterschrift soll unter dem Foto angezeigt werden?\n\nSchreibe den Text in das Textfeld oder wähle eine Option.",
            reply_markup=_caption_keyboard(),
        )
        return WAITING_FOR_CAPTION

    if data == "photo_skip_date":
        pending["taken_at"] = ""
        await query.edit_message_text("Datum: übersprungen")
        await query.message.reply_text(
            "Welche Bildunterschrift soll unter dem Foto angezeigt werden?\n\nSchreibe den Text in das Textfeld oder wähle eine Option.",
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
        try:
            await query.edit_message_text("Upload abgebrochen.")
        except Exception:
            try:
                await query.edit_message_caption(caption="Upload abgebrochen.")
            except Exception:
                pass
        user = update.effective_user
        _discard_pending_submission(context, user_id=user.id if user else None)
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

    record = ImageRecord(
        image_id=pending["image_id"],
        telegram_file_id=pending["telegram_file_id"],
        local_original_path=pending["original_path"],
        local_rendered_path=None,
        location=location,
        taken_at=taken_at,
        caption=caption,
        uploaded_by=user.id,
        created_at=utcnow_iso(),
        status="processing",
        last_error=None,
    )
    services.database.upsert_image(record)
    await message.reply_text("Dein Foto wird jetzt verarbeitet.")

    fit_mode = services.database.get_setting("image_fit_mode") or "fill"
    rendered_path = services.storage.rendered_path(record.image_id)
    try:
        record, warnings = await _process_image(services, record, rendered_path, show_caption=show_caption, fit_mode=fit_mode)
        services.database.upsert_image(record)
        if record.status not in ("failed", "display_failed"):
            from app.slideshow import reschedule_slideshow_job
            reschedule_slideshow_job(context.application)
        await message.reply_text(_build_success_reply(record, warnings))
        return ConversationHandler.END
    except Exception as exc:
        logger.exception("Processing failed for image %s", record.image_id)
        record.status = "failed"
        record.last_error = str(exc)
        record.local_rendered_path = str(rendered_path) if rendered_path.exists() else None
        services.database.upsert_image(record)
        await message.reply_text(f"Verarbeitung fehlgeschlagen: {exc}")
        return ConversationHandler.END
    finally:
        reservation = get_reservation(context)
        if reservation.owner_user_id == user.id:
            reservation.owner_user_id = None
            reservation.image_id = None
        context.user_data.pop(PENDING_SUBMISSION_KEY, None)


async def _process_image(
    services: Any, record: ImageRecord, rendered_path: Path, *, show_caption: bool = True, fit_mode: str = "fill",
) -> tuple[ImageRecord, list[str]]:
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
        raise RenderError(f"Failed to render image: {exc}") from exc
    record.local_rendered_path = str(rendered_path)

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
    display_result = await asyncio.to_thread(services.display.display, display_request)

    if not display_result.success:
        logger.warning("Display failed for image %s: %s", record.image_id, display_result.message)
        record.status = "display_failed"
        record.last_error = display_result.message
        return record, warnings

    services.database.set_setting("current_image_displayed_at", utcnow_iso())
    services.storage.cleanup_rendered_cache()

    record.status = "displayed_with_warnings" if warnings else "displayed"
    record.last_error = " | ".join(warnings) if warnings else None
    return record, warnings


def _build_success_reply(record: ImageRecord, warnings: list[str]) -> str:
    if record.status == "display_failed":
        return f"Foto gerendert, aber die Anzeige konnte nicht aktualisiert werden: {record.last_error}"
    lines = [
        "Das Foto wurde erfolgreich an den Rahmen gesendet.",
        f"Bild-ID: {record.image_id}",
    ]
    if warnings:
        lines.append("Warnungen:")
        lines.extend(f"- {w}" for w in warnings)
    return "\n".join(lines)


def _make_unexpected_handler(state: int):
    async def handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        if update.effective_message is not None:
            await update.effective_message.reply_text("Bitte beantworte die aktuelle Frage oder nutze /cancel.")
        return state

    return handler


_unexpected_text_choice = _make_unexpected_handler(WAITING_FOR_TEXT_CHOICE)
_unexpected_location = _make_unexpected_handler(WAITING_FOR_LOCATION)
_unexpected_taken_at = _make_unexpected_handler(WAITING_FOR_TAKEN_AT)
_unexpected_caption = _make_unexpected_handler(WAITING_FOR_CAPTION)
_unexpected_preview = _make_unexpected_handler(WAITING_FOR_PREVIEW_CONFIRM)


async def _conversation_timeout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user if update else None
    _discard_pending_submission(context, user_id=user.id if user else None)
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
        fallbacks=[CommandHandler("cancel", unexpected_cancel)],
        allow_reentry=False,
        name="photo_upload",
        persistent=False,
        conversation_timeout=300,
    )


async def unexpected_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from app.commands import cancel_command

    return await cancel_command(update, context)
