from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from app.auth import require_admin, require_whitelist
from app.display_state import (
    DISPLAY_TRANSITION_KEYS,
    begin_display_transition,
    clear_display_transition,
    commit_display_success,
    read_current_payload_image_id,
)
from app.fs_utils import safe_unlink
from app.models import AppServices, DisplayRequest, DisplayResult, ImageRecord
from app.orientation import format_orientation_label

COMMAND_CALLBACK_PREFIX = "cmd|"


def get_services(context: ContextTypes.DEFAULT_TYPE) -> AppServices:
    return context.application.bot_data["services"]


def get_display_lock(context: ContextTypes.DEFAULT_TYPE) -> asyncio.Lock:
    return context.application.bot_data["display_lock"]


def get_active_orientation(services: AppServices) -> str:
    return services.display.current_orientation()


def _command_callback_data(action: str) -> str:
    return f"{COMMAND_CALLBACK_PREFIX}{action}"


def _quick_actions_help(is_admin: bool) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("Status", callback_data=_command_callback_data("status")),
            InlineKeyboardButton("Liste", callback_data=_command_callback_data("list")),
            InlineKeyboardButton("Löschen", callback_data=_command_callback_data("delete")),
        ],
        [
            InlineKeyboardButton("Vorheriges", callback_data=_command_callback_data("prev")),
            InlineKeyboardButton("Nächstes", callback_data=_command_callback_data("next")),
        ],
        [InlineKeyboardButton("Abbrechen", callback_data=_command_callback_data("close"))],
    ]
    if is_admin:
        rows.append([InlineKeyboardButton("Einstellungen", callback_data="settings|open")])
    return InlineKeyboardMarkup(rows)


def _quick_actions_status(is_admin: bool) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("Liste", callback_data=_command_callback_data("list")),
            InlineKeyboardButton("Löschen", callback_data=_command_callback_data("delete")),
        ],
        [
            InlineKeyboardButton("Vorheriges", callback_data=_command_callback_data("prev")),
            InlineKeyboardButton("Nächstes", callback_data=_command_callback_data("next")),
        ],
        [InlineKeyboardButton("Abbrechen", callback_data=_command_callback_data("close"))],
    ]
    if is_admin:
        rows.append([InlineKeyboardButton("Einstellungen", callback_data="settings|open")])
    return InlineKeyboardMarkup(rows)


def _quick_actions_list() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Vorheriges", callback_data=_command_callback_data("prev")),
                InlineKeyboardButton("Nächstes", callback_data=_command_callback_data("next")),
            ],
            [InlineKeyboardButton("Löschen", callback_data=_command_callback_data("delete"))],
            [InlineKeyboardButton("Abbrechen", callback_data=_command_callback_data("close"))],
        ]
    )


def _build_help_text(is_admin: bool) -> str:
    lines = [
        "Sende ein Foto, um den Upload-Prozess zu starten.",
        "Ich frage optional nach:",
        "- wo das Foto aufgenommen wurde",
        "- wann es aufgenommen wurde",
        "- welche Bildunterschrift angezeigt werden soll",
        "",
        "Befehle:",
        "/help - diese Nachricht anzeigen",
        "/next - nächstes Bild anzeigen",
        "/prev - vorheriges Bild anzeigen",
        "/list - nächste Bilder und Zeitplan anzeigen",
        "/delete - ausgewähltes Bild löschen",
        "/status - Systemstatus anzeigen",
        "/myid - deine Telegram-Nutzer-ID anzeigen",
    ]
    if is_admin:
        lines.extend(
            [
                "/settings - Anzeigeeinstellungen anzeigen/ändern",
                #"/users - freigegebene Nutzer anzeigen",
                #"/unwhitelist - Nutzer entfernen",
            ]
        )
    return "\n".join(lines)


async def _render_status_text(services: AppServices) -> str:
    active_orientation = get_active_orientation(services)

    db_health_fn = getattr(services.database, "health_details", None)
    if callable(db_health_fn):
        db_details = db_health_fn()
    else:
        db_ok_fallback = bool(services.database.healthcheck())
        db_details = {"read_ok": db_ok_fallback, "write_ok": db_ok_fallback}
    db_ok = bool(db_details["read_ok"] and db_details["write_ok"])

    storage_health_fn = getattr(services.storage, "health_details", None)
    if callable(storage_health_fn):
        storage_details = storage_health_fn()
    else:
        storage_ok_fallback = bool(services.storage.healthcheck())
        storage_details = {
            "paths_exist": storage_ok_fallback,
            "writable": storage_ok_fallback,
            "free_bytes": None,
        }
    storage_ok = bool(storage_details["paths_exist"] and storage_details["writable"])
    payload_ok = services.display.payload_exists()
    diagnostics_fn = getattr(services.display, "runtime_settings_diagnostics", None)
    runtime_diagnostics = diagnostics_fn() if callable(diagnostics_fn) else {"degraded": False, "message": ""}

    inkypi_reachable = await asyncio.to_thread(services.display.ping_inkypi)
    if inkypi_reachable is None:
        inkypi_line = "– lokal"
    elif inkypi_reachable:
        inkypi_line = "✓ erreichbar"
    else:
        inkypi_line = "✗ nicht erreichbar"

    rotation_count = services.database.count_rotation_pool_images(active_orientation)
    hidden_count = services.database.count_hidden_rotation_images(active_orientation)
    displayed_count = services.database.count_displayed_images(active_orientation)
    rendered_count = services.database.count_rendered_images(active_orientation)
    displayed_at = services.database.get_setting("current_image_displayed_at")
    user_count = services.database.count_whitelisted_users()
    free_space = _format_free_space(storage_details["free_bytes"])

    image_lines = [
        f"- Bibliothek: {format_orientation_label(active_orientation)}",
        f"- In Rotation: {rotation_count} {'Bild' if rotation_count == 1 else 'Bilder'}",
        f"- Außerhalb der Rotation gespeichert: {hidden_count}",
        f"- Davon aktuell angezeigt: {displayed_count} {'Bild' if displayed_count == 1 else 'Bilder'}",
        f"- Aktuelles Bild: {_format_duration(displayed_at)}",
    ]
    if rendered_count > 0:
        image_lines.append(f"- Warteschlange: {rendered_count} neue{'s' if rendered_count == 1 else ''} Bild{'er' if rendered_count != 1 else ''} wartend")

    service_lines = [
        f"- Datenbank: {'✓ ok' if db_ok else '✗ Fehler'}",
        f"- Datenbank schreiben: {'✓ ok' if db_details['write_ok'] else '✗ Fehler'}",
        f"- Speicher: {'✓ ok' if storage_ok else '✗ Fehler'}",
        f"- Speicher schreibbar: {'✓ ok' if storage_details['writable'] else '✗ Fehler'}",
        f"- Freier Speicher: {free_space}",
        f"- InkyPi: {inkypi_line}",
        f"- InkyPi-Payload: {'✓ vorhanden' if payload_ok else '✗ nicht gefunden'}",
    ]

    warnings: list[str] = []
    if runtime_diagnostics["degraded"]:
        warnings.append(str(runtime_diagnostics["message"]))

    return "\n".join(
        [
            "Fotorahmen-Status",
            "",
            "Dienste:",
            *service_lines,
            "",
            "Bilder:",
            *image_lines,
            *(
                [
                    "",
                    "Warnungen:",
                    *(f"- {warning}" for warning in warnings),
                ]
                if warnings
                else []
            ),
            "",
            "Nutzer:",
            f"- Freigegebene Nutzer: {user_count}",
        ]
    )


async def _render_list_text(services: AppServices) -> str:
    active_orientation = get_active_orientation(services)
    payload_path = services.config.storage.current_payload_path
    if not payload_path.exists():
        return "Noch kein Bild vorhanden."

    try:
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return "Payload-Datei konnte nicht gelesen werden."

    current_image_id = payload.get("image_id")
    if not current_image_id:
        return "Kein aktuelles Bild erkannt."

    next_images = services.database.get_next_images(current_image_id, 5, active_orientation)
    total = services.database.count_displayed_images(active_orientation)
    current_pos = services.database.get_displayed_image_position(current_image_id, active_orientation)
    hidden_count = services.database.count_hidden_rotation_images(active_orientation)
    current_in_rotation = services.database.is_image_in_rotation_pool(current_image_id, active_orientation)

    lines = [f"Bilderliste {format_orientation_label(active_orientation)} ({total} gesamt)", ""]

    current_record = services.database.get_image_by_id(current_image_id)
    current_label = _image_label(current_record) if current_record else "(unbekannt)"
    if current_record and not current_in_rotation:
        lines.append(f"{current_label} (aktuell angezeigt, nicht Teil der aktuellen Bibliothek)")
    else:
        lines.append(f"{current_label}")
    lines.append(f"Außerhalb der Rotation gespeichert: {hidden_count}")

    interval = await asyncio.to_thread(services.display.get_slideshow_interval)

    from datetime import datetime, timedelta, timezone as tz
    from app.slideshow import (
        _set_next_fire_decision,
        compute_next_fire_decision,
        get_stored_next_fire_metadata,
        project_display_change_offsets,
    )
    from app.settings_conversation import _format_interval_label

    now = datetime.now(tz.utc)
    next_fire_raw = services.database.get_setting("slideshow_next_fire_at")
    remaining = 0
    next_fire_at: datetime | None = None
    if next_fire_raw:
        try:
            next_fire = datetime.fromisoformat(next_fire_raw)
            if next_fire.tzinfo is None:
                next_fire = next_fire.replace(tzinfo=tz.utc)
            next_fire_at = next_fire
            remaining = max(0, int((next_fire - now).total_seconds()))
        except (ValueError, TypeError):
            remaining = 0
    stored_mode, stored_detail = get_stored_next_fire_metadata(services)
    if remaining <= 0:
        decision = compute_next_fire_decision(services, active_orientation)
        remaining = decision.seconds
        stored_mode, stored_detail = decision.mode, decision.detail
        _set_next_fire_decision(services, decision)
        next_fire_at = now + timedelta(seconds=remaining)
    elif stored_mode is None:
        decision = compute_next_fire_decision(services, active_orientation)
        stored_mode, stored_detail = decision.mode, decision.detail

    remaining_str = _format_interval_label(remaining) if remaining > 0 else "weniger als 1 Minute"
    lines.append(f"  Wechsel in ca. {remaining_str}")
    lines.append(f"  {_format_timer_mode_label(stored_mode, stored_detail, interval)}")

    rendered_count = services.database.count_rendered_images(active_orientation)
    if rendered_count > 0:
        from app.conversations import _cooldown_remaining, _get_cooldown_seconds

        cooldown = _get_cooldown_seconds(services)
        cd_remaining = _cooldown_remaining(services, cooldown)
        lines.append("")
        lines.append(f"Warteschlange: {rendered_count} neue{'s' if rendered_count == 1 else ''} Bild{'er' if rendered_count != 1 else ''}")
        if cd_remaining > 0:
            lines.append(f"  Nächstes neues Bild in ca. {_format_interval_label(cd_remaining)}")

    if next_images:
        lines.append("")
        lines.append("Nächste Bilder:")
        projected_offsets = (
            project_display_change_offsets(services, len(next_images), first_fire_at=next_fire_at)
            if rendered_count == 0
            else []
        )
        for i, record in enumerate(next_images, 1):
            pos = ((current_pos or 0) + i - 1) % total + 1
            lines.append(f"{i}. [{pos}/{total}] {_image_label(record)}")
            if projected_offsets:
                offset = projected_offsets[i - 1]
            else:
                offset = remaining + (i - 1) * interval
            eta_str = _format_interval_label(offset) if offset > 0 else "weniger als 1 Minute"
            lines.append(f"   In ca. {eta_str}")

    return "\n".join(lines)


@require_whitelist
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None:
        return
    services = get_services(context)
    is_admin = services.auth.is_admin(user.id)
    await message.reply_text(_build_help_text(is_admin), reply_markup=_quick_actions_help(is_admin))


def _format_duration(since_iso: str | None) -> str:
    if not since_iso:
        return "unbekannt"
    from datetime import datetime, timezone
    try:
        since = datetime.fromisoformat(since_iso)
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - since
        total_seconds = max(0, int(delta.total_seconds()))
        minutes = total_seconds // 60
        hours = minutes // 60
        days = hours // 24
        if days >= 1:
            return f"seit {days} {'Tag' if days == 1 else 'Tagen'}"
        if hours >= 1:
            remaining_minutes = minutes % 60
            if remaining_minutes:
                return f"seit {hours} Std. {remaining_minutes} Min."
            return f"seit {hours} {'Stunde' if hours == 1 else 'Stunden'}"
        if minutes >= 1:
            return f"seit {minutes} {'Minute' if minutes == 1 else 'Minuten'}"
        return "seit weniger als einer Minute"
    except (ValueError, TypeError):
        return "unbekannt"


@require_whitelist
async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None:
        return
    services = get_services(context)
    is_admin = services.auth.is_admin(user.id)
    text = await _render_status_text(services)
    await message.reply_text(text, reply_markup=_quick_actions_status(is_admin))


async def myid_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if user is None or update.effective_message is None:
        return
    await update.effective_message.reply_text(f"Deine Telegram-Nutzer-ID lautet: {user.id}")


@require_admin
async def whitelist_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    services = get_services(context)
    if not context.args:
        await update.effective_message.reply_text("Verwendung: /whitelist <telegram_user_id>")
        return

    try:
        target_user_id = int(context.args[0])
    except ValueError:
        await update.effective_message.reply_text("Die Nutzer-ID muss numerisch sein, zum Beispiel: /whitelist 123456789")
        return

    services.auth.whitelist_user(target_user_id)
    logger.info("User %d whitelisted by admin %d", target_user_id, update.effective_user.id)
    await update.effective_message.reply_text(f"Nutzer {target_user_id} wurde freigegeben.")


@require_whitelist
async def refresh_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    lock = get_display_lock(context)
    if lock.locked():
        await update.effective_message.reply_text("Eine Aktualisierung läuft bereits. Bitte warten.")
        return
    async with lock:
        services = get_services(context)
        result = await asyncio.to_thread(services.display.refresh_only)
        await update.effective_message.reply_text(
            "Aktualisierung ausgelöst." if result.success else _friendly_display_error(result.message)
        )


def _friendly_display_error(message: str) -> str:
    lower = message.lower()
    if any(p in lower for p in (
        "request failed", "timed out", "connection refused",
        "no route to host", "network is unreachable",
    )):
        return "Display nicht erreichbar. Bitte prüfe die Verbindung zum Pi."
    return f"Anzeige fehlgeschlagen: {message}"


def _format_free_space(free_bytes: int | None) -> str:
    if free_bytes is None:
        return "unbekannt"
    if free_bytes >= 1024**3:
        return f"{free_bytes / (1024**3):.1f} GB"
    if free_bytes >= 1024**2:
        return f"{free_bytes / (1024**2):.0f} MB"
    if free_bytes >= 1024:
        return f"{free_bytes / 1024:.0f} KB"
    return f"{free_bytes} B"


def _image_label(record: ImageRecord) -> str:
    parts = []
    if record.caption:
        parts.append(f'"{record.caption}"')
    if record.location:
        parts.append(record.location)
    if record.taken_at:
        parts.append(record.taken_at)
    return " • ".join(parts) if parts else "(kein Text)"


def _format_timer_mode_label(mode: str | None, detail: str | None, interval_seconds: int) -> str:
    from app.settings_conversation import _format_interval_label

    if mode == "scheduled_daily" and detail:
        return f"Modus: täglicher Wechsel um {detail}"
    if mode == "cooldown_queue":
        return "Modus: Warteschlange / Cooldown"
    if mode == "quiet_hours" and detail:
        return f"Modus: Ruhezeit bis {detail}"
    if mode == "retry_busy":
        return "Modus: kurze Wiederholung, Anzeige ist gerade beschäftigt"
    if mode == "payload_missing":
        return "Modus: kurze Wiederholung, aktuelles Bild konnte nicht gelesen werden"
    if mode == "display_error":
        return "Modus: kurze Wiederholung nach Anzeigeproblem"
    if mode == "single_image":
        return "Modus: ein Bild in Rotation"
    return f"Modus: Intervall {_format_interval_label(interval_seconds)}"


@require_whitelist
async def list_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if message is None:
        return
    services = get_services(context)
    text = await _render_list_text(services)
    await message.reply_text(text, reply_markup=_quick_actions_list())


async def _authorize_command_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    query = update.callback_query
    user = update.effective_user
    if query is None:
        return False

    await query.answer()
    if user is None:
        return False

    services = get_services(context)
    services.auth.sync_user(user)
    if not services.auth.is_whitelisted(user.id):
        await _edit_query_message(
            query,
            "Du bist für diesen Fotorahmen noch nicht freigegeben. Bitte einen Admin, deine Telegram-ID hinzuzufügen.",
        )
        return False
    return True


async def command_action_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return
    if not await _authorize_command_callback(update, context):
        return

    action = (query.data or "").removeprefix(COMMAND_CALLBACK_PREFIX)
    services = get_services(context)
    user = update.effective_user
    is_admin = bool(user and services.auth.is_admin(user.id))

    if action == "status":
        await _edit_query_message(
            query,
            await _render_status_text(services),
            reply_markup=_quick_actions_status(is_admin),
        )
        return
    if action == "list":
        await _edit_query_message(query, await _render_list_text(services), reply_markup=_quick_actions_list())
        return
    if action == "help":
        await _edit_query_message(query, _build_help_text(is_admin), reply_markup=_quick_actions_help(is_admin))
        return
    if action == "close":
        await _delete_query_message(context, query)
        return
    if action == "next":
        await next_command(update, context)
        return
    if action == "prev":
        await prev_command(update, context)
        return
    if action == "delete":
        await delete_command(update, context)
        return

    await _edit_query_message(query, "Unbekannte Aktion.")


async def _display_target(services: AppServices, target: ImageRecord) -> DisplayResult:
    """Render, display, and upload a target image. Caller must hold display_lock."""
    try:
        rendered_path = Path(target.local_rendered_path) if target.local_rendered_path else None
        original_path = Path(target.local_original_path)

        if rendered_path is None or not rendered_path.exists():
            if not original_path.exists():
                return DisplayResult(False, f"Bilddatei für {target.image_id} nicht mehr vorhanden.")
            rendered_path = services.storage.rendered_path(target.image_id)
            await asyncio.to_thread(
                services.renderer.render,
                original_path,
                rendered_path,
                location=target.location,
                taken_at=target.taken_at,
                caption=target.caption,
            )
            target.local_rendered_path = str(rendered_path)
            services.database.upsert_image(target)

        show_caption = bool(target.caption or target.location or target.taken_at)
        fit_mode = services.database.get_setting("image_fit_mode") or "fill"
        display_request = DisplayRequest(
            image_id=target.image_id,
            original_path=original_path,
            composed_path=rendered_path,
            location=target.location,
            taken_at=target.taken_at,
            caption=target.caption,
            created_at=target.created_at,
            uploaded_by=target.uploaded_by,
            show_caption=show_caption,
            fit_mode=fit_mode,
        )

        return await asyncio.to_thread(services.display.display, display_request)
    except Exception as exc:
        logger.exception("Display target failed unexpectedly for %s", target.image_id)
        return DisplayResult(False, str(exc))


async def _advance_once_to_next_target(
    services: AppServices,
    *,
    current_image_id: str | None,
    active_orientation: str,
    transition_kind: str,
    allow_rendered_without_current: bool = True,
) -> tuple[ImageRecord | None, DisplayResult | None, bool]:
    rendered = services.database.get_oldest_rendered_image_for_orientation(active_orientation)
    if rendered is not None:
        target = rendered
    elif current_image_id is not None:
        target = services.database.get_adjacent_image(current_image_id, "next", active_orientation)
    elif allow_rendered_without_current:
        target = None
    else:
        target = None

    if target is None:
        return None, None, False

    mark_new_image = target.status == "rendered"
    begin_display_transition(services.database, target.image_id, transition_kind)
    result = await _display_target(services, target)

    if result.success:
        if mark_new_image:
            target.status = "displayed"
            target.last_error = None
        commit_display_success(
            services.database,
            target,
            mark_new_image=mark_new_image,
        )
    else:
        if mark_new_image:
            target.status = "display_failed"
            target.last_error = result.message
            services.database.apply_image_and_settings(
                target,
                clear_keys=DISPLAY_TRANSITION_KEYS,
            )
        else:
            clear_display_transition(services.database)

    return target, result, mark_new_image


async def _navigate(update: Update, context: ContextTypes.DEFAULT_TYPE, direction: str) -> None:
    message = update.effective_message
    if message is None:
        return

    lock = get_display_lock(context)
    if lock.locked():
        await message.reply_text("Eine Aktualisierung läuft bereits. Bitte warten.")
        return
    async with lock:
        await _navigate_locked(update, context, direction)


async def _navigate_locked(update: Update, context: ContextTypes.DEFAULT_TYPE, direction: str) -> None:
    services = get_services(context)
    message = update.effective_message
    if message is None:
        return
    active_orientation = get_active_orientation(services)

    if direction == "next":
        target, result, _ = await _advance_once_to_next_target(
            services,
            current_image_id=None,
            active_orientation=active_orientation,
            transition_kind="manual_next",
            allow_rendered_without_current=True,
        )
        if target is not None and result is not None:
            total = services.database.count_displayed_images(active_orientation)
            position = services.database.get_displayed_image_position(target.image_id, active_orientation)
            if result.success:
                await message.reply_text(f"Bild {position} von {total}: {target.image_id}")
            else:
                await message.reply_text(_friendly_display_error(result.message))
            if result.success:
                from app.slideshow import reschedule_slideshow_job
                reschedule_slideshow_job(context.application)
            return

    payload_path = services.config.storage.current_payload_path
    if not payload_path.exists():
        await message.reply_text("Noch kein Bild vorhanden.")
        return

    try:
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        await message.reply_text("Aktuelle Payload-Datei konnte nicht gelesen werden.")
        return

    current_image_id = payload.get("image_id")
    if not current_image_id:
        await message.reply_text("Kein aktuelles Bild erkannt.")
        return

    if direction == "next":
        target, result, _ = await _advance_once_to_next_target(
            services,
            current_image_id=current_image_id,
            active_orientation=active_orientation,
            transition_kind="manual_next",
            allow_rendered_without_current=True,
        )
        if target is None or result is None:
            await message.reply_text("Kein weiteres Bild vorhanden.")
            return
        total = services.database.count_displayed_images(active_orientation)
        position = services.database.get_displayed_image_position(target.image_id, active_orientation)
        if result.success:
            await message.reply_text(f"Bild {position} von {total}: {target.image_id}")
            from app.slideshow import reschedule_slideshow_job
            reschedule_slideshow_job(context.application)
        else:
            await message.reply_text(_friendly_display_error(result.message))
        return
    else:
        target = services.database.get_adjacent_image(current_image_id, direction, active_orientation)
    if target is None:
        await message.reply_text("Kein weiteres Bild vorhanden.")
        return

    total = services.database.count_displayed_images(active_orientation)
    position = services.database.get_displayed_image_position(target.image_id, active_orientation)
    begin_display_transition(
        services.database,
        target.image_id,
        "manual_next" if direction == "next" else "manual_prev",
    )
    result = await _display_target(services, target)
    if result.success:
        commit_display_success(
            services.database,
            target,
            mark_new_image=False,
        )
        await message.reply_text(f"Bild {position} von {total}: {target.image_id}")
        from app.slideshow import reschedule_slideshow_job
        reschedule_slideshow_job(context.application)
    else:
        clear_display_transition(services.database)
        await message.reply_text(_friendly_display_error(result.message))


@require_whitelist
async def next_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _navigate(update, context, "next")


@require_whitelist
async def prev_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _navigate(update, context, "prev")


_DELETE_PAGE_SIZE = 10


def _get_current_image_id(services: AppServices) -> str | None:
    return read_current_payload_image_id(services.config.storage.current_payload_path)


def _build_delete_page(
    images: list[ImageRecord],
    offset: int,
    current_image_id: str | None,
    total: int,
) -> tuple[str, InlineKeyboardMarkup]:
    page = images[offset : offset + _DELETE_PAGE_SIZE]
    lines = [f"Bilder zum Löschen ({total} gesamt)\n"]
    for i, record in enumerate(page):
        num = i + 1
        marker = " ▶" if record.image_id == current_image_id else ""
        lines.append(f"{num}. {_image_label(record)}{marker}")
    text = "\n".join(lines)

    # Number buttons — two rows of 5
    number_buttons = [
        InlineKeyboardButton(str(i + 1), callback_data=f"del|s|{offset + i}|{record.image_id}")
        for i, record in enumerate(page)
    ]
    keyboard_rows = [number_buttons[:5]]
    if len(number_buttons) > 5:
        keyboard_rows.append(number_buttons[5:])

    # Navigation row
    nav_row = []
    if offset > 0:
        nav_row.append(InlineKeyboardButton("← Zurück", callback_data=f"del|p|{max(0, offset - _DELETE_PAGE_SIZE)}"))
    if offset + _DELETE_PAGE_SIZE < total:
        nav_row.append(InlineKeyboardButton("Weiter →", callback_data=f"del|p|{offset + _DELETE_PAGE_SIZE}"))
    if nav_row:
        keyboard_rows.append(nav_row)

    keyboard_rows.append([InlineKeyboardButton("Abbrechen", callback_data="del|c")])
    return text, InlineKeyboardMarkup(keyboard_rows)


@require_whitelist
async def delete_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if message is None:
        return

    services = get_services(context)
    current_image_id = _get_current_image_id(services)
    if not current_image_id:
        await message.reply_text("Kein Bild zum Löschen vorhanden.")
        return

    active_orientation = get_active_orientation(services)
    images = services.database.get_all_displayed_images_ordered(current_image_id, active_orientation)
    if not images:
        await message.reply_text("Keine Bilder in der aktuellen Bibliothek.")
        return

    text, keyboard = _build_delete_page(images, 0, current_image_id, len(images))
    await message.reply_text(text, reply_markup=keyboard)


async def _delete_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return
    await query.answer()

    offset = int(query.data.split("|")[2])
    services = get_services(context)
    current_image_id = _get_current_image_id(services)
    active_orientation = get_active_orientation(services)
    images = services.database.get_all_displayed_images_ordered(
        current_image_id or "", active_orientation
    )
    if not images:
        await _edit_query_message(query, "Keine Bilder mehr vorhanden.")
        return

    if offset >= len(images):
        offset = 0
    text, keyboard = _build_delete_page(images, offset, current_image_id, len(images))
    try:
        await query.edit_message_text(text, reply_markup=keyboard)
    except Exception:
        logger.warning("Could not edit delete page message")


async def _delete_select_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return
    await query.answer()

    parts = query.data.split("|")
    index = int(parts[2])
    image_id = parts[3]
    page_offset = (index // _DELETE_PAGE_SIZE) * _DELETE_PAGE_SIZE

    services = get_services(context)
    record = services.database.get_image_by_id(image_id)
    if record is None:
        await _edit_query_message(query, "Bild nicht mehr vorhanden.")
        return

    current_image_id = _get_current_image_id(services)
    is_current = record.image_id == current_image_id
    label = _image_label(record)
    caption = f"{label}\n\n{'▶ Aktuell angezeigt' if is_current else ''}\nDieses Bild löschen?".strip()

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Löschen", callback_data=f"del|y|{image_id}"),
            InlineKeyboardButton("Zurück", callback_data=f"del|b|{page_offset}"),
        ],
        [InlineKeyboardButton("Abbrechen", callback_data="del|c")],
    ])

    # Try to find an image file to show as preview
    image_path = None
    if is_current:
        candidate = services.config.storage.current_image_path
        if candidate.exists():
            image_path = candidate
    if image_path is None and record.local_rendered_path:
        candidate = Path(record.local_rendered_path)
        if candidate.exists():
            image_path = candidate
    if image_path is None:
        candidate = Path(record.local_original_path)
        if candidate.exists():
            image_path = candidate

    chat_id = query.message.chat_id
    message_id = query.message.message_id

    if image_path is not None:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        except Exception:
            pass
        with open(image_path, "rb") as photo:
            await context.bot.send_photo(
                chat_id=chat_id,
                photo=photo,
                caption=caption,
                reply_markup=keyboard,
            )
    else:
        try:
            await query.edit_message_text(caption, reply_markup=keyboard)
        except Exception:
            logger.warning("Could not edit message for delete preview")


async def _delete_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return
    await query.answer()

    image_id = query.data.split("|")[2]
    services = get_services(context)
    record = services.database.get_image_by_id(image_id)
    if record is None:
        await _edit_query_message(query, "Bild nicht mehr vorhanden.")
        return

    current_image_id = _get_current_image_id(services)
    is_current = image_id == current_image_id
    active_orientation = get_active_orientation(services)

    if is_current:
        lock = get_display_lock(context)
        if lock.locked():
            await _edit_query_message(query, "Eine Aktualisierung läuft bereits. Bitte warten.")
            return

        async with lock:
            replacement = services.database.get_adjacent_image(image_id, "next", active_orientation)
            if replacement is None:
                replacement = services.database.get_adjacent_image(image_id, "prev", active_orientation)

            if replacement is None:
                await _edit_query_message(
                    query,
                    "Das letzte Bild kann nicht gelöscht werden. Lade zuerst ein neues Bild hoch.",
                )
                return

            begin_display_transition(services.database, replacement.image_id, "delete_replace")
            result = await _display_target(services, replacement)
            total = services.database.count_displayed_images(active_orientation)
            if result.success:
                commit_display_success(
                    services.database,
                    replacement,
                    mark_new_image=False,
                )
                services.database.delete_image(image_id)
                for file_path_str in (record.local_original_path, record.local_rendered_path):
                    safe_unlink(file_path_str, logger=logger)
                await _edit_query_message(
                    query,
                    f"Bild gelöscht. Zeige jetzt {_image_label(replacement)} ({total} Bilder verbleibend).",
                )
                from app.slideshow import reschedule_slideshow_job
                reschedule_slideshow_job(context.application)
            else:
                clear_display_transition(services.database)
                await _edit_query_message(
                    query,
                    f"Aktuelles Bild konnte nicht ersetzt werden. {_friendly_display_error(result.message)}",
                )
    else:
        # Not the current image — just delete, no display change needed
        services.database.delete_image(image_id)
        for file_path_str in (record.local_original_path, record.local_rendered_path):
            safe_unlink(file_path_str, logger=logger)

        total = services.database.count_displayed_images(active_orientation)
        await _edit_query_message(
            query,
            f"Bild gelöscht ({total} Bilder verbleibend).",
        )


async def _delete_back_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return
    await query.answer()

    offset = int(query.data.split("|")[2])
    services = get_services(context)
    current_image_id = _get_current_image_id(services)
    active_orientation = get_active_orientation(services)
    images = services.database.get_all_displayed_images_ordered(
        current_image_id or "", active_orientation
    )
    if not images:
        await _edit_query_message(query, "Keine Bilder mehr vorhanden.")
        return

    if offset >= len(images):
        offset = 0
    text, keyboard = _build_delete_page(images, offset, current_image_id, len(images))

    chat_id = query.message.chat_id
    message_id = query.message.message_id
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass
    await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=keyboard)


async def _delete_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return
    await query.answer()
    await _delete_query_message(context, query)


@require_admin
async def users_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if message is None:
        return
    services = get_services(context)
    users = services.database.get_whitelisted_users()
    if not users:
        await message.reply_text("Keine freigegebenen Nutzer.")
        return
    lines = [f"Freigegebene Nutzer ({len(users)}):"]
    for u in users:
        user_id = u["telegram_user_id"]
        name = u.get("display_name") or (f"@{u['username']}" if u.get("username") else str(user_id))
        admin_marker = " (Admin)" if u.get("is_admin") else ""
        lines.append(f"- {user_id} {name}{admin_marker}")
    await message.reply_text("\n".join(lines))


@require_admin
async def unwhitelist_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None:
        return
    if not context.args:
        await message.reply_text("Verwendung: /unwhitelist <telegram_user_id>")
        return
    try:
        target_id = int(context.args[0])
    except ValueError:
        await message.reply_text("Die Nutzer-ID muss numerisch sein, z.B.: /unwhitelist 123456789")
        return
    if target_id == user.id:
        await message.reply_text("Du kannst dich nicht selbst entfernen.")
        return
    services = get_services(context)
    removed = services.database.remove_whitelist(target_id)
    if removed:
        logger.info("User %d removed from whitelist by admin %d", target_id, user.id)
        await message.reply_text(f"Nutzer {target_id} wurde entfernt.")
    else:
        await message.reply_text(f"Nutzer {target_id} nicht gefunden.")


async def stray_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    services = get_services(context)
    user = update.effective_user
    if user is None or update.effective_message is None:
        return
    services.auth.sync_user(user)
    if services.auth.is_whitelisted(user.id):
        await update.effective_message.reply_text("Sende ein Foto, um einen neuen Upload zu starten, oder nutze /help.")
    else:
        await update.effective_message.reply_text(
            "Du bist für diesen Fotorahmen nicht freigegeben. Nutze /myid und teile deine ID mit einem Admin."
        )


async def _edit_query_message(query, text: str, reply_markup: InlineKeyboardMarkup | None = None) -> None:
    message = query.message
    has_media = bool(
        getattr(message, "photo", None)
        or getattr(message, "document", None)
        or getattr(message, "animation", None)
        or getattr(message, "video", None)
    )
    if has_media:
        try:
            await query.edit_message_caption(caption=text, reply_markup=reply_markup)
            return
        except Exception as exc:
            if "message is not modified" in str(exc).lower():
                return
            pass
    try:
        await query.edit_message_text(text, reply_markup=reply_markup)
    except Exception as exc:
        if "message is not modified" in str(exc).lower():
            return
        try:
            if not has_media:
                await query.edit_message_caption(caption=text, reply_markup=reply_markup)
        except Exception as caption_exc:
            if "message is not modified" in str(caption_exc).lower():
                return
            logger.warning("Could not edit query message")


async def _delete_query_message(context: ContextTypes.DEFAULT_TYPE, query) -> None:
    message = query.message
    chat_id = getattr(message, "chat_id", None)
    message_id = getattr(message, "message_id", None)

    try:
        if hasattr(context, "bot"):
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
            return
        app_bot = getattr(context.application, "bot", None)
        if app_bot is not None:
            await app_bot.delete_message(chat_id=chat_id, message_id=message_id)
            return
    except Exception:
        pass

    try:
        await _edit_query_message(query, "", reply_markup=None)
    except Exception:
        logger.warning("Could not delete or clear query message")
