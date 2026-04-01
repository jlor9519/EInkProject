from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import CallbackQueryHandler, CommandHandler, ContextTypes, ConversationHandler, MessageHandler, filters

from app.auth import require_admin
from app.commands import _delete_query_message, _display_target, _edit_query_message, get_display_lock, get_services
from app.display_state import DISPLAY_TRANSITION_KEYS, begin_display_transition, clear_display_transition, commit_display_success
from app.orientation import format_orientation_label, normalize_orientation_value

WAITING_FOR_SETTINGS_CHOICE, WAITING_FOR_SETTINGS_VALUE = range(10, 12)
PENDING_SETTINGS_KEY = "pending_settings_choice"
PENDING_IMAGE_TUNING_FIELD_KEY = "pending_image_tuning_field"
IMAGE_TUNING_DRAFT_KEY = "image_tuning_draft"
IMAGE_TUNING_BASE_KEY = "image_tuning_base"
SETTINGS_CALLBACK_PREFIX = "settings|"


@dataclass(slots=True)
class _SettingDef:
    label: str
    key: str               # top-level key in device.json, or db setting key
    subkey: str | None     # key inside image_settings, or None
    kind: str              # "image_tuning" | "orientation" | "fit_mode" | "integer" | "interval" | "sleep_schedule"


_SETTINGS: list[_SettingDef] = [
    _SettingDef("Bildoptimierung",     "image_settings",       None,         "image_tuning"),
    _SettingDef("Ausrichtung",         "orientation",          None,         "orientation"),
    _SettingDef("Bildanpassung",       "image_fit_mode",       None,         "fit_mode"),
    _SettingDef("Anzeigedauer",        "slideshow_interval",   None,         "interval"),
    _SettingDef("Ruhezeit",            "sleep_schedule",       None,         "sleep_schedule"),
    _SettingDef("Wartezeit neue Bilder", "new_image_cooldown", None,         "cooldown"),
    _SettingDef("Täglicher Bildwechsel", "scheduled_change_time", None,     "scheduled_time"),
    _SettingDef("Bilder in Rotation",  "rotation_limit",       None,         "integer"),
]

_IMAGE_TUNING_FIELDS: list[tuple[str, str]] = [
    ("Sättigung", "saturation"),
    ("Kontrast", "contrast"),
    ("Schärfe", "sharpness"),
    ("Helligkeit", "brightness"),
]
_IMAGE_TUNING_LABELS = {key: label for label, key in _IMAGE_TUNING_FIELDS}

_FIT_MODE_LABELS = {"fill": "Zuschneiden", "contain": "Einpassen"}

_INTERVAL_MIN = 300       # 5 minutes
_INTERVAL_MAX = 604800    # 7 days


def _format_interval_label(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds} Sekunden"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} {'Minute' if minutes == 1 else 'Minuten'}"
    hours = minutes // 60
    rem_min = minutes % 60
    if hours < 24:
        return f"{hours} Std. {rem_min} Min." if rem_min else f"{hours} {'Stunde' if hours == 1 else 'Stunden'}"
    days = hours // 24
    rem_h = hours % 24
    return f"{days} {'Tag' if days == 1 else 'Tage'} {rem_h} Std." if rem_h else f"{days} {'Tag' if days == 1 else 'Tage'}"


def _parse_interval_input(text: str) -> int | None:
    """Parse user interval input into seconds. Returns None if invalid."""
    text = text.strip().lower().replace(",", ".")
    _UNIT_MAP = {
        "s": 1, "sek": 1, "sekunde": 1, "sekunden": 1,
        "m": 60, "min": 60, "minute": 60, "minuten": 60,
        "h": 3600, "std": 3600, "stunde": 3600, "stunden": 3600,
        "d": 86400, "tag": 86400, "tage": 86400,
    }
    for unit, factor in sorted(_UNIT_MAP.items(), key=lambda x: -len(x[0])):
        if text.endswith(unit):
            num_str = text[: -len(unit)].strip()
            try:
                return int(float(num_str) * factor)
            except ValueError:
                return None
    # No unit — assume hours
    try:
        return int(float(text) * 3600)
    except ValueError:
        return None

def _parse_time_string(s: str) -> str | None:
    """Validate and normalize a time string to HH:MM. Returns None if invalid."""
    s = s.strip()
    if ":" in s:
        parts = s.split(":", 1)
    else:
        parts = [s, "0"]
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except (ValueError, IndexError):
        return None
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return None
    return f"{hour:02d}:{minute:02d}"


_FIT_MODE_MAP = {
    "zuschneiden": "fill",
    "fill": "fill",
    "crop": "fill",
    "füllen": "fill",
    "einpassen": "contain",
    "contain": "contain",
    "letterbox": "contain",
    "anpassen": "contain",
}


def _get_current_value(settings: dict[str, Any], s: _SettingDef) -> str:
    if s.kind == "image_tuning":
        return "4 Werte"
    if s.kind == "orientation":
        return format_orientation_label(str(settings.get("orientation", "?")))
    if s.kind == "fit_mode":
        raw = str(settings.get("image_fit_mode", "fill"))
        return _FIT_MODE_LABELS.get(raw, raw)
    if s.kind == "integer":
        return _format_rotation_limit_value(settings.get(s.key, 100))
    if s.kind == "interval":
        raw = settings.get(s.key, 86400)
        try:
            return _format_interval_label(int(raw))
        except (ValueError, TypeError):
            return "24 Stunden"
    if s.kind == "sleep_schedule":
        raw = settings.get("sleep_schedule")
        if not raw:
            return "Keine"
        return str(raw)
    if s.kind == "cooldown":
        raw = settings.get(s.key, 3600)
        try:
            val = int(raw)
        except (ValueError, TypeError):
            val = 3600
        return "Deaktiviert" if val == 0 else _format_interval_label(val)
    if s.kind == "scheduled_time":
        raw = settings.get(s.key)
        return str(raw) if raw else "Deaktiviert"
    if s.subkey:
        return str(settings.get(s.key, {}).get(s.subkey, "?"))
    return str(settings.get(s.key, "?"))


def _settings_callback_data(*parts: object) -> str:
    return f"{SETTINGS_CALLBACK_PREFIX}{'|'.join(str(part) for part in parts)}"


def _format_rotation_limit_value(raw_value: Any) -> str:
    try:
        parsed = int(str(raw_value))
    except (TypeError, ValueError):
        parsed = 100
    return "Unbegrenzt" if parsed == 0 else str(parsed)


def _normalize_tuning_value(raw_value: Any) -> float:
    try:
        return float(raw_value)
    except (TypeError, ValueError):
        return 1.0


def _load_image_tuning_values(settings: dict[str, Any]) -> dict[str, float]:
    raw_settings = settings.get("image_settings")
    image_settings = raw_settings if isinstance(raw_settings, dict) else {}
    return {
        key: _normalize_tuning_value(image_settings.get(key))
        for _, key in _IMAGE_TUNING_FIELDS
    }


def _clear_image_tuning_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop(PENDING_IMAGE_TUNING_FIELD_KEY, None)
    context.user_data.pop(IMAGE_TUNING_DRAFT_KEY, None)
    context.user_data.pop(IMAGE_TUNING_BASE_KEY, None)


def _ensure_image_tuning_state(
    context: ContextTypes.DEFAULT_TYPE,
    services,
) -> tuple[dict[str, float], dict[str, float]]:
    draft = context.user_data.get(IMAGE_TUNING_DRAFT_KEY)
    base = context.user_data.get(IMAGE_TUNING_BASE_KEY)
    if isinstance(draft, dict) and isinstance(base, dict):
        return draft, base

    current = _load_image_tuning_values(services.display.read_device_settings())
    draft = dict(current)
    base = dict(current)
    context.user_data[IMAGE_TUNING_DRAFT_KEY] = draft
    context.user_data[IMAGE_TUNING_BASE_KEY] = base
    return draft, base


def _format_tuning_value(value: float) -> str:
    return f"{value:.1f}"


def _load_settings_snapshot(services) -> dict[str, Any]:
    device_settings = services.display.read_device_settings()
    device_settings["image_fit_mode"] = services.database.get_setting("image_fit_mode") or "fill"
    device_settings["slideshow_interval"] = services.display.get_slideshow_interval()
    cooldown_raw = services.database.get_setting("new_image_cooldown")
    device_settings["new_image_cooldown"] = int(cooldown_raw) if cooldown_raw is not None else 3600
    schedule = services.display.get_sleep_schedule()
    device_settings["sleep_schedule"] = f"{schedule[0]}–{schedule[1]}" if schedule else ""
    device_settings["scheduled_change_time"] = services.database.get_setting("scheduled_change_time") or ""
    rotation_limit_raw = services.database.get_setting("rotation_limit")
    try:
        device_settings["rotation_limit"] = int(rotation_limit_raw) if rotation_limit_raw is not None else 100
    except (TypeError, ValueError):
        device_settings["rotation_limit"] = 100
    return device_settings


def _format_settings_menu_text(notice: str | None = None) -> str:
    if notice:
        return f"{notice}\n\nEinstellungen"
    return "Einstellungen"


def _settings_menu_keyboard(settings: dict[str, Any]) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("Bildoptimierung", callback_data=_settings_callback_data("select", 0)),
            InlineKeyboardButton(f"Ausrichtung: {_get_current_value(settings, _SETTINGS[1])}", callback_data=_settings_callback_data("select", 1)),
        ],
        [
            InlineKeyboardButton(f"Bildanpassung: {_get_current_value(settings, _SETTINGS[2])}", callback_data=_settings_callback_data("select", 2)),
            InlineKeyboardButton(f"Anzeigedauer: {_get_current_value(settings, _SETTINGS[3])}", callback_data=_settings_callback_data("select", 3)),
        ],
        [
            InlineKeyboardButton(f"Ruhezeit: {_get_current_value(settings, _SETTINGS[4])}", callback_data=_settings_callback_data("select", 4)),
            InlineKeyboardButton(f"Neue Bilder: {_get_current_value(settings, _SETTINGS[5])}", callback_data=_settings_callback_data("select", 5)),
        ],
        [
            InlineKeyboardButton(f"Täglicher Wechsel: {_get_current_value(settings, _SETTINGS[6])}", callback_data=_settings_callback_data("select", 6)),
            InlineKeyboardButton(f"Bilder in Rotation: {_get_current_value(settings, _SETTINGS[7])}", callback_data=_settings_callback_data("select", 7)),
        ],
        [InlineKeyboardButton("Abbrechen", callback_data=_settings_callback_data("close"))],
    ]
    return InlineKeyboardMarkup(rows)


def _settings_prompt_keyboard(idx: int, s: _SettingDef) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if s.kind == "orientation":
        rows.append(
            [
                InlineKeyboardButton("Hochformat", callback_data=_settings_callback_data("apply", idx, "vertical")),
                InlineKeyboardButton("Querformat", callback_data=_settings_callback_data("apply", idx, "horizontal")),
            ]
        )
    elif s.kind == "fit_mode":
        rows.append(
            [
                InlineKeyboardButton("Zuschneiden", callback_data=_settings_callback_data("apply", idx, "fill")),
                InlineKeyboardButton("Einpassen", callback_data=_settings_callback_data("apply", idx, "contain")),
            ]
        )
    elif s.kind == "integer":
        rows.append([InlineKeyboardButton("Unbegrenzt", callback_data=_settings_callback_data("apply", idx, "unlimited"))])
    elif s.kind in {"sleep_schedule", "scheduled_time"}:
        rows.append([InlineKeyboardButton("Deaktivieren", callback_data=_settings_callback_data("disable", idx))])

    rows.append(
        [
            InlineKeyboardButton("Zurück", callback_data=_settings_callback_data("back")),
            InlineKeyboardButton("Abbrechen", callback_data=_settings_callback_data("close")),
        ]
    )
    return InlineKeyboardMarkup(rows)


def _prompt_text_for_setting(services, idx: int, notice: str | None = None) -> str:
    s = _SETTINGS[idx]
    if s.kind == "cooldown":
        cooldown_raw = services.database.get_setting("new_image_cooldown")
        cooldown_val = int(cooldown_raw) if cooldown_raw is not None else 3600
        current = "Deaktiviert" if cooldown_val == 0 else _format_interval_label(cooldown_val)
    elif s.kind == "fit_mode":
        current_raw = services.database.get_setting("image_fit_mode") or "fill"
        current = _FIT_MODE_LABELS.get(current_raw, current_raw)
    elif s.kind == "integer":
        current = _format_rotation_limit_value(services.database.get_setting(s.key) or "100")
    elif s.kind == "interval":
        raw_seconds = services.display.get_slideshow_interval()
        current = _format_interval_label(raw_seconds)
    elif s.kind == "sleep_schedule":
        sleep_sched = services.display.get_sleep_schedule()
        current = f"{sleep_sched[0]}–{sleep_sched[1]}" if sleep_sched else "Keine"
    elif s.kind == "scheduled_time":
        raw = services.database.get_setting("scheduled_change_time")
        current = str(raw) if raw else "Deaktiviert"
    else:
        current = _get_current_value(services.display.read_device_settings(), s)

    lines: list[str] = []
    if notice:
        lines.extend([notice, ""])

    if s.kind == "cooldown":
        lines.extend(
            [
                f"Aktueller Wert für {s.label}: {current}",
                "",
                "Wie lange soll ein neues Bild mindestens angezeigt werden, bevor das nächste neue Bild erscheint?",
                "Beispiele: 30m, 1h, 2h, 6h",
                "0 = deaktiviert (jedes neue Bild wird sofort angezeigt)",
                "(Maximum 24 Stunden)",
            ]
        )
    elif s.kind == "orientation":
        lines.extend(
            [
                f"Aktueller Wert für {s.label}: {current}",
                "Wähle den neuen Wert per Button.",
            ]
        )
    elif s.kind == "fit_mode":
        lines.extend(
            [
                f"Aktueller Wert für {s.label}: {current}",
                "",
                "Zuschneiden — Bild wird auf den Rahmen zugeschnitten (Ränder werden ggf. abgeschnitten)",
                "Einpassen — Bild wird vollständig angezeigt (unscharfer Hintergrund füllt die Ränder)",
                "",
                "Wähle den neuen Wert per Button.",
            ]
        )
    elif s.kind == "interval":
        lines.extend(
            [
                f"Aktueller Wert für {s.label}: {current}",
                "",
                "Wie lange soll jedes Bild angezeigt werden?",
                "Beispiele: 30m, 1h, 2h, 6h, 1d",
                "(Minimum 5 Minuten, Maximum 7 Tage)",
            ]
        )
    elif s.kind == "sleep_schedule":
        lines.extend(
            [
                f"Aktueller Wert für {s.label}: {current}",
                "",
                "Gib die Ruhezeit im Format HH:MM-HH:MM ein (z.B. 22:00-08:00).",
                "Das Display wechselt das Bild in dieser Zeit nicht.",
                "",
                "Oder deaktiviere die Ruhezeit per Button.",
            ]
        )
    elif s.kind == "scheduled_time":
        lines.extend(
            [
                f"Aktueller Wert für {s.label}: {current}",
                "",
                "Gib die Uhrzeit ein, zu der täglich ein neues Bild angezeigt werden soll (z.B. 8:00, 08:30).",
                "Wenn aktiv, wird der Bildwechsel genau zu dieser Zeit ausgelöst — unabhängig von der Anzeigedauer.",
                "",
                "Oder deaktiviere den täglichen Bildwechsel per Button.",
            ]
        )
    elif s.kind == "integer":
        lines.extend(
            [
                f"Aktueller Wert für {s.label}: {current}",
                "",
                "Wie viele der neuesten Bilder pro Bibliothek sollen in der Rotation bleiben?",
                "Ältere Bilder bleiben gespeichert, werden aber nicht mehr automatisch angezeigt.",
                "",
                "Erlaubt sind Werte zwischen 1 und 1000.",
                "Oder aktiviere Unbegrenzt per Button.",
            ]
        )
    else:
        lines.extend(
            [
                f"Aktueller Wert für {s.label}: {current}",
                "Gib den neuen Wert ein (z.B. 1.0, 1.4, 2.0):",
            ]
        )
    return "\n".join(lines)


def _format_image_tuning_menu_text(notice: str | None = None) -> str:
    if notice:
        return f"{notice}\n\nBildoptimierung"
    return "Bildoptimierung"


def _image_tuning_menu_keyboard(draft: dict[str, float]) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                f"Sättigung: {_format_tuning_value(draft['saturation'])}",
                callback_data=_settings_callback_data("tuning_field", "saturation"),
            ),
        ],
        [
            InlineKeyboardButton(
                f"Kontrast: {_format_tuning_value(draft['contrast'])}",
                callback_data=_settings_callback_data("tuning_field", "contrast"),
            ),
        ],
        [
            InlineKeyboardButton(
                f"Schärfe: {_format_tuning_value(draft['sharpness'])}",
                callback_data=_settings_callback_data("tuning_field", "sharpness"),
            ),
        ],
        [
            InlineKeyboardButton(
                f"Helligkeit: {_format_tuning_value(draft['brightness'])}",
                callback_data=_settings_callback_data("tuning_field", "brightness"),
            ),
        ],
        [InlineKeyboardButton("Speichern", callback_data=_settings_callback_data("tuning_save"))],
        [
            InlineKeyboardButton("Zurück", callback_data=_settings_callback_data("tuning_back")),
            InlineKeyboardButton("Abbrechen", callback_data=_settings_callback_data("close")),
        ],
    ]
    return InlineKeyboardMarkup(rows)


def _image_tuning_prompt_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Zurück", callback_data=_settings_callback_data("tuning_menu")),
                InlineKeyboardButton("Abbrechen", callback_data=_settings_callback_data("close")),
            ]
        ]
    )


async def _show_image_tuning_menu(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    notice: str | None = None,
) -> int:
    services = get_services(context)
    context.user_data.pop(PENDING_SETTINGS_KEY, None)
    context.user_data.pop(PENDING_IMAGE_TUNING_FIELD_KEY, None)
    draft, _ = _ensure_image_tuning_state(context, services)
    await _respond_settings_message(
        update,
        _format_image_tuning_menu_text(notice),
        reply_markup=_image_tuning_menu_keyboard(draft),
    )
    return WAITING_FOR_SETTINGS_CHOICE


async def _show_image_tuning_prompt(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    field_key: str,
    *,
    notice: str | None = None,
) -> int:
    services = get_services(context)
    draft, _ = _ensure_image_tuning_state(context, services)
    label = _IMAGE_TUNING_LABELS[field_key]
    context.user_data[PENDING_IMAGE_TUNING_FIELD_KEY] = field_key
    context.user_data.pop(PENDING_SETTINGS_KEY, None)

    lines: list[str] = []
    if notice:
        lines.extend([notice, ""])
    lines.extend(
        [
            f"Aktueller Entwurf für {label}: {_format_tuning_value(draft[field_key])}",
            "",
            "Gib den neuen Wert ein (z.B. 1.0, 1.4, 2.0):",
        ]
    )
    await _respond_settings_message(
        update,
        "\n".join(lines),
        reply_markup=_image_tuning_prompt_keyboard(),
    )
    return WAITING_FOR_SETTINGS_VALUE


def _next_fire_delay_for_orientation(services, active_orientation: str) -> int | None:
    rendered_count = services.database.count_rendered_images(active_orientation)
    if rendered_count <= 0:
        return None
    from app.conversations import _cooldown_remaining, _get_cooldown_seconds

    cooldown = _get_cooldown_seconds(services)
    if cooldown <= 0:
        return 1
    remaining = _cooldown_remaining(services, cooldown)
    return max(1, remaining) if remaining > 0 else 1


async def _switch_orientation_library(context: ContextTypes.DEFAULT_TYPE, orientation: str) -> tuple[bool, str]:
    services = get_services(context)
    lock = get_display_lock(context)
    async with lock:
        target = services.database.get_newest_eligible_orientation_image(orientation)
        if target is None:
            return False, (
                f"Es gibt noch keine Bilder für {format_orientation_label(orientation)}. "
                "Das aktuell angezeigte Bild bleibt unverändert."
            )

        was_rendered = target.status == "rendered"
        begin_display_transition(services.database, target.image_id, "orientation_switch")
        result = await _display_target(services, target)
        if not result.success:
            if was_rendered:
                target.status = "display_failed"
                target.last_error = result.message
                services.database.apply_image_and_settings(target, clear_keys=DISPLAY_TRANSITION_KEYS)
            else:
                clear_display_transition(services.database)
            return False, f"Passendes Bild konnte nicht angezeigt werden: {result.message}"

        if was_rendered:
            target.status = "displayed"
            target.last_error = None
        commit_display_success(
            services.database,
            target,
            mark_new_image=was_rendered,
        )

        from app.slideshow import reschedule_slideshow_job

        first_seconds = _next_fire_delay_for_orientation(services, orientation)
        if first_seconds is None:
            reschedule_slideshow_job(context.application)
        else:
            reschedule_slideshow_job(context.application, first_seconds=first_seconds)

    return True, f"Zeige jetzt {format_orientation_label(orientation)}-Bild {target.image_id}."


@require_admin
async def settings_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return await _show_settings_menu(update, context)


async def _respond_settings_message(
    update: Update,
    text: str,
    *,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    query = update.callback_query
    if query is not None:
        await _edit_query_message(query, text, reply_markup=reply_markup)
        return
    if update.effective_message is not None:
        await update.effective_message.reply_text(text, reply_markup=reply_markup)


async def _delete_settings_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return
    await _delete_query_message(context, query)


async def _show_settings_menu(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    notice: str | None = None,
) -> int:
    services = get_services(context)
    context.user_data.pop(PENDING_SETTINGS_KEY, None)
    _clear_image_tuning_state(context)
    try:
        snapshot = _load_settings_snapshot(services)
    except Exception as exc:
        logger.exception("Failed to read device settings")
        await _respond_settings_message(update, f"Fehler beim Lesen der Einstellungen: {exc}")
        return ConversationHandler.END
    await _respond_settings_message(
        update,
        _format_settings_menu_text(notice),
        reply_markup=_settings_menu_keyboard(snapshot),
    )
    return WAITING_FOR_SETTINGS_CHOICE


async def _show_setting_prompt(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    idx: int,
    *,
    notice: str | None = None,
) -> int:
    services = get_services(context)
    context.user_data[PENDING_SETTINGS_KEY] = idx
    await _respond_settings_message(
        update,
        _prompt_text_for_setting(services, idx, notice),
        reply_markup=_settings_prompt_keyboard(idx, _SETTINGS[idx]),
    )
    return WAITING_FOR_SETTINGS_VALUE


async def _authorize_settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    query = update.callback_query
    user = update.effective_user
    if query is None:
        return False

    await query.answer()
    if user is None:
        return False

    services = get_services(context)
    services.auth.sync_user(user)
    if not services.auth.is_admin(user.id):
        await _edit_query_message(query, "Dieser Befehl ist nur für Admins verfügbar.")
        return False
    return True


async def settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await _authorize_settings_callback(update, context):
        return ConversationHandler.END

    parts = (update.callback_query.data or "").split("|")
    action = parts[1] if len(parts) > 1 else ""
    if action in {"open", "menu"}:
        return await _show_settings_menu(update, context)
    if action == "close":
        context.user_data.pop(PENDING_SETTINGS_KEY, None)
        _clear_image_tuning_state(context)
        await _delete_settings_message(update, context)
        return ConversationHandler.END
    if action == "back":
        return await _show_settings_menu(update, context)
    if action == "tuning_menu":
        return await _show_image_tuning_menu(update, context)
    if action == "tuning_field" and len(parts) > 2:
        field_key = parts[2]
        if field_key not in _IMAGE_TUNING_LABELS:
            await _respond_settings_message(update, "Unbekannte Bildoptimierung.")
            return WAITING_FOR_SETTINGS_CHOICE
        return await _show_image_tuning_prompt(update, context, field_key)
    if action == "tuning_back":
        return await _show_settings_menu(update, context)
    if action == "tuning_save":
        return await _save_image_tuning(update, context)
    if action == "select" and len(parts) > 2:
        try:
            idx = int(parts[2])
        except ValueError:
            await _respond_settings_message(update, "Unbekannte Einstellung.")
            return WAITING_FOR_SETTINGS_CHOICE
        if idx < 0 or idx >= len(_SETTINGS):
            await _respond_settings_message(update, "Unbekannte Einstellung.")
            return WAITING_FOR_SETTINGS_CHOICE
        if _SETTINGS[idx].kind == "image_tuning":
            return await _show_image_tuning_menu(update, context)
        return await _show_setting_prompt(update, context, idx)
    if action == "apply" and len(parts) > 3:
        try:
            idx = int(parts[2])
        except ValueError:
            await _respond_settings_message(update, "Unbekannte Einstellung.")
            return WAITING_FOR_SETTINGS_CHOICE
        return await _apply_setting_value(update, context, idx, parts[3])
    if action == "disable" and len(parts) > 2:
        try:
            idx = int(parts[2])
        except ValueError:
            await _respond_settings_message(update, "Unbekannte Einstellung.")
            return WAITING_FOR_SETTINGS_CHOICE
        return await _apply_setting_value(update, context, idx, "keine")

    await _respond_settings_message(update, "Unbekannte Auswahl.")
    return WAITING_FOR_SETTINGS_CHOICE


async def receive_settings_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.effective_message is None:
        return ConversationHandler.END
    text = (update.effective_message.text or "").strip()
    try:
        choice = int(text)
    except ValueError:
        await update.effective_message.reply_text(
            f"Bitte antworte mit einer Zahl zwischen 1 und {len(_SETTINGS)}, oder nutze den Button „Abbrechen“."
        )
        return WAITING_FOR_SETTINGS_CHOICE
    if choice < 1 or choice > len(_SETTINGS):
        await update.effective_message.reply_text(
            f"Ungültige Auswahl. Bitte wähle eine Zahl zwischen 1 und {len(_SETTINGS)}, oder nutze den Button „Abbrechen“."
        )
        return WAITING_FOR_SETTINGS_CHOICE

    idx = choice - 1
    if _SETTINGS[idx].kind == "image_tuning":
        return await _show_image_tuning_menu(update, context)
    return await _show_setting_prompt(update, context, idx)


async def receive_settings_value(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.effective_message is None:
        return ConversationHandler.END
    pending_tuning_field = context.user_data.get(PENDING_IMAGE_TUNING_FIELD_KEY)
    if isinstance(pending_tuning_field, str):
        return await _apply_image_tuning_value(update, context, pending_tuning_field, update.effective_message.text or "")
    idx = context.user_data.pop(PENDING_SETTINGS_KEY, None)
    if idx is None:
        return WAITING_FOR_SETTINGS_CHOICE
    return await _apply_setting_value(update, context, idx, update.effective_message.text or "")


async def _apply_image_tuning_value(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    field_key: str,
    raw_text: str,
) -> int:
    text = raw_text.strip().lower()
    try:
        value = float(text.replace(",", "."))
    except ValueError:
        return await _show_image_tuning_prompt(
            update,
            context,
            field_key,
            notice="Ungültiger Wert. Bitte gib eine Zahl ein (z.B. 1.0), oder nutze den Button „Abbrechen“.",
        )
    if value < 0.1 or value > 3.0:
        return await _show_image_tuning_prompt(
            update,
            context,
            field_key,
            notice="Der Wert muss zwischen 0.1 und 3.0 liegen. Bitte erneut eingeben oder den Button „Abbrechen“ nutzen.",
        )

    services = get_services(context)
    draft, _ = _ensure_image_tuning_state(context, services)
    draft[field_key] = value
    context.user_data.pop(PENDING_IMAGE_TUNING_FIELD_KEY, None)
    return await _show_image_tuning_menu(
        update,
        context,
        notice=f"{_IMAGE_TUNING_LABELS[field_key]} ist jetzt {_format_tuning_value(value)}.",
    )


async def _save_image_tuning(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    services = get_services(context)
    draft, base = _ensure_image_tuning_state(context, services)
    changed_updates = {
        key: value
        for key, value in draft.items()
        if _normalize_tuning_value(base.get(key)) != value
    }
    if not changed_updates:
        return await _show_settings_menu(update, context, notice="Bildoptimierung unverändert.")

    try:
        result = services.display.apply_device_settings({"image_settings": changed_updates}, refresh_current=True)
    except Exception as exc:
        logger.exception("Failed to write grouped image settings")
        return await _show_image_tuning_menu(
            update,
            context,
            notice=f"Fehler beim Speichern der Bildoptimierung: {exc}",
        )

    confirmed_settings = result.confirmed_settings if result.confirmed_settings else {}
    confirmed_image_settings = confirmed_settings.get("image_settings")
    if isinstance(confirmed_image_settings, dict):
        for key, value in confirmed_image_settings.items():
            if key in draft:
                draft[key] = _normalize_tuning_value(value)

    path_note = f" (device.json: {result.device_config_path})" if result.device_config_path else ""
    status_prefix = "Bildoptimierung wurde gespeichert" if result.success else "Bildoptimierung wurde teilweise gespeichert"
    return await _show_settings_menu(update, context, notice=f"{status_prefix}{path_note}.\n{result.message}")


async def _apply_setting_value(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    idx: int,
    raw_text: str,
) -> int:
    s = _SETTINGS[idx]
    text = raw_text.strip().lower()
    services = get_services(context)

    if s.kind == "image_tuning":
        return await _show_image_tuning_menu(update, context)

    if s.kind == "cooldown":
        if text in ("0", "aus", "off", "deaktiviert", "deaktivieren"):
            seconds = 0
        else:
            seconds = _parse_interval_input(text)
        if seconds is None:
            return await _show_setting_prompt(
                update,
                context,
                idx,
                notice="Ungültiges Format. Beispiele: 30m, 1h, 2h, 0 — oder nutze den Button „Abbrechen“.",
            )
        if seconds < 0 or seconds > 86400:
            return await _show_setting_prompt(
                update,
                context,
                idx,
                notice="Der Wert muss zwischen 0 und 24 Stunden liegen. Bitte erneut eingeben oder den Button „Abbrechen“ nutzen.",
            )
        services.database.set_setting("new_image_cooldown", str(seconds))
        label = "Deaktiviert" if seconds == 0 else _format_interval_label(seconds)
        return await _show_settings_menu(update, context, notice=f"{s.label} ist jetzt {label}.")

    if s.kind == "orientation":
        orientation = normalize_orientation_value(text)
        if orientation is None:
            return await _show_setting_prompt(
                update,
                context,
                idx,
                notice="Ungültiger Wert. Bitte gib Hochformat oder Querformat ein, oder nutze den Button „Abbrechen“.",
            )
        updates = {
            "orientation": orientation,
            "inverted_image": orientation == "vertical",
        }
        try:
            result = services.display.apply_device_settings(updates, refresh_current=False)
        except Exception as exc:
            logger.exception("Failed to write orientation settings")
            await _respond_settings_message(update, f"Fehler beim Speichern der Einstellungen: {exc}")
            return WAITING_FOR_SETTINGS_VALUE

        orientation_label = format_orientation_label(orientation)
        path_note = f" (device.json: {result.device_config_path})" if result.device_config_path else ""
        if not result.success:
            return await _show_settings_menu(
                update,
                context,
                notice=f"{s.label} wurde als {orientation_label} gespeichert{path_note}.\n{result.message}",
            )

        _, switch_message = await _switch_orientation_library(context, orientation)
        return await _show_settings_menu(
            update,
            context,
            notice=f"{s.label} ist jetzt {orientation_label}{path_note}.\n{switch_message}\n{result.message}",
        )

    if s.kind == "fit_mode":
        normalized = " ".join(text.split())
        fit_mode = _FIT_MODE_MAP.get(normalized)
        if fit_mode is None:
            return await _show_setting_prompt(
                update,
                context,
                idx,
                notice="Ungültiger Wert. Bitte gib Zuschneiden oder Einpassen ein, oder nutze den Button „Abbrechen“.",
            )
        services.database.set_setting("image_fit_mode", fit_mode)
        label = _FIT_MODE_LABELS.get(fit_mode, fit_mode)
        return await _show_settings_menu(update, context, notice=f"{s.label} ist jetzt {label}.")

    if s.kind == "integer":
        if text in ("unlimited", "unbegrenzt", "ohne limit", "kein limit", "0"):
            services.database.set_setting(s.key, "0")
            return await _show_settings_menu(
                update,
                context,
                notice=f"{s.label} ist jetzt Unbegrenzt. Alle gespeicherten Bilder bleiben in der Rotation.",
            )
        try:
            int_value = int(text.replace(",", ""))
        except ValueError:
            return await _show_setting_prompt(
                update,
                context,
                idx,
                notice="Ungültiger Wert. Bitte gib eine ganze Zahl ein, oder nutze den Button „Abbrechen“.",
            )
        if int_value < 1 or int_value > 1000:
            return await _show_setting_prompt(
                update,
                context,
                idx,
                notice="Der Wert muss zwischen 1 und 1000 liegen. Bitte erneut eingeben oder den Button „Abbrechen“ nutzen.",
            )
        services.database.set_setting(s.key, str(int_value))
        return await _show_settings_menu(
            update,
            context,
            notice=f"{s.label} ist jetzt {int_value}. Ältere Bilder bleiben gespeichert.",
        )

    if s.kind == "sleep_schedule":
        if text in ("keine", "kein", "no", "off", "deaktivieren", "deaktiviert"):
            try:
                result = services.display.set_sleep_schedule(None, None)
            except Exception as exc:
                logger.exception("Failed to disable sleep schedule")
                await _respond_settings_message(update, f"Fehler beim Speichern: {exc}")
                return WAITING_FOR_SETTINGS_VALUE
            status = "Ruhezeit ist deaktiviert" if result.success else "Ruhezeit wurde deaktiviert"
            return await _show_settings_menu(update, context, notice=f"{status}.\n{result.message}")

        normalized = text.replace("–", "-").replace("—", "-")
        parts = normalized.split("-", 1)
        if len(parts) != 2:
            return await _show_setting_prompt(
                update,
                context,
                idx,
                notice="Ungültiges Format. Bitte im Format HH:MM-HH:MM eingeben (z.B. 22:00-08:00), oder den Button „Abbrechen“ nutzen.",
            )
        sleep_start = _parse_time_string(parts[0])
        wake_up = _parse_time_string(parts[1])
        if sleep_start is None or wake_up is None:
            return await _show_setting_prompt(
                update,
                context,
                idx,
                notice="Ungültige Uhrzeit. Bitte im Format HH:MM-HH:MM eingeben (z.B. 22:00-08:00), oder den Button „Abbrechen“ nutzen.",
            )
        if sleep_start == wake_up:
            return await _show_setting_prompt(
                update,
                context,
                idx,
                notice="Schlaf- und Aufwachzeit dürfen nicht gleich sein. Bitte erneut eingeben oder den Button „Abbrechen“ nutzen.",
            )
        try:
            result = services.display.set_sleep_schedule(sleep_start, wake_up)
        except Exception as exc:
            logger.exception("Failed to set sleep schedule")
            await _respond_settings_message(update, f"Fehler beim Speichern: {exc}")
            return WAITING_FOR_SETTINGS_VALUE
        if result.success:
            status = f"Ruhezeit ist jetzt {sleep_start}–{wake_up}. Das Display wechselt das Bild in dieser Zeit nicht"
        else:
            status = f"Ruhezeit wurde als {sleep_start}–{wake_up} gespeichert"
        return await _show_settings_menu(update, context, notice=f"{status}.\n{result.message}")

    if s.kind == "interval":
        seconds = _parse_interval_input(text)
        if seconds is None:
            return await _show_setting_prompt(
                update,
                context,
                idx,
                notice="Ungültiges Format. Beispiele: 30m, 1h, 2h, 1d — oder nutze den Button „Abbrechen“.",
            )
        if seconds < _INTERVAL_MIN or seconds > _INTERVAL_MAX:
            return await _show_setting_prompt(
                update,
                context,
                idx,
                notice="Der Wert muss zwischen 5 Minuten und 7 Tagen liegen. Bitte erneut eingeben oder den Button „Abbrechen“ nutzen.",
            )
        try:
            result = services.display.set_slideshow_interval(seconds)
        except Exception as exc:
            logger.exception("Failed to set slideshow interval")
            await _respond_settings_message(update, f"Fehler beim Speichern: {exc}")
            return WAITING_FOR_SETTINGS_VALUE
        label = _format_interval_label(seconds)
        status = f"{s.label} ist jetzt {label}" if result.success else f"{s.label} wurde als {label} gespeichert"
        from app.slideshow import reschedule_slideshow_job

        reschedule_slideshow_job(context.application, interval_seconds=seconds)
        scheduled = services.database.get_setting("scheduled_change_time") or ""
        note = (
            f"\nDer Timer wurde ab jetzt auf {label} zurückgesetzt."
            if not scheduled
            else f"\nHinweis: Täglicher Bildwechsel um {scheduled} ist aktiv und überschreibt diese Anzeigedauer momentan."
        )
        return await _show_settings_menu(update, context, notice=f"{status}.{note}\n{result.message}")

    if s.kind == "scheduled_time":
        if text in ("keine", "kein", "no", "off", "deaktivieren", "deaktiviert"):
            services.database.set_setting("scheduled_change_time", "")
            from app.slideshow import reschedule_slideshow_job

            reschedule_slideshow_job(context.application)
            return await _show_settings_menu(
                update,
                context,
                notice="Täglicher Bildwechsel ist deaktiviert. Die Anzeigedauer-Einstellung wird wieder verwendet.",
            )
        time_str = _parse_time_string(text)
        if time_str is None:
            return await _show_setting_prompt(
                update,
                context,
                idx,
                notice="Ungültige Uhrzeit. Bitte im Format HH:MM eingeben (z.B. 08:00, 8:30), oder den Button „Abbrechen“ nutzen.",
            )
        services.database.set_setting("scheduled_change_time", time_str)
        from app.slideshow import reschedule_slideshow_job

        reschedule_slideshow_job(context.application)
        return await _show_settings_menu(
            update,
            context,
            notice=(
                f"Täglicher Bildwechsel ist jetzt um {time_str} Uhr. "
                "Die Anzeigedauer-Einstellung wird ignoriert, solange dieser Wert aktiv ist."
            ),
        )

    return await _show_settings_menu(update, context, notice="Unbekannte Einstellung.")


async def _settings_unexpected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    state = context.user_data.get(PENDING_SETTINGS_KEY) or context.user_data.get(PENDING_IMAGE_TUNING_FIELD_KEY)
    if update.effective_message is not None:
        if state is None:
            await update.effective_message.reply_text("Bitte nutze die Buttons im Einstellungs-Menü oder den Button „Abbrechen“.")
        else:
            await update.effective_message.reply_text("Bitte beantworte die aktuelle Frage, nutze die Buttons oder den Button „Abbrechen“.")
    return WAITING_FOR_SETTINGS_VALUE if state is not None else WAITING_FOR_SETTINGS_CHOICE


async def _settings_timeout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.pop(PENDING_SETTINGS_KEY, None)
    _clear_image_tuning_state(context)
    if update and update.effective_message:
        await update.effective_message.reply_text(
            "Einstellungs-Sitzung nach 2 Minuten Inaktivität beendet. Nutze /settings um neu zu starten."
        )
    return ConversationHandler.END


def build_settings_conversation() -> ConversationHandler:
    return ConversationHandler(
        entry_points=[
            CommandHandler("settings", settings_entry),
            CallbackQueryHandler(settings_callback, pattern=r"^settings\|open$"),
        ],
        states={
            WAITING_FOR_SETTINGS_CHOICE: [
                CallbackQueryHandler(settings_callback, pattern=r"^settings\|"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_settings_choice),
                MessageHandler(filters.ALL & ~filters.COMMAND, _settings_unexpected),
            ],
            WAITING_FOR_SETTINGS_VALUE: [
                CallbackQueryHandler(settings_callback, pattern=r"^settings\|"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_settings_value),
                MessageHandler(filters.ALL & ~filters.COMMAND, _settings_unexpected),
            ],
            ConversationHandler.TIMEOUT: [
                MessageHandler(filters.ALL, _settings_timeout),
            ],
        },
        allow_reentry=True,
        name="settings",
        persistent=False,
        conversation_timeout=120,
    )
