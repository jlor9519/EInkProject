from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

from telegram import Update
from telegram.ext import CommandHandler, ContextTypes, ConversationHandler, MessageHandler, filters

from app.auth import require_admin
from app.commands import _display_target, get_display_lock, get_services
from app.database import utcnow_iso
from app.orientation import format_orientation_label, normalize_orientation_value

WAITING_FOR_SETTINGS_CHOICE, WAITING_FOR_SETTINGS_VALUE = range(10, 12)
PENDING_SETTINGS_KEY = "pending_settings_choice"


@dataclass(slots=True)
class _SettingDef:
    label: str
    key: str               # top-level key in device.json, or db setting key
    subkey: str | None     # key inside image_settings, or None
    kind: str              # "float" | "orientation" | "fit_mode" | "integer" | "interval" | "sleep_schedule"


_SETTINGS: list[_SettingDef] = [
    _SettingDef("Sättigung",           "image_settings",       "saturation", "float"),
    _SettingDef("Kontrast",            "image_settings",       "contrast",   "float"),
    _SettingDef("Schärfe",             "image_settings",       "sharpness",  "float"),
    _SettingDef("Helligkeit",          "image_settings",       "brightness", "float"),
    _SettingDef("Ausrichtung",         "orientation",          None,         "orientation"),
    _SettingDef("Bildanpassung",       "image_fit_mode",       None,         "fit_mode"),
    _SettingDef("Anzeigedauer",        "slideshow_interval",   None,         "interval"),
    _SettingDef("Ruhezeit",            "sleep_schedule",       None,         "sleep_schedule"),
    _SettingDef("Wartezeit neue Bilder", "new_image_cooldown", None,         "cooldown"),
    _SettingDef("Täglicher Bildwechsel", "scheduled_change_time", None,     "scheduled_time"),
]

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
    if s.kind == "orientation":
        return format_orientation_label(str(settings.get("orientation", "?")))
    if s.kind == "fit_mode":
        raw = str(settings.get("image_fit_mode", "fill"))
        return _FIT_MODE_LABELS.get(raw, raw)
    if s.kind == "integer":
        return str(settings.get(s.key, "50"))
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


def _format_settings_list(settings: dict[str, Any]) -> str:
    lines = ["Aktuelle Einstellungen:", ""]
    for i, s in enumerate(_SETTINGS, 1):
        lines.append(f"{i}. {s.label}: {_get_current_value(settings, s)}")
    lines.append("")
    lines.append("Welche Einstellung möchtest du ändern? Antworte mit der Nummer oder /cancel.")
    return "\n".join(lines)


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
        result = await _display_target(services, target)
        if not result.success:
            if was_rendered:
                target.status = "display_failed"
                target.last_error = result.message
                services.database.upsert_image(target)
            return False, f"Passendes Bild konnte nicht angezeigt werden: {result.message}"

        if was_rendered:
            target.status = "displayed"
            target.last_error = None
            services.database.upsert_image(target)
            services.database.set_setting("last_new_image_displayed_at", utcnow_iso())

        services.database.set_setting("current_image_displayed_at", utcnow_iso())

        from app.slideshow import reschedule_slideshow_job

        first_seconds = _next_fire_delay_for_orientation(services, orientation)
        if first_seconds is None:
            reschedule_slideshow_job(context.application)
        else:
            reschedule_slideshow_job(context.application, first_seconds=first_seconds)

    return True, f"Zeige jetzt {format_orientation_label(orientation)}-Bild {target.image_id}."


@require_admin
async def settings_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    services = get_services(context)
    if update.effective_message is None:
        return ConversationHandler.END
    try:
        device_settings = services.display.read_device_settings()
    except Exception as exc:
        logger.exception("Failed to read device settings")
        await update.effective_message.reply_text(f"Fehler beim Lesen der Einstellungen: {exc}")
        return ConversationHandler.END
    # Inject database-stored settings for display
    device_settings["image_fit_mode"] = services.database.get_setting("image_fit_mode") or "fill"
    device_settings["slideshow_interval"] = services.display.get_slideshow_interval()
    cooldown_raw = services.database.get_setting("new_image_cooldown")
    device_settings["new_image_cooldown"] = int(cooldown_raw) if cooldown_raw is not None else 3600
    schedule = services.display.get_sleep_schedule()
    device_settings["sleep_schedule"] = f"{schedule[0]}–{schedule[1]}" if schedule else ""
    device_settings["scheduled_change_time"] = services.database.get_setting("scheduled_change_time") or ""
    await update.effective_message.reply_text(_format_settings_list(device_settings))
    return WAITING_FOR_SETTINGS_CHOICE


async def receive_settings_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.effective_message is None:
        return ConversationHandler.END
    text = (update.effective_message.text or "").strip()
    try:
        choice = int(text)
    except ValueError:
        await update.effective_message.reply_text(
            f"Bitte antworte mit einer Zahl zwischen 1 und {len(_SETTINGS)}, oder nutze /cancel."
        )
        return WAITING_FOR_SETTINGS_CHOICE
    if choice < 1 or choice > len(_SETTINGS):
        await update.effective_message.reply_text(
            f"Ungültige Auswahl. Bitte wähle eine Zahl zwischen 1 und {len(_SETTINGS)}, oder nutze /cancel."
        )
        return WAITING_FOR_SETTINGS_CHOICE

    s = _SETTINGS[choice - 1]
    context.user_data[PENDING_SETTINGS_KEY] = choice - 1

    services = get_services(context)
    if s.kind == "cooldown":
        cooldown_raw = services.database.get_setting("new_image_cooldown")
        cooldown_val = int(cooldown_raw) if cooldown_raw is not None else 3600
        current = "Deaktiviert" if cooldown_val == 0 else _format_interval_label(cooldown_val)
    elif s.kind == "fit_mode":
        current_raw = services.database.get_setting("image_fit_mode") or "fill"
        current = _FIT_MODE_LABELS.get(current_raw, current_raw)
    elif s.kind == "integer":
        current = services.database.get_setting(s.key) or "50"
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

    if s.kind == "cooldown":
        await update.effective_message.reply_text(
            f"Aktueller Wert für {s.label}: {current}\n\n"
            "Wie lange soll ein neues Bild mindestens angezeigt werden, bevor das nächste neue Bild erscheint?\n"
            "Beispiele: 30m, 1h, 2h, 6h\n"
            "0 = deaktiviert (jedes neue Bild wird sofort angezeigt)\n"
            "(Maximum 24 Stunden)"
        )
    elif s.kind == "orientation":
        await update.effective_message.reply_text(
            f"Aktueller Wert für {s.label}: {current}\n"
            "Gib den neuen Wert ein: Hochformat oder Querformat."
        )
    elif s.kind == "fit_mode":
        await update.effective_message.reply_text(
            f"Aktueller Wert für {s.label}: {current}\n\n"
            "Zuschneiden — Bild wird auf den Rahmen zugeschnitten (Ränder werden ggf. abgeschnitten)\n"
            "Einpassen — Bild wird vollständig angezeigt (unscharfer Hintergrund füllt die Ränder)\n\n"
            "Gib den neuen Wert ein: Zuschneiden oder Einpassen."
        )
    elif s.kind == "interval":
        await update.effective_message.reply_text(
            f"Aktueller Wert für {s.label}: {current}\n\n"
            "Wie lange soll jedes Bild angezeigt werden?\n"
            "Beispiele: 30m, 1h, 2h, 6h, 1d\n"
            "(Minimum 5 Minuten, Maximum 7 Tage)"
        )
    elif s.kind == "sleep_schedule":
        await update.effective_message.reply_text(
            f"Aktueller Wert für {s.label}: {current}\n\n"
            "Gib die Ruhezeit im Format HH:MM-HH:MM ein (z.B. 22:00-08:00).\n"
            "Das Display wechselt das Bild in dieser Zeit nicht.\n\n"
            "Oder gib \"keine\" ein, um die Ruhezeit zu deaktivieren."
        )
    elif s.kind == "scheduled_time":
        await update.effective_message.reply_text(
            f"Aktueller Wert für {s.label}: {current}\n\n"
            "Gib die Uhrzeit ein, zu der täglich ein neues Bild angezeigt werden soll (z.B. 8:00, 08:30).\n"
            "Wenn aktiv, wird der Bildwechsel genau zu dieser Zeit ausgelöst — unabhängig von der Anzeigedauer.\n\n"
            "Oder gib \"keine\" ein, um den täglichen Bildwechsel zu deaktivieren."
        )
    else:
        await update.effective_message.reply_text(
            f"Aktueller Wert für {s.label}: {current}\n"
            "Gib den neuen Wert ein (z.B. 1.0, 1.4, 2.0):"
        )
    return WAITING_FOR_SETTINGS_VALUE


async def receive_settings_value(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.effective_message is None:
        return ConversationHandler.END
    idx = context.user_data.pop(PENDING_SETTINGS_KEY, None)
    if idx is None:
        return ConversationHandler.END

    s = _SETTINGS[idx]
    text = (update.effective_message.text or "").strip().lower()
    services = get_services(context)

    if s.kind == "cooldown":
        if text in ("0", "aus", "off", "deaktiviert", "deaktivieren"):
            seconds = 0
        else:
            seconds = _parse_interval_input(text)
        if seconds is None:
            await update.effective_message.reply_text(
                "Ungültiges Format. Beispiele: 30m, 1h, 2h, 0 — oder nutze /cancel."
            )
            context.user_data[PENDING_SETTINGS_KEY] = idx
            return WAITING_FOR_SETTINGS_VALUE
        if seconds < 0 or seconds > 86400:
            await update.effective_message.reply_text(
                "Der Wert muss zwischen 0 und 24 Stunden liegen. Bitte erneut eingeben oder /cancel."
            )
            context.user_data[PENDING_SETTINGS_KEY] = idx
            return WAITING_FOR_SETTINGS_VALUE
        services.database.set_setting("new_image_cooldown", str(seconds))
        label = "Deaktiviert" if seconds == 0 else _format_interval_label(seconds)
        await update.effective_message.reply_text(f"{s.label} ist jetzt {label}.")
        return ConversationHandler.END

    if s.kind == "orientation":
        orientation = normalize_orientation_value(text)
        if orientation is None:
            await update.effective_message.reply_text(
                "Ungültiger Wert. Bitte gib Hochformat oder Querformat ein, oder nutze /cancel."
            )
            context.user_data[PENDING_SETTINGS_KEY] = idx
            return WAITING_FOR_SETTINGS_VALUE
        updates = {
            "orientation": orientation,
            "inverted_image": orientation == "vertical",
        }
        try:
            result = services.display.apply_device_settings(updates, refresh_current=False)
        except Exception as exc:
            logger.exception("Failed to write orientation settings")
            await update.effective_message.reply_text(f"Fehler beim Speichern der Einstellungen: {exc}")
            return ConversationHandler.END

        orientation_label = format_orientation_label(orientation)
        path_note = f" (device.json: {result.device_config_path})" if result.device_config_path else ""
        if not result.success:
            await update.effective_message.reply_text(
                f"{s.label} wurde als {orientation_label} gespeichert{path_note}.\n{result.message}"
            )
            return ConversationHandler.END

        switched, switch_message = await _switch_orientation_library(context, orientation)
        await update.effective_message.reply_text(
            f"{s.label} ist jetzt {orientation_label}{path_note}.\n{switch_message}\n{result.message}"
        )
        return ConversationHandler.END
    elif s.kind == "fit_mode":
        normalized = " ".join(text.split())
        fit_mode = _FIT_MODE_MAP.get(normalized)
        if fit_mode is None:
            await update.effective_message.reply_text(
                "Ungültiger Wert. Bitte gib Zuschneiden oder Einpassen ein, oder nutze /cancel."
            )
            context.user_data[PENDING_SETTINGS_KEY] = idx
            return WAITING_FOR_SETTINGS_VALUE
        services.database.set_setting("image_fit_mode", fit_mode)
        label = _FIT_MODE_LABELS.get(fit_mode, fit_mode)
        await update.effective_message.reply_text(f"{s.label} ist jetzt {label}.")
        return ConversationHandler.END
    elif s.kind == "integer":
        try:
            int_value = int(text.replace(",", ""))
        except ValueError:
            await update.effective_message.reply_text(
                "Ungültiger Wert. Bitte gib eine ganze Zahl ein, oder nutze /cancel."
            )
            context.user_data[PENDING_SETTINGS_KEY] = idx
            return WAITING_FOR_SETTINGS_VALUE
        if int_value < 5 or int_value > 500:
            await update.effective_message.reply_text(
                "Der Wert muss zwischen 5 und 500 liegen. Bitte erneut eingeben oder /cancel."
            )
            context.user_data[PENDING_SETTINGS_KEY] = idx
            return WAITING_FOR_SETTINGS_VALUE
        services.database.set_setting(s.key, str(int_value))
        await update.effective_message.reply_text(f"{s.label} ist jetzt {int_value} Bilder.")
        return ConversationHandler.END
    elif s.kind == "sleep_schedule":
        if text in ("keine", "kein", "no", "off", "deaktivieren"):
            try:
                result = services.display.set_sleep_schedule(None, None)
            except Exception as exc:
                logger.exception("Failed to disable sleep schedule")
                await update.effective_message.reply_text(f"Fehler beim Speichern: {exc}")
                return ConversationHandler.END
            status = "Ruhezeit ist deaktiviert" if result.success else "Ruhezeit wurde deaktiviert"
            await update.effective_message.reply_text(f"{status}.\n{result.message}")
            return ConversationHandler.END
        # Expect HH:MM-HH:MM (en-dash or hyphen)
        raw = text.replace("–", "-").replace("—", "-")
        parts = raw.split("-", 1)
        if len(parts) != 2:
            await update.effective_message.reply_text(
                "Ungültiges Format. Bitte im Format HH:MM-HH:MM eingeben (z.B. 22:00-08:00), oder /cancel."
            )
            context.user_data[PENDING_SETTINGS_KEY] = idx
            return WAITING_FOR_SETTINGS_VALUE
        sleep_start = _parse_time_string(parts[0])
        wake_up = _parse_time_string(parts[1])
        if sleep_start is None or wake_up is None:
            await update.effective_message.reply_text(
                "Ungültige Uhrzeit. Bitte im Format HH:MM-HH:MM eingeben (z.B. 22:00-08:00), oder /cancel."
            )
            context.user_data[PENDING_SETTINGS_KEY] = idx
            return WAITING_FOR_SETTINGS_VALUE
        if sleep_start == wake_up:
            await update.effective_message.reply_text(
                "Schlaf- und Aufwachzeit dürfen nicht gleich sein. Bitte erneut eingeben oder /cancel."
            )
            context.user_data[PENDING_SETTINGS_KEY] = idx
            return WAITING_FOR_SETTINGS_VALUE
        try:
            result = services.display.set_sleep_schedule(sleep_start, wake_up)
        except Exception as exc:
            logger.exception("Failed to set sleep schedule")
            await update.effective_message.reply_text(f"Fehler beim Speichern: {exc}")
            return ConversationHandler.END
        if result.success:
            status = f"Ruhezeit ist jetzt {sleep_start}–{wake_up}. Das Display wechselt das Bild in dieser Zeit nicht"
        else:
            status = f"Ruhezeit wurde als {sleep_start}–{wake_up} gespeichert"
        await update.effective_message.reply_text(f"{status}.\n{result.message}")
        return ConversationHandler.END
    elif s.kind == "interval":
        seconds = _parse_interval_input(text)
        if seconds is None:
            await update.effective_message.reply_text(
                "Ungültiges Format. Beispiele: 30m, 1h, 2h, 1d — oder nutze /cancel."
            )
            context.user_data[PENDING_SETTINGS_KEY] = idx
            return WAITING_FOR_SETTINGS_VALUE
        if seconds < _INTERVAL_MIN or seconds > _INTERVAL_MAX:
            await update.effective_message.reply_text(
                f"Der Wert muss zwischen 5 Minuten und 7 Tagen liegen. Bitte erneut eingeben oder /cancel."
            )
            context.user_data[PENDING_SETTINGS_KEY] = idx
            return WAITING_FOR_SETTINGS_VALUE
        try:
            result = services.display.set_slideshow_interval(seconds)
        except Exception as exc:
            logger.exception("Failed to set slideshow interval")
            await update.effective_message.reply_text(f"Fehler beim Speichern: {exc}")
            return ConversationHandler.END
        label = _format_interval_label(seconds)
        status = f"{s.label} ist jetzt {label}" if result.success else f"{s.label} wurde als {label} gespeichert"
        await update.effective_message.reply_text(f"{status}.\n{result.message}")
        from app.slideshow import reschedule_slideshow_job
        reschedule_slideshow_job(context.application, interval_seconds=seconds)
        return ConversationHandler.END
    elif s.kind == "scheduled_time":
        if text in ("keine", "kein", "no", "off", "deaktivieren", "deaktiviert"):
            services.database.set_setting("scheduled_change_time", "")
            await update.effective_message.reply_text("Täglicher Bildwechsel ist deaktiviert. Die Anzeigedauer-Einstellung wird wieder verwendet.")
            from app.slideshow import reschedule_slideshow_job
            reschedule_slideshow_job(context.application)
            return ConversationHandler.END
        time_str = _parse_time_string(text)
        if time_str is None:
            await update.effective_message.reply_text(
                "Ungültige Uhrzeit. Bitte im Format HH:MM eingeben (z.B. 08:00, 8:30), oder /cancel."
            )
            context.user_data[PENDING_SETTINGS_KEY] = idx
            return WAITING_FOR_SETTINGS_VALUE
        services.database.set_setting("scheduled_change_time", time_str)
        await update.effective_message.reply_text(
            f"Täglicher Bildwechsel ist jetzt um {time_str} Uhr. Die Anzeigedauer-Einstellung wird ignoriert, solange dieser Wert aktiv ist."
        )
        from app.slideshow import reschedule_slideshow_job
        reschedule_slideshow_job(context.application)
        return ConversationHandler.END
    else:
        try:
            value = float(text.replace(",", "."))
        except ValueError:
            await update.effective_message.reply_text(
                "Ungültiger Wert. Bitte gib eine Zahl ein (z.B. 1.0), oder nutze /cancel."
            )
            context.user_data[PENDING_SETTINGS_KEY] = idx
            return WAITING_FOR_SETTINGS_VALUE
        if value < 0.1 or value > 3.0:
            await update.effective_message.reply_text(
                "Der Wert muss zwischen 0.1 und 3.0 liegen. Bitte erneut eingeben oder /cancel."
            )
            context.user_data[PENDING_SETTINGS_KEY] = idx
            return WAITING_FOR_SETTINGS_VALUE

        updates = {"image_settings": {str(s.subkey): value}}
        requested_value = value

    try:
        result = services.display.apply_device_settings(updates, refresh_current=True)
    except Exception as exc:
        logger.exception("Failed to write device settings")
        await update.effective_message.reply_text(f"Fehler beim Speichern der Einstellungen: {exc}")
        return ConversationHandler.END

    confirmed_value = _get_current_value(result.confirmed_settings, s) if result.confirmed_settings else str(requested_value)
    path_note = f" (device.json: {result.device_config_path})" if result.device_config_path else ""
    status_prefix = (
        f"{s.label} ist jetzt {confirmed_value}"
        if result.success
        else f"{s.label} wurde als {confirmed_value} gespeichert"
    )
    await update.effective_message.reply_text(f"{status_prefix}{path_note}.\n{result.message}")
    return ConversationHandler.END


async def _settings_unexpected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    state = context.user_data.get(PENDING_SETTINGS_KEY)
    if update.effective_message is not None:
        await update.effective_message.reply_text("Bitte beantworte die aktuelle Frage oder nutze /cancel.")
    return WAITING_FOR_SETTINGS_VALUE if state is not None else WAITING_FOR_SETTINGS_CHOICE


async def _settings_timeout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.pop(PENDING_SETTINGS_KEY, None)
    if update and update.effective_message:
        await update.effective_message.reply_text(
            "Einstellungs-Sitzung nach 2 Minuten Inaktivität beendet. Nutze /settings um neu zu starten."
        )
    return ConversationHandler.END


async def settings_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.pop(PENDING_SETTINGS_KEY, None)
    if update.effective_message is not None:
        await update.effective_message.reply_text("Einstellungs-Änderung abgebrochen.")
    return ConversationHandler.END


def build_settings_conversation() -> ConversationHandler:
    return ConversationHandler(
        entry_points=[CommandHandler("settings", settings_entry)],
        states={
            WAITING_FOR_SETTINGS_CHOICE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_settings_choice),
                MessageHandler(filters.ALL & ~filters.COMMAND, _settings_unexpected),
            ],
            WAITING_FOR_SETTINGS_VALUE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_settings_value),
                MessageHandler(filters.ALL & ~filters.COMMAND, _settings_unexpected),
            ],
            ConversationHandler.TIMEOUT: [
                MessageHandler(filters.ALL, _settings_timeout),
            ],
        },
        fallbacks=[CommandHandler("cancel", settings_cancel)],
        allow_reentry=True,
        name="settings",
        persistent=False,
        conversation_timeout=120,
    )
