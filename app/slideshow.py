from __future__ import annotations

import json
import logging
from datetime import datetime, time as dt_time, timedelta, timezone

from telegram.ext import Application

from app.commands import _display_target
from app.conversations import _cooldown_remaining, _get_cooldown_seconds
from app.database import utcnow_iso

logger = logging.getLogger(__name__)

JOB_NAME = "slideshow_advance"


def _set_next_fire_at(services, seconds: int) -> None:
    """Store the expected next-fire timestamp so /list can show an accurate countdown."""
    next_fire = datetime.now(timezone.utc) + timedelta(seconds=seconds)
    services.database.set_setting("slideshow_next_fire_at", next_fire.isoformat())


def schedule_slideshow_job(application: Application) -> None:
    """Schedule the slideshow auto-advance job. Called once on startup."""
    if application.job_queue is None:
        logger.warning("JobQueue not available — auto-advance disabled. Install: pip install 'python-telegram-bot[job-queue]'")
        return
    services = application.bot_data["services"]
    interval = services.display.get_slideshow_interval()

    # If rendered images are waiting, fire sooner based on cooldown
    rendered_count = services.database.count_rendered_images()
    if rendered_count > 0:
        cooldown = _get_cooldown_seconds(services)
        remaining = _cooldown_remaining(services, cooldown)
        first = max(1, remaining) if remaining > 0 else 1
        logger.info("Startup: %d rendered image(s) waiting, first fire in %ds", rendered_count, first)
    else:
        first = interval

    application.job_queue.run_repeating(
        _advance_slideshow,
        interval=interval,
        first=first,
        name=JOB_NAME,
    )
    _set_next_fire_at(services, first)
    logger.info("Slideshow job scheduled with interval %ds", interval)


def reschedule_slideshow_job(
    application: Application,
    interval_seconds: int | None = None,
    first_seconds: int | None = None,
) -> None:
    """Remove and re-schedule the slideshow job.

    Args:
        interval_seconds: Repeating interval. Default: read from device settings.
        first_seconds: Delay before first fire. Default: same as interval_seconds.
    """
    if application.job_queue is None:
        return
    jobs = application.job_queue.get_jobs_by_name(JOB_NAME)
    for job in jobs:
        job.schedule_removal()

    services = application.bot_data["services"]
    if interval_seconds is None:
        interval_seconds = services.display.get_slideshow_interval()
    if first_seconds is None:
        first_seconds = interval_seconds

    application.job_queue.run_repeating(
        _advance_slideshow,
        interval=interval_seconds,
        first=first_seconds,
        name=JOB_NAME,
    )
    # Sync the displayed-at timestamp so /list countdown matches the new timer
    services.database.set_setting("current_image_displayed_at", utcnow_iso())
    _set_next_fire_at(services, first_seconds)
    logger.info("Slideshow job rescheduled with interval %ds, first in %ds", interval_seconds, first_seconds)


def _seconds_until_wake_up(schedule: tuple[str, str]) -> int:
    """Seconds from now until the sleep window's wake-up time."""
    _, wake_up_str = schedule
    wh, wm = wake_up_str.split(":")
    wake_up = dt_time(int(wh), int(wm))
    now = datetime.now()
    target = now.replace(hour=wake_up.hour, minute=wake_up.minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return max(1, int((target - now).total_seconds()))


def _is_in_sleep_window(schedule: tuple[str, str]) -> bool:
    """Check if current local time falls inside the sleep window."""
    sleep_start_str, wake_up_str = schedule
    try:
        sh, sm = sleep_start_str.split(":")
        wh, wm = wake_up_str.split(":")
        sleep_start = dt_time(int(sh), int(sm))
        wake_up = dt_time(int(wh), int(wm))
    except (ValueError, AttributeError):
        return False

    now = datetime.now().time()

    if sleep_start > wake_up:
        # Overnight window, e.g. 22:00-08:00
        return now >= sleep_start or now < wake_up
    else:
        # Same-day window, e.g. 13:00-15:00
        return sleep_start <= now < wake_up


async def _try_display_next_rendered(context, services, lock) -> bool:
    """Try to display the next rendered (cooldown-queued) image. Returns True if displayed."""
    rendered = services.database.get_oldest_rendered_image()
    if rendered is None:
        return False

    cooldown = _get_cooldown_seconds(services)
    remaining = _cooldown_remaining(services, cooldown)

    if remaining > 0:
        # Cooldown not expired yet — reschedule for when it expires
        _reschedule_for(context.application, remaining)
        logger.info("Rendered image %s waiting, cooldown expires in %ds", rendered.image_id, remaining)
        return True  # Signal that we handled the tick (don't also do normal rotation)

    # Cooldown expired — display this rendered image
    from pathlib import Path as _Path
    rendered_path = _Path(rendered.local_rendered_path) if rendered.local_rendered_path else None
    if rendered_path is None or not rendered_path.exists():
        # Re-render if needed
        rendered_path = services.storage.rendered_path(rendered.image_id)
        original_path = _Path(rendered.local_original_path)
        if not original_path.exists():
            logger.warning("Rendered image %s has no files, marking failed", rendered.image_id)
            rendered.status = "failed"
            rendered.last_error = "Original file missing"
            services.database.upsert_image(rendered)
            return True

    result = await _display_target(services, rendered)
    if result.success:
        rendered.status = "displayed"
        rendered.last_error = None
        services.database.upsert_image(rendered)
        services.database.set_setting("current_image_displayed_at", utcnow_iso())
        services.database.set_setting("last_new_image_displayed_at", utcnow_iso())
        logger.info("Displayed rendered image %s from cooldown queue", rendered.image_id)

        # If more rendered images are waiting, use cooldown as next interval
        more = services.database.count_rendered_images()
        next_interval = cooldown if (more > 0 and cooldown > 0) else services.display.get_slideshow_interval()
        _reschedule_for(context.application, next_interval)
    else:
        rendered.status = "display_failed"
        rendered.last_error = result.message
        services.database.upsert_image(rendered)
        logger.warning("Failed to display rendered image %s: %s", rendered.image_id, result.message)

    return True


def _reschedule_for(application: Application, seconds: int) -> None:
    """Reschedule the slideshow job to fire in `seconds`."""
    if application.job_queue is None:
        return
    jobs = application.job_queue.get_jobs_by_name(JOB_NAME)
    for job in jobs:
        job.schedule_removal()
    services = application.bot_data["services"]
    interval = services.display.get_slideshow_interval()
    first = max(1, seconds)
    application.job_queue.run_repeating(
        _advance_slideshow,
        interval=interval,
        first=first,
        name=JOB_NAME,
    )
    _set_next_fire_at(services, first)
    logger.info("Slideshow rescheduled, next fire in %ds", first)


async def _advance_slideshow(context) -> None:
    """Auto-advance to the next image. Called by JobQueue."""
    logger.debug("Slideshow auto-advance job triggered")
    services = context.application.bot_data["services"]
    lock = context.application.bot_data["display_lock"]

    # Check sleep schedule
    schedule = services.display.get_sleep_schedule()
    if schedule and _is_in_sleep_window(schedule):
        wake_seconds = _seconds_until_wake_up(schedule)
        _reschedule_for(context.application, wake_seconds)
        logger.info("Sleep active — rescheduled slideshow for wake-up in %ds", wake_seconds)
        return

    # Skip if display is busy (non-blocking check)
    if lock.locked():
        logger.info("Skipping auto-advance — display is busy")
        return

    async with lock:
        # Priority: display rendered (cooldown-queued) images first
        if await _try_display_next_rendered(context, services, lock):
            return

        # Normal rotation
        payload_path = services.config.storage.current_payload_path
        if not payload_path.exists():
            return

        try:
            payload = json.loads(payload_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            logger.warning("Auto-advance: could not read payload")
            return

        current_image_id = payload.get("image_id")
        if not current_image_id:
            return

        target = services.database.get_adjacent_image(current_image_id, "next")
        if target is None:
            logger.debug("Auto-advance: no next image (only 1 in rotation?)")
            return

        result = await _display_target(services, target)

        if result.success:
            services.database.set_setting("current_image_displayed_at", utcnow_iso())
            interval = services.display.get_slideshow_interval()
            _set_next_fire_at(services, interval)
            logger.info("Auto-advanced to image %s", target.image_id)
        else:
            logger.warning("Auto-advance failed: %s", result.message)
