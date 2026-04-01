from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from telegram.ext import Application

from app.commands import _display_target
from app.conversations import _cooldown_remaining, _get_cooldown_seconds
from app.display_state import (
    DISPLAY_TRANSITION_KEYS,
    begin_display_transition,
    clear_display_transition,
    commit_display_success,
)
from app.time_utils import (
    is_in_local_time_window,
    seconds_until_local_time,
    seconds_until_wake_up_time,
)

logger = logging.getLogger(__name__)

JOB_NAME = "slideshow_advance"
RETRY_DELAY_SECONDS = 60


@dataclass(slots=True)
class NextFireDecision:
    seconds: int
    mode: str
    detail: str | None = None


def _set_next_fire_at(services, seconds: int) -> None:
    """Store the expected next-fire timestamp so /list can show an accurate countdown."""
    next_fire = datetime.now(timezone.utc) + timedelta(seconds=seconds)
    services.database.set_setting("slideshow_next_fire_at", next_fire.isoformat())


def _set_next_fire_decision(services, decision: NextFireDecision) -> None:
    _set_next_fire_at(services, decision.seconds)
    services.database.set_setting("slideshow_next_fire_mode", decision.mode)
    services.database.set_setting("slideshow_next_fire_detail", decision.detail or "")


def get_stored_next_fire_metadata(services) -> tuple[str | None, str | None]:
    mode = services.database.get_setting("slideshow_next_fire_mode") or None
    detail = services.database.get_setting("slideshow_next_fire_detail") or None
    return mode, detail


def schedule_slideshow_job(application: Application) -> None:
    """Schedule the slideshow auto-advance job. Called once on startup."""
    if application.job_queue is None:
        logger.warning("JobQueue not available — auto-advance disabled. Install: pip install 'python-telegram-bot[job-queue]'")
        return
    services = application.bot_data["services"]
    active_orientation = services.display.current_orientation()
    interval = services.display.get_slideshow_interval()
    decision = compute_next_fire_decision(services, active_orientation)

    application.job_queue.run_repeating(
        _advance_slideshow,
        interval=interval,
        first=decision.seconds,
        name=JOB_NAME,
    )
    _set_next_fire_decision(services, decision)
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
        decision = compute_next_fire_decision(services, services.display.current_orientation())
    else:
        decision = NextFireDecision(seconds=first_seconds, mode="manual_reset")

    application.job_queue.run_repeating(
        _advance_slideshow,
        interval=interval_seconds,
        first=decision.seconds,
        name=JOB_NAME,
    )
    _set_next_fire_decision(services, decision)
    logger.info("Slideshow job rescheduled with interval %ds, first in %ds", interval_seconds, decision.seconds)


def _get_scheduled_time(services) -> str | None:
    """Return the scheduled daily change time as 'HH:MM', or None if disabled."""
    return services.database.get_setting("scheduled_change_time") or None


def _seconds_until_scheduled_occurrence(time_str: str, occurrence_index: int = 0) -> int:
    """Seconds from now until the next occurrence of HH:MM, optionally N days later."""
    return seconds_until_local_time(time_str, occurrence_index)


def _seconds_until_time(time_str: str) -> int:
    """Seconds from now until the next occurrence of HH:MM (today or tomorrow)."""
    return _seconds_until_scheduled_occurrence(time_str, 0)


def compute_next_fire_decision(services, active_orientation: str | None = None) -> NextFireDecision:
    if active_orientation is None:
        active_orientation = services.display.current_orientation()

    schedule = services.display.get_sleep_schedule()
    if schedule and _is_in_sleep_window(schedule):
        return NextFireDecision(
            seconds=_seconds_until_wake_up(schedule),
            mode="quiet_hours",
            detail=schedule[1],
        )

    rendered_count = services.database.count_rendered_images(active_orientation)
    if rendered_count > 0:
        cooldown = _get_cooldown_seconds(services)
        remaining = _cooldown_remaining(services, cooldown)
        first = max(1, remaining) if remaining > 0 else 1
        logger.info("Next fire uses rendered queue/cooldown in %ds", first)
        return NextFireDecision(seconds=first, mode="cooldown_queue")

    scheduled = _get_scheduled_time(services)
    if scheduled:
        first = _seconds_until_time(scheduled)
        logger.info("Next fire uses scheduled mode in %ds (at %s)", first, scheduled)
        return NextFireDecision(seconds=first, mode="scheduled_daily", detail=scheduled)

    interval = services.display.get_slideshow_interval()
    return NextFireDecision(seconds=interval, mode="interval", detail=str(interval))


def _seconds_until_wake_up(schedule: tuple[str, str]) -> int:
    """Seconds from now until the sleep window's wake-up time."""
    _, wake_up_str = schedule
    return seconds_until_wake_up_time(wake_up_str)


def _is_in_sleep_window(schedule: tuple[str, str]) -> bool:
    """Check if current local time falls inside the sleep window."""
    sleep_start_str, wake_up_str = schedule
    try:
        return is_in_local_time_window(sleep_start_str, wake_up_str)
    except (ValueError, AttributeError):
        return False


async def _try_display_next_rendered(context, services) -> bool:
    """Try to display the next rendered (cooldown-queued) image. Returns True if displayed."""
    active_orientation = services.display.current_orientation()
    rendered = services.database.get_oldest_rendered_image_for_orientation(active_orientation)
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
            _reschedule_for(
                context.application,
                NextFireDecision(seconds=RETRY_DELAY_SECONDS, mode="payload_missing"),
            )
            return True

    begin_display_transition(services.database, rendered.image_id, "slideshow")
    result = await _display_target(services, rendered)
    if result.success:
        rendered.status = "displayed"
        rendered.last_error = None
        commit_display_success(
            services.database,
            rendered,
            mark_new_image=True,
        )
        logger.info("Displayed rendered image %s from cooldown queue", rendered.image_id)

        # If more rendered images are waiting, use cooldown as next interval
        next_decision = compute_next_fire_decision(services, active_orientation)
        _reschedule_for(context.application, next_decision)
    else:
        rendered.status = "display_failed"
        rendered.last_error = result.message
        services.database.apply_image_and_settings(
            rendered,
            clear_keys=DISPLAY_TRANSITION_KEYS,
        )
        logger.warning("Failed to display rendered image %s: %s", rendered.image_id, result.message)
        _reschedule_for(
            context.application,
            NextFireDecision(seconds=RETRY_DELAY_SECONDS, mode="display_error", detail=result.message),
        )

    return True


def _reschedule_for(application: Application, decision: int | NextFireDecision) -> None:
    """Reschedule the slideshow job to fire in `seconds`."""
    if application.job_queue is None:
        return
    jobs = application.job_queue.get_jobs_by_name(JOB_NAME)
    for job in jobs:
        job.schedule_removal()
    services = application.bot_data["services"]
    interval = services.display.get_slideshow_interval()
    if isinstance(decision, int):
        decision = NextFireDecision(seconds=decision, mode="manual_reset")
    first = max(1, decision.seconds)
    application.job_queue.run_repeating(
        _advance_slideshow,
        interval=interval,
        first=first,
        name=JOB_NAME,
    )
    _set_next_fire_decision(services, NextFireDecision(seconds=first, mode=decision.mode, detail=decision.detail))
    logger.info("Slideshow rescheduled, next fire in %ds", first)


async def _advance_slideshow(context) -> None:
    """Auto-advance to the next image. Called by JobQueue."""
    logger.debug("Slideshow auto-advance job triggered")
    services = context.application.bot_data["services"]
    lock = context.application.bot_data["display_lock"]
    active_orientation = services.display.current_orientation()

    # Check sleep schedule
    schedule = services.display.get_sleep_schedule()
    if schedule and _is_in_sleep_window(schedule):
        decision = NextFireDecision(
            seconds=_seconds_until_wake_up(schedule),
            mode="quiet_hours",
            detail=schedule[1],
        )
        _reschedule_for(context.application, decision)
        logger.info("Sleep active — rescheduled slideshow for wake-up in %ds", decision.seconds)
        return

    # Skip if display is busy (non-blocking check)
    if lock.locked():
        logger.info("Skipping auto-advance — display is busy")
        _reschedule_for(
            context.application,
            NextFireDecision(seconds=RETRY_DELAY_SECONDS, mode="retry_busy"),
        )
        return

    async with lock:
        # Priority: display rendered (cooldown-queued) images first
        if await _try_display_next_rendered(context, services):
            return

        # Normal rotation
        payload_path = services.config.storage.current_payload_path
        if not payload_path.exists():
            _reschedule_for(
                context.application,
                NextFireDecision(seconds=RETRY_DELAY_SECONDS, mode="payload_missing"),
            )
            return

        try:
            payload = json.loads(payload_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            logger.warning("Auto-advance: could not read payload")
            _reschedule_for(
                context.application,
                NextFireDecision(seconds=RETRY_DELAY_SECONDS, mode="payload_missing"),
            )
            return

        current_image_id = payload.get("image_id")
        if not current_image_id:
            _reschedule_for(
                context.application,
                NextFireDecision(seconds=RETRY_DELAY_SECONDS, mode="payload_missing"),
            )
            return

        target = services.database.get_adjacent_image(current_image_id, "next", active_orientation)
        if target is None:
            logger.debug("Auto-advance: no next image (only 1 in rotation?)")
            base_decision = compute_next_fire_decision(services, active_orientation)
            _reschedule_for(
                context.application,
                NextFireDecision(seconds=base_decision.seconds, mode="single_image", detail=base_decision.detail),
            )
            return

        begin_display_transition(services.database, target.image_id, "slideshow")
        result = await _display_target(services, target)

        if result.success:
            commit_display_success(
                services.database,
                target,
                mark_new_image=False,
            )
            _reschedule_for(context.application, compute_next_fire_decision(services, active_orientation))
            logger.info("Auto-advanced to image %s", target.image_id)
        else:
            logger.warning("Auto-advance failed: %s", result.message)
            clear_display_transition(services.database)
            _reschedule_for(
                context.application,
                NextFireDecision(seconds=RETRY_DELAY_SECONDS, mode="display_error", detail=result.message),
            )
