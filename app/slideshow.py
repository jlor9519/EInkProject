from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from telegram.ext import Application

from app.commands import _display_target
from app.conversations import _cooldown_remaining, _get_cooldown_seconds
from app.display_state import (
    begin_display_transition,
    commit_display_success,
    next_rotation_target,
    record_new_image_display_failure,
    record_rotation_display_failure,
)
from app.models import DISPLAY_VERIFICATION_ASSUMED_AFTER_RECOVERY
from app.time_utils import (
    is_in_local_time_window,
    local_now,
    move_local_datetime_to_window_end,
    next_local_time_occurrence,
    seconds_until_wake_up_time,
)

logger = logging.getLogger(__name__)

JOB_NAME = "slideshow_advance"
RETRY_DELAY_SECONDS = 60
# Give InkyPi time to come up after a (nightly) reboot before the first slideshow fire.
STARTUP_GRACE_SECONDS = 120


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
    if decision.seconds < STARTUP_GRACE_SECONDS:
        decision = NextFireDecision(seconds=STARTUP_GRACE_SECONDS, mode=decision.mode, detail=decision.detail)

    application.job_queue.run_repeating(
        _advance_slideshow,
        interval=interval,
        first=decision.seconds,
        name=JOB_NAME,
    )
    _set_next_fire_decision(services, decision)
    logger.info("Slideshow job scheduled with interval %ds, first in %ds", interval, decision.seconds)


def reschedule_slideshow_job(
    application: Application,
    interval_seconds: int | None = None,
    first_seconds: int | None = None,
    *,
    restart_interval: bool = False,
) -> None:
    """Remove and re-schedule the slideshow job.

    Args:
        interval_seconds: Repeating interval. Default: read from device settings.
        first_seconds: Delay before first fire. Default: computed by compute_next_fire_decision.
        restart_interval: Count the display interval from now instead of from when the
            current image was shown (used when the interval itself was just changed).
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
        decision = compute_next_fire_decision(
            services,
            services.display.current_orientation(),
            restart_interval=restart_interval,
        )
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


def _get_current_image_displayed_at(services) -> datetime | None:
    raw = services.database.get_setting("current_image_displayed_at")
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _adjust_candidate_for_quiet_hours(
    candidate_utc: datetime,
    schedule: tuple[str, str] | None,
) -> datetime:
    if not schedule:
        return candidate_utc
    adjusted_local = move_local_datetime_to_window_end(candidate_utc.astimezone(), schedule[0], schedule[1])
    return adjusted_local.astimezone(timezone.utc)


def project_display_change_offsets(
    services,
    count: int,
    *,
    first_fire_at: datetime | None = None,
) -> list[int]:
    if count <= 0:
        return []

    now_local = local_now()
    now_utc = now_local.astimezone(timezone.utc)
    schedule = services.display.get_sleep_schedule()
    scheduled_time = _get_scheduled_time(services)

    if scheduled_time:
        offsets: list[int] = []
        for occurrence_index in range(count):
            candidate_local = next_local_time_occurrence(
                scheduled_time,
                occurrence_index,
                reference=now_local,
            )
            if schedule:
                candidate_local = move_local_datetime_to_window_end(candidate_local, schedule[0], schedule[1])
            candidate_utc = candidate_local.astimezone(timezone.utc)
            offsets.append(max(0, int((candidate_utc - now_utc).total_seconds())))
        return offsets

    if first_fire_at is None:
        decision = compute_next_fire_decision(services)
        first_fire_at = now_utc + timedelta(seconds=decision.seconds)
    elif first_fire_at.tzinfo is None:
        first_fire_at = first_fire_at.replace(tzinfo=timezone.utc)

    interval = services.display.get_slideshow_interval()
    candidate_local = first_fire_at.astimezone()
    offsets = []
    for _ in range(count):
        if schedule:
            candidate_local = move_local_datetime_to_window_end(candidate_local, schedule[0], schedule[1])
        candidate_utc = candidate_local.astimezone(timezone.utc)
        offsets.append(max(0, int((candidate_utc - now_utc).total_seconds())))
        candidate_local = candidate_local + timedelta(seconds=interval)
    return offsets


def compute_next_fire_decision(
    services,
    active_orientation: str | None = None,
    *,
    restart_interval: bool = False,
) -> NextFireDecision:
    """Decide when the slideshow should fire next.

    A newly uploaded image stays at least for the new-image cooldown and otherwise follows
    the normal schedule (daily time or display interval), whichever comes later. The display
    interval is measured from when the current image was shown, so restarts do not reset it.
    """
    if active_orientation is None:
        active_orientation = services.display.current_orientation()

    now_local = local_now()
    now_utc = now_local.astimezone(timezone.utc)
    schedule = services.display.get_sleep_schedule()
    if schedule and _is_in_sleep_window(schedule):
        return NextFireDecision(
            seconds=_seconds_until_wake_up(schedule),
            mode="quiet_hours",
            detail=schedule[1],
        )

    rendered_count = services.database.count_rendered_images(active_orientation)
    cooldown = _get_cooldown_seconds(services)
    remaining = _cooldown_remaining(services, cooldown)
    if rendered_count > 0:
        first = max(1, remaining) if remaining > 0 else 1
        logger.info("Next fire uses rendered queue/cooldown in %ds", first)
        return NextFireDecision(seconds=first, mode="cooldown_queue")
    hold_until_utc = now_utc + timedelta(seconds=remaining)

    scheduled = _get_scheduled_time(services)
    if scheduled:
        candidate_local = next_local_time_occurrence(scheduled, reference=hold_until_utc.astimezone())
        candidate_utc = _adjust_candidate_for_quiet_hours(candidate_local.astimezone(timezone.utc), schedule)
        first = max(1, int((candidate_utc - now_utc).total_seconds()))
        logger.info("Next fire uses scheduled mode in %ds (at %s)", first, scheduled)
        return NextFireDecision(seconds=first, mode="scheduled_daily", detail=scheduled)

    interval = services.display.get_slideshow_interval()
    interval_base = None if restart_interval else _get_current_image_displayed_at(services)
    interval_due_utc = (interval_base or now_utc) + timedelta(seconds=interval)
    held = remaining > 0 and hold_until_utc > interval_due_utc
    candidate_utc = max(hold_until_utc if held else interval_due_utc, now_utc)
    candidate_utc = _adjust_candidate_for_quiet_hours(candidate_utc, schedule)
    first = max(1, int((candidate_utc - now_utc).total_seconds()))
    if held:
        logger.info("Next fire holds new image for %ds", first)
        return NextFireDecision(seconds=first, mode="new_image_hold")
    return NextFireDecision(seconds=first, mode="interval", detail=str(interval))


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
        if result.verification_state == DISPLAY_VERIFICATION_ASSUMED_AFTER_RECOVERY:
            rendered.status = "displayed_with_warnings"
            rendered.last_error = result.verification_detail
        else:
            rendered.status = "displayed"
            rendered.last_error = None
        commit_display_success(
            services.database,
            rendered,
            mark_new_image=True,
            verification_state=result.verification_state or "verified",
            verification_detail=result.verification_detail,
        )
        logger.info("Displayed rendered image %s from cooldown queue", rendered.image_id)

        # If more rendered images are waiting, use cooldown as next interval
        next_decision = compute_next_fire_decision(services, active_orientation)
        _reschedule_for(context.application, next_decision)
    else:
        record_new_image_display_failure(services.database, rendered, result.message)
        schedule_display_retry(context.application, result.message)

    return True


def schedule_display_retry(application: Application, detail: str | None = None) -> None:
    """Fire the slideshow again soon, e.g. to retry a queued image whose display failed."""
    _reschedule_for(
        application,
        NextFireDecision(seconds=RETRY_DELAY_SECONDS, mode="display_error", detail=detail),
    )


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

        target = next_rotation_target(services.database, current_image_id, active_orientation)
        if target is None:
            logger.debug("Auto-advance: no next image (only 1 in rotation?)")
            base_decision = compute_next_fire_decision(services, active_orientation, restart_interval=True)
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
                verification_state=result.verification_state or "verified",
                verification_detail=result.verification_detail,
            )
            _reschedule_for(context.application, compute_next_fire_decision(services, active_orientation))
            logger.info("Auto-advanced to image %s", target.image_id)
        else:
            logger.warning("Auto-advance to %s failed: %s", target.image_id, result.message)
            record_rotation_display_failure(services.database, target.image_id)
            _reschedule_for(
                context.application,
                NextFireDecision(seconds=RETRY_DELAY_SECONDS, mode="display_error", detail=result.message),
            )
