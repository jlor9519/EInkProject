from __future__ import annotations

from datetime import datetime, time as dt_time, timedelta, timezone


def local_now() -> datetime:
    return datetime.now(timezone.utc).astimezone()


def local_today_iso() -> str:
    return local_now().date().isoformat()


def _parse_local_time(time_str: str) -> dt_time:
    hour_str, minute_str = time_str.split(":")
    return dt_time(int(hour_str), int(minute_str))


def next_local_time_occurrence(
    time_str: str,
    occurrence_index: int = 0,
    *,
    reference: datetime | None = None,
) -> datetime:
    now = (reference or local_now()).astimezone()
    target_time = _parse_local_time(time_str)
    target = now.replace(
        hour=target_time.hour,
        minute=target_time.minute,
        second=0,
        microsecond=0,
    )
    if target <= now:
        target += timedelta(days=1)
    if occurrence_index > 0:
        target += timedelta(days=occurrence_index)
    return target


def seconds_until_local_time(time_str: str, occurrence_index: int = 0) -> int:
    now = local_now()
    target = next_local_time_occurrence(time_str, occurrence_index, reference=now)
    return max(1, int((target - now).total_seconds()))


def seconds_until_wake_up_time(wake_up_str: str) -> int:
    return seconds_until_local_time(wake_up_str)


def _window_bounds_containing(
    moment: datetime,
    start_str: str,
    end_str: str,
) -> tuple[datetime, datetime] | None:
    local_moment = moment.astimezone()
    start = _parse_local_time(start_str)
    end = _parse_local_time(end_str)

    if start <= end:
        window_start = local_moment.replace(hour=start.hour, minute=start.minute, second=0, microsecond=0)
        window_end = local_moment.replace(hour=end.hour, minute=end.minute, second=0, microsecond=0)
        if window_start <= local_moment < window_end:
            return window_start, window_end
        return None

    today_start = local_moment.replace(hour=start.hour, minute=start.minute, second=0, microsecond=0)
    today_end = local_moment.replace(hour=end.hour, minute=end.minute, second=0, microsecond=0)
    if local_moment.time() < end:
        window_start = today_start - timedelta(days=1)
        window_end = today_end
    else:
        window_start = today_start
        window_end = today_end + timedelta(days=1)

    if window_start <= local_moment < window_end:
        return window_start, window_end
    return None


def is_local_datetime_in_window(moment: datetime, start_str: str, end_str: str) -> bool:
    return _window_bounds_containing(moment, start_str, end_str) is not None


def move_local_datetime_to_window_end(moment: datetime, start_str: str, end_str: str) -> datetime:
    bounds = _window_bounds_containing(moment, start_str, end_str)
    if bounds is None:
        return moment.astimezone()
    return bounds[1]


def is_in_local_time_window(start_str: str, end_str: str) -> bool:
    return is_local_datetime_in_window(local_now(), start_str, end_str)
