from __future__ import annotations

from datetime import datetime, time as dt_time, timedelta, timezone


def local_now() -> datetime:
    return datetime.now(timezone.utc).astimezone()


def local_today_iso() -> str:
    return local_now().date().isoformat()


def seconds_until_local_time(time_str: str, occurrence_index: int = 0) -> int:
    hour_str, minute_str = time_str.split(":")
    now = local_now()
    target_time = dt_time(int(hour_str), int(minute_str))
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
    return max(1, int((target - now).total_seconds()))


def seconds_until_wake_up_time(wake_up_str: str) -> int:
    return seconds_until_local_time(wake_up_str)


def is_in_local_time_window(start_str: str, end_str: str) -> bool:
    sh, sm = start_str.split(":")
    eh, em = end_str.split(":")
    start = dt_time(int(sh), int(sm))
    end = dt_time(int(eh), int(em))
    now = local_now().time()
    if start > end:
        return now >= start or now < end
    return start <= now < end
