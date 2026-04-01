from __future__ import annotations

import unittest
from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

from app.time_utils import (
    is_in_local_time_window,
    is_local_datetime_in_window,
    move_local_datetime_to_window_end,
    seconds_until_local_time,
    seconds_until_wake_up_time,
)


class TimeUtilsTests(unittest.TestCase):
    def test_seconds_until_local_time_uses_aware_local_datetime(self) -> None:
        berlin = ZoneInfo("Europe/Berlin")
        now = datetime(2026, 10, 25, 8, 30, tzinfo=berlin)

        with patch("app.time_utils.local_now", return_value=now):
            self.assertEqual(seconds_until_local_time("09:00"), 1800)

    def test_seconds_until_wake_up_time_handles_overnight_window(self) -> None:
        berlin = ZoneInfo("Europe/Berlin")
        now = datetime(2026, 3, 29, 23, 0, tzinfo=berlin)

        with patch("app.time_utils.local_now", return_value=now):
            self.assertEqual(seconds_until_wake_up_time("08:00"), 9 * 3600)

    def test_is_in_local_time_window_handles_overnight_quiet_hours(self) -> None:
        berlin = ZoneInfo("Europe/Berlin")
        now = datetime(2026, 3, 29, 23, 30, tzinfo=berlin)

        with patch("app.time_utils.local_now", return_value=now):
            self.assertTrue(is_in_local_time_window("22:00", "08:00"))

    def test_move_local_datetime_to_window_end_handles_same_day_window(self) -> None:
        berlin = ZoneInfo("Europe/Berlin")
        candidate = datetime(2026, 4, 1, 22, 30, tzinfo=berlin)

        adjusted = move_local_datetime_to_window_end(candidate, "22:00", "23:30")

        self.assertEqual(adjusted, datetime(2026, 4, 1, 23, 30, tzinfo=berlin))

    def test_move_local_datetime_to_window_end_handles_overnight_window(self) -> None:
        berlin = ZoneInfo("Europe/Berlin")
        candidate = datetime(2026, 3, 29, 23, 30, tzinfo=berlin)

        adjusted = move_local_datetime_to_window_end(candidate, "22:00", "08:00")

        self.assertEqual(adjusted, datetime(2026, 3, 30, 8, 0, tzinfo=berlin))

    def test_local_datetime_window_respects_sleep_start_and_wake_up_boundaries(self) -> None:
        berlin = ZoneInfo("Europe/Berlin")
        at_sleep_start = datetime(2026, 3, 29, 22, 0, tzinfo=berlin)
        at_wake_up = datetime(2026, 3, 30, 8, 0, tzinfo=berlin)

        self.assertTrue(is_local_datetime_in_window(at_sleep_start, "22:00", "08:00"))
        self.assertFalse(is_local_datetime_in_window(at_wake_up, "22:00", "08:00"))
        self.assertEqual(
            move_local_datetime_to_window_end(at_sleep_start, "22:00", "08:00"),
            datetime(2026, 3, 30, 8, 0, tzinfo=berlin),
        )
        self.assertEqual(
            move_local_datetime_to_window_end(at_wake_up, "22:00", "08:00"),
            at_wake_up,
        )


if __name__ == "__main__":
    unittest.main()
