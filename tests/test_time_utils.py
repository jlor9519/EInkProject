from __future__ import annotations

import os
import time
import unittest
from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

from app.time_utils import (
    is_in_local_time_window,
    is_local_datetime_in_window,
    move_local_datetime_to_window_end,
    next_local_time_occurrence,
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



class _BerlinTimezoneMixin:
    """Run the test with the process timezone set to Europe/Berlin."""

    def setUp(self) -> None:
        self._old_tz = os.environ.get("TZ")
        os.environ["TZ"] = "Europe/Berlin"
        time.tzset()

    def tearDown(self) -> None:
        if self._old_tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = self._old_tz
        time.tzset()


@unittest.skipUnless(hasattr(time, "tzset"), "needs time.tzset to switch the process timezone")
class DaylightSavingTests(_BerlinTimezoneMixin, unittest.TestCase):
    """Clocks in Berlin go back from CEST (+02:00) to CET (+01:00) on Sun 2026-10-25 at 03:00."""

    def test_daily_time_keeps_wall_clock_across_dst_change(self) -> None:
        saturday_evening = datetime(2026, 10, 24, 20, 0, tzinfo=ZoneInfo("Europe/Berlin"))

        target = next_local_time_occurrence("08:00", 1, reference=saturday_evening)

        self.assertEqual(target.astimezone(ZoneInfo("Europe/Berlin")).strftime("%Y-%m-%d %H:%M"), "2026-10-26 08:00")
        self.assertEqual(target.utcoffset().total_seconds(), 3600)

    def test_quiet_hours_end_at_wake_up_time_across_dst_change(self) -> None:
        night = datetime(2026, 10, 24, 23, 30, tzinfo=ZoneInfo("Europe/Berlin"))

        wake_up = move_local_datetime_to_window_end(night, "22:00", "07:00")

        self.assertEqual(wake_up.astimezone(ZoneInfo("Europe/Berlin")).strftime("%Y-%m-%d %H:%M"), "2026-10-25 07:00")


@unittest.skipUnless(hasattr(time, "tzset"), "needs time.tzset to switch the process timezone")
class EndOfDayTimeTests(_BerlinTimezoneMixin, unittest.TestCase):
    def test_quiet_hours_starting_at_24_00_are_treated_as_midnight(self) -> None:
        berlin = ZoneInfo("Europe/Berlin")
        early_morning = datetime(2026, 4, 1, 3, 0, tzinfo=berlin)

        self.assertTrue(is_local_datetime_in_window(early_morning, "24:00", "07:00"))
        wake_up = move_local_datetime_to_window_end(early_morning, "24:00", "07:00")
        self.assertEqual(wake_up.astimezone(berlin).strftime("%H:%M"), "07:00")

    def test_inkypi_playlist_ending_at_24_00_maps_to_midnight_quiet_start(self) -> None:
        from app.inkypi_adapter import InkyPiAdapter

        data = {"playlist_config": {"playlists": [{"start_time": "07:00", "end_time": "24:00"}]}}

        self.assertEqual(InkyPiAdapter._extract_sleep_schedule(None, data), ("00:00", "07:00"))


if __name__ == "__main__":
    unittest.main()
