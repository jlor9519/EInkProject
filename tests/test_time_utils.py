from __future__ import annotations

import unittest
from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

from app.time_utils import is_in_local_time_window, seconds_until_local_time, seconds_until_wake_up_time


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


if __name__ == "__main__":
    unittest.main()
