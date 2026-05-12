"""Mock integration tests — verify start-date behaviour for tap-outbrain."""
import datetime

from .base import OutbrainBaseTest

import tap_outbrain
from tap_outbrain import get_date_ranges


class OutbrainStartDateTest(OutbrainBaseTest):
    """Verify that start_date drives the initial sync window correctly."""

    @staticmethod
    def name():
        return "tap_outbrain_start_date_test"

    def test_start_date_sets_initial_from_date(self):
        """First /periodic request uses start_date - 2 days as from_date."""
        config = self.get_mock_config(start_date="2024-03-01T00:00:00Z")
        expected_from = datetime.date(2024, 2, 28)  # 2024-03-01 minus 2 days
        params_list = self._collect_periodic_params(config=config)
        self.assertGreater(len(params_list), 0, "No /periodic requests made")
        self.assertEqual(params_list[0]["from"], expected_from)

    def test_later_start_date_produces_fewer_requests(self):
        """A more recent start_date produces fewer /periodic requests than an older one."""
        recent_cfg = self.get_mock_config(start_date="2024-06-01T00:00:00Z")
        old_cfg = self.get_mock_config(start_date="2020-01-01T00:00:00Z")
        recent_params = self._collect_periodic_params(config=recent_cfg)
        old_params = self._collect_periodic_params(config=old_cfg)
        self.assertLess(
            len(recent_params), len(old_params),
            msg="Recent start_date should produce fewer /periodic requests",
        )

    def test_default_start_date_set_from_config(self):
        """After do_sync, DEFAULT_START_DATE equals the YYYY-MM-DD part of start_date."""
        config = self.get_mock_config(start_date="2024-05-15T00:00:00Z")
        self._run_mock_sync(config=config)
        self.assertEqual(tap_outbrain.DEFAULT_START_DATE, "2024-05-15")

    def test_start_date_sliced_to_date_only(self):
        """DEFAULT_START_DATE never retains the time component of start_date."""
        config = self.get_mock_config(start_date="2024-01-01T12:30:00Z")
        self._run_mock_sync(config=config)
        ds = tap_outbrain.DEFAULT_START_DATE
        try:
            datetime.datetime.strptime(ds, "%Y-%m-%d")
        except ValueError:
            self.fail(f"DEFAULT_START_DATE '{ds}' is not in YYYY-MM-DD format")

    def test_sync_completes_without_raising(self):
        """do_sync with mocked HTTP layer completes without raising."""
        self._run_mock_sync()

    def test_date_ranges_cover_start_to_today(self):
        """Date ranges starting from a given date extend to today."""
        start = datetime.date(2024, 1, 1)
        end = datetime.date.today()
        ranges = get_date_ranges(start, end, 100)
        self.assertGreater(len(ranges), 0)
        self.assertEqual(ranges[0]["from_date"], start)
        for r in ranges:
            self.assertLessEqual(r["from_date"], r["to_date"])

    def test_date_range_interval_in_days_observed(self):
        """Each date range span is at most interval_in_days days wide."""
        start = datetime.date(2023, 1, 1)
        end = datetime.date(2023, 12, 31)
        interval = 30
        ranges = get_date_ranges(start, end, interval)
        for r in ranges:
            span = (r["to_date"] - r["from_date"]).days
            with self.subTest(range=r):
                self.assertLessEqual(
                    span,
                    interval - 1,
                    msg=f"Span {span} exceeds interval {interval - 1}",
                )

    def test_date_ranges_are_non_overlapping_and_contiguous(self):
        """Date ranges must be contiguous (next from = prev to + 1 day) and non-overlapping."""
        start = datetime.date(2023, 6, 1)
        end = datetime.date(2023, 9, 30)
        ranges = get_date_ranges(start, end, 30)
        for i in range(1, len(ranges)):
            prev_to = ranges[i - 1]["to_date"]
            curr_from = ranges[i]["from_date"]
            with self.subTest(i=i):
                self.assertEqual(
                    curr_from,
                    prev_to + datetime.timedelta(days=1),
                    msg=f"Gap/overlap between range {i - 1} and {i}",
                )

    def test_later_start_date_produces_fewer_date_ranges(self):
        """A more recent start_date produces fewer date-range chunks than an older one."""
        recent_start = datetime.date(2024, 6, 1)
        old_start = datetime.date(2020, 1, 1)
        end = datetime.date.today()
        recent_ranges = get_date_ranges(recent_start, end, 30)
        old_ranges = get_date_ranges(old_start, end, 30)
        self.assertLess(
            len(recent_ranges),
            len(old_ranges),
            msg="Recent start_date should produce fewer date-range chunks",
        )

    def test_campaign_performance_obeys_start_date(self):
        """campaign_performance is INCREMENTAL and therefore obeys start_date."""
        meta = self.expected_metadata()
        self.assertTrue(
            meta["campaign_performance"][self.OBEYS_START_DATE],
            "campaign_performance should obey start_date",
        )

    def test_campaign_does_not_obey_start_date(self):
        """campaign is FULL_TABLE and does not obey start_date."""
        meta = self.expected_metadata()
        self.assertFalse(
            meta["campaign"][self.OBEYS_START_DATE],
            "campaign should NOT obey start_date",
        )
