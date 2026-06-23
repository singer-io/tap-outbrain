"""Mock integration tests - verify bookmark / state behaviour for tap-outbrain.

tap-outbrain uses `campaign_performance` as its only INCREMENTAL stream.
Bookmarks are stored in state as:
    state['campaign_performance'][<campaign_id>] = '<YYYY-MM-DD>'
"""
import datetime

from singer import metadata as singer_metadata

from .base import OutbrainBaseTest

from tap_outbrain.discover import discover
from tap_outbrain import get_date_ranges


class OutbrainBookmarkTest(OutbrainBaseTest):
    """Verify state/bookmark behaviour with fully mocked HTTP responses."""

    def test_state_written_after_sync(self):
        """write_state is called at least once when a campaign has performance data."""
        campaigns = [self.make_campaign_record("c001")]
        perf = {"c001": [self.make_performance_record("c001", "2024-05-10")]}
        captured, _ = self._run_mock_sync(campaigns=campaigns, perf_records_by_campaign=perf)
        self.assertGreater(len(captured["states"]), 0, "write_state was never called")

    def test_state_contains_campaign_performance_bookmark(self):
        """After sync, state['campaign_performance'][campaign_id] is set."""
        campaigns = [self.make_campaign_record("c001")]
        perf = {"c001": [self.make_performance_record("c001", "2024-05-10")]}
        _, final_state = self._run_mock_sync(campaigns=campaigns, perf_records_by_campaign=perf)
        self.assertIn("c001", final_state.get("campaign_performance", {}))

    def test_bookmark_is_last_from_date(self):
        """Bookmark is updated to the fromDate of the last performance record."""
        campaigns = [self.make_campaign_record("c001")]
        perf = {
            "c001": [
                self.make_performance_record("c001", "2024-05-01"),
                self.make_performance_record("c001", "2024-05-15"),
            ]
        }
        _, final_state = self._run_mock_sync(campaigns=campaigns, perf_records_by_campaign=perf)
        self.assertEqual(final_state["campaign_performance"]["c001"], "2024-05-15")

    def test_separate_bookmarks_per_campaign(self):
        """Each campaign has its own independent bookmark in state."""
        campaigns = [
            self.make_campaign_record("c001"),
            self.make_campaign_record("c002"),
        ]
        perf = {
            "c001": [self.make_performance_record("c001", "2024-03-01")],
            "c002": [self.make_performance_record("c002", "2024-04-15")],
        }
        _, final_state = self._run_mock_sync(campaigns=campaigns, perf_records_by_campaign=perf)
        self.assertEqual(final_state["campaign_performance"]["c001"], "2024-03-01")
        self.assertEqual(final_state["campaign_performance"]["c002"], "2024-04-15")

    def test_no_bookmark_written_for_no_campaigns(self):
        """With 0 campaigns, no campaign-level bookmarks are written."""
        _, final_state = self._run_mock_sync(campaigns=[])
        self.assertEqual(final_state["campaign_performance"], {})

    def test_sync_completes_without_raising(self):
        """do_sync with mock credentials does not raise any exception."""
        self._run_mock_sync(campaigns=[self.make_campaign_record("c001")])

    def test_existing_bookmark_shifts_start_date(self):
        """When a bookmark exists, first /periodic from_date = bookmark - 2 days."""
        campaign_id = "c001"
        bookmarked_date = "2024-06-20"
        expected_from = datetime.date(2024, 6, 18)  # 2024-06-20 - 2 days
        campaigns = [self.make_campaign_record(campaign_id)]
        initial_state = {"campaign_performance": {campaign_id: bookmarked_date}}
        params_list = self._collect_periodic_params(
            campaigns=campaigns,
            initial_state=initial_state,
        )
        self.assertGreater(len(params_list), 0, "No /periodic requests were made")
        self.assertEqual(params_list[0]["from"], expected_from)

    def test_no_bookmark_uses_default_start_date(self):
        """Without a bookmark the first from_date equals config start_date."""
        campaign_id = "c001"
        config = self.get_mock_config(start_date="2024-03-01T00:00:00Z")
        expected_from = datetime.date(2024, 3, 1)
        params_list = self._collect_periodic_params(
            config=config,
            campaigns=[self.make_campaign_record(campaign_id)],
        )
        self.assertGreater(len(params_list), 0)
        self.assertEqual(params_list[0]["from"], expected_from)

    def test_campaign_performance_is_incremental_in_metadata(self):
        """Singer metadata confirms campaign_performance uses INCREMENTAL replication."""
        catalog = discover()
        cp = next(s for s in catalog.streams if s.tap_stream_id == "campaign_performance")
        mdata_map = singer_metadata.to_map(cp.metadata)
        method = singer_metadata.get(mdata_map, (), "forced-replication-method")
        self.assertEqual(method, self.INCREMENTAL)

    def test_campaign_is_full_table_in_metadata(self):
        """Singer metadata confirms campaign uses FULL_TABLE replication."""
        catalog = discover()
        c = next(s for s in catalog.streams if s.tap_stream_id == "campaign")
        mdata_map = singer_metadata.to_map(c.metadata)
        method = singer_metadata.get(mdata_map, (), "forced-replication-method")
        self.assertEqual(method, self.FULL_TABLE)

    def test_campaign_performance_valid_replication_key_is_from_date(self):
        """valid-replication-keys for campaign_performance is 'fromDate'."""
        catalog = discover()
        cp = next(s for s in catalog.streams if s.tap_stream_id == "campaign_performance")
        mdata_map = singer_metadata.to_map(cp.metadata)
        rep_keys = singer_metadata.get(mdata_map, (), "valid-replication-keys")
        self.assertIn("fromDate", rep_keys)

    def test_get_date_ranges_returns_empty_when_start_equals_end(self):
        """get_date_ranges with start == end returns an empty list."""
        d = datetime.date(2024, 1, 1)
        self.assertEqual(get_date_ranges(d, d, 30), [])

    def test_get_date_ranges_contiguous_and_non_overlapping(self):
        """Date ranges from get_date_ranges are contiguous with no gaps."""
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
                    f"Gap/overlap between range {i-1} and {i}",
                )

    def test_bookmark_start_date_shifts_two_days_before(self):
        """The bookmark date minus 2 days equals the expected sync from_date."""
        bookmarked = datetime.date(2024, 6, 20)
        expected_from = bookmarked - datetime.timedelta(days=2)
        ranges = get_date_ranges(expected_from, datetime.date.today(), 30)
        self.assertGreater(len(ranges), 0)
        self.assertEqual(ranges[0]["from_date"], expected_from)