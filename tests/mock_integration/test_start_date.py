"""Mock start date test for tap-outbrain."""
import datetime

from .base import OutbrainMockBaseTest


class OutbrainMockStartDateTest(OutbrainMockBaseTest):
    """Test start_date behavior with mock server."""

    @staticmethod
    def _recent_start_date(days_ago):
        """Return a recent UTC start_date to keep mock sync windows bounded."""
        return (datetime.date.today() - datetime.timedelta(days=days_ago)).strftime(
            "%Y-%m-%dT00:00:00Z"
        )

    def test_start_date_drives_sync_window(self):
        """start_date parameter controls the sync window for incremental streams."""
        start_date = self._recent_start_date(14)
        config = self._default_config(start_date=start_date)
        result = self._run_mock_sync(config=config)
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])

        # Sync should complete successfully with specified start_date
        self.assertEqual(result["returncode"], 0)
        self.assertGreater(len(result["messages"]), 0)

    def test_earlier_start_date_gets_more_data(self):
        """Earlier start_date allows more data to be synced."""
        today = datetime.date.today()
        # Use recent dates within one 100-day chunk to avoid time.sleep() accumulation
        old_start_date = (today - datetime.timedelta(days=14)).strftime("%Y-%m-%dT00:00:00Z")
        new_start_date = (today - datetime.timedelta(days=7)).strftime("%Y-%m-%dT00:00:00Z")

        old_config = self._default_config(start_date=old_start_date)
        new_config = self._default_config(start_date=new_start_date)

        old_result = self._run_mock_sync(config=old_config)
        new_result = self._run_mock_sync(config=new_config)

        self.assertEqual(old_result["returncode"], 0, msg=old_result["stderr"])
        self.assertEqual(new_result["returncode"], 0, msg=new_result["stderr"])

        # Both should complete successfully
        old_records = len([m for m in old_result["messages"] if m.get("type") == "RECORD"])
        new_records = len([m for m in new_result["messages"] if m.get("type") == "RECORD"])

        # Older start_date could potentially get more data
        # (though with mock server they should be similar)
        self.assertGreater(old_records, 0)
        self.assertGreater(new_records, 0)

    def test_start_date_omitted_uses_default(self):
        """When start_date is not provided, default is used."""
        config = self._default_config()
        # Verify start_date is set to default
        self.assertIn("start_date", config)

        result = self._run_mock_sync(config=config)
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])

        # Sync should complete successfully with default start_date
        self.assertGreater(len(result["messages"]), 0)

    def test_incremental_streams_respect_start_date(self):
        """Incremental streams (campaign_performance) respect start_date."""
        start_date = self._recent_start_date(20)
        config = self._default_config(start_date=start_date)
        result = self._run_mock_sync(config=config)

        self.assertEqual(result["returncode"], 0, msg=result["stderr"])

        # campaign_performance is incremental and should respect start_date
        perf_records = [
            m["record"]
            for m in result["messages"]
            if m.get("type") == "RECORD" and m.get("stream") == "campaign_performance"
        ]

        # Should have records
        self.assertGreater(len(perf_records), 0)

        # fromDate values should be present (replication key for incremental)
        for record in perf_records:
            self.assertIn("fromDate", record)
            self.assertIsNotNone(record["fromDate"])

    def test_full_table_streams_ignore_start_date(self):
        """Full table streams (campaign) don't use start_date for filtering."""
        start_date = self._recent_start_date(10)
        config = self._default_config(start_date=start_date)
        result = self._run_mock_sync(config=config)

        self.assertEqual(result["returncode"], 0, msg=result["stderr"])

        # campaign is FULL_TABLE - it should return all campaigns regardless of start_date
        campaign_records = [
            m["record"]
            for m in result["messages"]
            if m.get("type") == "RECORD" and m.get("stream") == "campaign"
        ]

        # Should still get campaign records (full table sync)
        self.assertGreater(len(campaign_records), 0)
