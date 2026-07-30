"""Mock automatic fields test for tap-outbrain."""
try:
    from .base import OutbrainMockBaseTest
except ImportError:
    from base import OutbrainMockBaseTest


class OutbrainMockAutomaticFieldsTest(OutbrainMockBaseTest):
    """Test that primary and replication keys are always replicated."""

    def test_campaign_key_properties_in_records(self):
        """Campaign key properties (id) are present in all records."""
        result = self._run_mock_sync()
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])

        campaign_records = [
            m["record"]
            for m in result["messages"]
            if m.get("type") == "RECORD" and m.get("stream") == "campaign"
        ]
        self.assertGreater(len(campaign_records), 0, "No campaign records emitted")

        for record in campaign_records:
            self.assertIn("id", record, "Primary key 'id' missing from campaign record")
            self.assertIsNotNone(record["id"], "Primary key 'id' is None")

    def test_campaign_performance_key_properties_in_records(self):
        """Campaign_performance key properties are present in all records."""
        result = self._run_mock_sync()
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])

        perf_records = [
            m["record"]
            for m in result["messages"]
            if m.get("type") == "RECORD" and m.get("stream") == "campaign_performance"
        ]
        self.assertGreater(len(perf_records), 0, "No campaign_performance records emitted")

        for record in perf_records:
            self.assertIn(
                "campaignId", record,
                "Primary key 'campaignId' missing from campaign_performance record"
            )
            self.assertIn(
                "fromDate", record,
                "Replication key 'fromDate' missing from campaign_performance record"
            )
            self.assertIsNotNone(record["campaignId"])
            self.assertIsNotNone(record["fromDate"])

    def test_replication_keys_present_when_incremental(self):
        """Replication keys are present for INCREMENTAL streams."""
        result = self._run_mock_sync()
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])

        # campaign_performance is INCREMENTAL and should have fromDate replication key
        perf_records = [
            m["record"]
            for m in result["messages"]
            if m.get("type") == "RECORD" and m.get("stream") == "campaign_performance"
        ]

        for record in perf_records:
            self.assertIn("fromDate", record, "Replication key 'fromDate' missing")
            self.assertIsNotNone(record["fromDate"], "Replication key value is None")
