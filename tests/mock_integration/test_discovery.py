"""Mock discovery test for tap-outbrain."""
from .base import OutbrainMockBaseTest


class OutbrainMockDiscoveryTest(OutbrainMockBaseTest):
    """Test discovery with mock server."""

    def test_default_streams_are_discovered(self):
        """Both campaign and campaign_performance streams are discovered."""
        result = self._run_mock_sync()
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])

        schema_streams = {
            msg.get("stream")
            for msg in result["messages"]
            if msg.get("type") == "SCHEMA"
        }

        self.assertEqual(schema_streams, {"campaign", "campaign_performance"})

    def test_discovery_returns_schemas(self):
        """Sync returns SCHEMA messages for all expected streams."""
        result = self._run_mock_sync()
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])

        schema_messages = [m for m in result["messages"] if m.get("type") == "SCHEMA"]
        self.assertGreater(len(schema_messages), 0)

        for schema in schema_messages:
            self.assertIn("stream", schema)
            self.assertIn("schema", schema)
            self.assertIn("key_properties", schema)

    def test_campaign_stream_has_expected_schema(self):
        """Campaign stream schema includes expected fields."""
        result = self._run_mock_sync()
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])

        schema_messages = {
            m["stream"]: m
            for m in result["messages"]
            if m.get("type") == "SCHEMA"
        }
        self.assertIn("campaign", schema_messages)

        campaign_schema = schema_messages["campaign"]["schema"]
        self.assertIn("properties", campaign_schema)
        props = campaign_schema["properties"]
        
        expected_fields = {"id", "name", "enabled", "budget", "cpc"}
        for field in expected_fields:
            self.assertIn(field, props, f"Field '{field}' missing from campaign schema")

    def test_campaign_performance_stream_has_expected_schema(self):
        """Campaign_performance stream schema includes expected fields."""
        result = self._run_mock_sync()
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])

        schema_messages = {
            m["stream"]: m
            for m in result["messages"]
            if m.get("type") == "SCHEMA"
        }
        self.assertIn("campaign_performance", schema_messages)

        perf_schema = schema_messages["campaign_performance"]["schema"]
        self.assertIn("properties", perf_schema)
        props = perf_schema["properties"]
        
        expected_fields = {"campaignId", "fromDate", "impressions", "clicks"}
        for field in expected_fields:
            self.assertIn(field, props, f"Field '{field}' missing from campaign_performance schema")

    def test_records_are_emitted(self):
        """Sync emits RECORD messages with actual data."""
        result = self._run_mock_sync()
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])

        record_messages = [m for m in result["messages"] if m.get("type") == "RECORD"]
        self.assertGreater(len(record_messages), 0, "No RECORD messages emitted")
