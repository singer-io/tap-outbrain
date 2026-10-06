"""Mock all fields test for tap-outbrain."""
from .base import OutbrainMockBaseTest


class OutbrainMockAllFieldsTest(OutbrainMockBaseTest):
    """Test that all schema fields are included in synced records."""

    def test_campaign_records_contain_all_schema_fields(self):
        """Campaign records include all fields defined in schema."""
        result = self._run_mock_sync()
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])

        schema_messages = {
            m["stream"]: m
            for m in result["messages"]
            if m.get("type") == "SCHEMA"
        }
        campaign_schema = schema_messages["campaign"]["schema"]
        schema_fields = set(campaign_schema["properties"].keys())

        record_messages = [
            m for m in result["messages"]
            if m.get("type") == "RECORD" and m.get("stream") == "campaign"
        ]
        self.assertGreater(len(record_messages), 0, "No campaign records emitted")

        for record_msg in record_messages:
            record_fields = set(record_msg["record"].keys())
            # All schema fields should be present in the record
            missing_fields = schema_fields - record_fields
            self.assertEqual(
                len(missing_fields), 0,
                f"Record missing fields: {missing_fields}"
            )

    def test_campaign_performance_records_contain_all_schema_fields(self):
        """Campaign_performance records include all fields defined in schema."""
        result = self._run_mock_sync()
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])

        schema_messages = {
            m["stream"]: m
            for m in result["messages"]
            if m.get("type") == "SCHEMA"
        }
        perf_schema = schema_messages["campaign_performance"]["schema"]
        schema_fields = set(perf_schema["properties"].keys())

        record_messages = [
            m for m in result["messages"]
            if m.get("type") == "RECORD" and m.get("stream") == "campaign_performance"
        ]
        self.assertGreater(len(record_messages), 0, "No campaign_performance records emitted")

        for record_msg in record_messages:
            record_fields = set(record_msg["record"].keys())
            # All schema fields should be present in the record
            missing_fields = schema_fields - record_fields
            self.assertEqual(
                len(missing_fields), 0,
                f"Record missing fields: {missing_fields}"
            )

    def test_key_properties_are_present_in_records(self):
        """Key properties are always present in synced records."""
        result = self._run_mock_sync()
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])

        schema_messages = {
            m["stream"]: m
            for m in result["messages"]
            if m.get("type") == "SCHEMA"
        }

        # Check campaign records
        campaign_keys = set(schema_messages["campaign"]["key_properties"])
        campaign_records = [
            m for m in result["messages"]
            if m.get("type") == "RECORD" and m.get("stream") == "campaign"
        ]
        for record in campaign_records:
            for key_prop in campaign_keys:
                self.assertIn(
                    key_prop, record["record"],
                    f"Campaign key property '{key_prop}' missing from record"
                )

        # Check campaign_performance records
        perf_keys = set(schema_messages["campaign_performance"]["key_properties"])
        perf_records = [
            m for m in result["messages"]
            if m.get("type") == "RECORD" and m.get("stream") == "campaign_performance"
        ]
        for record in perf_records:
            for key_prop in perf_keys:
                self.assertIn(
                    key_prop, record["record"],
                    f"Campaign_performance key property '{key_prop}' missing from record"
                )
