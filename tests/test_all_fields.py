import unittest
from base import OutbrainBaseTest

try:
    from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest

    class OutbrainAllFieldsTest(AllFieldsTest, OutbrainBaseTest):
        """Ensure a fully selected catalog replicates all expected fields."""

        @staticmethod
        def name():
            return "tap_outbrain_all_fields_test"

        def streams_to_test(self):
            return self.expected_stream_names()

        def test_sync_emits_schema_for_campaign_stream(self):
            """A SCHEMA message is emitted for the campaign stream."""
            captured, _ = self._run_mock_sync()
            self.assertIn("campaign", captured["schemas"])

        def test_sync_emits_schema_for_campaign_performance_stream(self):
            """A SCHEMA message is emitted for the campaign_performance stream."""
            captured, _ = self._run_mock_sync()
            self.assertIn("campaign_performance", captured["schemas"])

except ImportError:
    @unittest.skip("tap_tester not available; use mock_integration tests instead")
    class OutbrainAllFieldsTest(unittest.TestCase):
        pass
