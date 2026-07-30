import unittest
from base import OutbrainBaseTest

try:
    from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest

    class OutbrainAutomaticFields(MinimumSelectionTest, OutbrainBaseTest):
        """Verify that primary and replication keys are always replicated."""

        @staticmethod
        def name():
            return "tap_outbrain_automatic_fields_test"

        def streams_to_test(self):
            return self.expected_stream_names()

except ImportError:
    @unittest.skip("tap_tester not available; use mock_integration tests instead")
    class OutbrainAutomaticFields(unittest.TestCase):
        pass
