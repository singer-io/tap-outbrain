from base import OutbrainBaseTest
from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest


class OutbrainAutomaticFields(MinimumSelectionTest, OutbrainBaseTest):
    """Verify that primary and replication keys are always replicated."""

    @staticmethod
    def name():
        return "tap_outbrain_automatic_fields_test"

    def streams_to_test(self):
        return self.expected_stream_names()
