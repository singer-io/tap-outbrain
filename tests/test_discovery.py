from base import OutbrainBaseTest
from tap_tester.base_suite_tests.discovery_test import DiscoveryTest as TT_DiscoveryTest


class DiscoveryTest(TT_DiscoveryTest, OutbrainBaseTest):
    """Tap-outbrain discovery test using the standard tap-tester suite."""

    @staticmethod
    def name():
        return "tap_outbrain_discovery_test"

    def streams_to_test(self):
        return self.expected_stream_names()
