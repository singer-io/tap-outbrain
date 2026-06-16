import unittest

try:
    from tap_tester.base_suite_tests.pagination_test import PaginationTest
except ImportError:
    try:
        from tap_tester.base_suite_tests.pagenation_test import PaginationTest
    except ImportError:
        PaginationTest = None
from base import OutbrainBaseTest

@unittest.skipIf(PaginationTest is None, "tap_tester PaginationTest is unavailable in this environment")
class OutbrainPaginationTest(PaginationTest, OutbrainBaseTest):
    """
    Ensure tap can replicate multiple pages of data for streams that use pagination.
    """

    @staticmethod
    def name():
        return "tap_tester_outbrain_pagination_test"

    def streams_to_test(self):
        # Include parent stream because selecting campaign_performance auto-selects campaign.
        return {
            'campaign',
            'campaign_performance',
        }

    def test_record_count_greater_than_page_limit(self):
        """Only campaign_performance is guaranteed to exceed one page in live fixtures."""
        stream = 'campaign_performance'

        page_limit = self.expected_page_size(stream)
        record_count = self.record_count_by_stream.get(stream, -1)

        self.assertGreater(record_count, page_limit)