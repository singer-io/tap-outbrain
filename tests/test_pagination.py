import unittest
from base import OutbrainBaseTest

try:
    from tap_tester.base_suite_tests.pagination_test import PaginationTest
except ImportError:
    try:
        from tap_tester.base_suite_tests.pagenation_test import PaginationTest
    except ImportError:
        PaginationTest = None

try:
    assert PaginationTest is not None

    class OutbrainPaginationTest(PaginationTest, OutbrainBaseTest):
        """
        Ensure tap can replicate multiple pages of data for streams that use pagination.
        """

        @staticmethod
        def name():
            return "tap_outbrain_pagination_test"

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

except (ImportError, AssertionError):
    # Skip this module if tap_tester is not available
    @unittest.skip("tap_tester not available; use mock_integration tests instead")
    class OutbrainPaginationTest(unittest.TestCase):
        pass
