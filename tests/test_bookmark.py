import unittest
from base import OutbrainBaseTest

try:
    from tap_tester.base_suite_tests.bookmark_test import BookmarkTest as TT_BookmarkTest

    class OutbrainBookmarkTest(TT_BookmarkTest, OutbrainBaseTest):
        """Verify state/bookmark behaviour for incremental streams."""

        @staticmethod
        def name():
            return "tap_outbrain_bookmark_test"

        def streams_to_test(self):
            # Only incremental streams respect bookmarks
            return {s for s, m in self.expected_replication_method().items()
                    if m == self.INCREMENTAL}

except ImportError:
    @unittest.skip("tap_tester not available; use mock_integration tests instead")
    class OutbrainBookmarkTest(unittest.TestCase):
        pass
