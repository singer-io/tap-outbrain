from base import OutbrainBaseTest
from tap_tester import menagerie
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class OutbrainBookmarkTest(BookmarkTest, OutbrainBaseTest):
    """Verify incremental streams save bookmarks and respect them on subsequent syncs."""

    bookmark_format = "%Y-%m-%d"
    initial_bookmarks = None

    @staticmethod
    def name():
        return "tap_outbrain_bookmark_test"

    def streams_to_test(self):
        return {s for s, m in self.expected_replication_method().items()
                if m == self.INCREMENTAL}

    def perform_and_verify_table_and_field_selection(self, conn_id, test_catalogs):
        """Include required parent streams during selection validation."""
        metadata = self.expected_metadata()
        catalogs_by_stream = {
            catalog.get("stream_name"): catalog
            for catalog in menagerie.get_catalogs(conn_id)
        }

        expected_streams = set(self.streams_to_test())
        for stream in list(expected_streams):
            parent = metadata.get(stream, {}).get(self.PARENT_STREAM)
            while parent:
                expected_streams.add(parent)
                parent = metadata.get(parent, {}).get(self.PARENT_STREAM)

        selection_catalogs = [
            catalogs_by_stream[stream_name]
            for stream_name in expected_streams
            if stream_name in catalogs_by_stream
        ]

        return super().perform_and_verify_table_and_field_selection(conn_id, selection_catalogs)
