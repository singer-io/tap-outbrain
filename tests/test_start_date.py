import unittest
from base import OutbrainBaseTest

try:
    from tap_tester import menagerie
    from tap_tester.base_suite_tests.start_date_test import StartDateTest as TT_StartDateTest

    class StartDateTest(TT_StartDateTest, OutbrainBaseTest):
        """Verify that the tap's start_date config controls how far back data is synced."""

        # start_date_1: far enough in the past that the sync should return more records
        start_date_1 = "2021-01-01T00:00:00Z"
        # start_date_2: more recent date so the sync should return fewer records
        start_date_2 = "2024-01-20T00:00:00Z"

        @staticmethod
        def name():
            return "tap_outbrain_start_date_test"

        def streams_to_test(self):
            return {s for s, m in self.expected_replication_method().items()
                    if m == self.INCREMENTAL}

        def perform_and_verify_table_and_field_selection(self, conn_id, test_catalogs):
            """Include required parent streams during selection validation.

            The tap auto-selects `campaign` when `campaign_performance` is selected,
            so we must include parent catalogs to satisfy tap-tester's selection
            assertions in setup.
            """
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

            return super().perform_and_verify_table_and_field_selection(
                conn_id, selection_catalogs
            )

except ImportError:
    @unittest.skip("tap_tester not available; use mock_integration tests instead")
    class StartDateTest(unittest.TestCase):
        pass
