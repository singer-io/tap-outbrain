import os
from datetime import timedelta


def _is_mock_mode() -> bool:
    """Return True when tests should run against mocked HTTP responses."""
    mode = os.environ.get("INTEGRATION_TEST_MODE", "auto").lower()
    if mode == "live":
        return False
    if mode == "mock":
        return True
    required_env = (
        "TAP_OUTBRAIN_ACCOUNT_ID",
        "TAP_OUTBRAIN_USERNAME",
        "TAP_OUTBRAIN_PASSWORD",
        "TAP_OUTBRAIN_ACCESS_TOKEN",
    )
    has_live_creds = bool(os.environ.get("TAP_OUTBRAIN_API_CREDS")) or all(
        os.environ.get(var) for var in required_env
    )
    return not has_live_creds

if _is_mock_mode():
    import _mock_tap_tester as _mock_tt
    import tap_tester as _tap_tester

    # Ensure tap_tester package-level references use mock stubs.
    _tap_tester.connections = _mock_tt.connections
    _tap_tester.menagerie = _mock_tt.menagerie
    _tap_tester.runner = _mock_tt.runner

    # If any suite modules are imported/reused, rebind their module-level
    # references to the mock stubs so they never call real tap-tester I/O.
    import sys
    for _suite_module in (
        "tap_tester.base_suite_tests.discovery_test",
        "tap_tester.base_suite_tests.bookmark_test",
        "tap_tester.base_suite_tests.start_date_test",
        "tap_tester.base_suite_tests.pagination_test",
    ):
        _mod = sys.modules.get(_suite_module)
        if _mod is None:
            continue
        if hasattr(_mod, "connections"):
            _mod.connections = _mock_tt.connections
        if hasattr(_mod, "menagerie"):
            _mod.menagerie = _mock_tt.menagerie
        if hasattr(_mod, "runner"):
            _mod.runner = _mock_tt.runner

    from mock_base import MockOutbrainBaseTest as OutbrainBaseTest
else:
    from tap_tester import connections, menagerie, runner  # noqa: F401
    from tap_tester.base_suite_tests.base_case import BaseCase

    class OutbrainBaseTest(BaseCase):  # type: ignore[no-redef]
        """Setup expectations for test sub classes.

        Metadata describing streams. A bunch of shared methods that are used
        in tap-tester tests. Shared tap-specific methods (as needed).
        """
        start_date = "2024-01-01T00:00:00Z"
        bookmark_format = "%Y-%m-%d"
        PARENT_STREAM = "parent-stream"

        @staticmethod
        def tap_name():
            return "tap-outbrain"

        @staticmethod
        def get_type():
            return "platform.outbrain"

        def get_properties(self, original=True):
            return {
                "start_date": self.start_date,
                "user_agent": "tap-outbrain <api_user_agent@example.com>",
            }

        def get_credentials(self):
            return {
                "account_id": os.environ["TAP_OUTBRAIN_ACCOUNT_ID"],
                "username": os.environ["TAP_OUTBRAIN_USERNAME"],
                "password": os.environ["TAP_OUTBRAIN_PASSWORD"],
                "access_token": os.environ["TAP_OUTBRAIN_ACCESS_TOKEN"]
            }

        @classmethod
        def expected_metadata(cls):
            """The expected streams and metadata about the streams."""
            return {
                "campaign": {
                    cls.PRIMARY_KEYS: {"id"},
                    cls.REPLICATION_METHOD: cls.FULL_TABLE,
                    cls.REPLICATION_KEYS: set(),
                    cls.OBEYS_START_DATE: False,
                    cls.RESPECTS_START_DATE: False,
                    cls.API_LIMIT: 50,
                },
                "campaign_performance": {
                    cls.PRIMARY_KEYS: {"campaignId", "fromDate"},
                    cls.REPLICATION_METHOD: cls.INCREMENTAL,
                    cls.REPLICATION_KEYS: {"fromDate"},
                    cls.OBEYS_START_DATE: True,
                    cls.RESPECTS_START_DATE: True,
                    cls.LOOK_BACK_WINDOW: timedelta(days=2),
                    cls.PARENT_STREAM: "campaign",
                    cls.API_LIMIT: 100,
                },
            }

        @classmethod
        def expected_stream_names(cls) -> set:
            return set(cls.expected_metadata().keys())

        def expected_primary_keys(self, stream=None) -> dict:
            primary_keys = {
                table: properties.get(self.PRIMARY_KEYS, set())
                for table, properties in self.expected_metadata().items()
            }
            if stream is None:
                return primary_keys
            return primary_keys[stream]

        def expected_replication_keys(self, stream=None) -> dict:
            replication_keys = {
                table: properties.get(self.REPLICATION_KEYS, set())
                for table, properties in self.expected_metadata().items()
            }
            if stream is None:
                return replication_keys
            return replication_keys[stream]

        def expected_replication_method(self, stream=None) -> dict:
            replication_method = {
                table: properties.get(self.REPLICATION_METHOD, None)
                for table, properties in self.expected_metadata().items()
            }
            if stream is None:
                return replication_method
            return replication_method[stream]

        def expected_lookback_window(self, stream=None):
            lookback = {
                "campaign": timedelta(days=0),
                "campaign_performance": timedelta(days=2),
            }
            if stream is None:
                return lookback
            return lookback[stream]
