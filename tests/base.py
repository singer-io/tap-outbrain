import os
import unittest
from datetime import timedelta

from tap_tester import connections, menagerie, runner  # noqa: F401
from tap_tester.base_suite_tests.base_case import BaseCase


def _resolve_mode() -> str:
    mode = os.environ.get("INTEGRATION_TEST_MODE", "auto").lower()
    if mode in {"live", "mock"}:
        return mode

    required_env = (
        "TAP_OUTBRAIN_ACCOUNT_ID",
        "TAP_OUTBRAIN_USERNAME",
        "TAP_OUTBRAIN_PASSWORD",
        "TAP_OUTBRAIN_ACCESS_TOKEN",
    )
    has_live_creds = bool(os.environ.get("TAP_OUTBRAIN_API_CREDS")) or all(
        os.environ.get(var) for var in required_env
    )
    return "live" if has_live_creds else "mock"


class OutbrainBaseTest(BaseCase):
    """Setup expectations for test sub classes.

    Metadata describing streams. A bunch of shared methods that are used
    in tap-tester tests. Shared tap-specific methods (as needed).
    """
    start_date = "2024-01-01T00:00:00Z"
    bookmark_format = "%Y-%m-%d"
    PARENT_STREAM = "parent-stream"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        if _resolve_mode() != "live":
            raise unittest.SkipTest(
                "Root integration tests run only in live mode. "
                "Use tests/mock_integration for mock mode."
            )

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

            