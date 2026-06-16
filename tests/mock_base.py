"""MockOutbrainBaseTest — drop-in replacement for OutbrainBaseTest when running
integration tests without a live Outbrain account.

Usage
─────
Set INTEGRATION_TEST_MODE=mock (or leave unset when TAP_OUTBRAIN_API_CREDS is
absent) and the existing test files automatically pick up this class through
tests/base.py.

HTTP mocking
────────────
OutbrainClient.request() is patched with a side-effect function that serves
dynamically generated JSON built from the tap's own JSON Schema files.
"""
from __future__ import annotations

import _mock_tap_tester  # noqa: F401 — must be imported first to inject stubs
import datetime
from datetime import timedelta
from pathlib import Path
from tap_tester.base_suite_tests.base_case import BaseCase
from mock_data_generator import FIXTURES


class MockOutbrainBaseTest(BaseCase):
    """
    Integration-test base that exercises the tap against mocked HTTP responses.

    Inherits all catalog/sync helpers from tap_tester's BaseCase so that the
    full set of base suite tests (AllFieldsTest, DiscoveryTest, etc.) can be
    used alongside mock mode without modification.
    """

    start_date = "2024-01-01T00:00:00Z"
    bookmark_format = "%Y-%m-%d"
    PARENT_STREAM = "parent-stream"

    PRIMARY_KEYS = "table-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    REPLICATION_KEYS = "valid-replication-keys"
    RESPECTS_START_DATE = "table-start-date-usage"

    INCREMENTAL = "INCREMENTAL"
    FULL_TABLE = "FULL_TABLE"

    @staticmethod
    def tap_name() -> str:
        return "tap-outbrain"

    @staticmethod
    def get_type() -> str:
        return "platform.outbrain"

    def get_properties(self, original: bool = True) -> dict:
        return {
            "start_date": self.start_date,
        }

    def get_credentials(self) -> dict:
        return {
            "account_id": "mock_account_001",
            "username": "mock@example.com",
            "password": "mock_password",
            "access_token": "mock_access_token",
        }

    @staticmethod
    def get_mock_config(start_date=None) -> dict:
        """Return a config dict with fake credentials — no real API calls."""
        return {
            "account_id": "test_account_001",
            "username": "test@example.com",
            "password": "test_password",
            "access_token": "mock_access_token",
            "start_date": start_date or "2024-01-01T00:00:00Z",
        }

    @classmethod
    def expected_metadata(cls) -> dict:
        return {
            "campaign": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.RESPECTS_START_DATE: False,
                cls.API_LIMIT: 1,
            },
            "campaign_performance": {
                cls.PRIMARY_KEYS: {"campaignId", "fromDate"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"fromDate"},
                cls.RESPECTS_START_DATE: True,
                cls.LOOK_BACK_WINDOW: timedelta(days=2),
                cls.PARENT_STREAM: "campaign",
                cls.API_LIMIT: 10,
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

    def _build_mock_request(self):
        """
        Return a side_effect callable for patching OutbrainClient.request.

        The callable receives the same arguments as OutbrainClient.request and
        returns mock JSON by looking up the path/url fragment in the
        in-memory FIXTURES map generated from the tap's JSON Schema files.
        Unknown paths return an empty payload so the tap continues
        without errors.
        """
        def _side_effect(url, access_token, params=None):
            from unittest.mock import MagicMock
            
            params = params or {}
            resp = MagicMock()

            def _as_date_string(value, fallback="2024-01-01"):
                if value is None:
                    return fallback
                text = str(value)
                return text.split("T", 1)[0]
            
            # Map different endpoints to mock data
            if "/campaigns" in url:
                # Return mock campaign records from fixtures
                campaigns = FIXTURES.get("campaign", [])
                resp.json.return_value = {
                    "campaigns": campaigns,
                    "totalCount": len(campaigns),
                }
            elif "/periodic" in url or "performance" in url:
                # Build multiple performance rows keyed off request date range.
                # Generate records starting from params['from'] to ensure records advance
                # forward in time across syncs for proper bookmark progression.
                perf_template = (FIXTURES.get("campaign_performance") or [{}])[0]
                
                # Use 'from' date (range start) so records increment forward from there
                from_date_str = _as_date_string(params.get("from", params.get("to")))
                
                # Parse the date and generate records spanning the range
                try:
                    from_date_obj = datetime.datetime.strptime(from_date_str, "%Y-%m-%d")
                except:
                    from_date_obj = datetime.datetime(2024, 1, 1)
                
                # Generate multiple records to test pagination (mock API_LIMIT is 10)
                record_count = 12
                results = []
                for i in range(record_count):
                    record_date = from_date_obj + datetime.timedelta(days=i)
                    record_from_date = record_date.strftime("%Y-%m-%d")
                    
                    metrics = {}
                    for key, value in perf_template.items():
                        if key in ("campaignId", "fromDate"):
                            continue
                        if key in {"impressions", "clicks", "conversions"}:
                            metrics[key] = str(int(value))
                        else:
                            metrics[key] = str(value)
                    
                    results.append({
                        "metadata": {
                            "fromDate": record_from_date,
                        },
                        "metrics": metrics,
                    })
                
                resp.json.return_value = {
                    "totalResults": len(results),
                    "results": results,
                }
            else:
                # Fallback: empty response
                resp.json.return_value = {}

            return resp

        return _side_effect

    # Delegate helper methods to the base test class helpers
    @staticmethod
    def _tap_root():
        return Path(__file__).resolve().parents[1]
