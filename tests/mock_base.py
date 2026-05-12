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
from tap_tester.base_suite_tests.base_case import BaseCase
from mock_data_generator import FIXTURES
from base import OutbrainBaseTest as _BaseTest


class MockOutbrainBaseTest(BaseCase):
    """
    Integration-test base that exercises the tap against mocked HTTP responses.

    Inherits all catalog/sync helpers from tap_tester's BaseCase so that the
    full set of base suite tests (AllFieldsTest, DiscoveryTest, etc.) can be
    used alongside mock mode without modification.
    """

    start_date = "2024-01-01T00:00:00Z"

    PRIMARY_KEYS = "primary_keys"
    REPLICATION_METHOD = "replication_method"
    REPLICATION_KEYS = "replication_keys"
    RESPECTS_START_DATE = "respects_start_date"

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
            },
            "campaign_performance": {
                cls.PRIMARY_KEYS: {"campaignId", "fromDate"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"fromDate"},
                cls.RESPECTS_START_DATE: True,
            },
        }

    @classmethod
    def expected_stream_names(cls) -> set:
        return set(cls.expected_metadata().keys())

    @classmethod
    def expected_primary_keys(cls) -> dict:
        return {
            stream: meta[cls.PRIMARY_KEYS]
            for stream, meta in cls.expected_metadata().items()
        }

    @classmethod
    def expected_replication_keys(cls) -> dict:
        return {
            stream: meta[cls.REPLICATION_KEYS]
            for stream, meta in cls.expected_metadata().items()
        }

    @classmethod
    def expected_replication_method(cls) -> dict:
        return {
            stream: meta[cls.REPLICATION_METHOD]
            for stream, meta in cls.expected_metadata().items()
        }

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
            
            # Map different endpoints to mock data
            if "/campaigns" in url:
                # Return mock campaign records from fixtures
                campaigns = FIXTURES.get("campaign", [])
                resp.json.return_value = {
                    "campaigns": campaigns,
                    "totalCount": len(campaigns),
                }
            elif "/periodic" in url or "performance" in url:
                # Return mock performance records from fixtures
                perf_records = FIXTURES.get("campaign_performance", [])
                results = []
                for rec in perf_records:
                    results.append({
                        "metadata": {
                            "fromDate": rec.get("fromDate", "2024-01-01"),
                        },
                        "metrics": {
                            k: str(v)
                            for k, v in rec.items()
                            if k not in ("campaignId", "fromDate")
                        },
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
        return _BaseTest._tap_root()

    @classmethod
    def _load_schema(cls, stream_name: str) -> dict:
        return _BaseTest._load_schema(stream_name)

    @staticmethod
    def _make_selected_catalog():
        return _BaseTest._make_selected_catalog()

    @staticmethod
    def make_campaign_record(campaign_id, **overrides):
        return _BaseTest.make_campaign_record(campaign_id, **overrides)

    @staticmethod
    def make_performance_record(campaign_id, from_date, **overrides):
        return _BaseTest.make_performance_record(campaign_id, from_date, **overrides)

    def _collect_periodic_params(self, config=None, campaigns=None, initial_state=None):
        return _BaseTest()._collect_periodic_params(config, campaigns, initial_state)

    def _run_mock_sync(self, campaigns=None, perf_records_by_campaign=None,
                       config=None, state=None):
        return _BaseTest()._run_mock_sync(campaigns, perf_records_by_campaign, config, state)
