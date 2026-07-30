"""Base class for tap-outbrain live integration tests."""
import json
import os
import unittest
from datetime import timedelta
from pathlib import Path

try:
    from tap_tester import connections, menagerie, runner  # noqa: F401
    from tap_tester.base_suite_tests.base_case import BaseCase
    HAS_TAP_TESTER = True
except ImportError:
    # Fallback for environments without tap_tester (e.g., local testing)
    HAS_TAP_TESTER = False
    BaseCase = unittest.TestCase


def _config_paths():
    """Resolve config file paths from environment or default locations."""
    env_path = os.environ.get("TAP_OUTBRAIN_CONFIG_JSON") or os.environ.get("OUTBRAIN_CONFIG_JSON")
    if env_path:
        yield Path(env_path)

    yield Path(__file__).resolve().parent / "config.json"
    yield Path(__file__).resolve().parents[1] / "config.json"


def _load_credentials_from_config():
    """Load Outbrain credentials from config.json file."""
    for config_path in _config_paths():
        if not config_path.is_file():
            continue

        with config_path.open("r", encoding="utf-8") as config_file:
            config = json.load(config_file)

        account_id = config.get("account_id")
        username = config.get("username")
        password = config.get("password")
        if account_id and username and password:
            return {
                "account_id": account_id,
                "username": username,
                "password": password,
            }

    return None


def _resolve_mode() -> str:
    """Determine if tests should run in 'live' or 'mock' mode."""
    mode = os.environ.get("INTEGRATION_TEST_MODE", "auto").lower()
    if mode == "mock":
        return "mock"

    if _load_credentials_from_config() is not None:
        return "live"

    required_env = (
        "TAP_OUTBRAIN_ACCOUNT_ID",
        "TAP_OUTBRAIN_USERNAME",
        "TAP_OUTBRAIN_PASSWORD",
    )
    has_live_creds = all(os.environ.get(var) for var in required_env)

    if mode == "live" and not has_live_creds:
        return "mock"

    return "live" if has_live_creds else "mock"


class OutbrainBaseTest(BaseCase):
    """Shared helpers and metadata for all tap-outbrain live integration tests."""

    PRIMARY_KEYS = "primary_keys"
    REPLICATION_METHOD = "replication_method"
    REPLICATION_KEYS = "replication_keys"
    OBEYS_START_DATE = "obeys_start_date"
    RESPECTS_START_DATE = "respects_start_date"
    API_LIMIT = "api_limit"
    LOOK_BACK_WINDOW = "look_back_window"
    PARENT_STREAM = "parent_stream"
    PARENT = "parent"

    INCREMENTAL = "INCREMENTAL"
    FULL_TABLE = "FULL_TABLE"
    DEFAULT_START_DATE = "2024-01-01"
    start_date = "2024-01-01T00:00:00Z"
    bookmark_format = "%Y-%m-%d"

    @classmethod
    def setUpClass(cls):
        """Skip root integration tests if not in live mode."""
        super().setUpClass()
        if _resolve_mode() != "live":
            raise unittest.SkipTest(
                "Root integration tests run only in live mode. "
                "Use tests/mock_integration for mock mode."
            )

    @staticmethod
    def _tap_root():
        """Absolute path to the tap-outbrain package root."""
        return os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "tap_outbrain")
        )

    @classmethod
    def _load_schema(cls, stream_name: str) -> dict:
        """Load a schema JSON from the tap_outbrain/schemas/ directory."""
        path = os.path.join(cls._tap_root(), "schemas", f"{stream_name}.json")
        with open(path) as fh:
            return json.load(fh)

    @staticmethod
    def get_mock_config(start_date=None) -> dict:
        """Return a config dict with fake credentials — no real API calls.

        access_token is provided so do_sync skips generate_token() entirely.
        """
        return {
            "account_id": "test_account_001",
            "username": "test@example.com",
            "password": "test_password",
            "access_token": "mock_access_token",
            "start_date": start_date or "2024-01-01T00:00:00Z",
        }

    def setUp(self):
        self.config = self.get_mock_config()

    @classmethod
    def expected_metadata(cls) -> dict:
        """Expected streams and their key metadata attributes."""
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

    @classmethod
    def expected_primary_keys(cls) -> dict:
        return {
            s: meta[cls.PRIMARY_KEYS]
            for s, meta in cls.expected_metadata().items()
        }

    @classmethod
    def expected_replication_keys(cls) -> dict:
        return {
            s: meta[cls.REPLICATION_KEYS]
            for s, meta in cls.expected_metadata().items()
        }

    @classmethod
    def expected_replication_method(cls) -> dict:
        return {
            s: meta[cls.REPLICATION_METHOD]
            for s, meta in cls.expected_metadata().items()
        }

    @classmethod
    def expected_lookback_window(cls, stream=None):
        lookback = {
            "campaign": timedelta(days=0),
            "campaign_performance": timedelta(days=2),
        }
        if stream is None:
            return lookback
        return lookback[stream]

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
        """Load credentials from config file or environment variables."""
        credentials = _load_credentials_from_config()
        if credentials is not None:
            return credentials

        return {
            "account_id": os.environ["TAP_OUTBRAIN_ACCOUNT_ID"],
            "username": os.environ["TAP_OUTBRAIN_USERNAME"],
            "password": os.environ["TAP_OUTBRAIN_PASSWORD"],
        }

    # -- end of OutbrainBaseTest --


# Keep this marker here so the file ends cleanly
if False:
    def _make_selected_catalog():
        """Return a discover() catalog with all streams marked as selected.

        Singer's ``catalog.get_selected_streams()`` only yields streams whose
        root metadata entry (breadcrumb == ()) contains ``selected: True``.
        """
        catalog = discover()
        for entry in catalog.streams:
            for m in entry.metadata:
                if not m.get("breadcrumb"):
                    m["metadata"]["selected"] = True
        return catalog

    @staticmethod
    def make_campaign_record(campaign_id, **overrides):
        """Return a campaign dict as returned by the Outbrain /campaigns endpoint."""
        record = {
            "id": campaign_id,
            "name": f"Campaign {campaign_id}",
            "campaignOnAir": True,
            "onAirReason": None,
            "enabled": True,
            "budget": {
                "id": f"budget_{campaign_id}",
                "name": f"Budget {campaign_id}",
                "shared": False,
                "amount": 1000.0,
                "currency": "USD",
                "amountRemaining": 500.0,
                "amountSpent": 500.0,
                # parse_campaign() calls parse_datetime() on these — must be ISO strings
                "creationTime": "2024-01-01T00:00:00+00:00",
                "lastModified": "2024-01-02T00:00:00+00:00",
                "startDate": "2024-01-01",
                "endDate": None,
                "runForever": True,
                "type": "daily",
                "pacing": "automatic",
                "dailyTarget": 100.0,
                "maximumAmount": None,
            },
            "cpc": 0.5,
        }
        record.update(overrides)
        return record

    @staticmethod
    def make_performance_record(campaign_id, from_date, **overrides):
        """Return a flat performance record dict (pre-API-response-transform format)."""
        record = {
            "campaignId": campaign_id,
            "fromDate": from_date,
            "impressions": 1000,
            "clicks": 50,
            "ctr": 0.05,
            "spend": 25.0,
            "ecpc": 0.5,
            "conversions": 5,
            "conversionRate": 0.1,
            "cpa": 5.0,
        }
        record.update(overrides)
        return record

    def _collect_periodic_params(self, config=None, campaigns=None, initial_state=None):
        """Run a mock sync and collect every ``params`` dict sent to /periodic requests.

        Test strategy
        -------------
        The outer-scope list ``periodic_params`` is captured by the inner
        ``fake_request`` closure via Python's standard closure mechanism.
        Closures hold a reference to mutable objects in the enclosing scope, so
        every ``periodic_params.append(...)`` inside ``fake_request`` updates
        the same list that is ultimately returned to callers — there is no
        inconsistency or hidden shared state between test runs because each
        invocation of ``_collect_periodic_params`` allocates a fresh
        ``periodic_params = []`` list.

        The inner ``fake_request``:
          * handles ``/campaigns`` requests for paginated campaign fetching;
          * handles ``/periodic`` requests (the performance sink) and records
            the full ``params`` dict so callers can assert on ``from``, ``to``,
            ``campaignId``, etc.

        Useful for start_date and bookmark tests that need to inspect the
        ``from_date`` / ``to_date`` window the tap computes for each API call.
        """

        run_config = config or self.config
        mock_campaigns = campaigns if campaigns is not None else [self.make_campaign_record("c001")]
        state = copy.deepcopy(initial_state) if initial_state else {"campaign_performance": {}}
        periodic_params = []

        # WHY fake_request is defined here:
        # tap_outbrain.request() is the single HTTP gateway used by the tap.
        # By patching it with this inner function we intercept every outbound
        # call without spawning a real HTTP connection.  Defining it inline
        # (rather than as a class-level helper) lets it close over the local
        # test fixtures (``mock_campaigns``, ``periodic_params``) without
        # having to pass them as arguments, keeping the patch site clean.
        #
        # No inconsistency risk: ``periodic_params`` is freshly allocated on
        # every call to ``_collect_periodic_params``; the closure holds a
        # reference to *that* list only, so repeated test calls never share
        # state.
        def fake_request(url, access_token, params):
            resp = MagicMock()
            if "/campaigns" in url:
                offset = params.get("offset", 0)
                limit = params.get("limit", 50)
                page = mock_campaigns[offset: offset + limit]
                resp.json.return_value = {
                    "campaigns": page,
                    "totalCount": len(mock_campaigns),
                }
            elif "/periodic" in url:
                periodic_params.append(dict(params))
                from_d = params.get("from")
                from_str = from_d.isoformat() if hasattr(from_d, "isoformat") else str(from_d)
                cid = params.get("campaignId")
                resp.json.return_value = {
                    "totalResults": 1,
                    "results": [{
                        "metadata": {"fromDate": from_str},
                        "metrics": {
                            "impressions": "100", "clicks": "5", "ctr": "0.05",
                            "spend": "10.0", "ecpc": "2.0", "conversions": "1",
                            "conversionRate": "0.2", "cpa": "10.0",
                        },
                    }],
                }
            return resp

        catalog = self._make_selected_catalog()
        with patch("tap_outbrain.request", side_effect=fake_request), \
             patch("singer.write_schema"), \
             patch("singer.write_record"), \
             patch("singer.write_state"), \
             patch("time.sleep"):
            tap_outbrain.do_sync(catalog, run_config, state)

        return periodic_params

    def _run_mock_sync(self, campaigns=None, perf_records_by_campaign=None,
                       config=None, state=None):
        """Run do_sync with a fully mocked HTTP layer and capture Singer output.

        Test strategy
        -------------
        All outbound HTTP calls are intercepted by ``fake_request``.  Singer
        output functions (``write_schema``, ``write_record``, ``write_state``)
        are replaced with lightweight capture helpers that accumulate emitted
        messages into ``captured``.  This lets integration tests make
        assertions about:

        * which SCHEMA messages were emitted (``captured["schemas"]``);
        * which RECORD messages were emitted (``captured["records"]``);
        * how the state evolved after each sync cycle (``captured["states"]``).

        The ``fake_request`` inner function handles two URL patterns:
          * ``/campaigns`` - returns paginated campaign fixtures so that the
            tap's pagination logic can be exercised without a live API;
          * ``/periodic``  - returns performance fixtures keyed by
            ``campaign_id``; a minimal auto-generated record is used when no
            fixture is provided for a campaign (prevents ``IndexError`` inside
            ``sync_performance``).

        Each invocation allocates independent ``captured`` and ``final_state``
        objects, so tests are fully isolated from one another.

        Parameters
        ----------
        campaigns :
            List of campaign dicts (from make_campaign_record).  Pagination is
            handled automatically — the fake request pages through the list.
        perf_records_by_campaign :
            Dict mapping campaign_id -> list of flat performance record dicts.
            If a campaign has no entry, a default record is auto-generated per
            date-range request (prevents IndexError in sync_performance).
        config :
            Override config; defaults to get_mock_config().
        state :
            Override initial state; defaults to {"campaign_performance": {}}.

        Returns
        -------
        (captured, final_state)
            captured  = {"schemas": {...}, "records": {...}, "states": [...]}
            final_state = the mutated state dict after do_sync completes.
        """

        catalog = self._make_selected_catalog()
        run_config = config or self.config
        final_state = copy.deepcopy(state) if state is not None else {"campaign_performance": {}}
        mock_campaigns = campaigns if campaigns is not None else []
        perf_map = perf_records_by_campaign or {}

        captured = {"schemas": {}, "records": {}, "states": []}

        # WHY fake_request is defined here:
        # tap_outbrain.request() is the tap's sole HTTP gateway.  Replacing it
        # with this closure lets us control exactly what each endpoint returns
        # without any real network calls.  The function is defined locally so
        # it can close over ``mock_campaigns``, ``perf_map``, and ``captured``
        # directly, which avoids threading those fixtures through additional
        # arguments and keeps the unittest.mock.patch() call site simple.
        #
        # No inconsistency risk: every call to ``_run_mock_sync`` allocates
        # its own fresh ``captured``, ``final_state``, ``mock_campaigns``, and
        # ``perf_map`` objects.  The closure captures references to *those*
        # objects only; no mutable state is shared across test invocations, so
        # parallel or sequential test execution cannot produce cross-test
        # pollution.
        def fake_request(url, access_token, params):
            resp = MagicMock()
            if "/campaigns" in url:
                offset = params.get("offset", 0)
                limit = params.get("limit", 50)
                page = mock_campaigns[offset: offset + limit]
                resp.json.return_value = {
                    "campaigns": page,
                    "totalCount": len(mock_campaigns),
                }
            elif "/periodic" in url:
                cid = params.get("campaignId")
                records_for = perf_map.get(cid)
                if not records_for:  # None or empty -> auto-generate to avoid IndexError
                    from_d = params.get("from")
                    from_str = (
                        from_d.isoformat() if hasattr(from_d, "isoformat") else str(from_d)
                    )
                    records_for = [self.make_performance_record(cid, from_str)]
                results = [
                    {
                        "metadata": {"fromDate": r["fromDate"]},
                        "metrics": {
                            k: str(v)
                            for k, v in r.items()
                            if k not in ("campaignId", "fromDate")
                        },
                    }
                    for r in records_for
                ]
                resp.json.return_value = {
                    "totalResults": len(results),
                    "results": results,
                }
            return resp

        def _cap_schema(stream_name, schema, **kwargs):
            captured["schemas"][stream_name] = schema

        def _cap_record(stream_name, record, **kwargs):
            captured["records"].setdefault(stream_name, []).append(record)

        def _cap_state(s):
            captured["states"].append(copy.deepcopy(s))

        with patch("tap_outbrain.request", side_effect=fake_request), \
             patch("singer.write_schema", side_effect=_cap_schema), \
             patch("singer.write_record", side_effect=_cap_record), \
             patch("singer.write_state", side_effect=_cap_state), \
             patch("time.sleep"):
            tap_outbrain.do_sync(catalog, run_config, final_state)

        return captured, final_state
