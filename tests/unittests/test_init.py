"""Unit tests for tap_outbrain/__init__.py"""

import unittest
from unittest.mock import patch, MagicMock
import datetime

import tap_outbrain
from tap_outbrain import (
    get_date_ranges,
    parse_datetime,
    parse_performance,
    parse_campaign,
    sync_campaign_page,
    do_discover,
    do_sync,
    StreamSelectionError,
    generate_token,
    get_campaigns_page,
    MARKETERS_CAMPAIGNS_MAX_LIMIT,
)


# ---------------------------------------------------------------------------
# get_date_ranges
# ---------------------------------------------------------------------------

class TestGetDateRanges(unittest.TestCase):

    def test_start_equals_end_returns_empty(self):
        """start == end: while condition `start < end` is False, returns []."""
        d = datetime.date(2024, 1, 1)
        result = get_date_ranges(d, d, 100)
        self.assertEqual(result, [])

    def test_start_greater_than_end_returns_empty(self):
        """start > end must return []."""
        start = datetime.date(2024, 6, 1)
        end = datetime.date(2024, 1, 1)
        self.assertEqual(get_date_ranges(start, end, 30), [])

    def test_multiple_intervals(self):
        """Span that produces 3 intervals — verify count and first range boundaries."""
        start = datetime.date(2023, 1, 1)  # non-leap year for predictable arithmetic
        end = datetime.date(2023, 4, 1)    # 90 days later
        result = get_date_ranges(start, end, 30)
        # Should produce 3 ranges (Jan1-Jan30, Jan31-Mar1, Mar2-Mar31)
        self.assertEqual(len(result), 3)
        # First range
        self.assertEqual(result[0]['from_date'], datetime.date(2023, 1, 1))
        self.assertEqual(result[0]['to_date'], datetime.date(2023, 1, 30))
        # Each range's from_date should be earlier than to_date or equal
        for r in result:
            self.assertLessEqual(r['from_date'], r['to_date'])

    def test_interval_larger_than_span_caps_to_date(self):
        """Interval larger than the span: to_date capped to `end`."""
        start = datetime.date(2024, 3, 1)
        end = datetime.date(2024, 3, 5)
        result = get_date_ranges(start, end, 30)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['to_date'], end)

    def test_single_day_interval(self):
        """interval_in_days=1: loop is `while start < end`, so end itself is excluded."""
        start = datetime.date(2024, 1, 1)
        end = datetime.date(2024, 1, 3)
        result = get_date_ranges(start, end, 1)
        # Jan 1 and Jan 2 are covered; Jan 3 itself stops the loop (not a new range)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['from_date'], datetime.date(2024, 1, 1))
        self.assertEqual(result[0]['to_date'], datetime.date(2024, 1, 1))
        self.assertEqual(result[1]['from_date'], datetime.date(2024, 1, 2))
        self.assertEqual(result[1]['to_date'], datetime.date(2024, 1, 2))


# ---------------------------------------------------------------------------
# parse_datetime
# ---------------------------------------------------------------------------

class TestParseDatetime(unittest.TestCase):

    def test_iso_utc_string(self):
        """ISO UTC datetime string round-trips correctly."""
        result = parse_datetime("2024-06-15T12:30:00+00:00")
        self.assertTrue(result.endswith("Z"))

    def test_naive_datetime_string(self):
        """Naive datetime string: returns isoformat ending with Z."""
        result = parse_datetime("2024-01-01T00:00:00")
        self.assertIn("2024-01-01", result)
        self.assertTrue(result.endswith("Z"))

    def test_date_only_string(self):
        """Date-only string (no time) parses without error."""
        result = parse_datetime("2024-03-01")
        self.assertIn("2024-03-01", result)
        self.assertTrue(result.endswith("Z"))


# ---------------------------------------------------------------------------
# parse_performance
# ---------------------------------------------------------------------------

class TestParsePerformance(unittest.TestCase):

    def _make_result(self, metrics=None, metadata=None):
        return {
            'metrics': metrics or {},
            'metadata': metadata or {},
        }

    def test_all_fields_present(self):
        """All metric fields are correctly parsed."""
        result = self._make_result(
            metrics={
                'impressions': '100',
                'clicks': '5',
                'ctr': '0.05',
                'spend': '12.50',
                'ecpc': '2.50',
                'conversions': '2',
                'conversionRate': '0.4',
                'cpa': '6.25',
            },
            metadata={'fromDate': '2024-01-01'},
        )
        parsed = parse_performance(result, {'campaignId': 'abc'})
        self.assertEqual(parsed['impressions'], 100)
        self.assertEqual(parsed['clicks'], 5)
        self.assertAlmostEqual(parsed['ctr'], 0.05)
        self.assertAlmostEqual(parsed['spend'], 12.50)
        self.assertEqual(parsed['fromDate'], '2024-01-01')
        self.assertEqual(parsed['campaignId'], 'abc')

    def test_missing_metrics_default_to_zero(self):
        """Missing metrics default to 0."""
        result = self._make_result()
        parsed = parse_performance(result, {})
        self.assertEqual(parsed['impressions'], 0)
        self.assertEqual(parsed['clicks'], 0)
        self.assertAlmostEqual(parsed['spend'], 0.0)

    def test_extra_fields_merged(self):
        """extra_fields are merged into the output."""
        result = self._make_result()
        parsed = parse_performance(result, {'campaignId': 'x', 'linkId': 'y'})
        self.assertEqual(parsed['campaignId'], 'x')
        self.assertEqual(parsed['linkId'], 'y')

    def test_from_date_comes_from_metadata(self):
        """fromDate sourced from metadata, not metrics."""
        result = self._make_result(metadata={'fromDate': '2024-06-01'})
        parsed = parse_performance(result, {})
        self.assertEqual(parsed['fromDate'], '2024-06-01')


# ---------------------------------------------------------------------------
# parse_campaign
# ---------------------------------------------------------------------------

class TestParseCampaign(unittest.TestCase):

    def test_campaign_without_budget_unchanged(self):
        """Campaign with no budget key is returned as-is."""
        campaign = {'id': 'c1', 'name': 'Test'}
        result = parse_campaign(campaign)
        self.assertEqual(result['id'], 'c1')
        self.assertNotIn('budget', result)

    def test_campaign_with_none_budget_unchanged(self):
        """Campaign with budget=None is returned without modification."""
        campaign = {'id': 'c2', 'budget': None}
        result = parse_campaign(campaign)
        self.assertIsNone(result['budget'])

    def test_campaign_with_budget_parses_datetimes(self):
        """Budget creationTime and lastModified are converted to ISO strings."""
        campaign = {
            'id': 'c3',
            'budget': {
                'creationTime': '2024-01-01T10:00:00+00:00',
                'lastModified': '2024-02-28T08:00:00+00:00',
            }
        }
        result = parse_campaign(campaign)
        self.assertTrue(result['budget']['creationTime'].endswith('Z'))
        self.assertTrue(result['budget']['lastModified'].endswith('Z'))

    def test_parse_campaign_returns_same_object(self):
        """parse_campaign returns the mutated input dict."""
        campaign = {
            'id': 'c4',
            'budget': {
                'creationTime': '2024-01-01T00:00:00',
                'lastModified': '2024-01-02T00:00:00',
            }
        }
        result = parse_campaign(campaign)
        self.assertIs(result, campaign)


# ---------------------------------------------------------------------------
# sync_campaign_page
# ---------------------------------------------------------------------------

class TestSyncCampaignPage(unittest.TestCase):

    def setUp(self):
        self.state = {'campaign_performance': {}}
        self.access_token = 'tok'
        self.account_id = 'acc1'

    @patch('tap_outbrain.sync_campaign_performance')
    @patch('singer.write_record')
    def test_writes_records_and_syncs_performance(self, mock_write_record, mock_sync_perf):
        """Each campaign triggers write_record for campaigns + sync_campaign_performance."""
        campaign_page = {
            'campaigns': [
                {'id': 'c1', 'name': 'C1'},
                {'id': 'c2', 'name': 'C2'},
            ]
        }
        selected_streams = ['campaigns', 'campaign_performance']

        sync_campaign_page(self.state, self.access_token, self.account_id,
                           campaign_page, selected_streams)

        self.assertEqual(mock_write_record.call_count, 2)
        self.assertEqual(mock_sync_perf.call_count, 2)
        mock_sync_perf.assert_any_call(self.state, self.access_token, self.account_id, 'c1')
        mock_sync_perf.assert_any_call(self.state, self.access_token, self.account_id, 'c2')

    @patch('tap_outbrain.sync_campaign_performance')
    @patch('singer.write_record')
    def test_skips_sync_when_campaign_performance_not_selected(self, mock_write_record, mock_sync_perf):
        """When campaign_performance not in selected_streams, sync is skipped."""
        campaign_page = {'campaigns': [{'id': 'c1', 'name': 'C1'}]}
        selected_streams = ['campaigns']  # campaign_performance absent

        sync_campaign_page(self.state, self.access_token, self.account_id,
                           campaign_page, selected_streams)

        mock_sync_perf.assert_not_called()
        mock_write_record.assert_not_called()

    @patch('tap_outbrain.sync_campaign_performance')
    @patch('singer.write_record')
    def test_empty_campaigns_list(self, mock_write_record, mock_sync_perf):
        """Empty campaigns list produces no writes or syncs."""
        campaign_page = {'campaigns': []}
        selected_streams = ['campaigns', 'campaign_performance']

        sync_campaign_page(self.state, self.access_token, self.account_id,
                           campaign_page, selected_streams)

        mock_write_record.assert_not_called()
        mock_sync_perf.assert_not_called()


# ---------------------------------------------------------------------------
# generate_token
# ---------------------------------------------------------------------------

class TestGenerateToken(unittest.TestCase):

    @patch('tap_outbrain.OutbrainClient')
    def test_returns_token_from_response(self, mock_client_cls):
        """generate_token extracts OB-TOKEN-V1 from the JSON response."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {'OB-TOKEN-V1': 'mytoken123'}
        mock_instance = mock_client_cls.return_value
        mock_instance.make_request.return_value = mock_resp

        token = generate_token('user', 'pass')
        self.assertEqual(token, 'mytoken123')

    @patch('tap_outbrain.OutbrainClient')
    def test_returns_none_when_token_missing(self, mock_client_cls):
        """Returns None when token key is absent in response."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {}
        mock_instance = mock_client_cls.return_value
        mock_instance.make_request.return_value = mock_resp

        token = generate_token('user', 'pass')
        self.assertIsNone(token)

    @patch('tap_outbrain.OutbrainClient')
    def test_calls_correct_endpoint(self, mock_client_cls):
        """generate_token calls the /login endpoint."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {'OB-TOKEN-V1': 't'}
        mock_instance = mock_client_cls.return_value
        mock_instance.make_request.return_value = mock_resp

        generate_token('u', 'p')
        args, kwargs = mock_instance.make_request.call_args
        self.assertIn('/login', args[1])


# ---------------------------------------------------------------------------
# StreamSelectionError
# ---------------------------------------------------------------------------

class TestStreamSelectionError(unittest.TestCase):

    def test_is_exception(self):
        """StreamSelectionError is an Exception subclass."""
        self.assertTrue(issubclass(StreamSelectionError, Exception))

    def test_can_be_raised_and_caught(self):
        """StreamSelectionError can be raised with a message."""
        with self.assertRaises(StreamSelectionError) as ctx:
            raise StreamSelectionError("campaign not selected")
        self.assertIn("campaign not selected", str(ctx.exception))


# ---------------------------------------------------------------------------
# do_discover
# ---------------------------------------------------------------------------

class TestDoDiscover(unittest.TestCase):

    @patch('sys.stdout')
    @patch('tap_outbrain.discover')
    def test_do_discover_dumps_catalog(self, mock_discover, mock_stdout):
        """do_discover calls discover() and writes JSON to stdout."""
        mock_catalog = MagicMock()
        mock_catalog.to_dict.return_value = {'streams': []}
        mock_discover.return_value = mock_catalog

        do_discover()

        mock_discover.assert_called_once()
        mock_catalog.to_dict.assert_called_once()


# ---------------------------------------------------------------------------
# do_sync — stream selection error paths
# ---------------------------------------------------------------------------

class TestDoSync(unittest.TestCase):

    def _make_catalog(self, stream_names):
        """Build a minimal singer.Catalog stub with given selected streams."""
        import singer
        catalog_entries = []
        for name in stream_names:
            entry = MagicMock()
            entry.stream = name
            catalog_entries.append(entry)
        mock_catalog = MagicMock(spec=singer.Catalog)
        mock_catalog.get_selected_streams.return_value = catalog_entries
        return mock_catalog

    def _make_config(self):
        return {
            'account_id': 'acct1',
            'username': 'user',
            'password': 'pass',
            'start_date': '2024-01-01T00:00:00Z',
            'access_token': 'tok123',
        }

    @patch('tap_outbrain.sync_campaigns')
    @patch('singer.write_schema')
    def test_do_sync_raises_when_campaign_not_selected(self, mock_write_schema, mock_sync):
        """do_sync raises StreamSelectionError when campaigns stream is missing."""
        catalog = self._make_catalog(['campaign_performance'])
        config = self._make_config()

        with self.assertRaises(StreamSelectionError):
            do_sync(catalog, config, {'campaign_performance': {}})

    @patch('tap_outbrain.sync_campaigns')
    @patch('singer.write_schema')
    def test_do_sync_success_with_both_streams(self, mock_write_schema, mock_sync):
        """do_sync succeeds and calls sync_campaigns when both streams selected."""
        catalog = self._make_catalog(['campaign', 'campaign_performance'])
        config = self._make_config()

        do_sync(catalog, config, {'campaign_performance': {}})

        mock_sync.assert_called_once()
        self.assertEqual(mock_write_schema.call_count, 2)

    @patch('tap_outbrain.sync_campaigns')
    @patch('singer.write_schema')
    def test_do_sync_updates_global_config(self, mock_write_schema, mock_sync):
        """do_sync updates the module-level CONFIG with config values."""
        catalog = self._make_catalog(['campaign', 'campaign_performance'])
        config = self._make_config()
        config['user_agent'] = 'test-agent'

        do_sync(catalog, config, {'campaign_performance': {}})

        self.assertEqual(tap_outbrain.CONFIG.get('user_agent'), 'test-agent')

    @patch('tap_outbrain.sync_campaigns')
    @patch('singer.write_schema')
    def test_do_sync_sets_start_date(self, mock_write_schema, mock_sync):
        """do_sync slices start_date to YYYY-MM-DD for DEFAULT_START_DATE."""
        catalog = self._make_catalog(['campaign', 'campaign_performance'])
        config = self._make_config()
        config['start_date'] = '2024-06-15T00:00:00Z'

        do_sync(catalog, config, {'campaign_performance': {}})

        self.assertEqual(tap_outbrain.DEFAULT_START_DATE, '2024-06-15')

    @patch('tap_outbrain.generate_token')
    @patch('tap_outbrain.sync_campaigns')
    @patch('singer.write_schema')
    def test_do_sync_generates_token_when_no_access_token(
        self, mock_write_schema, mock_sync, mock_gen_token
    ):
        """do_sync calls generate_token when access_token not in config."""
        mock_gen_token.return_value = 'generated_tok'
        catalog = self._make_catalog(['campaign', 'campaign_performance'])
        config = {
            'account_id': 'acct1',
            'username': 'user',
            'password': 'pass',
            'start_date': '2024-01-01T00:00:00Z',
            # no access_token
        }

        do_sync(catalog, config, {'campaign_performance': {}})

        mock_gen_token.assert_called_once_with('user', 'pass')

    @patch('tap_outbrain.generate_token')
    @patch('tap_outbrain.sync_campaigns')
    @patch('singer.write_schema')
    def test_do_sync_raises_runtime_error_when_token_is_none(
        self, mock_write_schema, mock_sync, mock_gen_token
    ):
        """do_sync raises RuntimeError when generate_token returns None."""
        mock_gen_token.return_value = None
        catalog = self._make_catalog(['campaign'])
        config = {
            'account_id': 'acct1',
            'username': 'user',
            'password': 'pass',
            'start_date': '2024-01-01T00:00:00Z',
        }

        with self.assertRaises(RuntimeError):
            do_sync(catalog, config, {'campaign_performance': {}})


# ---------------------------------------------------------------------------
# get_campaigns_page
# ---------------------------------------------------------------------------

class TestGetCampaignsPage(unittest.TestCase):

    @patch('tap_outbrain.request')
    def test_returns_json_response(self, mock_request):
        """get_campaigns_page calls request and returns parsed JSON."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {'campaigns': [], 'totalCount': 0}
        mock_request.return_value = mock_resp

        result = get_campaigns_page('acct1', 'tok', 0)
        self.assertEqual(result['totalCount'], 0)
        mock_request.assert_called_once()

    @patch('tap_outbrain.request')
    def test_passes_limit_and_offset(self, mock_request):
        """Correct limit and offset are passed to the API."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {'campaigns': [], 'totalCount': 0}
        mock_request.return_value = mock_resp

        get_campaigns_page('acct1', 'tok', 50)
        _, _, params = mock_request.call_args[0]
        self.assertEqual(params['limit'], MARKETERS_CAMPAIGNS_MAX_LIMIT)
        self.assertEqual(params['offset'], 50)


# ---------------------------------------------------------------------------
# request() helper
# ---------------------------------------------------------------------------

class TestRequestHelper(unittest.TestCase):

    def setUp(self):
        # Clear CONFIG before each test
        tap_outbrain.CONFIG.clear()

    @patch('tap_outbrain.OutbrainClient')
    def test_request_sets_ob_token_header(self, mock_client_cls):
        """request() sets OB-TOKEN-V1 header from access_token."""
        mock_instance = mock_client_cls.return_value
        mock_instance.make_request.return_value = MagicMock()

        tap_outbrain.request('http://api/', 'mytoken', {'q': 1})

        _, kwargs = mock_instance.make_request.call_args
        self.assertEqual(kwargs['headers']['OB-TOKEN-V1'], 'mytoken')

    @patch('tap_outbrain.OutbrainClient')
    def test_request_adds_user_agent_when_in_config(self, mock_client_cls):
        """request() adds User-Agent header when user_agent is in CONFIG."""
        tap_outbrain.CONFIG['user_agent'] = 'my-agent/1.0'
        mock_instance = mock_client_cls.return_value
        mock_instance.make_request.return_value = MagicMock()

        tap_outbrain.request('http://api/', 'tok', {})

        _, kwargs = mock_instance.make_request.call_args
        self.assertEqual(kwargs['headers']['User-Agent'], 'my-agent/1.0')

    @patch('tap_outbrain.OutbrainClient')
    def test_request_no_user_agent_when_absent_from_config(self, mock_client_cls):
        """request() does NOT add User-Agent when user_agent not in CONFIG."""
        mock_instance = mock_client_cls.return_value
        mock_instance.make_request.return_value = MagicMock()

        tap_outbrain.request('http://api/', 'tok', {})

        _, kwargs = mock_instance.make_request.call_args
        self.assertNotIn('User-Agent', kwargs['headers'])


# ---------------------------------------------------------------------------
# sync_performance
# ---------------------------------------------------------------------------

class TestSyncPerformance(unittest.TestCase):

    def _make_api_response(self, from_date='2024-01-05', total=1):
        return {
            'totalResults': total,
            'results': [
                {
                    'metadata': {'fromDate': from_date},
                    'metrics': {
                        'impressions': '10', 'clicks': '1',
                        'ctr': '0.1', 'spend': '5.0',
                        'ecpc': '5.0', 'conversions': '0',
                        'conversionRate': '0.0', 'cpa': '0.0',
                    }
                }
            ]
        }

    @patch('time.sleep')
    @patch('singer.write_state')
    @patch('singer.write_record')
    @patch('tap_outbrain.request')
    def test_sync_performance_writes_records_and_state(
        self, mock_request, mock_write_record, mock_write_state, mock_sleep
    ):
        """sync_performance writes records and state for each date range."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = self._make_api_response('2024-01-05')
        mock_request.return_value = mock_resp

        state = {'campaign_performance': {}}
        tap_outbrain.sync_performance(
            state, 'tok', 'acct1', 'campaign_performance', 'c1',
            {'campaignId': 'c1'}, {'campaignId': 'c1'}
        )

        mock_write_record.assert_called()
        mock_write_state.assert_called()

    @patch('time.sleep')
    @patch('singer.write_state')
    @patch('singer.write_record')
    @patch('tap_outbrain.request')
    def test_sync_performance_updates_state_with_last_from_date(
        self, mock_request, mock_write_record, mock_write_state, mock_sleep
    ):
        """After sync, state contains the fromDate of the last record."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = self._make_api_response('2024-02-10')
        mock_request.return_value = mock_resp

        state = {'campaign_performance': {}}
        tap_outbrain.sync_performance(
            state, 'tok', 'acct1', 'campaign_performance', 'c1',
            {'campaignId': 'c1'}, {'campaignId': 'c1'}
        )

        self.assertEqual(state['campaign_performance']['c1'], '2024-02-10')

    @patch('time.sleep')
    @patch('singer.write_state')
    @patch('singer.write_record')
    @patch('tap_outbrain.request')
    def test_sync_performance_uses_state_start_date(
        self, mock_request, mock_write_record, mock_write_state, mock_sleep
    ):
        """sync_performance uses state start date (minus 2 days) as from_date."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = self._make_api_response('2024-03-01')
        mock_request.return_value = mock_resp

        state = {'campaign_performance': {'c1': '2024-03-15'}}
        tap_outbrain.sync_performance(
            state, 'tok', 'acct1', 'campaign_performance', 'c1',
            {'campaignId': 'c1'}, {'campaignId': 'c1'}
        )

        # Verify the FIRST request used from_date = 2024-03-13 (2 days before state date)
        first_call_params = mock_request.call_args_list[0][0][2]
        expected_from = datetime.date(2024, 3, 13)
        self.assertEqual(first_call_params['from'], expected_from)


# ---------------------------------------------------------------------------
# sync_campaign_performance
# ---------------------------------------------------------------------------

class TestSyncCampaignPerformance(unittest.TestCase):

    @patch('tap_outbrain.sync_performance')
    def test_delegates_to_sync_performance(self, mock_sync_perf):
        """sync_campaign_performance correctly delegates to sync_performance."""
        state = {'campaign_performance': {}}
        tap_outbrain.sync_campaign_performance(state, 'tok', 'acct1', 'c123')

        mock_sync_perf.assert_called_once_with(
            state, 'tok', 'acct1',
            'campaign_performance', 'c123',
            {'campaignId': 'c123'}, {'campaignId': 'c123'}
        )


# ---------------------------------------------------------------------------
# get_campaign_pages
# ---------------------------------------------------------------------------

class TestGetCampaignPages(unittest.TestCase):

    @patch('tap_outbrain.get_campaigns_page')
    def test_single_page(self, mock_get_page):
        """Single page (totalCount <= limit) yields one page."""
        mock_get_page.return_value = {
            'campaigns': [{'id': 'c1'}],
            'totalCount': 1,
        }
        pages = list(tap_outbrain.get_campaign_pages('acct1', 'tok'))
        self.assertEqual(len(pages), 1)
        mock_get_page.assert_called_once_with('acct1', 'tok', 0)

    @patch('tap_outbrain.get_campaigns_page')
    def test_multiple_pages(self, mock_get_page):
        """Multiple pages: offsets advance correctly."""
        mock_get_page.side_effect = [
            {'campaigns': [{'id': f'c{i}'} for i in range(50)], 'totalCount': 75},
            {'campaigns': [{'id': f'c{i}'} for i in range(25)], 'totalCount': 75},
        ]
        pages = list(tap_outbrain.get_campaign_pages('acct1', 'tok'))
        self.assertEqual(len(pages), 2)
        calls = mock_get_page.call_args_list
        self.assertEqual(calls[0][0][2], 0)    # first offset = 0
        self.assertEqual(calls[1][0][2], 50)   # second offset = 50

    @patch('tap_outbrain.get_campaigns_page')
    def test_raises_when_campaign_count_exceeds_ceiling(self, mock_get_page):
        """Raises Exception when totalCount exceeds TAP_CAMPAIGN_COUNT_ERROR_CEILING."""
        mock_get_page.return_value = {
            'campaigns': [],
            'totalCount': tap_outbrain.TAP_CAMPAIGN_COUNT_ERROR_CEILING + 1,
        }
        with self.assertRaises(Exception) as ctx:
            list(tap_outbrain.get_campaign_pages('acct1', 'tok'))
        self.assertIn('more than can be retrieved', str(ctx.exception))


# ---------------------------------------------------------------------------
# sync_campaigns
# ---------------------------------------------------------------------------

class TestSyncCampaigns(unittest.TestCase):

    @patch('tap_outbrain.sync_campaign_page')
    @patch('tap_outbrain.get_campaign_pages')
    def test_iterates_all_pages(self, mock_pages, mock_sync_page):
        """sync_campaigns calls sync_campaign_page once per campaign page."""
        page1 = {'campaigns': [{'id': 'c1'}], 'totalCount': 2}
        page2 = {'campaigns': [{'id': 'c2'}], 'totalCount': 2}
        mock_pages.return_value = iter([page1, page2])

        state = {'campaign_performance': {}}
        tap_outbrain.sync_campaigns(state, 'tok', 'acct1', ['campaign', 'campaign_performance'])

        self.assertEqual(mock_sync_page.call_count, 2)

    @patch('tap_outbrain.sync_campaign_page')
    @patch('tap_outbrain.get_campaign_pages')
    def test_zero_pages(self, mock_pages, mock_sync_page):
        """sync_campaigns handles empty page iterator gracefully."""
        mock_pages.return_value = iter([])
        state = {'campaign_performance': {}}
        tap_outbrain.sync_campaigns(state, 'tok', 'acct1', ['campaign'])
        mock_sync_page.assert_not_called()
