"""Unit tests for tap_outbrain/discover.py"""

import unittest
from unittest.mock import patch

from singer.catalog import Catalog, CatalogEntry
from tap_outbrain.discover import discover


_MOCK_CAMPAIGN_SCHEMA = {
    "type": "object",
    "properties": {
        "id": {"type": ["null", "string"]},
        "name": {"type": ["null", "string"]},
    }
}

_MOCK_PERFORMANCE_SCHEMA = {
    "type": "object",
    "properties": {
        "campaignId": {"type": ["null", "string"]},
        "fromDate": {"type": ["null", "string"], "format": "date-time"},
        "impressions": {"type": ["null", "integer"]},
    }
}

_MOCK_SCHEMAS = {
    'campaign': _MOCK_CAMPAIGN_SCHEMA,
    'campaign_performance': _MOCK_PERFORMANCE_SCHEMA,
}


def _build_mock_metadata(stream_name):
    """Build a minimal metadata list for a stream (Singer format)."""
    base = [
        {
            'breadcrumb': [],
            'metadata': {
                'table-key-properties': ['id'] if stream_name == 'campaign' else ['campaignId', 'fromDate'],
                'forced-replication-method': 'FULL_TABLE' if stream_name == 'campaign' else 'INCREMENTAL',
            }
        }
    ]
    if stream_name == 'campaign_performance':
        base[0]['metadata']['parent-tap-stream-id'] = 'campaign'
    return base


class TestDiscover(unittest.TestCase):

    @patch('tap_outbrain.discover.get_schemas')
    def test_returns_catalog_instance(self, mock_get_schemas):
        """discover() returns a singer Catalog object."""
        mock_get_schemas.return_value = (
            _MOCK_SCHEMAS,
            {s: _build_mock_metadata(s) for s in _MOCK_SCHEMAS},
        )
        result = discover()
        self.assertIsInstance(result, Catalog)

    @patch('tap_outbrain.discover.get_schemas')
    def test_catalog_contains_all_streams(self, mock_get_schemas):
        """Catalog contains one entry per schema returned by get_schemas."""
        mock_get_schemas.return_value = (
            _MOCK_SCHEMAS,
            {s: _build_mock_metadata(s) for s in _MOCK_SCHEMAS},
        )
        result = discover()
        stream_names = [e.stream for e in result.streams]
        self.assertIn('campaign', stream_names)
        self.assertIn('campaign_performance', stream_names)
        self.assertEqual(len(result.streams), 2)

    @patch('tap_outbrain.discover.get_schemas')
    def test_catalog_entry_has_correct_key_properties(self, mock_get_schemas):
        """Each CatalogEntry carries the correct key_properties from metadata."""
        mock_get_schemas.return_value = (
            _MOCK_SCHEMAS,
            {s: _build_mock_metadata(s) for s in _MOCK_SCHEMAS},
        )
        result = discover()
        entry_by_name = {e.stream: e for e in result.streams}

        self.assertEqual(entry_by_name['campaign'].key_properties, ['id'])
        self.assertEqual(
            entry_by_name['campaign_performance'].key_properties,
            ['campaignId', 'fromDate'],
        )

    @patch('tap_outbrain.discover.get_schemas')
    def test_tap_stream_id_matches_stream_name(self, mock_get_schemas):
        """tap_stream_id should equal the stream name for every entry."""
        mock_get_schemas.return_value = (
            _MOCK_SCHEMAS,
            {s: _build_mock_metadata(s) for s in _MOCK_SCHEMAS},
        )
        result = discover()
        for entry in result.streams:
            self.assertEqual(entry.tap_stream_id, entry.stream)

    @patch('tap_outbrain.discover.get_schemas')
    def test_catalog_entry_schema_matches_input(self, mock_get_schemas):
        """The Schema on each CatalogEntry matches what get_schemas returned."""
        mock_get_schemas.return_value = (
            _MOCK_SCHEMAS,
            {s: _build_mock_metadata(s) for s in _MOCK_SCHEMAS},
        )
        result = discover()
        entry_by_name = {e.stream: e for e in result.streams}

        # Schema.to_dict() should equal the original schema dict
        self.assertEqual(
            entry_by_name['campaign'].schema.to_dict(), _MOCK_CAMPAIGN_SCHEMA
        )
        self.assertEqual(
            entry_by_name['campaign_performance'].schema.to_dict(),
            _MOCK_PERFORMANCE_SCHEMA,
        )

    @patch('tap_outbrain.discover.get_schemas')
    def test_catalog_entry_metadata_preserved(self, mock_get_schemas):
        """Metadata list from get_schemas is stored on each CatalogEntry."""
        field_metadata = {s: _build_mock_metadata(s) for s in _MOCK_SCHEMAS}
        mock_get_schemas.return_value = (_MOCK_SCHEMAS, field_metadata)

        result = discover()
        entry_by_name = {e.stream: e for e in result.streams}

        self.assertEqual(
            entry_by_name['campaign_performance'].metadata,
            field_metadata['campaign_performance'],
        )

    @patch('tap_outbrain.discover.get_schemas')
    def test_discover_with_single_stream(self, mock_get_schemas):
        """discover() handles a catalog with only one stream."""
        single_schema = {'campaign': _MOCK_CAMPAIGN_SCHEMA}
        mock_get_schemas.return_value = (
            single_schema,
            {'campaign': _build_mock_metadata('campaign')},
        )
        result = discover()
        self.assertEqual(len(result.streams), 1)
        self.assertEqual(result.streams[0].stream, 'campaign')

    @patch('tap_outbrain.discover.get_schemas')
    def test_discover_propagates_get_schemas_exception(self, mock_get_schemas):
        """If get_schemas raises, discover() propagates the exception."""
        mock_get_schemas.side_effect = RuntimeError("Schema load failed")
        with self.assertRaises(RuntimeError):
            discover()


    @patch('tap_outbrain.discover.get_schemas')
    def test_discover_logs_and_reraises_schema_error(self, mock_get_schemas):
        """When Schema.from_dict fails, discover() re-raises the exception."""
        # Provide a schema that is valid dict but will fail Schema.from_dict indirectly
        bad_schemas = {'campaign': None}  # None will cause AttributeError in Schema.from_dict
        mock_get_schemas.return_value = (
            bad_schemas,
            {'campaign': _build_mock_metadata('campaign')},
        )
        with self.assertRaises(Exception):
            discover()
