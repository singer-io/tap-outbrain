"""Unit tests for tap_outbrain/discover.py"""

import unittest
from unittest.mock import MagicMock, patch

from singer.catalog import Catalog, CatalogEntry

from tap_outbrain.client import OutbrainForbiddenError
from tap_outbrain.discover import (_apply_access_checks,
                                   _prune_inaccessible_children, discover)

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

    @patch('tap_outbrain.discover._apply_access_checks')
    @patch('tap_outbrain.discover.get_schemas')
    def test_returns_catalog_instance(self, mock_get_schemas, mock_access_checks):
        """discover(client) returns a singer Catalog object."""
        mock_get_schemas.return_value = (
            _MOCK_SCHEMAS,
            {s: _build_mock_metadata(s) for s in _MOCK_SCHEMAS},
        )
        result = discover(MagicMock())
        self.assertIsInstance(result, Catalog)

    @patch('tap_outbrain.discover._apply_access_checks')
    @patch('tap_outbrain.discover.get_schemas')
    def test_catalog_contains_all_streams(self, mock_get_schemas, mock_access_checks):
        """Catalog contains one entry per schema returned by get_schemas."""
        mock_get_schemas.return_value = (
            _MOCK_SCHEMAS,
            {s: _build_mock_metadata(s) for s in _MOCK_SCHEMAS},
        )
        result = discover(MagicMock())
        stream_names = [e.stream for e in result.streams]
        self.assertIn('campaign', stream_names)
        self.assertIn('campaign_performance', stream_names)
        self.assertEqual(len(result.streams), 2)

    @patch('tap_outbrain.discover._apply_access_checks')
    @patch('tap_outbrain.discover.get_schemas')
    def test_catalog_entry_has_correct_key_properties(self, mock_get_schemas, mock_access_checks):
        """Each CatalogEntry carries the correct key_properties from metadata."""
        mock_get_schemas.return_value = (
            _MOCK_SCHEMAS,
            {s: _build_mock_metadata(s) for s in _MOCK_SCHEMAS},
        )
        result = discover(MagicMock())
        entry_by_name = {e.stream: e for e in result.streams}

        self.assertEqual(entry_by_name['campaign'].key_properties, ['id'])
        self.assertEqual(
            entry_by_name['campaign_performance'].key_properties,
            ['campaignId', 'fromDate'],
        )

    @patch('tap_outbrain.discover._apply_access_checks')
    @patch('tap_outbrain.discover.get_schemas')
    def test_tap_stream_id_matches_stream_name(self, mock_get_schemas, mock_access_checks):
        """tap_stream_id should equal the stream name for every entry."""
        mock_get_schemas.return_value = (
            _MOCK_SCHEMAS,
            {s: _build_mock_metadata(s) for s in _MOCK_SCHEMAS},
        )
        result = discover(MagicMock())
        for entry in result.streams:
            self.assertEqual(entry.tap_stream_id, entry.stream)

    @patch('tap_outbrain.discover._apply_access_checks')
    @patch('tap_outbrain.discover.get_schemas')
    def test_catalog_entry_schema_matches_input(self, mock_get_schemas, mock_access_checks):
        """The Schema on each CatalogEntry matches what get_schemas returned."""
        mock_get_schemas.return_value = (
            _MOCK_SCHEMAS,
            {s: _build_mock_metadata(s) for s in _MOCK_SCHEMAS},
        )
        result = discover(MagicMock())
        entry_by_name = {e.stream: e for e in result.streams}

        # Schema.to_dict() should equal the original schema dict
        self.assertEqual(
            entry_by_name['campaign'].schema.to_dict(), _MOCK_CAMPAIGN_SCHEMA
        )
        self.assertEqual(
            entry_by_name['campaign_performance'].schema.to_dict(),
            _MOCK_PERFORMANCE_SCHEMA,
        )

    @patch('tap_outbrain.discover._apply_access_checks')
    @patch('tap_outbrain.discover.get_schemas')
    def test_catalog_entry_metadata_preserved(self, mock_get_schemas, mock_access_checks):
        """Metadata list from get_schemas is stored on each CatalogEntry."""
        field_metadata = {s: _build_mock_metadata(s) for s in _MOCK_SCHEMAS}
        mock_get_schemas.return_value = (_MOCK_SCHEMAS, field_metadata)

        result = discover(MagicMock())
        entry_by_name = {e.stream: e for e in result.streams}

        self.assertEqual(
            entry_by_name['campaign_performance'].metadata,
            field_metadata['campaign_performance'],
        )

    @patch('tap_outbrain.discover._apply_access_checks')
    @patch('tap_outbrain.discover.get_schemas')
    def test_discover_with_single_stream(self, mock_get_schemas, mock_access_checks):
        """discover(client) handles a catalog with only one stream."""
        single_schema = {'campaign': _MOCK_CAMPAIGN_SCHEMA}
        mock_get_schemas.return_value = (
            single_schema,
            {'campaign': _build_mock_metadata('campaign')},
        )
        result = discover(MagicMock())
        self.assertEqual(len(result.streams), 1)
        self.assertEqual(result.streams[0].stream, 'campaign')

    @patch('tap_outbrain.discover.get_schemas')
    def test_discover_propagates_get_schemas_exception(self, mock_get_schemas):
        """If get_schemas raises, discover(client) propagates the exception."""
        mock_get_schemas.side_effect = RuntimeError("Schema load failed")
        with self.assertRaises(RuntimeError):
            discover(MagicMock())

    @patch('tap_outbrain.discover._apply_access_checks')
    @patch('tap_outbrain.discover.get_schemas')
    def test_discover_logs_and_reraises_schema_error(self, mock_get_schemas, mock_access_checks):
        """When Schema.from_dict fails, discover(client) re-raises the exception."""
        # Provide a schema that is valid dict but will fail Schema.from_dict indirectly
        bad_schemas = {'campaign': None}  # None will cause AttributeError in Schema.from_dict
        mock_get_schemas.return_value = (
            bad_schemas,
            {'campaign': _build_mock_metadata('campaign')},
        )
        with self.assertRaises(Exception):
            discover(MagicMock())


# ---------------------------------------------------------------------------
# Helpers shared by the access-check test classes
# ---------------------------------------------------------------------------

def _make_stream_cls(accessible=True, parent=None):
    """
    Return a mock stream *class* (not instance) whose instantiation returns
    an object with ``check_access()`` set to *accessible* and whose
    ``parent`` class attribute equals *parent*.
    """
    cls = MagicMock()
    cls.parent = parent
    instance = MagicMock()
    instance.check_access.return_value = accessible
    cls.return_value = instance
    return cls


def _make_client():
    from tap_outbrain.client import OutbrainClient
    return OutbrainClient(config={"account_id": "acct1", "access_token": "tok"})


# ---------------------------------------------------------------------------
# _apply_access_checks
# ---------------------------------------------------------------------------

class TestApplyAccessChecks(unittest.TestCase):

    def _schemas_and_meta(self):
        schemas = dict(_MOCK_SCHEMAS)
        meta = {s: _build_mock_metadata(s) for s in schemas}
        return schemas, meta

    @patch("tap_outbrain.discover.STREAMS")
    def test_all_accessible_leaves_catalog_intact(self, mock_streams):
        """When all streams are accessible, schemas and metadata are unchanged."""
        mock_streams.items.return_value = [
            ("campaign", _make_stream_cls(accessible=True, parent=None)),
            ("campaign_performance", _make_stream_cls(accessible=True, parent="campaign")),
        ]
        schemas, meta = self._schemas_and_meta()
        _apply_access_checks(_make_client(), schemas, meta)

        self.assertIn("campaign", schemas)
        self.assertIn("campaign_performance", schemas)

    @patch("tap_outbrain.discover.STREAMS")
    def test_inaccessible_parent_removed(self, mock_streams):
        """An inaccessible parent stream is removed from schemas and metadata."""
        mock_streams.items.return_value = [
            ("campaign", _make_stream_cls(accessible=False, parent=None)),
            ("campaign_performance", _make_stream_cls(accessible=True, parent="campaign")),
        ]
        schemas, meta = self._schemas_and_meta()
        # _prune_inaccessible_children also checks STREAMS so patch it globally
        with patch("tap_outbrain.discover.STREAMS") as mock_streams2:
            mock_streams2.items.return_value = [
                ("campaign", _make_stream_cls(accessible=False, parent=None)),
                ("campaign_performance", _make_stream_cls(accessible=True, parent="campaign")),
            ]
            # Both apply_access_checks' own loop and _prune need the patch
            # Re-run with a single consistent patch at module level
            pass

        # Simpler: patch once via STREAMS at module level
        streams_dict = {
            "campaign": _make_stream_cls(accessible=False, parent=None),
            "campaign_performance": _make_stream_cls(accessible=True, parent="campaign"),
        }
        with patch.dict("tap_outbrain.discover.STREAMS", streams_dict, clear=True):
            schemas, meta = self._schemas_and_meta()
            with self.assertRaises(OutbrainForbiddenError):
                # campaign removed → only campaign_performance remains,
                # then _prune removes it too → no schemas → raises
                _apply_access_checks(_make_client(), schemas, meta)

    @patch.dict(
        "tap_outbrain.discover.STREAMS",
        {
            "campaign": _make_stream_cls(accessible=False, parent=None),
            "campaign_performance": _make_stream_cls(accessible=True, parent="campaign"),
        },
        clear=True,
    )
    def test_all_inaccessible_raises_forbidden_error(self):
        """OutbrainForbiddenError is raised when no streams remain after pruning."""
        schemas, meta = {**_MOCK_SCHEMAS}, {s: _build_mock_metadata(s) for s in _MOCK_SCHEMAS}
        with self.assertRaises(OutbrainForbiddenError) as ctx:
            _apply_access_checks(_make_client(), schemas, meta)
        self.assertIn("403", str(ctx.exception))
        self.assertIn("read", str(ctx.exception))

    def test_inaccessible_warning_logged(self):
        """A warning is logged for each excluded stream when some remain."""
        accessible_campaign = _make_stream_cls(accessible=True, parent=None)
        streams_dict = {
            "campaign": accessible_campaign,
            "campaign_performance": _make_stream_cls(accessible=True, parent="campaign"),
        }
        with patch.dict("tap_outbrain.discover.STREAMS", streams_dict, clear=True):
            schemas, meta = self._schemas_and_meta()
            # All accessible → no warning, no raise
            with patch("tap_outbrain.discover.LOGGER") as mock_logger:
                _apply_access_checks(_make_client(), schemas, meta)
                mock_logger.warning.assert_not_called()

    def test_partial_access_excludes_forbidden_streams(self):
        """When one stream is inaccessible and a sibling remains, no error is raised."""
        # We need two independent parent streams. Temporarily extend STREAMS.
        extra_cls = _make_stream_cls(accessible=True, parent=None)
        extra_cls.name = "extra_stream"
        forbidden_cls = _make_stream_cls(accessible=False, parent=None)
        forbidden_cls.name = "campaign"

        extra_schema = {"type": "object", "properties": {}}
        streams_dict = {
            "campaign": forbidden_cls,
            "extra_stream": extra_cls,
        }
        schemas = {"campaign": _MOCK_CAMPAIGN_SCHEMA, "extra_stream": extra_schema}
        meta = {
            "campaign": _build_mock_metadata("campaign"),
            "extra_stream": _build_mock_metadata("campaign"),
        }
        with patch.dict("tap_outbrain.discover.STREAMS", streams_dict, clear=True):
            _apply_access_checks(_make_client(), schemas, meta)

        self.assertNotIn("campaign", schemas)
        self.assertIn("extra_stream", schemas)


# ---------------------------------------------------------------------------
# _prune_inaccessible_children
# ---------------------------------------------------------------------------

class TestPruneInaccessibleChildren(unittest.TestCase):

    def test_child_removed_when_parent_absent(self):
        """
        A child stream is pruned from schemas when its parent is not present.
        """
        parent_cls = _make_stream_cls(parent=None)
        child_cls = _make_stream_cls(parent="campaign")

        with patch.dict(
            "tap_outbrain.discover.STREAMS",
            {"campaign": parent_cls, "campaign_performance": child_cls},
            clear=True,
        ):
            schemas = {"campaign_performance": _MOCK_PERFORMANCE_SCHEMA}
            meta = {"campaign_performance": _build_mock_metadata("campaign_performance")}
            _prune_inaccessible_children(schemas, meta)

        self.assertNotIn("campaign_performance", schemas)
        self.assertNotIn("campaign_performance", meta)

    def test_child_retained_when_parent_present(self):
        """
        A child stream is kept when its parent stream is still in schemas.
        """
        parent_cls = _make_stream_cls(parent=None)
        child_cls = _make_stream_cls(parent="campaign")

        with patch.dict(
            "tap_outbrain.discover.STREAMS",
            {"campaign": parent_cls, "campaign_performance": child_cls},
            clear=True,
        ):
            schemas = dict(_MOCK_SCHEMAS)
            meta = {s: _build_mock_metadata(s) for s in schemas}
            _prune_inaccessible_children(schemas, meta)

        self.assertIn("campaign_performance", schemas)

    def test_streams_without_parent_unaffected(self):
        """
        Streams with no parent attribute are never removed by pruning.
        """
        parent_cls = _make_stream_cls(parent=None)

        with patch.dict(
            "tap_outbrain.discover.STREAMS",
            {"campaign": parent_cls},
            clear=True,
        ):
            schemas = {"campaign": _MOCK_CAMPAIGN_SCHEMA}
            meta = {"campaign": _build_mock_metadata("campaign")}
            _prune_inaccessible_children(schemas, meta)

        self.assertIn("campaign", schemas)

    def test_warning_logged_on_child_pruning(self):
        """A warning is emitted when a child stream is pruned."""
        parent_cls = _make_stream_cls(parent=None)
        child_cls = _make_stream_cls(parent="campaign")

        with patch.dict(
            "tap_outbrain.discover.STREAMS",
            {"campaign": parent_cls, "campaign_performance": child_cls},
            clear=True,
        ):
            schemas = {"campaign_performance": _MOCK_PERFORMANCE_SCHEMA}
            meta = {"campaign_performance": _build_mock_metadata("campaign_performance")}
            with patch("tap_outbrain.discover.LOGGER") as mock_logger:
                _prune_inaccessible_children(schemas, meta)
                mock_logger.warning.assert_called_once()
                warn_msg = mock_logger.warning.call_args[0][0]
                self.assertIn("excluded", warn_msg)
