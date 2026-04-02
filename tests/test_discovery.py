"""Catalog structure tests for tap-outbrain stream discovery.

Calls tap_outbrain.discover.discover() directly — no HTTP calls required
because discovery only reads local schema JSON files.  These tests are
valid in both live and mock environments.
"""

from singer import metadata

try:
    from base import OutbrainBaseTest
except ImportError:
    from tests.base import OutbrainBaseTest

from tap_outbrain.discover import discover


class OutbrainDiscoveryTest(OutbrainBaseTest):
    """Verify discover() returns the correct catalog without any API calls."""

    def _catalog(self):
        return discover()

    def test_discovery_returns_expected_streams(self):
        """All expected streams are present in the catalog."""
        catalog = self._catalog()
        discovered = {s.tap_stream_id for s in catalog.streams}
        self.assertEqual(discovered, self.expected_stream_names())

    def test_discovery_stream_count(self):
        """Exactly 2 streams are discovered."""
        catalog = self._catalog()
        self.assertEqual(len(catalog.streams), 2)

    def test_stream_names_follow_naming_convention(self):
        """Stream names contain only lowercase letters and underscores."""
        catalog = self._catalog()
        for stream in catalog.streams:
            with self.subTest(stream=stream.tap_stream_id):
                self.assertRegex(
                    stream.tap_stream_id,
                    r"^[a-z_]+$",
                    msg=f"Stream '{stream.tap_stream_id}' violates naming convention",
                )

    def test_tap_stream_id_matches_stream_name(self):
        """tap_stream_id should match the stream field on every entry."""
        catalog = self._catalog()
        for entry in catalog.streams:
            with self.subTest(stream=entry.tap_stream_id):
                self.assertEqual(entry.tap_stream_id, entry.stream)

    def test_discovery_primary_keys(self):
        """key_properties match expected for every discovered stream."""
        catalog = self._catalog()
        expected = self.expected_primary_keys()
        for stream in catalog.streams:
            with self.subTest(stream=stream.tap_stream_id):
                self.assertEqual(
                    set(stream.key_properties or []),
                    expected[stream.tap_stream_id],
                )

    def test_discovery_schema_has_properties(self):
        """Every stream schema must expose at least one property."""
        catalog = self._catalog()
        for stream in catalog.streams:
            with self.subTest(stream=stream.tap_stream_id):
                schema_dict = stream.schema.to_dict()
                self.assertIn("properties", schema_dict)
                self.assertGreater(len(schema_dict["properties"]), 0)

    def test_campaign_schema_has_expected_fields(self):
        """campaign stream schema includes id, name, enabled, budget, cpc."""
        catalog = self._catalog()
        campaign = next(s for s in catalog.streams if s.tap_stream_id == "campaign")
        props = campaign.schema.to_dict()["properties"]
        for field in ("id", "name", "enabled", "budget", "cpc"):
            with self.subTest(field=field):
                self.assertIn(field, props)

    def test_campaign_performance_schema_has_expected_fields(self):
        """campaign_performance schema includes all metric fields."""
        catalog = self._catalog()
        cp = next(s for s in catalog.streams if s.tap_stream_id == "campaign_performance")
        props = cp.schema.to_dict()["properties"]
        for field in ("campaignId", "fromDate", "impressions", "clicks", "spend"):
            with self.subTest(field=field):
                self.assertIn(field, props)

    def test_discovery_replication_method(self):
        """forced-replication-method matches expected for every stream."""
        catalog = self._catalog()
        expected = self.expected_replication_method()
        for stream in catalog.streams:
            with self.subTest(stream=stream.tap_stream_id):
                mdata = metadata.to_map(stream.metadata)
                actual = metadata.get(mdata, (), "forced-replication-method")
                self.assertEqual(actual, expected[stream.tap_stream_id])

    def test_discovery_replication_keys_for_incremental_streams(self):
        """INCREMENTAL streams must have valid-replication-keys metadata."""
        catalog = self._catalog()
        for stream in catalog.streams:
            mdata_map = metadata.to_map(stream.metadata)
            method = metadata.get(mdata_map, (), "forced-replication-method")
            if method != self.INCREMENTAL:
                continue
            with self.subTest(stream=stream.tap_stream_id):
                rep_keys = metadata.get(mdata_map, (), "valid-replication-keys") or []
                self.assertGreater(
                    len(rep_keys), 0,
                    msg=f"{stream.tap_stream_id} misses valid-replication-keys",
                )

    def test_full_table_streams_have_no_replication_keys(self):
        """FULL_TABLE streams must NOT have valid-replication-keys set."""
        catalog = self._catalog()
        for stream in catalog.streams:
            mdata_map = metadata.to_map(stream.metadata)
            method = metadata.get(mdata_map, (), "forced-replication-method")
            if method != self.FULL_TABLE:
                continue
            with self.subTest(stream=stream.tap_stream_id):
                rep_keys = metadata.get(mdata_map, (), "valid-replication-keys") or []
                self.assertEqual(
                    rep_keys, [],
                    msg=f"{stream.tap_stream_id} should not have replication keys",
                )

    def test_single_top_level_breadcrumb_per_stream(self):
        """Each stream must have exactly one top-level metadata breadcrumb (breadcrumb=())."""
        catalog = self._catalog()
        for stream in catalog.streams:
            with self.subTest(stream=stream.tap_stream_id):
                # Singer stores the root breadcrumb as an empty tuple ()
                top_level = [
                    m for m in stream.metadata if not m.get("breadcrumb")
                ]
                self.assertEqual(
                    len(top_level), 1,
                    msg=f"{stream.tap_stream_id} has {len(top_level)} top-level breadcrumbs (expected 1)",
                )

    def test_key_properties_have_automatic_inclusion(self):
        """Primary key and replication key fields have 'automatic' inclusion."""
        catalog = self._catalog()
        for stream in catalog.streams:
            mdata_map = metadata.to_map(stream.metadata)
            pk_fields = metadata.get(mdata_map, (), "table-key-properties") or []
            for field in pk_fields:
                with self.subTest(stream=stream.tap_stream_id, field=field):
                    inclusion = metadata.get(mdata_map, ("properties", field), "inclusion")
                    self.assertEqual(
                        inclusion, "automatic",
                        msg=f"{stream.tap_stream_id}.{field} should be 'automatic'",
                    )

    def test_non_key_fields_have_available_inclusion(self):
        """Non-key, non-replication fields have 'available' inclusion."""
        catalog = self._catalog()
        for stream in catalog.streams:
            mdata_map = metadata.to_map(stream.metadata)
            pk_fields = set(metadata.get(mdata_map, (), "table-key-properties") or [])
            rep_keys = set(metadata.get(mdata_map, (), "valid-replication-keys") or [])
            auto_fields = pk_fields | rep_keys
            schema_props = stream.schema.to_dict().get("properties", {})
            for field in schema_props:
                if field in auto_fields:
                    continue
                with self.subTest(stream=stream.tap_stream_id, field=field):
                    inclusion = metadata.get(mdata_map, ("properties", field), "inclusion")
                    self.assertEqual(
                        inclusion, "available",
                        msg=f"{stream.tap_stream_id}.{field} should be 'available'",
                    )

    def test_campaign_performance_has_parent_tap_stream_id(self):
        """campaign_performance must have parent-tap-stream-id == 'campaign'."""
        catalog = self._catalog()
        cp = next(s for s in catalog.streams if s.tap_stream_id == "campaign_performance")
        mdata_map = metadata.to_map(cp.metadata)
        parent = metadata.get(mdata_map, (), "parent-tap-stream-id")
        self.assertEqual(parent, "campaign")

    def test_campaign_has_no_parent_tap_stream_id(self):
        """campaign (parent stream) must NOT have parent-tap-stream-id metadata."""
        catalog = self._catalog()
        c = next(s for s in catalog.streams if s.tap_stream_id == "campaign")
        mdata_map = metadata.to_map(c.metadata)
        parent = metadata.get(mdata_map, (), "parent-tap-stream-id")
        self.assertIsNone(parent)
