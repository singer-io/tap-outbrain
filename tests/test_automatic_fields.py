"""Mock integration tests — verify automatic (primary-key + replication-key)
fields are always present in synced records.
"""

from singer import metadata

from .base import OutbrainBaseTest

from tap_outbrain.discover import discover


class OutbrainAutomaticFieldsTest(OutbrainBaseTest):
    """Primary-key and replication-key fields must always be replicated."""

    @staticmethod
    def name():
        return "tap_outbrain_automatic_fields_test"

    def _get_catalog(self):
        return self._make_selected_catalog()

    def _get_automatic_fields(self, stream_name: str) -> set:
        """Return the set of field names that have 'automatic' inclusion for a stream."""
        # Use raw catalog (not selected) for metadata inspection
        catalog = discover()
        stream = next(s for s in catalog.streams if s.tap_stream_id == stream_name)
        mdata_map = metadata.to_map(stream.metadata)
        pk = set(metadata.get(mdata_map, (), "table-key-properties") or [])
        # Replication keys are exposed via Singer metadata (valid-replication-keys).
        # The value may be a bare string or a list, so normalise to a set.
        rk_val = metadata.get(mdata_map, (), "valid-replication-keys") or []
        rk = set([rk_val] if isinstance(rk_val, str) else rk_val)
        return pk | rk

    def test_campaign_automatic_fields_are_correct(self):
        """campaign automatic fields = {id} (key only, no replication key)."""
        auto = self._get_automatic_fields("campaign")
        self.assertEqual(auto, {"id"})

    def test_campaign_performance_automatic_fields_are_correct(self):
        """campaign_performance automatic fields = {campaignId, fromDate}."""
        auto = self._get_automatic_fields("campaign_performance")
        self.assertEqual(auto, {"campaignId", "fromDate"})

    def test_campaign_key_has_automatic_inclusion_metadata(self):
        """'id' in campaign must carry inclusion=automatic in Singer metadata."""
        catalog = discover()
        campaign = next(s for s in catalog.streams if s.tap_stream_id == "campaign")
        mdata_map = metadata.to_map(campaign.metadata)
        self.assertEqual(
            metadata.get(mdata_map, ("properties", "id"), "inclusion"),
            "automatic",
        )

    def test_campaign_performance_keys_have_automatic_inclusion_metadata(self):
        """campaignId and fromDate in campaign_performance must be 'automatic'."""
        catalog = discover()
        cp = next(s for s in catalog.streams if s.tap_stream_id == "campaign_performance")
        mdata_map = metadata.to_map(cp.metadata)
        for field in ("campaignId", "fromDate"):
            with self.subTest(field=field):
                self.assertEqual(
                    metadata.get(mdata_map, ("properties", field), "inclusion"),
                    "automatic",
                    msg=f"{field} should have inclusion=automatic",
                )

    def test_non_key_campaign_fields_have_available_inclusion(self):
        """Non-key campaign fields (name, budget, cpc, …) must be 'available'."""
        catalog = discover()
        campaign = next(s for s in catalog.streams if s.tap_stream_id == "campaign")
        mdata_map = metadata.to_map(campaign.metadata)
        for field in ("name", "campaignOnAir", "enabled", "cpc"):
            with self.subTest(field=field):
                self.assertEqual(
                    metadata.get(mdata_map, ("properties", field), "inclusion"),
                    "available",
                )

    def test_non_key_performance_fields_have_available_inclusion(self):
        """Non-key performance fields (impressions, clicks, …) must be 'available'."""
        catalog = discover()
        cp = next(s for s in catalog.streams if s.tap_stream_id == "campaign_performance")
        mdata_map = metadata.to_map(cp.metadata)
        for field in ("impressions", "clicks", "spend", "ctr", "cpa"):
            with self.subTest(field=field):
                self.assertEqual(
                    metadata.get(mdata_map, ("properties", field), "inclusion"),
                    "available",
                )

    def test_campaign_id_always_in_campaign_records(self):
        """The primary key 'id' must be present in every campaign record."""
        campaigns = [self.make_campaign_record("c001"), self.make_campaign_record("c002")]
        captured, _ = self._run_mock_sync(campaigns=campaigns)
        records = captured["records"].get("campaign", [])
        self.assertGreater(len(records), 0, "No campaign records emitted")
        for record in records:
            with self.subTest(record_id=record.get("id")):
                self.assertIn("id", record)
                self.assertIsNotNone(record["id"])

    def test_campaign_performance_keys_always_in_records(self):
        """campaignId and fromDate must appear in every performance record."""
        c_id = "c001"
        campaigns = [self.make_campaign_record(c_id)]
        perf = {c_id: [self.make_performance_record(c_id, "2024-05-01")]}
        captured, _ = self._run_mock_sync(campaigns=campaigns, perf_records_by_campaign=perf)
        records = captured["records"].get("campaign_performance", [])
        self.assertGreater(len(records), 0, "No performance records emitted")
        for record in records:
            with self.subTest(campaign=record.get("campaignId")):
                self.assertIn("campaignId", record)
                self.assertIn("fromDate", record)
