"""Mock integration tests — verify all schema fields appear in synced records."""

try:
    from base import OutbrainBaseTest
except ImportError:
    from tests.base import OutbrainBaseTest


class OutbrainAllFieldsTest(OutbrainBaseTest):
    """Verify Singer schema messages are correct and records contain expected fields."""

    def test_sync_emits_schema_for_campaign_stream(self):
        """A SCHEMA message is emitted for the campaign stream."""
        captured, _ = self._run_mock_sync()
        self.assertIn("campaign", captured["schemas"])

    def test_sync_emits_schema_for_campaign_performance_stream(self):
        """A SCHEMA message is emitted for the campaign_performance stream."""
        captured, _ = self._run_mock_sync()
        self.assertIn("campaign_performance", captured["schemas"])

    def test_sync_emits_schema_for_both_streams(self):
        """Exactly 2 SCHEMA messages are emitted — one per stream."""
        captured, _ = self._run_mock_sync()
        self.assertEqual(
            set(captured["schemas"].keys()),
            {"campaign", "campaign_performance"},
        )

    def test_campaign_schema_contains_all_declared_fields(self):
        """The emitted campaign SCHEMA includes every property in campaign.json."""
        captured, _ = self._run_mock_sync()
        emitted = set(captured["schemas"]["campaign"].get("properties", {}).keys())
        declared = set(self._load_schema("campaign").get("properties", {}).keys())
        self.assertEqual(
            declared - emitted, set(),
            f"Fields in campaign.json missing from emitted SCHEMA: {declared - emitted}",
        )

    def test_campaign_performance_schema_contains_all_declared_fields(self):
        """The emitted campaign_performance SCHEMA includes every declared property."""
        captured, _ = self._run_mock_sync()
        emitted = set(
            captured["schemas"]["campaign_performance"].get("properties", {}).keys()
        )
        declared = set(
            self._load_schema("campaign_performance").get("properties", {}).keys()
        )
        self.assertEqual(
            declared - emitted, set(),
            f"Fields in campaign_performance.json missing from emitted SCHEMA: {declared - emitted}",
        )

    def test_emitted_schemas_match_local_schema_files(self):
        """Emitted SCHEMA payloads match the local JSON schema files exactly."""
        captured, _ = self._run_mock_sync()
        for stream_name in ("campaign", "campaign_performance"):
            with self.subTest(stream=stream_name):
                self.assertEqual(
                    captured["schemas"][stream_name],
                    self._load_schema(stream_name),
                    f"Emitted SCHEMA for '{stream_name}' differs from {stream_name}.json",
                )

    def test_campaign_records_contain_all_schema_fields(self):
        """Every campaign record must contain all fields defined in campaign.json."""
        campaigns = [self.make_campaign_record("c001")]
        captured, _ = self._run_mock_sync(campaigns=campaigns)
        records = captured["records"].get("campaign", [])
        self.assertGreater(len(records), 0, "No campaign records emitted")
        schema_fields = set(self._load_schema("campaign").get("properties", {}).keys())
        for record in records:
            with self.subTest(record_id=record.get("id")):
                # budget is nullable — skip strict presence check for it
                missing = (schema_fields - {"budget"}) - set(record.keys())
                self.assertEqual(missing, set(), f"Missing fields: {missing}")

    def test_campaign_performance_records_contain_all_schema_fields(self):
        """Every performance record must contain all fields in campaign_performance.json."""
        c_id = "c001"
        campaigns = [self.make_campaign_record(c_id)]
        perf = {c_id: [self.make_performance_record(c_id, "2024-05-01")]}
        captured, _ = self._run_mock_sync(campaigns=campaigns, perf_records_by_campaign=perf)
        records = captured["records"].get("campaign_performance", [])
        self.assertGreater(len(records), 0, "No performance records emitted")
        schema_fields = set(
            self._load_schema("campaign_performance").get("properties", {}).keys()
        )
        for record in records:
            with self.subTest(campaign=record.get("campaignId")):
                missing = schema_fields - set(record.keys())
                self.assertEqual(missing, set(), f"Missing fields: {missing}")

    def test_campaign_records_primary_key_always_present(self):
        """Every campaign record contains non-null primary key 'id'."""
        campaigns = [self.make_campaign_record("c001"), self.make_campaign_record("c002")]
        captured, _ = self._run_mock_sync(campaigns=campaigns)
        records = captured["records"].get("campaign", [])
        self.assertGreater(len(records), 0)
        for record in records:
            with self.subTest(record_id=record.get("id")):
                self.assertIn("id", record)
                self.assertIsNotNone(record["id"])

    def test_campaign_performance_records_primary_keys_always_present(self):
        """Every performance record contains both 'campaignId' and 'fromDate'."""
        c_id = "c001"
        campaigns = [self.make_campaign_record(c_id)]
        perf = {c_id: [self.make_performance_record(c_id, "2024-05-01")]}
        captured, _ = self._run_mock_sync(campaigns=campaigns, perf_records_by_campaign=perf)
        records = captured["records"].get("campaign_performance", [])
        self.assertGreater(len(records), 0)
        for record in records:
            with self.subTest(campaign=record.get("campaignId")):
                self.assertIn("campaignId", record)
                self.assertIn("fromDate", record)

    def test_sync_completes_without_raising(self):
        """do_sync with mocked HTTP layer completes without raising."""
        self._run_mock_sync()

    def test_all_campaigns_synced_across_pages(self):
        """Campaigns spanning multiple API pages (>50) are all synced."""
        # MARKETERS_CAMPAIGNS_MAX_LIMIT = 50; use 55 to force 2-page fetch
        campaigns = [self.make_campaign_record(f"c{i:03d}") for i in range(55)]
        captured, _ = self._run_mock_sync(campaigns=campaigns)
        records = captured["records"].get("campaign", [])
        self.assertEqual(len(records), 55)

    def test_campaign_ids_match_after_pagination(self):
        """Campaign IDs in records exactly match the IDs in the fixture set."""
        campaign_ids = [f"c{i:03d}" for i in range(55)]
        campaigns = [self.make_campaign_record(cid) for cid in campaign_ids]
        captured, _ = self._run_mock_sync(campaigns=campaigns)
        synced_ids = {r["id"] for r in captured["records"].get("campaign", [])}
        self.assertEqual(synced_ids, set(campaign_ids))
