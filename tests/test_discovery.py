import unittest
from base import OutbrainBaseTest

try:
    from tap_tester.base_suite_tests.discovery_test import DiscoveryTest as TT_DiscoveryTest

    class DiscoveryTest(TT_DiscoveryTest, OutbrainBaseTest):
        """Tap-outbrain discovery test using the standard tap-tester suite."""

        @staticmethod
        def name():
            return "tap_outbrain_discovery_test"

        def streams_to_test(self):
            return self.expected_stream_names()

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

except ImportError:
    @unittest.skip("tap_tester not available; use mock_integration tests instead")
    class DiscoveryTest(unittest.TestCase):
        pass
