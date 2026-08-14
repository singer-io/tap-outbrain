"""Unit tests for tap_outbrain/streams.py"""

import unittest
from unittest.mock import MagicMock, patch

from tap_outbrain.client import OUTBRAIN_API_BASE, OutbrainForbiddenError
from tap_outbrain.streams import (
    BaseStream,
    Campaign,
    CampaignPerformance,
    STREAMS,
)


class TestBaseStreamInit(unittest.TestCase):
    """Test BaseStream.__init__ method."""

    def test_init_with_client(self):
        """BaseStream.__init__ sets self.client to provided client."""
        mock_client = MagicMock()
        stream = BaseStream(client=mock_client)
        self.assertIs(stream.client, mock_client)

    def test_init_without_client(self):
        """BaseStream.__init__ with no client sets self.client to None."""
        stream = BaseStream()
        self.assertIsNone(stream.client)

    def test_class_attributes(self):
        """BaseStream has correct class-level attributes."""
        self.assertIsNone(BaseStream.name)
        self.assertEqual(BaseStream.key_properties, [])
        self.assertIsNone(BaseStream.replication_keys)
        self.assertIsNone(BaseStream.replication_method)
        self.assertIsNone(BaseStream.parent)


class TestBaseStreamGetProbeUrl(unittest.TestCase):
    """Test BaseStream.get_probe_url method."""

    def test_get_probe_url_not_implemented(self):
        """BaseStream.get_probe_url raises NotImplementedError."""
        stream = BaseStream()
        with self.assertRaises(NotImplementedError) as cm:
            stream.get_probe_url()
        self.assertIn("BaseStream", str(cm.exception))
        self.assertIn("get_probe_url", str(cm.exception))


class TestBaseStreamCheckAccess(unittest.TestCase):
    """Test BaseStream.check_access method."""

    def test_check_access_child_stream_returns_true(self):
        """Child streams (with parent set) return True immediately."""
        # Create a subclass that sets parent and implement get_probe_url
        class ChildStream(BaseStream):
            parent = "some_parent"
            def get_probe_url(self):
                return "http://example.com"

        stream = ChildStream(client=MagicMock())
        # Should return True without calling client
        result = stream.check_access()
        self.assertTrue(result)

    def test_check_access_parent_stream_success(self):
        """Parent stream returns True when API request succeeds."""
        mock_client = MagicMock()
        mock_client.config = {"access_token": "test_token"}
        mock_client.make_request.return_value = MagicMock()

        class ParentStream(BaseStream):
            def get_probe_url(self):
                return "http://example.com/test"

        stream = ParentStream(client=mock_client)
        result = stream.check_access()

        self.assertTrue(result)
        mock_client.make_request.assert_called_once_with(
            "GET",
            "http://example.com/test",
            headers={"OB-TOKEN-V1": "test_token"},
            params={"limit": 1}
        )

    def test_check_access_parent_stream_forbidden(self):
        """Parent stream returns False when API responds with 403 Forbidden."""
        mock_client = MagicMock()
        mock_client.config = {"access_token": "test_token"}
        mock_client.make_request.side_effect = OutbrainForbiddenError("403 Forbidden")

        class ParentStream(BaseStream):
            def get_probe_url(self):
                return "http://example.com/test"

        stream = ParentStream(client=mock_client)
        result = stream.check_access()

        self.assertFalse(result)

    @patch('tap_outbrain.streams.LOGGER')
    def test_check_access_logs_warning_on_forbidden(self, mock_logger):
        """check_access logs warning when OutbrainForbiddenError is raised."""
        mock_client = MagicMock()
        mock_client.config = {"access_token": "test_token"}
        forbidden_error = OutbrainForbiddenError("403 Access denied")
        mock_client.make_request.side_effect = forbidden_error

        class ParentStream(BaseStream):
            def get_probe_url(self):
                return "http://example.com/test"

        stream = ParentStream(client=mock_client)
        stream.check_access()

        # Verify logger.warning was called
        mock_logger.warning.assert_called_once()
        call_args = mock_logger.warning.call_args[0]
        self.assertIn("Permission Error", call_args[0])


class TestCampaignStream(unittest.TestCase):
    """Test Campaign stream."""

    def test_campaign_class_attributes(self):
        """Campaign has correct class-level attributes."""
        self.assertEqual(Campaign.name, "campaign")
        self.assertEqual(Campaign.key_properties, ["id"])
        self.assertIsNone(Campaign.replication_keys)
        self.assertEqual(Campaign.replication_method, "FULL_TABLE")
        self.assertIsNone(Campaign.parent)

    def test_campaign_get_probe_url(self):
        """Campaign.get_probe_url returns correct endpoint."""
        mock_client = MagicMock()
        mock_client.config = {"account_id": "12345"}

        stream = Campaign(client=mock_client)
        url = stream.get_probe_url()

        expected_url = f"{OUTBRAIN_API_BASE}/marketers/12345/campaigns"
        self.assertEqual(url, expected_url)

    def test_campaign_get_probe_url_different_account(self):
        """Campaign.get_probe_url uses account_id from config."""
        mock_client = MagicMock()
        mock_client.config = {"account_id": "99999"}

        stream = Campaign(client=mock_client)
        url = stream.get_probe_url()

        expected_url = f"{OUTBRAIN_API_BASE}/marketers/99999/campaigns"
        self.assertEqual(url, expected_url)


class TestCampaignPerformanceStream(unittest.TestCase):
    """Test CampaignPerformance stream."""

    def test_campaign_performance_class_attributes(self):
        """CampaignPerformance has correct class-level attributes."""
        self.assertEqual(CampaignPerformance.name, "campaign_performance")
        self.assertEqual(
            CampaignPerformance.key_properties,
            ["campaignId", "fromDate"]
        )
        self.assertEqual(CampaignPerformance.bookmark_properties, ["fromDate"])
        self.assertEqual(CampaignPerformance.replication_keys, "fromDate")
        self.assertEqual(CampaignPerformance.replication_method, "INCREMENTAL")
        self.assertEqual(CampaignPerformance.parent, "campaign")

    def test_campaign_performance_inherits_check_access(self):
        """CampaignPerformance inherits check_access from BaseStream."""
        # Child stream should return True without API call
        stream = CampaignPerformance(client=MagicMock())
        result = stream.check_access()
        self.assertTrue(result)


class TestStreamsRegistry(unittest.TestCase):
    """Test STREAMS module-level registry."""

    def test_streams_registry_contains_campaign(self):
        """STREAMS registry contains Campaign class."""
        self.assertIn("campaign", STREAMS)
        self.assertIs(STREAMS["campaign"], Campaign)

    def test_streams_registry_contains_campaign_performance(self):
        """STREAMS registry contains CampaignPerformance class."""
        self.assertIn("campaign_performance", STREAMS)
        self.assertIs(STREAMS["campaign_performance"], CampaignPerformance)

    def test_streams_registry_has_both_streams(self):
        """STREAMS registry contains exactly two streams."""
        self.assertEqual(len(STREAMS), 2)
        self.assertEqual(
            set(STREAMS.keys()),
            {"campaign", "campaign_performance"}
        )
