import unittest
from unittest.mock import patch, mock_open
import json
from singer import metadata

from tap_outbrain.schema import get_schemas
from tap_outbrain.streams import STREAMS


class TestCatalogMetadata(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures with mock schema data."""
        self.mock_campaign_schema = {
            "type": "object",
            "properties": {
                "id": {"type": ["null", "string"]},
                "name": {"type": ["null", "string"]},
                "enabled": {"type": ["null", "boolean"]}
            }
        }
        
        self.mock_campaign_performance_schema = {
            "type": "object", 
            "properties": {
                "campaignId": {"type": ["null", "string"]},
                "fromDate": {"type": ["null", "string"], "format": "date-time"},
                "impressions": {"type": ["null", "number"]},
                "clicks": {"type": ["null", "number"]}
            }
        }

    @patch('builtins.open', new_callable=mock_open)
    @patch('tap_outbrain.schema.get_abs_path')
    def test_parent_tap_stream_id_present_for_child_stream(self, mock_get_abs_path, mock_file):
        """Test that parent-tap-stream-id is set correctly for child streams."""
        # Mock file paths
        mock_get_abs_path.side_effect = lambda x: f"/mocked/path/{x}"
        
        # Mock file contents for both schemas
        def mock_file_content(filename, mode):
            if 'campaign_performance.json' in filename:
                return mock_open(read_data=json.dumps(self.mock_campaign_performance_schema))()
            elif 'campaign.json' in filename:
                return mock_open(read_data=json.dumps(self.mock_campaign_schema))()
            return mock_open()()
        
        mock_file.side_effect = mock_file_content
        
        schemas, field_metadata = get_schemas()
        
        # Verify campaign_performance has parent-tap-stream-id metadata
        campaign_performance_metadata = field_metadata['campaign_performance']
        metadata_map = metadata.to_map(campaign_performance_metadata)
        
        self.assertIn('parent-tap-stream-id', metadata_map[()])
        self.assertEqual(metadata_map[()]['parent-tap-stream-id'], 'campaign')

    @patch('builtins.open', new_callable=mock_open)
    @patch('tap_outbrain.schema.get_abs_path')
    def test_parent_tap_stream_id_absent_for_parent_stream(self, mock_get_abs_path, mock_file):
        """Test that parent-tap-stream-id is not set for parent streams."""
        # Mock file paths
        mock_get_abs_path.side_effect = lambda x: f"/mocked/path/{x}"
        
        # Mock file contents for both schemas
        def mock_file_content(filename, mode):
            if 'campaign_performance.json' in filename:
                return mock_open(read_data=json.dumps(self.mock_campaign_performance_schema))()
            elif 'campaign.json' in filename:
                return mock_open(read_data=json.dumps(self.mock_campaign_schema))()
            return mock_open()()
        
        mock_file.side_effect = mock_file_content
        
        schemas, field_metadata = get_schemas()
        
        # Verify campaign does NOT have parent-tap-stream-id metadata
        campaign_metadata = field_metadata['campaign']
        metadata_map = metadata.to_map(campaign_metadata)
        
        self.assertNotIn('parent-tap-stream-id', metadata_map[()])

    @patch('builtins.open', new_callable=mock_open)
    @patch('tap_outbrain.schema.get_abs_path')
    def test_stream_metadata_structure(self, mock_get_abs_path, mock_file):
        """Test the overall structure of stream metadata including parent relationships."""
        # Mock file paths
        mock_get_abs_path.side_effect = lambda x: f"/mocked/path/{x}"
        
        # Mock file contents for both schemas
        def mock_file_content(filename, mode):
            if 'campaign_performance.json' in filename:
                return mock_open(read_data=json.dumps(self.mock_campaign_performance_schema))()
            elif 'campaign.json' in filename:
                return mock_open(read_data=json.dumps(self.mock_campaign_schema))()
            return mock_open()()
        
        mock_file.side_effect = mock_file_content
        
        schemas, field_metadata = get_schemas()
        
        # Test campaign stream metadata
        campaign_metadata = field_metadata['campaign']
        campaign_metadata_map = metadata.to_map(campaign_metadata)
        
        # Verify standard metadata is present
        self.assertIn('table-key-properties', campaign_metadata_map[()])
        self.assertEqual(campaign_metadata_map[()]['table-key-properties'], ['id'])
        self.assertEqual(campaign_metadata_map[()]['forced-replication-method'], 'FULL_TABLE')
        
        # Test campaign_performance stream metadata  
        campaign_performance_metadata = field_metadata['campaign_performance']
        campaign_performance_metadata_map = metadata.to_map(campaign_performance_metadata)
        
        # Verify standard metadata is present
        self.assertIn('table-key-properties', campaign_performance_metadata_map[()])
        self.assertEqual(campaign_performance_metadata_map[()]['table-key-properties'], ['campaignId', 'fromDate'])
        self.assertEqual(campaign_performance_metadata_map[()]['forced-replication-method'], 'INCREMENTAL')
        
        # Verify parent relationship is set
        self.assertIn('parent-tap-stream-id', campaign_performance_metadata_map[()])
        self.assertEqual(campaign_performance_metadata_map[()]['parent-tap-stream-id'], 'campaign')

    def test_streams_configuration(self):
        """Test that STREAMS configuration has correct parent attribute."""
        # Verify Campaign class does not have parent attribute
        self.assertFalse(hasattr(STREAMS['campaign'], 'parent'))
        
        # Verify CampaignPerformance class has parent attribute set correctly
        self.assertTrue(hasattr(STREAMS['campaign_performance'], 'parent'))
        self.assertEqual(STREAMS['campaign_performance'].parent, 'campaign')

    @patch('builtins.open', new_callable=mock_open)
    @patch('tap_outbrain.schema.get_abs_path')
    def test_automatic_inclusion_metadata(self, mock_get_abs_path, mock_file):
        """Test that key properties and replication keys are marked with automatic inclusion."""
        # Mock file paths
        mock_get_abs_path.side_effect = lambda x: f"/mocked/path/{x}"
        
        # Mock file contents for both schemas
        def mock_file_content(filename, mode):
            if 'campaign_performance.json' in filename:
                return mock_open(read_data=json.dumps(self.mock_campaign_performance_schema))()
            elif 'campaign.json' in filename:
                return mock_open(read_data=json.dumps(self.mock_campaign_schema))()
            return mock_open()()
        
        mock_file.side_effect = mock_file_content
        
        schemas, field_metadata = get_schemas()
        
        # Test campaign_performance stream - both key properties should be automatic
        campaign_performance_metadata = field_metadata['campaign_performance']
        campaign_performance_metadata_map = metadata.to_map(campaign_performance_metadata)
        
        # fromDate is both a key property and replication key, so it should be marked as automatic
        self.assertEqual(
            campaign_performance_metadata_map[('properties', 'fromDate')]['inclusion'], 
            'automatic'
        )
        
        # campaignId is also a key property, so it should be marked as automatic
        self.assertEqual(
            campaign_performance_metadata_map[('properties', 'campaignId')]['inclusion'], 
            'automatic'
        )
        
        # Non-key properties should be marked as available
        self.assertEqual(
            campaign_performance_metadata_map[('properties', 'impressions')]['inclusion'], 
            'available'
        )

    @patch('builtins.open', new_callable=mock_open)
    @patch('tap_outbrain.schema.get_abs_path')
    def test_schema_loading(self, mock_get_abs_path, mock_file):
        """Test that schemas are loaded correctly from JSON files."""
        # Mock file paths
        mock_get_abs_path.side_effect = lambda x: f"/mocked/path/{x}"
        
        # Mock file contents for both schemas
        def mock_file_content(filename, mode):
            if 'campaign_performance.json' in filename:
                return mock_open(read_data=json.dumps(self.mock_campaign_performance_schema))()
            elif 'campaign.json' in filename:
                return mock_open(read_data=json.dumps(self.mock_campaign_schema))()
            return mock_open()()
        
        mock_file.side_effect = mock_file_content
        
        schemas, field_metadata = get_schemas()
        
        # Verify schemas are loaded correctly
        self.assertIn('campaign', schemas)
        self.assertIn('campaign_performance', schemas)
        
        # Verify schema structure
        self.assertEqual(schemas['campaign'], self.mock_campaign_schema)
        self.assertEqual(schemas['campaign_performance'], self.mock_campaign_performance_schema)
        
        # Verify metadata is created for both streams
        self.assertIn('campaign', field_metadata)
        self.assertIn('campaign_performance', field_metadata)
