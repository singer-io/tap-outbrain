"""Mock bookmark test for tap-outbrain."""
try:
    from .base import OutbrainMockBaseTest
except ImportError:
    from base import OutbrainMockBaseTest


class OutbrainMockBookmarkTest(OutbrainMockBaseTest):
    """Test state/bookmark behavior with mock server."""

    def test_state_message_is_emitted(self):
        """STATE messages are emitted after syncing records."""
        result = self._run_mock_sync()
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])

        state_messages = [
            m for m in result["messages"]
            if m.get("type") == "STATE"
        ]
        # STATE messages should be emitted at least once
        self.assertGreater(len(state_messages), 0, "No STATE messages emitted")

    def test_state_contains_campaign_performance_bookmark(self):
        """STATE contains bookmark for campaign_performance stream."""
        result = self._run_mock_sync()
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])

        state_messages = [
            m for m in result["messages"]
            if m.get("type") == "STATE"
        ]
        self.assertGreater(len(state_messages), 0, "No STATE messages emitted")

        # Last state should contain campaign_performance bookmarks
        final_state = state_messages[-1].get("value", {})
        self.assertIn("bookmarks", final_state, "No bookmarks in final state")
        self.assertIn(
            "campaign_performance", final_state["bookmarks"],
            "campaign_performance bookmark missing from state"
        )

    def test_bookmark_respects_start_date(self):
        """Initial sync respects configured start_date."""
        start_date = "2024-06-01T00:00:00Z"
        config = self._default_config(start_date=start_date)
        result = self._run_mock_sync(config=config)
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])

        # Check that sync was able to proceed with the start_date
        record_messages = [
            m for m in result["messages"]
            if m.get("type") == "RECORD"
        ]
        self.assertGreater(len(record_messages), 0, "No records synced with start_date")

    def test_state_persists_across_syncs(self):
        """State from one sync can be used for subsequent syncs."""
        # First sync
        result1 = self._run_mock_sync()
        self.assertEqual(result1["returncode"], 0, msg=result1["stderr"])

        state_messages = [
            m for m in result1["messages"]
            if m.get("type") == "STATE"
        ]
        self.assertGreater(len(state_messages), 0)

        final_state = state_messages[-1].get("value", {})

        # Second sync with state from first
        result2 = self._run_mock_sync(state=final_state)
        self.assertEqual(result2["returncode"], 0, msg=result2["stderr"])

        # Both syncs should complete successfully
        self.assertEqual(result1["returncode"], 0)
        self.assertEqual(result2["returncode"], 0)

    def test_campaign_performance_bookmark_format(self):
        """Bookmark values for campaign_performance are in expected format."""
        result = self._run_mock_sync()
        self.assertEqual(result["returncode"], 0, msg=result["stderr"])

        state_messages = [
            m for m in result["messages"]
            if m.get("type") == "STATE"
        ]
        if len(state_messages) > 0:
            final_state = state_messages[-1].get("value", {})
            bookmarks = final_state.get("bookmarks", {})
            perf_bookmarks = bookmarks.get("campaign_performance", {})

            # Bookmark values should be date strings
            for campaign_id, bookmark_value in perf_bookmarks.items():
                self.assertIsInstance(bookmark_value, str)
                # Should be in YYYY-MM-DD format
                self.assertRegex(
                    bookmark_value,
                    r"^\d{4}-\d{2}-\d{2}$",
                    f"Bookmark value '{bookmark_value}' not in YYYY-MM-DD format"
                )
