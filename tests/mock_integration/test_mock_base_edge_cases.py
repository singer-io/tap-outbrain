"""Branch coverage tests for Outbrain mock base helpers."""

import datetime
import json
import os
import urllib.error
import urllib.request
from unittest.mock import patch

from .base import OutbrainMockBaseTest


class OutbrainMockBaseBranchesTest(OutbrainMockBaseTest):
    """Exercise fallback and defensive branches in mock base helpers."""

    def test_to_iso_datetime_covers_all_paths(self):
        """_to_iso_datetime returns string/isoformat/stringified values correctly."""
        raw = "2026-01-01T00:00:00Z"
        self.assertEqual(self._to_iso_datetime(raw), raw)

        dt_value = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
        self.assertEqual(self._to_iso_datetime(dt_value), dt_value.isoformat())

        self.assertEqual(self._to_iso_datetime(123), "123")

    def test_periodic_endpoint_without_from_uses_default_date(self):
        """Periodic endpoint supports missing from param and falls back to yesterday."""
        url = f"{self._server_base_url}/reports/marketers/mock/periodic"
        with urllib.request.urlopen(url, timeout=5) as resp:
            payload = json.loads(resp.read().decode("utf-8"))

        self.assertIn("results", payload)
        self.assertGreater(len(payload["results"]), 0)

    def test_unknown_path_returns_404(self):
        """Unknown mock endpoint should return HTTP 404."""
        url = f"{self._server_base_url}/unknown-endpoint"
        with self.assertRaises(urllib.error.HTTPError) as exc:
            urllib.request.urlopen(url, timeout=5)

        self.assertEqual(exc.exception.code, 404)

    def test_run_mock_sync_stitch_tap_path_runner_branch(self):
        """_run_mock_sync uses STITCH_TAP_PATH sibling python when available."""
        tap_path = "/usr/local/share/virtualenvs/tap-workday/bin/tap-workday"
        config = self._default_config()
        config["account_id"] = "mock_account_branch_runner"

        os.environ["STITCH_TAP_PATH"] = "/tmp/already-set-path"
        previous = os.environ.get("STITCH_TAP_PATH")
        os.environ["STITCH_TAP_PATH"] = tap_path
        try:
            result = self._run_mock_sync(config=config)
        finally:
            os.environ["STITCH_TAP_PATH"] = previous

        self.assertEqual(result["returncode"], 0, msg=result["stderr"])

    def test_run_mock_sync_ignores_non_json_stdout_lines(self):
        """_run_mock_sync skips non-JSON stdout lines while keeping valid messages."""

        class FakeResult:
            returncode = 0
            stdout = 'not-json\n{"type":"STATE","value":{}}\n'
            stderr = ""

        config = self._default_config()
        config["account_id"] = "mock_account_non_json_stdout"

        with patch("tests.mock_integration.base.subprocess.run", return_value=FakeResult()):
            result = self._run_mock_sync(config=config)

        self.assertEqual(result["returncode"], 0)
        self.assertEqual(len(result["messages"]), 1)
        self.assertEqual(result["messages"][0].get("type"), "STATE")

    def test_run_mock_sync_restores_existing_stitch_tap_path(self):
        """When STITCH_TAP_PATH already exists, _run_mock_sync restores it."""
        os.environ["STITCH_TAP_PATH"] = "/tmp/original-tap-path"
        original = os.environ.get("STITCH_TAP_PATH")
        os.environ.pop("STITCH_TAP_PATH", None)

        tap_path = "/usr/local/share/virtualenvs/tap-workday/bin/tap-workday"
        config = self._default_config()
        config["account_id"] = "mock_account_branch_restore"

        previous = os.environ.get("STITCH_TAP_PATH")
        os.environ["STITCH_TAP_PATH"] = tap_path
        try:
            result = self._run_mock_sync(config=config)
        finally:
            os.environ.pop("STITCH_TAP_PATH", None)
            os.environ["STITCH_TAP_PATH"] = original

        self.assertEqual(result["returncode"], 0, msg=result["stderr"])
