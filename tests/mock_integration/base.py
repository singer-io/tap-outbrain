"""Base class for tap-outbrain mock integration tests with mock server."""
import copy
import datetime
import http.server
import json
import os
import socketserver
import sys
import tempfile
import threading
import unittest
from urllib.parse import parse_qs, urlparse


class _ThreadingTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    """Thread pool TCP server for mock API."""
    allow_reuse_address = True
    daemon_threads = True


class OutbrainBaseTest(unittest.TestCase):
    """Base class for mock integration tests with embedded API server."""

    _server = None
    _server_thread = None
    _server_base_url = None
    _server_calls = []

    @staticmethod
    def _now():
        """Return current UTC time with no seconds/microseconds."""
        return datetime.datetime.now(datetime.timezone.utc).replace(second=0, microsecond=0)

    @staticmethod
    def _to_iso_datetime(value):
        """Convert datetime to ISO format string."""
        if isinstance(value, str):
            return value
        return value.isoformat() if hasattr(value, 'isoformat') else str(value)

    @classmethod
    def _mock_campaign_response(cls, campaign_id="c001", **overrides):
        """Generate mock campaign response matching Outbrain API format."""
        campaign = {
            "id": campaign_id,
            "name": f"Campaign {campaign_id}",
            "campaignOnAir": True,
            "enabled": True,
            "budget": {
                "id": f"budget_{campaign_id}",
                "amount": 1000.0,
                "currency": "USD",
                "amountSpent": 500.0,
                "creationTime": "2024-01-01T00:00:00+00:00",
                "lastModified": "2024-01-02T00:00:00+00:00",
                "startDate": "2024-01-01",
            },
            "cpc": 0.5,
        }
        campaign.update(overrides)
        return campaign

    @classmethod
    def _mock_performance_response(cls, campaign_id="c001", from_date="2024-01-01", **overrides):
        """Generate mock performance response for campaign_performance stream."""
        performance = {
            "campaignId": campaign_id,
            "fromDate": from_date,
            "impressions": 10000,
            "clicks": 500,
            "ctr": 0.05,
            "spend": 250.0,
            "conversions": 50,
            "conversionRate": 0.10,
        }
        performance.update(overrides)
        return performance

    @classmethod
    def setUpClass(cls):
        """Set up mock API server before running tests."""
        super().setUpClass()

        class _Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                """Handle GET requests to mock API."""
                parsed = urlparse(self.path)
                query = parse_qs(parsed.query)
                auth = self.headers.get("Authorization")

                cls._server_calls.append(
                    {
                        "path": parsed.path,
                        "query": copy.deepcopy(query),
                        "authorization": auth,
                    }
                )

                # Mock /login endpoint
                if parsed.path.endswith("/login"):
                    body = json.dumps({"OB-TOKEN-V1": "mock_token_123"}).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return

                # Mock /marketers/{id}/campaigns endpoint
                if parsed.path.endswith("/campaigns"):
                    campaigns = [
                        cls._mock_campaign_response("c001"),
                        cls._mock_campaign_response("c002"),
                    ]
                    body = json.dumps({"campaigns": campaigns, "totalCount": 2}).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return

                # Mock /reports/marketers/{id}/periodic endpoint
                if "/periodic" in parsed.path:
                    from_param = query.get("from", [None])[0]
                    if from_param:
                        perf_date = str(from_param)
                    else:
                        perf_date = (cls._now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

                    performances = [
                        cls._mock_performance_response("c001", perf_date),
                        cls._mock_performance_response("c002", perf_date),
                    ]
                    body = json.dumps({
                        "results": performances,
                        "totalResults": len(performances),
                    }).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return

                # Default 404
                self.send_response(404)
                self.end_headers()

            def log_message(self, format_str, *args):
                """Suppress server logging."""
                return

        cls._server = _ThreadingTCPServer(("127.0.0.1", 0), _Handler)
        host, port = cls._server.server_address
        cls._server_base_url = f"http://{host}:{port}"
        cls._server_thread = threading.Thread(
            target=cls._server.serve_forever, daemon=True
        )
        cls._server_thread.start()

    @classmethod
    def tearDownClass(cls):
        """Shut down mock server after tests."""
        if cls._server:
            cls._server.shutdown()
            cls._server.server_close()
        if cls._server_thread:
            cls._server_thread.join(timeout=2)
        super().tearDownClass()

    def setUp(self):
        """Clear server calls before each test."""
        self.__class__._server_calls = []

    @classmethod
    def _default_config(cls, start_date=None):
        """Return default mock config with fake Outbrain credentials."""
        if not start_date:
            start_date = (cls._now() - datetime.timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")

        return {
            "account_id": "mock_account_001",
            "username": "mock_user@example.com",
            "password": "mock_password",
            "start_date": start_date,
            "base_url": cls._server_base_url,
        }

    def _run_mock_sync(self, config=None, state=None):
        """Run tap-outbrain sync with mock configuration and return captured output."""
        run_config = config or self._default_config()
        run_state = state or {}
        repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "config.json")
            state_path = os.path.join(tmpdir, "state.json")
            catalog_path = os.path.join(tmpdir, "catalog.json")

            with open(config_path, "w", encoding="utf-8") as config_file:
                json.dump(run_config, config_file)

            with open(state_path, "w", encoding="utf-8") as state_file:
                json.dump(run_state, state_file)

            # Create a fully-selected catalog (mark all streams as selected)
            from tap_outbrain.discover import discover
            catalog = discover()
            catalog_streams = []
            for stream in catalog.streams:
                # Set selected=true on the root breadcrumb entry
                metadata = []
                for entry in stream.metadata:
                    entry_copy = {"breadcrumb": entry["breadcrumb"], "metadata": dict(entry["metadata"])}
                    if entry["breadcrumb"] == [] or entry["breadcrumb"] == ():
                        entry_copy["metadata"]["selected"] = True
                    metadata.append(entry_copy)
                catalog_streams.append({
                    "tap_stream_id": stream.tap_stream_id,
                    "stream": stream.stream,
                    "schema": stream.schema.to_dict(),
                    "key_properties": stream.key_properties,
                    "metadata": metadata,
                })
            catalog_dict = {"streams": catalog_streams}
            with open(catalog_path, "w", encoding="utf-8") as catalog_file:
                json.dump(catalog_dict, catalog_file)

            tap_cmd = os.getenv("STITCH_TAP_PATH")
            runner_python = sys.executable
            if tap_cmd:
                candidate_python = os.path.join(os.path.dirname(tap_cmd), "python")
                if os.path.exists(candidate_python):
                    runner_python = candidate_python

            driver = (
                "import json, sys\n"
                "sys.path.insert(0, '{}')\n"
                "import singer\n"
                "import tap_outbrain\n"
                "tap_outbrain.BASE_URL = '{}'\n"
                "from tap_outbrain import do_sync\n"
                "with open('{}', 'r') as c, open('{}', 'r') as s, open('{}', 'r') as cat:\n"
                "    config = json.load(c)\n"
                "    raw_state = json.load(s)\n"
                "    catalog = singer.Catalog.from_dict(json.load(cat))\n"
                "state = raw_state if raw_state else {{'campaign_performance': {{}}}}\n"
                "if 'campaign_performance' not in state:\n"
                "    state['campaign_performance'] = {{}}\n"
                "do_sync(catalog, config, state)\n"
            ).format(
                repo_root,
                self._server_base_url,
                config_path,
                state_path,
                catalog_path,
            )

            proc = subprocess.run(
                [runner_python, "-c", driver],
                capture_output=True,
                text=True,
                cwd=repo_root,
            )

            output_messages = []
            for line in proc.stdout.split("\n"):
                if line.strip():
                    try:
                        output_messages.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass

            return {
                "returncode": proc.returncode,
                "stdout": proc.stdout,
                "stderr": proc.stderr,
                "messages": output_messages,
                "request_calls": self._server_calls,
            }




# Import subprocess here to avoid import at module level issues
import subprocess