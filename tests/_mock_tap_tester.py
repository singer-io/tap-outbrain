"""
Stub implementations of tap_tester modules (connections, menagerie, runner)
and a minimal BaseCase, used when INTEGRATION_TEST_MODE=mock.

The stubs replace the live Stitch-platform infrastructure so the existing
integration test files run without modification and without a live account.

Flow
────
  connections.ensure_connection(test) → _MockConn(test)
  test.run_and_verify_check_mode(conn_id) → calls discover() directly
  test.perform_and_verify_table_and_field_selection(...) → marks streams selected
  runner.run_sync_mode(test, conn_id) → patches HTTP, calls sync(), captures records
  menagerie.get_exit_status(conn_id, job) → fake success payload
  runner.examine_target_output_file(...) → returns conn_id.record_counts
  menagerie.get_state / set_state → get/set conn_id.state
"""
from __future__ import annotations

import copy
import json
import os
import sys
import types
import unittest
from typing import Any, Dict

# Ensure tap_outbrain is importable from its source tree regardless of which
# virtualenv is active (e.g. tap-tester venv used by run-test does not have it
# installed, but the source lives one directory above this file).
_TAP_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _TAP_ROOT not in sys.path:
    sys.path.insert(0, _TAP_ROOT)

# Python 3.10 removed several aliases from `collections` that older packages
# (e.g. python-dateutil < 2.8.2) still reference.  Patch them back so that
# singer's dateutil dependency can parse datetime strings correctly.
import collections
import collections.abc as _collections_abc
for _attr in ("Callable", "Iterator", "Iterable", "Mapping", "MutableMapping",
              "MutableSet", "MutableSequence", "Sequence", "Set"):
    if not hasattr(collections, _attr):
        setattr(collections, _attr, getattr(_collections_abc, _attr))

# If tap_tester is not installed (e.g. unittest-only environments), pre-inject
# an empty stub so the `import tap_tester` below doesn't raise ModuleNotFoundError.
# We will populate it with our mock implementations further down in this file.
if "tap_tester" not in sys.modules:
    sys.modules["tap_tester"] = types.ModuleType("tap_tester")
import tap_tester as _real_tap_tester

# ─── Mock connection object ────────────────────────────────────────────────

class _MockConn:
    """Carries all per-test-run state between stub method calls."""

    def __init__(self, test_instance):
        self.test = test_instance
        self.catalog = None          # set by run_and_verify_check_mode
        self.state: Dict = {}
        self.records: Dict[str, list] = {}        # stream → list[record]
        self.schemas: Dict[str, dict] = {}        # stream → schema
        self.record_counts: Dict[str, int] = {}   # stream → count

    def run_sync(self) -> None:
        """Patch HTTP client and run sync(), capturing Singer output."""
        import io
        from unittest.mock import patch, MagicMock
        from tap_outbrain.sync import do_sync
        from tap_outbrain.client import OutbrainClient

        # Clear records from any previous run so counts reflect only this sync
        self.records = {}
        self.schemas = {}
        self.record_counts = {}

        captured = io.StringIO()
        mock_request = self.test._build_mock_request()

        with patch.object(OutbrainClient, "request", side_effect=mock_request):
            # Build a lightweight client instance
            client = OutbrainClient.__new__(OutbrainClient)
            client._OutbrainClient__access_token = "mock_access_token"
            client.base_url = "https://api.outbrainapi.com/amplify/v0.1"

            old_stdout = sys.stdout
            sys.stdout = captured
            try:
                do_sync(
                    client=client,
                    config=self.test.get_mock_config(),
                    catalog=self.catalog,
                    state=copy.deepcopy(self.state),
                )
            finally:
                sys.stdout = old_stdout

        # Parse the captured Singer lines
        for line in captured.getvalue().splitlines():
            try:
                msg = json.loads(line)
                if msg.get("type") == "SCHEMA":
                    stream = msg["stream"]
                    self.schemas[stream] = msg.get("schema", {})
                elif msg.get("type") == "RECORD":
                    stream = msg["stream"]
                    self.records.setdefault(stream, []).append(msg["record"])
                elif msg.get("type") == "STATE":
                    self.state = msg.get("value", {})
            except json.JSONDecodeError:
                pass

        self.record_counts = {s: len(r) for s, r in self.records.items()}


# ─── connections stub ──────────────────────────────────────────────────────

connections = types.ModuleType("tap_tester.connections")


def _ensure_connection(test_instance, **kwargs) -> _MockConn:
    return _MockConn(test_instance)


def _select_catalog_and_fields_via_metadata(
    conn_id: _MockConn, catalog: dict, schema_and_metadata: dict,
    auto_fields, non_selected_fields
) -> None:
    """Mark fields as selected/deselected in the in-memory catalog.

    Fields with ``inclusion: automatic`` (PKs and replication keys) are
    *always* marked selected, mirroring the real Stitch platform behaviour
    where automatic fields cannot be deselected.
    """
    from singer import metadata as md
    stream_id = catalog.get("tap_stream_id") or catalog.get("stream_name")
    for entry in conn_id.catalog.streams:
        if entry.tap_stream_id == stream_id or entry.stream == stream_id:
            mdata = md.to_map(entry.metadata)
            mdata[()]["selected"] = True
            for breadcrumb, meta in list(mdata.items()):
                if breadcrumb == ():
                    continue
                is_automatic = meta.get("inclusion", "") == "automatic"
                field_name = breadcrumb[-1] if breadcrumb else None
                if (field_name and non_selected_fields
                        and field_name in non_selected_fields
                        and not is_automatic):
                    meta["selected"] = False
                else:
                    meta["selected"] = True
            entry.metadata = md.to_list(mdata)
            break


def _select_catalog_via_metadata(
    conn_id: _MockConn, catalog: dict, schema_and_metadata: dict
) -> None:
    """Mark a stream as selected in the in-memory catalog."""
    from singer import metadata as md
    stream_id = catalog.get("tap_stream_id") or catalog.get("stream_name")
    for entry in conn_id.catalog.streams:
        if entry.tap_stream_id == stream_id or entry.stream == stream_id:
            mdata = md.to_map(entry.metadata)
            mdata[()]["selected"] = True
            entry.metadata = md.to_list(mdata)
            break


connections.ensure_connection = _ensure_connection
connections.select_catalog_and_fields_via_metadata = _select_catalog_and_fields_via_metadata
connections.select_catalog_via_metadata = _select_catalog_via_metadata


# ─── menagerie stub ────────────────────────────────────────────────────────

menagerie = types.ModuleType("tap_tester.menagerie")


def _get_exit_status(conn_id: _MockConn, job_name: Any) -> dict:
    return {
        "exit_status": {
            "discovery_exit_status": 0,
            "tap_exit_status": 0,
            "target_exit_status": 0,
            "check_exit_status": 0,
        }
    }


def _verify_sync_exit_status(test, exit_status: dict, job_name: Any) -> None:
    tap_status = exit_status.get("exit_status", {}).get("tap_exit_status", 0)
    test.assertEqual(tap_status, 0, "Mock sync exited with non-zero tap status")


def _get_state(conn_id: _MockConn) -> dict:
    return copy.deepcopy(conn_id.state)


def _set_state(conn_id: _MockConn, state: dict) -> None:
    conn_id.state = copy.deepcopy(state)


def _verify_check_exit_status(test, exit_status: dict, job_name: Any) -> None:
    check_status = exit_status.get("exit_status", {}).get("check_exit_status", 0)
    test.assertEqual(check_status, 0, "Mock check mode exited with non-zero status")


def _get_catalogs(conn_id: _MockConn) -> list:
    """Return catalog entries in the dict format expected by tap-tester BaseCase."""
    if conn_id.catalog is None:
        return []
    return [
        {
            "stream_name": entry.stream,
            "tap_stream_id": entry.tap_stream_id,
            "stream_id": entry.tap_stream_id,
        }
        for entry in conn_id.catalog.streams
    ]


def _get_annotated_schema(conn_id: _MockConn, stream_id: str) -> dict:
    """Return {schema, metadata} for the named stream.

    Breadcrumbs are returned as *lists* (not tuples) to match the JSON
    wire format expected by tap-tester's BaseCase assertion helpers.
    """
    if conn_id.catalog is None:
        return {"schema": {}, "metadata": []}
    for entry in conn_id.catalog.streams:
        if entry.tap_stream_id == stream_id or entry.stream == stream_id:
            return {
                "schema": entry.schema.to_dict(),
                "metadata": [
                    {"breadcrumb": list(m["breadcrumb"]), "metadata": m["metadata"]}
                    for m in entry.metadata
                ],
            }
    return {"schema": {}, "metadata": []}


menagerie.get_exit_status = _get_exit_status
menagerie.verify_sync_exit_status = _verify_sync_exit_status
menagerie.verify_check_exit_status = _verify_check_exit_status
menagerie.get_state = _get_state
menagerie.set_state = _set_state
menagerie.get_catalogs = _get_catalogs
menagerie.get_annotated_schema = _get_annotated_schema


def _select_catalog(conn_id: _MockConn, catalog_entry: dict) -> None:
    """Mark a single stream as selected in the catalog."""
    if conn_id.catalog is None:
        return
    from singer import metadata as md
    stream_name = catalog_entry.get("stream_name") or catalog_entry.get("tap_stream_id")
    for stream in conn_id.catalog.streams:
        if stream.stream == stream_name:
            mdata = md.to_map(stream.metadata)
            mdata[()]["selected"] = True
            stream.metadata = md.to_list(mdata)
            break


menagerie.select_catalog = _select_catalog


# ─── runner stub ──────────────────────────────────────────────────────────

runner = types.ModuleType("tap_tester.runner")


# ─── global tracking for get_records_from_target_output ──────────────────
# Set to the most recently synced _MockConn by _run_sync_mode so that the
# stateless runner.get_records_from_target_output() can return the right data.
_last_conn: Any = None


def _run_check_mode(test_instance, conn_id: _MockConn) -> _MockConn:
    """Run mock check mode (discover); returns conn_id as the job name."""
    from tap_outbrain.discover import discover
    conn_id.catalog = discover()
    return conn_id


def _run_sync_mode(test_instance, conn_id: _MockConn) -> _MockConn:
    """Run a mock sync; returns conn_id used as the 'job_name' in subsequent calls."""
    global _last_conn
    conn_id.run_sync()
    _last_conn = conn_id
    return conn_id


def _examine_target_output_file(
    test_instance, conn_id: _MockConn, streams, pk_fields
) -> dict:
    """Return record counts for each stream in the format expected by tap-tester."""
    return conn_id.record_counts


def _get_records_from_target_output(test_instance, target_schema: dict) -> list:
    """Return records for the most recently synced stream."""
    if _last_conn is None:
        return []
    # Find the stream name from target_schema (which has keys for the selected stream)
    for stream_name, schema in target_schema.items():
        return _last_conn.records.get(stream_name, [])
    return []


runner.run_check_mode = _run_check_mode
runner.run_sync_mode = _run_sync_mode
runner.examine_target_output_file = _examine_target_output_file
runner.get_records_from_target_output = _get_records_from_target_output


# ─── BaseCase stub ────────────────────────────────────────────────────────

class BaseCase(unittest.TestCase):
    """Minimal tap-tester BaseCase for mock mode test compatibility."""

    PRIMARY_KEYS = "primary_keys"
    REPLICATION_METHOD = "replication_method"
    REPLICATION_KEYS = "replication_keys"
    OBEYS_START_DATE = "obeys_start_date"
    RESPECTS_START_DATE = "respects_start_date"

    INCREMENTAL = "INCREMENTAL"
    FULL_TABLE = "FULL_TABLE"

    def setUp(self):
        """Create a fresh mock connection for this test."""
        self.conn_id = _ensure_connection(self)

    @staticmethod
    def get_stream_id(stream):
        """Return the stream ID (usually the stream name)."""
        return stream

    def run_and_verify_check_mode(self, conn_id: _MockConn):
        """Run discovery and verify it succeeds."""
        exit_status = runner.run_check_mode(self, conn_id)
        menagerie.verify_check_exit_status(self, exit_status, conn_id)

    def select_all_streams_and_fields(self, conn_id: _MockConn):
        """Mark all streams and fields as selected."""
        catalogs = menagerie.get_catalogs(conn_id)
        for catalog_entry in catalogs:
            stream_schema_and_metadata = menagerie.get_annotated_schema(
                conn_id, catalog_entry["stream_id"]
            )
            connections.select_catalog_via_metadata(
                conn_id, catalog_entry, stream_schema_and_metadata
            )

    def run_and_verify_sync(self, conn_id: _MockConn):
        """Run sync and verify it succeeds."""
        exit_status = runner.run_sync_mode(self, conn_id)
        menagerie.verify_sync_exit_status(self, exit_status, conn_id)

    def get_synced_records(self, conn_id: _MockConn, stream):
        """Return records synced for the given stream."""
        return conn_id.records.get(stream, [])

    def get_record_count_by_stream(self, conn_id: _MockConn) -> dict:
        """Return {stream: count} for all streams."""
        return conn_id.record_counts

    def get_state_by_stream(self, conn_id: _MockConn) -> dict:
        """Return the full state dict."""
        return menagerie.get_state(conn_id)

    def set_state_by_stream(self, conn_id: _MockConn, state: dict) -> None:
        """Set the full state dict."""
        menagerie.set_state(conn_id, state)
