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
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict

# Ensure tap_outbrain and the sibling tap-tester repo are importable from the
# workspace regardless of which virtualenv is active.
_TESTS_ROOT = os.path.dirname(__file__)
_TAP_ROOT = os.path.abspath(os.path.join(_TESTS_ROOT, ".."))
_WORKSPACE_ROOT = os.path.abspath(os.path.join(_TESTS_ROOT, "..", ".."))
_TAP_TESTER_ROOT = os.path.join(_WORKSPACE_ROOT, "tap-tester")
for _path in (_TAP_ROOT, _TAP_TESTER_ROOT):
    if _path not in sys.path:
        sys.path.insert(0, _path)

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

_PREEXISTING_CONNECTIONS_MODULE = sys.modules.get("tap_tester.connections")
_PREEXISTING_MENAGERIE_MODULE = sys.modules.get("tap_tester.menagerie")
_PREEXISTING_RUNNER_MODULE = sys.modules.get("tap_tester.runner")

# ─── Mock connection object ────────────────────────────────────────────────

class _MockConn:
    """Carries all per-test-run state between stub method calls."""

    def __init__(self, test_instance):
        self.test = test_instance
        self.catalog = None          # set by run_and_verify_check_mode
        self.state: Dict = {"campaign_performance": {}}
        self.records: Dict[str, list] = {}        # stream → list[record]
        self.schemas: Dict[str, dict] = {}        # stream → schema
        self.record_counts: Dict[str, int] = {}   # stream → count

    def run_sync(self) -> None:
        """Patch HTTP client and run sync(), capturing Singer output."""
        import io
        from unittest.mock import patch
        import tap_outbrain

        # Clear records from any previous run so counts reflect only this sync
        self.records = {}
        self.schemas = {}
        self.record_counts = {}

        captured = io.StringIO()
        mock_request = self.test._build_mock_request()
        config = {**self.test.get_credentials(), **self.test.get_properties()}

        old_stdout = sys.stdout
        sys.stdout = captured
        try:
            with patch("tap_outbrain.request", side_effect=mock_request), \
                    patch("tap_outbrain.time.sleep", return_value=None):
                tap_outbrain.do_sync(
                    catalog=self.catalog,
                    config=config,
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
                    selected_fields = self._selected_fields_for_stream(stream)
                    record = msg["record"]
                    if selected_fields is not None:
                        record = {
                            field: value
                            for field, value in record.items()
                            if field in selected_fields
                        }
                    self.records.setdefault(stream, []).append(record)
                elif msg.get("type") == "STATE":
                    self.state = msg.get("value", {})
            except json.JSONDecodeError:
                pass

        self.record_counts = {s: len(r) for s, r in self.records.items()}

    def _selected_fields_for_stream(self, stream_name: str):
        """Return the selected field names for a stream, or None if unavailable."""
        if self.catalog is None:
            return None

        for entry in self.catalog.streams:
            if entry.tap_stream_id != stream_name and entry.stream != stream_name:
                continue

            selected_fields = set()
            for item in entry.metadata:
                breadcrumb = item.get("breadcrumb", [])
                metadata = item.get("metadata", {})
                if not breadcrumb or breadcrumb[0] != "properties":
                    continue

                field_name = breadcrumb[-1]
                if metadata.get("selected") or metadata.get("inclusion") == "automatic":
                    selected_fields.add(field_name)
            return selected_fields

        return None


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
    
    Note: Singer discover() outputs only schema; metadata is built by tap-tester.
    In mock mode, we build metadata from scratch to match tap-tester expectations.
    """
    if conn_id.catalog is None:
        return {"schema": {}, "metadata": []}

    for entry in conn_id.catalog.streams:
        if entry.tap_stream_id == stream_id or entry.stream == stream_id:
            schema = entry.schema.to_dict() if hasattr(entry, "schema") else {}

            pk_key = getattr(conn_id.test, "PRIMARY_KEYS", "table-key-properties")
            rep_method_key = getattr(conn_id.test, "REPLICATION_METHOD", "forced-replication-method")
            rep_keys_key = getattr(conn_id.test, "REPLICATION_KEYS", "valid-replication-keys")
            parent_key = getattr(conn_id.test, "PARENT_STREAM", "parent-stream")

            expected_meta = {}
            if hasattr(conn_id.test, "expected_metadata"):
                expected_meta = conn_id.test.expected_metadata()
            stream_meta = expected_meta.get(entry.stream) or expected_meta.get(entry.tap_stream_id) or {}

            normalized_metadata = []
            existing_metadata = getattr(entry, "metadata", None) or []
            for item in existing_metadata:
                if isinstance(item, dict):
                    breadcrumb = list(item.get("breadcrumb", []))
                    item_metadata = item.get("metadata", {})
                else:
                    breadcrumb = list(getattr(item, "breadcrumb", []) or [])
                    item_metadata = getattr(item, "metadata", {}) or {}
                normalized_metadata.append({"breadcrumb": breadcrumb, "metadata": item_metadata})

            root_selected = False
            for item in normalized_metadata:
                if item.get("breadcrumb") == []:
                    root_selected = bool(item.get("metadata", {}).get("selected", False))
                    break

            # Rebuild root metadata deterministically so discovery tests always
            # receive exactly one canonical top-level breadcrumb entry.
            metadata = [item for item in normalized_metadata if item.get("breadcrumb") != []]
            root_metadata = {
                "selected": root_selected,
                pk_key: list(stream_meta.get(pk_key, getattr(entry, "key_properties", []) or [])),
                rep_method_key: stream_meta.get(rep_method_key, "FULL_TABLE"),
                rep_keys_key: list(stream_meta.get(rep_keys_key, []) or []),
            }
            if stream_meta.get(parent_key):
                root_metadata[parent_key] = stream_meta[parent_key]
            metadata.insert(0, {"breadcrumb": [], "metadata": root_metadata})

            return {
                "schema": schema,
                "metadata": metadata,
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


def _examine_target_output_for_fields() -> dict:
    """Return the replicated field names for each synced stream."""
    if _last_conn is None:
        return {}

    fields_by_stream = {}
    for stream_name, records in _last_conn.records.items():
        fields = set()
        for record in records:
            fields.update(record.keys())
        fields_by_stream[stream_name] = fields
    return fields_by_stream


def _get_records_from_target_output_all() -> dict:
    """Return target-style batches keyed by stream."""
    if _last_conn is None:
        return {}

    records_by_stream = {}
    for stream_name, records in _last_conn.records.items():
        records_by_stream[stream_name] = {
            "messages": [
                {"action": "upsert", "data": record}
                for record in records
            ],
            "schema": _last_conn.schemas.get(stream_name, {}),
            "key_names": None,
            "table_version": None,
        }
    return records_by_stream


runner.run_check_mode = _run_check_mode
runner.run_sync_mode = _run_sync_mode
runner.examine_target_output_file = _examine_target_output_file
runner.get_records_from_target_output = _get_records_from_target_output
runner.get_records_from_target_output_all = _get_records_from_target_output_all
runner.examine_target_output_for_fields = _examine_target_output_for_fields


# ─── BaseCase stub ────────────────────────────────────────────────────────

class BaseCase(unittest.TestCase):
    """Minimal tap-tester BaseCase for mock mode test compatibility."""

    PRIMARY_KEYS = "table-key-properties"
    UNSUPPORTED_FIELDS = "unsupported"
    REPLICATION_METHOD = "forced-replication-method"
    REPLICATION_KEYS = "valid-replication-keys"
    OBEYS_START_DATE = "obey-start-date"
    RESPECTS_START_DATE = "table-start-date-usage"
    LOOK_BACK_WINDOW = "table-look-back-window"
    PARENT_STREAM = "parent-stream"
    API_LIMIT = "API_LIMIT"

    INCREMENTAL = "INCREMENTAL"
    FULL_TABLE = "FULL_TABLE"

    @staticmethod
    def _strip_logging(kwargs):
        kwargs.pop("logging", None)
        return kwargs

    def setUp(self, **kwargs):
        """Create a fresh mock connection for this test."""
        self._strip_logging(kwargs)
        self.conn_id = _ensure_connection(self)

    def assertCountEqual(self, *args, **kwargs):
        return super().assertCountEqual(*args, **self._strip_logging(kwargs))

    def assertEqual(self, *args, **kwargs):
        return super().assertEqual(*args, **self._strip_logging(kwargs))

    def assertFalse(self, *args, **kwargs):
        return super().assertFalse(*args, **self._strip_logging(kwargs))

    def assertTrue(self, *args, **kwargs):
        return super().assertTrue(*args, **self._strip_logging(kwargs))

    def assertGreater(self, *args, **kwargs):
        return super().assertGreater(*args, **self._strip_logging(kwargs))

    def assertSetEqual(self, *args, **kwargs):
        return super().assertSetEqual(*args, **self._strip_logging(kwargs))

    @staticmethod
    def get_stream_id(stream):
        """Return the stream ID (usually the stream name)."""
        return stream

    @staticmethod
    def get_stream_name(stream_id):
        """Return the stream name from stream ID (usually identical)."""
        return stream_id

    @staticmethod
    def get_all_streams_and_fields(conn_id: _MockConn):
        catalogs = menagerie.get_catalogs(conn_id)
        streams_to_fields = {}

        for cat in catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat["stream_id"])
            streams_to_fields[cat["stream_name"]] = {
                item["breadcrumb"][-1]
                for item in catalog_entry["metadata"]
                if item["breadcrumb"] != []
                and item["metadata"].get("inclusion") != "unsupported"
            }

        return streams_to_fields

    @staticmethod
    def timedelta_formatted(value, delta=timedelta(days=0), date_format="%Y-%m-%dT%H:%M:%SZ"):
        """Apply a timedelta and return a string formatted date/datetime."""
        date_stripped = datetime.strptime(value, date_format) if isinstance(value, str) else value
        return datetime.strftime(date_stripped + delta, date_format)

    @staticmethod
    def parse_date(date_value):
        """Parse Singer date strings into timezone-aware datetime objects."""
        if isinstance(date_value, datetime):
            if date_value.tzinfo is None:
                return date_value.replace(tzinfo=timezone.utc)
            return date_value

        if date_value is None:
            return None

        text = str(date_value)
        if len(text) == 10:
            return datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc)

        if text.endswith("Z"):
            text = text[:-1] + "+00:00"

        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed

    def expected_primary_keys(self, stream=None):
        primary_keys = {
            table: properties.get(self.PRIMARY_KEYS, set())
            for table, properties in self.expected_metadata().items()
        }
        if stream is None:
            return primary_keys
        return primary_keys[stream]

    def expected_replication_keys(self, stream=None):
        replication_keys = {
            table: properties.get(self.REPLICATION_KEYS, set())
            for table, properties in self.expected_metadata().items()
        }
        if stream is None:
            return replication_keys
        return replication_keys[stream]

    def expected_automatic_fields(self, stream=None):
        automatic_fields = {
            table: properties.get(self.PRIMARY_KEYS, set())
            | properties.get(self.REPLICATION_KEYS, set())
            for table, properties in self.expected_metadata().items()
        }
        if stream is None:
            return automatic_fields
        return automatic_fields[stream]

    def expected_page_size(self, stream=None):
        page_size = {
            table: properties[self.API_LIMIT]
            for table, properties in self.expected_metadata().items()
            if properties.get(self.API_LIMIT)
        }
        if stream is None:
            return page_size
        return page_size[stream]

    def expected_unsupported_fields(self, stream=None):
        unsupported_fields = {
            table: set(properties.get(self.UNSUPPORTED_FIELDS, set()))
            for table, properties in self.expected_metadata().items()
        }
        if stream is None:
            return unsupported_fields
        return unsupported_fields[stream]

    def expected_start_date_behavior(self, stream=None):
        respect_start_date = {
            table: properties.get(self.RESPECTS_START_DATE, True)
            for table, properties in self.expected_metadata().items()
        }
        if stream is None:
            return respect_start_date
        return respect_start_date[stream]

    def get_bookmark_value(self, state, stream):
        replication_method = self.expected_replication_method(stream)
        stream_bookmark = state.get("bookmarks", {}).get(stream, {})
        stream_replication_key = self.expected_replication_keys(stream)
        if stream_bookmark and replication_method == self.INCREMENTAL:
            assert len(stream_replication_key) == 1
            return stream_bookmark.get(next(iter(stream_replication_key)))
        return None

    def get_bookmark_values(self, state, test_streams):
        bookmark_values = {}
        for stream in test_streams:
            bookmark_values[stream] = self.get_bookmark_value(state, stream)
        return bookmark_values

    def run_and_verify_check_mode(self, conn_id: _MockConn):
        """Run discovery and verify it succeeds."""
        check_job_name = runner.run_check_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)
        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, "A catalog was produced by discovery.")
        self.assertSetEqual(
            self.expected_stream_names(),
            {catalog["stream_name"] for catalog in found_catalogs},
            "Expected streams are present in catalog.",
        )
        return found_catalogs

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

    def select_streams_and_fields(self, conn_id: _MockConn, catalogs, streams_to_selected_fields=None):
        for catalog in catalogs:
            schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog["stream_id"])
            metadata = schema_and_metadata["metadata"]
            properties = {
                item["breadcrumb"][-1]
                for item in metadata
                if item["breadcrumb"] and item["breadcrumb"][0] == "properties"
            }

            if streams_to_selected_fields:
                non_selected_fields = properties - streams_to_selected_fields[catalog["stream_name"]]
            else:
                non_selected_fields = []

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema_and_metadata, [], non_selected_fields
            )

    def perform_and_verify_table_and_field_selection(self, conn_id: _MockConn, test_catalogs):
        expected_streams_to_selected_fields = self.streams_to_selected_fields()
        self.select_streams_and_fields(conn_id, test_catalogs, expected_streams_to_selected_fields)

        catalogs = menagerie.get_catalogs(conn_id)
        expected_selected = [catalog.get("stream_name") for catalog in test_catalogs]

        for catalog in catalogs:
            with self.subTest(catalog=catalog["stream_name"]):
                catalog_entry = menagerie.get_annotated_schema(conn_id, catalog["stream_id"])
                stream_selected = [
                    item["metadata"].get("selected", None)
                    for item in catalog_entry["metadata"]
                    if item["breadcrumb"] == []
                ][0]

                if catalog["stream_name"] not in expected_selected:
                    self.assertFalse(stream_selected)
                    continue

                self.assertTrue(stream_selected)

                fields_selected = {
                    item["breadcrumb"][-1]: item["metadata"].get("selected", None)
                    or item["metadata"].get("inclusion") == "automatic"
                    for item in catalog_entry["metadata"]
                    if item["breadcrumb"] != []
                }
                expected_selected_fields = expected_streams_to_selected_fields.get(
                    catalog["stream_name"], set()
                )
                expected_automatic_fields = self.expected_automatic_fields(catalog["stream_name"])
                actual_selected_fields = {
                    field for field, selected in fields_selected.items() if selected
                }

                if not expected_streams_to_selected_fields:
                    # "All fields" mode: every discoverable field should be selected.
                    for field, selected in fields_selected.items():
                        with self.subTest(field=field):
                            self.assertTrue(selected)
                else:
                    self.assertSetEqual(
                        expected_automatic_fields | expected_selected_fields,
                        actual_selected_fields,
                    )

    def run_sync_mode(self, conn_id: _MockConn):
        sync_job_name = runner.run_sync_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)
        return runner.examine_target_output_file(
            self, conn_id, self.expected_stream_names(), self.expected_primary_keys()
        )

    def run_and_verify_sync_mode(self, conn_id: _MockConn):
        sync_record_count = self.run_sync_mode(conn_id)
        self.assertGreater(sum(sync_record_count.values()), 0)
        return sync_record_count

    def run_and_verify_sync(self, conn_id: _MockConn):
        """Run sync and verify it succeeds."""
        sync_job_name = runner.run_sync_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)
        return runner.examine_target_output_file(
            self, conn_id, self.expected_stream_names(), self.expected_primary_keys()
        )

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


base_case = types.ModuleType("tap_tester.base_suite_tests.base_case")
base_case.BaseCase = BaseCase

_real_tap_tester.connections = connections
_real_tap_tester.menagerie = menagerie
_real_tap_tester.runner = runner

sys.modules["tap_tester.connections"] = connections
sys.modules["tap_tester.menagerie"] = menagerie
sys.modules["tap_tester.runner"] = runner
sys.modules["tap_tester.base_suite_tests.base_case"] = base_case

# If tap_tester submodules were already imported, patch those module objects
# in place so existing references in suite modules call the mock functions.
if _PREEXISTING_CONNECTIONS_MODULE is not None:
    _PREEXISTING_CONNECTIONS_MODULE.ensure_connection = _ensure_connection
    _PREEXISTING_CONNECTIONS_MODULE.select_catalog_and_fields_via_metadata = _select_catalog_and_fields_via_metadata
    _PREEXISTING_CONNECTIONS_MODULE.select_catalog_via_metadata = _select_catalog_via_metadata

if _PREEXISTING_MENAGERIE_MODULE is not None:
    _PREEXISTING_MENAGERIE_MODULE.get_exit_status = _get_exit_status
    _PREEXISTING_MENAGERIE_MODULE.verify_sync_exit_status = _verify_sync_exit_status
    _PREEXISTING_MENAGERIE_MODULE.verify_check_exit_status = _verify_check_exit_status
    _PREEXISTING_MENAGERIE_MODULE.get_state = _get_state
    _PREEXISTING_MENAGERIE_MODULE.set_state = _set_state
    _PREEXISTING_MENAGERIE_MODULE.get_catalogs = _get_catalogs
    _PREEXISTING_MENAGERIE_MODULE.get_annotated_schema = _get_annotated_schema
    _PREEXISTING_MENAGERIE_MODULE.select_catalog = _select_catalog

if _PREEXISTING_RUNNER_MODULE is not None:
    _PREEXISTING_RUNNER_MODULE.run_check_mode = _run_check_mode
    _PREEXISTING_RUNNER_MODULE.run_sync_mode = _run_sync_mode
    _PREEXISTING_RUNNER_MODULE.examine_target_output_file = _examine_target_output_file
    _PREEXISTING_RUNNER_MODULE.examine_target_output_for_fields = _examine_target_output_for_fields
    _PREEXISTING_RUNNER_MODULE.get_records_from_target_output = _get_records_from_target_output


# If tap_tester suite modules were imported before this file, they may still
# hold references to real connections/menagerie/runner objects. Rebind those
# module-level imports so every suite uses the mock stubs consistently.
for _suite_module in (
    "tap_tester.base_suite_tests.discovery_test",
    "tap_tester.base_suite_tests.bookmark_test",
    "tap_tester.base_suite_tests.start_date_test",
    "tap_tester.base_suite_tests.pagination_test",
):
    _mod = sys.modules.get(_suite_module)
    if _mod is None:
        continue
    if hasattr(_mod, "connections"):
        _mod.connections = connections
    if hasattr(_mod, "menagerie"):
        _mod.menagerie = menagerie
    if hasattr(_mod, "runner"):
        _mod.runner = runner


# ─── Mock data and test base (single-file mock support) ───────────────────

_SCHEMA_DIR = Path(__file__).parent.parent / "tap_outbrain" / "schemas"
_MOCK_DATES = [
    "2024-01-01",
    "2024-05-10",
    "2024-05-31",
]


def _pick_concrete_type(types: list[str]) -> str:
    """Return the first non-null JSON Schema type from a mixed list."""
    for schema_type in types:
        if schema_type != "null":
            return schema_type
    return "null"


def _generate_value(schema: dict, field_name: str = "", record_index: int = 0) -> Any:
    """Recursively synthesize one value that satisfies schema."""
    if not schema:
        return None

    raw_type = schema.get("type", "string")
    types = raw_type if isinstance(raw_type, list) else [raw_type]
    concrete = _pick_concrete_type(types)
    field_name_lower = field_name.lower()
    field_format = schema.get("format", "")

    if concrete == "null":
        return None
    if concrete == "integer":
        return record_index + 1
    if concrete == "number":
        return float(record_index + 1)
    if concrete == "boolean":
        return False
    if concrete == "string":
        if field_name_lower == "id" or field_name_lower.endswith("id"):
            return f"mock_{field_name}_{record_index + 1}"
        if field_format == "date":
            return _MOCK_DATES[record_index % len(_MOCK_DATES)]
        if field_format == "date-time" or "date" in field_name_lower:
            return f"{_MOCK_DATES[record_index % len(_MOCK_DATES)]}T10:00:00Z"
        if field_format == "uri" or "url" in field_name_lower or "link" in field_name_lower:
            return f"https://example.com/{field_name}"
        if "email" in field_name_lower:
            return f"mock_{field_name}@example.com"
        return f"mock_{field_name}"
    if concrete == "object":
        return {
            name: _generate_value(prop_schema, name, record_index)
            for name, prop_schema in schema.get("properties", {}).items()
        }
    if concrete == "array":
        items_schema = schema.get("items", {})
        return [_generate_value(items_schema, f"{field_name}_item", record_index)]

    return None


def _load_schema_file(schema_path: Path) -> dict:
    """Load and parse a JSON schema file."""
    try:
        with open(schema_path) as schema_file:
            return json.load(schema_file)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _generate_fixtures() -> dict:
    """Load all schemas and generate representative records."""
    fixtures = {}
    if not _SCHEMA_DIR.exists():
        return fixtures

    for schema_file in sorted(_SCHEMA_DIR.glob("*.json")):
        stream_name = schema_file.stem
        schema = _load_schema_file(schema_file)
        if not schema or "properties" not in schema:
            continue

        records = []
        for i in range(3):
            record = {
                field_name: _generate_value(field_schema, field_name, i)
                for field_name, field_schema in schema.get("properties", {}).items()
            }
            records.append(record)
        fixtures[stream_name] = records

    return fixtures


FIXTURES = _generate_fixtures()


class MockOutbrainBaseTest(BaseCase):
    """Integration-test base that runs the tap against mocked HTTP responses."""

    start_date = "2024-01-01T00:00:00Z"
    bookmark_format = "%Y-%m-%d"
    PARENT_STREAM = "parent-stream"

    PRIMARY_KEYS = "table-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    REPLICATION_KEYS = "valid-replication-keys"
    RESPECTS_START_DATE = "table-start-date-usage"

    INCREMENTAL = "INCREMENTAL"
    FULL_TABLE = "FULL_TABLE"

    @staticmethod
    def tap_name() -> str:
        return "tap-outbrain"

    @staticmethod
    def get_type() -> str:
        return "platform.outbrain"

    def get_properties(self, original: bool = True) -> dict:
        return {
            "start_date": self.start_date,
        }

    def get_credentials(self) -> dict:
        return {
            "account_id": "mock_account_001",
            "username": "mock@example.com",
            "password": "mock_password",
            "access_token": "mock_access_token",
        }

    @classmethod
    def expected_metadata(cls) -> dict:
        return {
            "campaign": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.RESPECTS_START_DATE: False,
                cls.API_LIMIT: 1,
            },
            "campaign_performance": {
                cls.PRIMARY_KEYS: {"campaignId", "fromDate"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"fromDate"},
                cls.RESPECTS_START_DATE: True,
                cls.LOOK_BACK_WINDOW: timedelta(days=2),
                cls.PARENT_STREAM: "campaign",
                cls.API_LIMIT: 10,
            },
        }

    @classmethod
    def expected_stream_names(cls) -> set:
        return set(cls.expected_metadata().keys())

    def expected_replication_method(self, stream=None) -> dict:
        replication_method = {
            table: properties.get(self.REPLICATION_METHOD, None)
            for table, properties in self.expected_metadata().items()
        }
        if stream is None:
            return replication_method
        return replication_method[stream]

    def expected_lookback_window(self, stream=None):
        lookback = {
            "campaign": timedelta(days=0),
            "campaign_performance": timedelta(days=2),
        }
        if stream is None:
            return lookback
        return lookback[stream]

    def _build_mock_request(self):
        """Return a side_effect callable for patching tap_outbrain.request."""

        def _side_effect(url, access_token, params=None):
            import datetime as dt
            from unittest.mock import MagicMock

            params = params or {}
            response = MagicMock()

            def _as_date_string(value, fallback="2024-01-01"):
                if value is None:
                    return fallback
                return str(value).split("T", 1)[0]

            if "/campaigns" in url:
                campaigns = FIXTURES.get("campaign", [])
                response.json.return_value = {
                    "campaigns": campaigns,
                    "totalCount": len(campaigns),
                }
                return response

            if "/periodic" in url or "performance" in url:
                perf_template = (FIXTURES.get("campaign_performance") or [{}])[0]

                from_date_str = _as_date_string(params.get("from", params.get("to")))
                to_date_str = _as_date_string(params.get("to", params.get("from")))

                try:
                    from_date_obj = dt.datetime.strptime(from_date_str, "%Y-%m-%d")
                    to_date_obj = dt.datetime.strptime(to_date_str, "%Y-%m-%d")
                except Exception:
                    from_date_obj = dt.datetime(2024, 1, 1)
                    to_date_obj = from_date_obj

                if to_date_obj < from_date_obj:
                    from_date_obj, to_date_obj = to_date_obj, from_date_obj

                span_days = (to_date_obj - from_date_obj).days + 1
                record_count = max(1, min(span_days, 30))
                results = []
                for i in range(record_count):
                    record_date = from_date_obj + dt.timedelta(days=i)
                    record_from_date = record_date.strftime("%Y-%m-%d")
                    metrics = {}
                    for key, value in perf_template.items():
                        if key in ("campaignId", "fromDate"):
                            continue
                        if key in {"impressions", "clicks", "conversions"}:
                            metrics[key] = str(int(value))
                        else:
                            metrics[key] = str(value)

                    results.append(
                        {
                            "metadata": {"fromDate": record_from_date},
                            "metrics": metrics,
                        }
                    )

                response.json.return_value = {
                    "totalResults": len(results),
                    "results": results,
                }
                return response

            response.json.return_value = {}
            return response

        return _side_effect
