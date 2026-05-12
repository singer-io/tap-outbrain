"""Dynamic mock-data generator for tap-outbrain tests.

At import time (i.e. the moment the test session first touches mock mode) this
module reads every JSON Schema from ``tap_outbrain/schemas/``, synthesises
representative records, and caches the full API envelope in the module-level dict
``FIXTURES``.

``mock_base.py`` reads directly from ``FIXTURES``; no fixture files are
read from disk during the test run.

Value-generation rules
──────────────────────
* ``integer``         → ``1``
* ``number``          → ``1.0``
* ``boolean``         → ``False``
* ``string``

  * ``format: date`` → ``YYYY-MM-DD`` date string
  * ``format: date-time`` *or* field name contains "date" → ISO-8601 datetime
  * ``format: uri``    *or* field name contains "url" / "link" → example URL
  * field name contains "email" → example e-mail address
  * anything else → ``"mock_<field_name>"``

* ``object``          → recurse over ``properties``; free-form → ``{}``
* ``array``           → single-element list built from ``items`` schema
* ``null``-only       → ``None``

The same schema always produces the same value, which keeps test assertions
deterministic.
"""
from __future__ import annotations

import json
import datetime
from pathlib import Path
from typing import Any


_SCHEMA_DIR = Path(__file__).parent.parent / "tap_outbrain" / "schemas"

# A set of representative dates that span a wide range so that
# start-date and bookmark tests can distinguish sync results:
#   - 2024-01-01: realistic start date
#   - 2024-05-10: mid-period date
#   - 2024-05-31: recent date
_MOCK_DATES = [
    "2024-01-01",
    "2024-05-10",
    "2024-05-31",
]


def _pick_concrete_type(types: list[str]) -> str:
    """Return the first non-null JSON Schema type from a mixed list."""
    for t in types:
        if t != "null":
            return t
    return "null"


def _generate_value(schema: dict, field_name: str = "", record_index: int = 0) -> Any:
    """Recursively synthesise one value that satisfies *schema*.

    The result is fully deterministic: the same schema + field_name always
    produces the same value.
    """
    if not schema:
        return None

    raw_type = schema.get("type", "string")
    types: list[str] = raw_type if isinstance(raw_type, list) else [raw_type]
    concrete = _pick_concrete_type(types)
    fmt: str = schema.get("format", "")
    fname_lower = field_name.lower()

    if concrete == "null":
        return None

    if concrete == "integer":
        return record_index + 1  # gives ids 1, 2, 3 per record

    if concrete == "number":
        return float(record_index + 1)

    if concrete == "boolean":
        return False

    if concrete == "string":
        if fmt == "date":
            # Return a YYYY-MM-DD date string
            return _MOCK_DATES[record_index % len(_MOCK_DATES)]
        elif fmt == "date-time" or "date" in fname_lower:
            # Return an ISO-8601 datetime
            return f"{_MOCK_DATES[record_index % len(_MOCK_DATES)]}T10:00:00Z"
        elif fmt == "uri" or "url" in fname_lower or "link" in fname_lower:
            return f"https://example.com/{field_name}"
        elif "email" in fname_lower:
            return f"mock_{field_name}@example.com"
        else:
            return f"mock_{field_name}"

    if concrete == "object":
        result = {}
        for prop_name, prop_schema in schema.get("properties", {}).items():
            result[prop_name] = _generate_value(prop_schema, prop_name, record_index)
        return result

    if concrete == "array":
        items_schema = schema.get("items", {})
        return [_generate_value(items_schema, f"{field_name}_item", record_index)]

    return None


def _load_schema_file(schema_path: Path) -> dict:
    """Load and parse a JSON schema file."""
    try:
        with open(schema_path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _generate_fixtures() -> dict:
    """Load all schemas and generate representative records.

    Returns a dict mapping stream names to lists of generated records.
    """
    fixtures = {}

    if not _SCHEMA_DIR.exists():
        return fixtures

    for schema_file in sorted(_SCHEMA_DIR.glob("*.json")):
        stream_name = schema_file.stem  # e.g., "campaign" from "campaign.json"
        schema = _load_schema_file(schema_file)

        if not schema or "properties" not in schema:
            continue

        # Generate 3 representative records to cover different scenarios
        records = []
        for i in range(3):
            record = {}
            for field_name, field_schema in schema.get("properties", {}).items():
                record[field_name] = _generate_value(field_schema, field_name, i)
            records.append(record)

        fixtures[stream_name] = records

    return fixtures


# Generate and cache fixtures at module import time
FIXTURES = _generate_fixtures()
