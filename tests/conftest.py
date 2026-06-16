"""
Integration test toggle.

Set INTEGRATION_TEST_MODE to control which backend is used:

    live  — tap-tester + real credentials
  mock  — mocked HTTP responses, no account needed
    auto  — (default) live if TAP_OUTBRAIN_API_CREDS is set or the 4
                     TAP_OUTBRAIN_* credential vars are all set, otherwise mock

When mock mode is active this module injects lightweight stub modules into
sys.modules so existing test files (which import tap_tester.*) work unchanged.
"""
import os
import sys
import types
import importlib


_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)


def _is_mock_mode() -> bool:
    mode = os.environ.get("INTEGRATION_TEST_MODE", "auto").lower()
    if mode == "live":
        return False
    if mode == "mock":
        return True

    required_env = (
        "TAP_OUTBRAIN_ACCOUNT_ID",
        "TAP_OUTBRAIN_USERNAME",
        "TAP_OUTBRAIN_PASSWORD",
        "TAP_OUTBRAIN_ACCESS_TOKEN",
    )
    has_live_creds = bool(os.environ.get("TAP_OUTBRAIN_API_CREDS")) or all(
        os.environ.get(var) for var in required_env
    )

    # auto: fall back to mock when no API credentials are configured
    return not has_live_creds


IS_MOCK_MODE = _is_mock_mode()


def _has_tap_tester_base_suites() -> bool:
    """Return True when tap_tester base suite test classes are importable."""
    required_modules = (
        "tap_tester.base_suite_tests.all_fields_test",
        "tap_tester.base_suite_tests.automatic_fields_test",
        "tap_tester.base_suite_tests.bookmark_test",
        "tap_tester.base_suite_tests.discovery_test",
        "tap_tester.base_suite_tests.start_date_test",
        "tap_tester.base_suite_tests.pagination_test",
    )
    try:
        for module_name in required_modules:
            importlib.import_module(module_name)
        return True
    except Exception:
        return False


def pytest_ignore_collect(collection_path, config):
    """Skip integration-suite modules when tap_tester base suites are unavailable."""
    if _has_tap_tester_base_suites():
        return False

    integration_files = {
        "test_all_fields.py",
        "test_automatic_fields.py",
        "test_bookmark.py",
        "test_discovery.py",
        "test_pagenation.py",
        "test_start_date.py",
    }
    return os.path.basename(str(collection_path)) in integration_files

if IS_MOCK_MODE:
    # ── inject stub tap_tester package into sys.modules BEFORE any test
    # file is collected and imports tap_tester.* ──────────────────────────
    try:
        # CI commonly imports this as tests.conftest, so prefer package-relative import.
        from . import _mock_tap_tester as _stubs  # type: ignore  # noqa: E402
    except ImportError:
        # Fallback for direct/local invocation where tests is not treated as a package.
        import _mock_tap_tester as _stubs  # type: ignore  # noqa: E402
    from pathlib import Path  # noqa: E402

    _workspace_root = Path(__file__).resolve().parents[2]
    _tap_tester_pkg = _workspace_root / "tap-tester" / "tap_tester"
    _base_suite_pkg = _tap_tester_pkg / "base_suite_tests"

    # Top-level tap_tester module
    _tt = types.ModuleType("tap_tester")
    _tt.__path__ = [str(_tap_tester_pkg)]
    _tt.connections = _stubs.connections
    _tt.menagerie = _stubs.menagerie
    _tt.runner = _stubs.runner

    # tap_tester.base_suite_tests.base_case
    _bst = types.ModuleType("tap_tester.base_suite_tests")
    _bst.__path__ = [str(_base_suite_pkg)]
    _bstbc = types.ModuleType("tap_tester.base_suite_tests.base_case")
    _bstbc.BaseCase = _stubs.BaseCase
    _bst.base_case = _bstbc
    _tt.base_suite_tests = _bst

    for _name, _mod in [
        ("tap_tester", _tt),
        ("tap_tester.connections", _stubs.connections),
        ("tap_tester.menagerie", _stubs.menagerie),
        ("tap_tester.runner", _stubs.runner),
        ("tap_tester.base_suite_tests", _bst),
        ("tap_tester.base_suite_tests.base_case", _bstbc),
    ]:
        sys.modules.setdefault(_name, _mod)
