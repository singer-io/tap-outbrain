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

if IS_MOCK_MODE:
    # ── inject stub tap_tester package into sys.modules BEFORE any test
    # file is collected and imports tap_tester.* ──────────────────────────
    import _mock_tap_tester as _stubs  # noqa: E402
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
