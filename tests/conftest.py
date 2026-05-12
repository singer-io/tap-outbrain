"""
Integration test toggle.

Set INTEGRATION_TEST_MODE to control which backend is used:

  live  — tap-tester + real credentials (requires TAP_OUTBRAIN_API_CREDS)
  mock  — mocked HTTP responses, no account needed
  auto  — (default) live if TAP_OUTBRAIN_API_CREDS is set, otherwise mock

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
    # auto: fall back to mock when no API credentials are configured
    return not bool(os.environ.get("TAP_OUTBRAIN_API_CREDS"))


IS_MOCK_MODE = _is_mock_mode()

if IS_MOCK_MODE:
    # ── inject stub tap_tester package into sys.modules BEFORE any test
    # file is collected and imports tap_tester.* ──────────────────────────
    import _mock_tap_tester as _stubs  # noqa: E402

    # Top-level tap_tester module
    _tt = types.ModuleType("tap_tester")
    _tt.connections = _stubs.connections
    _tt.menagerie = _stubs.menagerie
    _tt.runner = _stubs.runner

    # tap_tester.base_suite_tests.base_case
    _bst = types.ModuleType("tap_tester.base_suite_tests")
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
