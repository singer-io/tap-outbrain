"""Run tap-outbrain integration tests by execution mode.

Usage examples:
  python tests/run_integration_tests.py --mode live
  python tests/run_integration_tests.py --mode mock
  python tests/run_integration_tests.py --mode auto
"""

import argparse
import glob
import os
import subprocess
import sys


LIVE_TEST_FILES = [
    "tests/test_all_fields.py",
    "tests/test_automatic_fields.py",
    "tests/test_bookmark.py",
    "tests/test_discovery.py",
    "tests/test_pagenation.py",
    "tests/test_start_date.py",
]


TESTER_VENV = "/usr/local/share/virtualenvs/tap-tester"


def _tester_site_packages():
    """Return tap-tester site-packages path, or None if not found."""
    matches = glob.glob(os.path.join(TESTER_VENV, "lib", "python*", "site-packages"))
    return matches[0] if matches else None


def _credentials_are_valid() -> bool:
    """Return True only if the Outbrain credentials can successfully authenticate."""
    username = os.environ.get("TAP_OUTBRAIN_USERNAME", "")
    password = os.environ.get("TAP_OUTBRAIN_PASSWORD", "")
    if not username or not password:
        return False
    try:
        import urllib.request
        import base64
        import json as _json
        basic = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        req = urllib.request.Request(
            "https://api.outbrain.com/amplify/v0.1/login",
            headers={"Authorization": f"Basic {basic}"},
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            payload = _json.loads(resp.read().decode("utf-8"))
            return bool(payload.get("OB-TOKEN-V1"))
    except Exception:
        return False


def _resolve_mode(requested_mode: str) -> str:
    if requested_mode in {"live", "mock"}:
        return requested_mode

    required_env = (
        "TAP_OUTBRAIN_ACCOUNT_ID",
        "TAP_OUTBRAIN_USERNAME",
        "TAP_OUTBRAIN_PASSWORD",
        "TAP_OUTBRAIN_ACCESS_TOKEN",
    )
    has_live_env = bool(os.environ.get("TAP_OUTBRAIN_API_CREDS")) or all(
        os.environ.get(var) for var in required_env
    )
    if not has_live_env:
        return "mock"

    has_tap_tester = _tester_site_packages() is not None
    if not has_tap_tester:
        return "mock"

    has_valid_creds = _credentials_are_valid()
    return "live" if has_valid_creds else "mock"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run integration tests by mode")
    parser.add_argument(
        "--mode",
        choices=["live", "mock", "auto"],
        default=os.environ.get("INTEGRATION_TEST_MODE", "auto").lower(),
        help="live runs tests/*.py, mock runs tests/mock_integration",
    )
    args = parser.parse_args()

    mode = _resolve_mode(args.mode)
    targets = LIVE_TEST_FILES if mode == "live" else ["tests/mock_integration"]

    print("Selected integration test mode:", mode)
    print("Running:", " ".join([sys.executable, "-m", "pytest", *targets]))

    env = os.environ.copy()
    # tests/ (live) or tests/mock_integration (mock) must be on PYTHONPATH
    # so bare `from base import ...` imports resolve.
    pythonpath_parts = ["tests" if mode == "live" else "tests/mock_integration"]

    if mode == "live":
        # tap_tester lives in its own virtualenv which does not have pytest.
        # Run pytest with the current (tap-outbrain) Python that has pytest,
        # but expose tap-tester's site-packages so `import tap_tester` works.
        tester_sp = _tester_site_packages()
        if tester_sp:
            pythonpath_parts.append(tester_sp)

    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = ":".join(pythonpath_parts) + (":" + existing if existing else "")

    command = [sys.executable, "-m", "pytest", *targets]
    return subprocess.call(command, env=env)


if __name__ == "__main__":
    raise SystemExit(main())
