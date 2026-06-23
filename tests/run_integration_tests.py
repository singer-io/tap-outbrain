"""Run tap-outbrain integration tests by execution mode.

Usage examples:
  python tests/run_integration_tests.py --mode live
  python tests/run_integration_tests.py --mode mock
  python tests/run_integration_tests.py --mode auto
"""

import argparse
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


def _resolve_mode(requested_mode: str) -> str:
    if requested_mode in {"live", "mock"}:
        return requested_mode

    required_env = (
        "TAP_OUTBRAIN_ACCOUNT_ID",
        "TAP_OUTBRAIN_USERNAME",
        "TAP_OUTBRAIN_PASSWORD",
        "TAP_OUTBRAIN_ACCESS_TOKEN",
    )
    has_live_creds = bool(os.environ.get("TAP_OUTBRAIN_API_CREDS")) or all(
        os.environ.get(var) for var in required_env
    )
    return "live" if has_live_creds else "mock"


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

    command = [sys.executable, "-m", "pytest", *targets]
    return subprocess.call(command)


if __name__ == "__main__":
    raise SystemExit(main())
