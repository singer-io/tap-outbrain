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

    env = os.environ.copy()
    # Add tests/ to PYTHONPATH so `from base import ...` resolves for live tests,
    # and tests/mock_integration/ for mock tests.
    pythonpath = "tests" if mode == "live" else "tests/mock_integration"
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = pythonpath + (":" + existing if existing else "")

    command = [sys.executable, "-m", "pytest", *targets]
    return subprocess.call(command, env=env)


if __name__ == "__main__":
    raise SystemExit(main())


def _config_paths():
    """Resolve config file paths from environment or default locations."""
    env_path = os.environ.get("TAP_OUTBRAIN_CONFIG_JSON") or os.environ.get("OUTBRAIN_CONFIG_JSON")
    if env_path:
        yield Path(env_path)

    yield Path(__file__).resolve().parent / "config.json"
    yield Path(__file__).resolve().parents[1] / "config.json"


def _load_credentials_from_config():
    """Load Outbrain credentials from config.json file."""
    for config_path in _config_paths():
        if not config_path.is_file():
            continue

        with config_path.open("r", encoding="utf-8") as config_file:
            config = json.load(config_file)

        account_id = config.get("account_id")
        username = config.get("username")
        password = config.get("password")
        if account_id and username and password:
            return {
                "account_id": account_id,
                "username": username,
                "password": password,
            }

    return None


def _has_live_credentials() -> bool:
    """Check if live credentials are available."""
    if _load_credentials_from_config() is not None:
        return True

    required_env = (
        "TAP_OUTBRAIN_ACCOUNT_ID",
        "TAP_OUTBRAIN_USERNAME",
        "TAP_OUTBRAIN_PASSWORD",
    )
    return all(os.environ.get(var) for var in required_env)


def _has_tap_tester() -> bool:
    """Check if tap_tester is importable in the current Python environment."""
    try:
        import tap_tester  # noqa: F401
        return True
    except ImportError:
        return False


def _resolve_mode(requested_mode: str):
    """Determine which mode to run and return (mode, message)."""
    has_live_creds = _has_live_credentials()

    if requested_mode == "mock":
        return "mock", None

    if requested_mode == "live":
        if has_live_creds:
            return "live", None
        return "mock", "Live mode requested but Outbrain credentials are missing; running mock tests instead."

    if has_live_creds:
        if _has_tap_tester():
            return "live", None
        return "mock", "tap_tester not available in current environment; running mock tests instead."

    return "mock", "Outbrain credentials not found; running mock tests."


def _live_test_files() -> list:
    """Return list of live test files."""
    tests_dir = Path(__file__).resolve().parent
    return sorted(
        str(path.relative_to(tests_dir.parent))
        for path in tests_dir.glob("test*.py")
    )


def _mock_test_files() -> list:
    """Return list of mock test files."""
    tests_dir = Path(__file__).resolve().parent / "mock_integration"
    return sorted(
        str(path.relative_to(tests_dir.parent.parent))
        for path in tests_dir.glob("test*.py")
    )


def main():
    """Run integration tests based on mode."""
    parser = argparse.ArgumentParser(
        description="Run tap-outbrain integration tests",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python tests/run_integration_tests.py --mode live
  python tests/run_integration_tests.py --mode mock
  python tests/run_integration_tests.py --mode auto
        """,
    )
    parser.add_argument(
        "--mode",
        choices=["auto", "live", "mock"],
        default="auto",
        help="Execution mode (default: auto)",
    )

    args = parser.parse_args()

    mode, message = _resolve_mode(args.mode)
    if message:
        print(f"INFO: {message}")

    tests_dir = Path(__file__).resolve().parent

    if mode == "live":
        test_files = _live_test_files()
        pythonpath_dir = str(tests_dir)
    else:
        test_files = _mock_test_files()
        pythonpath_dir = str(tests_dir / "mock_integration")

    # Set environment variable for test execution and add tests dir to PYTHONPATH
    # so bare `from base import ...` imports resolve correctly
    env = os.environ.copy()
    env["INTEGRATION_TEST_MODE"] = mode
    existing_path = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = pythonpath_dir + (":" + existing_path if existing_path else "")

    print(f"\nRunning {mode.upper()} tests...")
    print(f"Test files: {', '.join(test_files)}\n")

    # Run tests using pytest for better output; fall back to unittest
    cmd = [sys.executable, "-m", "pytest"] + test_files + ["-v"]

    result = subprocess.run(cmd, env=env)
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
