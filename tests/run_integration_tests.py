"""Run tap-outbrain integration tests by execution mode.

Usage examples:
  python tests/run_integration_tests.py --mode live
  python tests/run_integration_tests.py --mode mock
  python tests/run_integration_tests.py --mode auto
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


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
        return "live", None

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

    if mode == "live":
        test_files = _live_test_files()
        env_var = "INTEGRATION_TEST_MODE"
    else:
        test_files = _mock_test_files()
        env_var = "INTEGRATION_TEST_MODE"

    # Set environment variable for test execution
    env = os.environ.copy()
    env[env_var] = mode

    print(f"\nRunning {mode.upper()} tests...")
    print(f"Test files: {', '.join(test_files)}\n")

    # Run tests
    cmd = [sys.executable, "-m", "unittest"] + [
        f.replace("/", ".").replace(".py", "")
        for f in test_files
    ]

    result = subprocess.run(cmd, env=env)
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
