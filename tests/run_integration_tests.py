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


def _has_stitch_source_access() -> bool:
    """Return True when the current Stitch account can create platform.outbrain sources."""
    host = os.environ.get("STITCH_API_HOST", "").rstrip("/")
    email = os.environ.get("STITCH_API_EMAIL", "")
    password = os.environ.get("SANDBOX_PASSWORD") or os.environ.get("STITCH_API_PASSWORD", "")
    if not host or not email or not password:
        return False

    try:
        import json as _json
        import urllib.error
        import urllib.request

        session_req = urllib.request.Request(
            f"{host}/session",
            data=_json.dumps({"email": email, "password": password}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(session_req, timeout=10) as resp:
            set_cookie = resp.headers.get("Set-Cookie", "")

        token = ""
        for part in set_cookie.split(";"):
            part = part.strip()
            if part.startswith("DASHSESS2="):
                token = part.split("=", 1)[1]
                break
        if not token:
            return False

        probe_payload = {
            "display_name": "tap-outbrain-access-check",
            "type": "platform.outbrain",
            "properties": {"frequency_in_minutes": "60"},
        }
        probe_req = urllib.request.Request(
            f"{host}/v4/sources",
            data=_json.dumps(probe_payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            method="POST",
        )

        try:
            with urllib.request.urlopen(probe_req, timeout=10):
                return True
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore")
            try:
                payload = _json.loads(body)
            except Exception:
                payload = {}

            if isinstance(payload, dict) and payload.get("error") == "AccessDenied":
                return False

            # Any non-AccessDenied validation error means source type is available.
            return True
    except Exception:
        return False


def _resolve_mode(requested_mode: str):
    if requested_mode in {"live", "mock"}:
        return requested_mode, None

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
        return "mock", "Outbrain credentials not found; running mock tests."

    has_tap_tester = _tester_site_packages() is not None
    if not has_tap_tester:
        return "mock", "tap-tester site-packages not found; running mock tests."

    # tap-tester's InMemoryBackend uses STITCH_TAP_PATH to invoke the tap.
    # If it is not set (or points to a non-existent file), the tap cannot run.
    tap_path = os.environ.get("STITCH_TAP_PATH", "")
    if not tap_path or not os.path.isfile(tap_path):
        return "mock", "STITCH_TAP_PATH is missing/invalid; running mock tests."

    has_valid_creds = _credentials_are_valid()
    if not has_valid_creds:
        return "mock", "Outbrain credentials failed login validation; running mock tests."

    if not _has_stitch_source_access():
        return "mock", "Stitch account cannot access source type platform.outbrain; running mock tests."

    return "live", None


def main() -> int:
    parser = argparse.ArgumentParser(description="Run integration tests by mode")
    parser.add_argument(
        "--mode",
        choices=["live", "mock", "auto"],
        default=os.environ.get("INTEGRATION_TEST_MODE", "auto").lower(),
        help="live runs tests/*.py, mock runs tests/mock_integration",
    )
    args = parser.parse_args()

    mode, note = _resolve_mode(args.mode)
    targets = LIVE_TEST_FILES if mode == "live" else ["tests/mock_integration"]

    print("Selected integration test mode:", mode)
    if note:
        print(note)
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
