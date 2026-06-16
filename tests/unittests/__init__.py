"""Unit test package bootstrap.

When tests are launched via tap-tester from a different working directory,
the tap project root may not be on sys.path. Add it so imports like
``import tap_outbrain`` remain stable.
"""

import os
import sys


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
