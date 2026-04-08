"""pytest configuration for the tap-outbrain integration-test suite.

Adding the ``tests/`` directory to ``sys.path`` here ensures that the shared
base module can always be imported with a plain ``from base import …`` statement,
regardless of whether pytest is invoked from the project root
(``pytest tests -v``) or directly from inside the ``tests/`` directory.
"""
import os
import sys

# Make ``tests/`` importable so that ``from base import OutbrainBaseTest``
# works in all integration-test files without a try/except fallback.
sys.path.insert(0, os.path.dirname(__file__))
