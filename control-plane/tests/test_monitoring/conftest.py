"""
conftest for test_monitoring — adds cdc-workers to sys.path so that tests
importing `from connectors.xxx import ...` can find the worker connectors.
"""
import sys
import os

# The cdc-workers connectors live one level up from control-plane
_CDC_WORKERS = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "cdc-workers")
)
if _CDC_WORKERS not in sys.path:
    sys.path.insert(0, _CDC_WORKERS)
