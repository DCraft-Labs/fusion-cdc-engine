"""Pytest config: make ``transform-worker`` importable as a top-level
package so the tests can ``from iceberg_writer import ...`` and
``from engine import ...`` exactly like the worker process does.
"""
import os
import sys

_TW_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _TW_DIR not in sys.path:
    sys.path.insert(0, _TW_DIR)
