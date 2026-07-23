"""v1.2.30 parallel-load correctness regression tests.

This is the canonical filename requested by the v1.2.30 release task
(``test_parallel_load_correctness.py``). The five regression tests live in
``test_v130_correctness.py`` (written first during the v1.2.30 fix pass);
we re-export the test CLASSES here so pytest collects them under this module
name as well. (Re-exporting classes — not bound methods — avoids the
``fixture 'self' not found`` error that module-level function aliases would
trigger.)

Tests cover the five defects fixed in v1.2.30:
  1. ``TestPartitionLoopContinuesPastShortChunk`` — Defect A: a bounded
     partition continues fetching while ``last_pk < pk_end``; a short chunk
     near the boundary does NOT trigger a premature DONE.
  2. ``TestAllPartitionsGetCheckpoint`` — Defect B: K=4 concurrent
     partitions each get a checkpoint row (composite key
     connection_id+stream_id+chunk_seq); every exit path reports.
  3. ``TestRowsEstimatedFromPartitioning`` — Defect C: ``rows_estimated``
     is density-based at enqueue, stamped by the worker on the FIRST
     checkpoint, never overwritten; ``progress_pct`` < 100 until done.
  4. ``TestNoDuplicateDequeue`` — Defect D: two workers can't dequeue the
     same task_id (atomic BLMOVE to a per-worker in-flight list).
  5. ``TestPrematureDoneFixRegression`` — Defect A regression: a 25M-key
     range with chunk_size 10k and 5 chunks at 50k rows must NOT mark DONE.
"""
import os
import sys

_SIBLING_DIR = os.path.dirname(os.path.abspath(__file__))
if _SIBLING_DIR not in sys.path:
    sys.path.insert(0, _SIBLING_DIR)

from test_v130_correctness import (  # noqa: E402,F401
    TestPartitionLoopContinuesPastShortChunk,
    TestAllPartitionsGetCheckpoint,
    TestRowsEstimatedFromPartitioning,
    TestNoDuplicateDequeue,
    TestPrematureDoneFixRegression,
)
