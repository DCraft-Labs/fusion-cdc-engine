"""v1.2.30 parallel-load correctness regression tests.

This module is the canonical filename requested by the v1.2.30 release task
(``test_parallel_load_correctness.py``). The five regression tests already
live in ``test_v130_correctness.py`` (written first during the v1.2.30 fix
pass); we re-export them here so pytest collects them under both module
names — keeping the release-task contract while avoiding code duplication.

Tests cover the five defects fixed in v1.2.30:
  1. ``test_partition_loop_continues_past_short_chunk`` — Defect A: a bounded
     partition continues fetching while ``last_pk < pk_end``; a short chunk
     near the boundary does NOT trigger a premature DONE.
  2. ``test_all_partitions_get_checkpoint`` — Defect B: K=4 concurrent
     partitions each get a checkpoint row (composite key
     connection_id+stream_id+chunk_seq); every exit path reports.
  3. ``test_rows_estimated_from_partitioning`` — Defect C: ``rows_estimated``
     is density-based at enqueue, stamped by the worker on the FIRST
     checkpoint, never overwritten; ``progress_pct`` < 100 until done.
  4. ``test_no_duplicate_dequeue`` — Defect D: two workers can't dequeue the
     same task_id (atomic BLMOVE to a per-worker in-flight list).
  5. ``test_premature_done_fix_regression`` — Defect A regression: a 25M-key
     range with chunk_size 10k and 5 chunks at 50k rows must NOT mark DONE.
"""
import os
import sys

# Re-export the test classes from the sibling module so pytest collects them
# under this module name as well. The sibling module is on sys.path because
# it lives in the same directory.
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

# pytest-friendly aliases matching the release-task test names.
test_partition_loop_continues_past_short_chunk = (
    TestPartitionLoopContinuesPastShortChunk.test_dense_range_fetches_both_chunks_and_stops_at_boundary
)
test_all_partitions_get_checkpoint = (
    TestAllPartitionsGetCheckpoint.test_four_concurrent_partitions_all_report
)
test_rows_estimated_from_partitioning = (
    TestRowsEstimatedFromPartitioning.test_worker_stamps_payload_estimate_on_first_checkpoint
)
test_no_duplicate_dequeue = (
    TestNoDuplicateDequeue.test_two_workers_never_get_same_task
)
test_premature_done_fix_regression = (
    TestPrematureDoneFixRegression.test_25m_range_does_not_done_at_50k
)
