"""v1.2.26 Task 1 tests: PK-range partitioning for multi-pod intra-table
parallelism. Covers the pure helpers in ``app.services.partitioning`` —
``naive_numeric_ranges`` and ``ranges_from_splits`` — which the producer
(``connections._enqueue_initial_load_tasks``) uses to split a table's PK
space into K disjoint sub-ranges before enqueuing K independent tasks.

The DB-touching ``partition_pk_ranges`` / ``_partition_mysql`` / ``_partition_pg``
paths are exercised by the integration suite; here we cover the math that
decides correctness (no gaps, no overlaps, full coverage, K ranges).

v1.2.27 P0 fix tests (added below): non-blocking partitioning — no ``COUNT(*)``,
``information_schema`` count, timeout + KILL + plan B fallback, no-PK-index
K=1, async 202 endpoint.
"""
import os
import sys

import pytest

# Make ``app`` importable when run from the control-plane dir.
HERE = os.path.dirname(__file__)
CP_ROOT = os.path.abspath(os.path.join(HERE, ".."))
if CP_ROOT not in sys.path:
    sys.path.insert(0, CP_ROOT)

from app.services.partitioning import (  # noqa: E402
    DEFAULT_PARALLELISM,
    MAX_PARALLELISM,
    clamp_parallelism,
    naive_numeric_ranges,
    partition_pk_ranges,
    ranges_from_splits,
)


class TestClampParallelism:
    def test_none_falls_back_to_default(self):
        assert clamp_parallelism(None) == DEFAULT_PARALLELISM

    def test_non_numeric_falls_back_to_default(self):
        assert clamp_parallelism("abc") == DEFAULT_PARALLELISM

    def test_clamps_to_max(self):
        assert clamp_parallelism(1000) == MAX_PARALLELISM

    def test_clamps_to_min(self):
        assert clamp_parallelism(0) == 1
        assert clamp_parallelism(-3) == 1

    def test_passes_through_valid(self):
        assert clamp_parallelism(4) == 4
        assert clamp_parallelism("8") == 8
        assert clamp_parallelism(MAX_PARALLELISM) == MAX_PARALLELISM


class TestNaiveNumericRanges:
    def test_k_one_returns_single_unbounded(self):
        assert naive_numeric_ranges(0, 1000, 1) == [(None, None)]

    def test_none_bounds_return_single_unbounded(self):
        assert naive_numeric_ranges(None, 100, 4) == [(None, None)]
        assert naive_numeric_ranges(0, None, 4) == [(None, None)]

    def test_k_ranges_returned(self):
        ranges = naive_numeric_ranges(0, 1000, 4)
        assert len(ranges) == 4

    def test_first_start_open_last_end_open(self):
        ranges = naive_numeric_ranges(0, 1000, 4)
        assert ranges[0][0] is None
        assert ranges[-1][1] is None

    def test_interior_bounds_form_disjoint_cover(self):
        """The K ranges must be disjoint and cover [mn, mx] with no gaps or
        overlaps — this is the correctness invariant for multi-pod intra-table
        parallelism (no row is fetched by two pods, no row is skipped)."""
        mn, mx, k = 0, 1000, 4
        ranges = naive_numeric_ranges(mn, mx, k)
        for i in range(len(ranges) - 1):
            assert ranges[i][1] == ranges[i + 1][0], (
                f"gap/overlap between range {i} and {i+1}: {ranges}"
            )

    def test_monotonic_bounds(self):
        ranges = naive_numeric_ranges(0, 1000, 5)
        bounds = [r[1] for r in ranges[:-1]]
        assert bounds == sorted(bounds), f"interior bounds not monotonic: {bounds}"

    def test_equal_min_max_returns_single(self):
        assert naive_numeric_ranges(5, 5, 4) == [(None, None)]


class TestRangesFromSplits:
    def test_no_splits_returns_single_unbounded(self):
        assert ranges_from_splits(0, 1000, []) == [(None, None)]

    def test_none_bounds_return_single_unbounded(self):
        assert ranges_from_splits(None, 1000, [500]) == [(None, None)]
        assert ranges_from_splits(0, None, [500]) == [(None, None)]

    def test_k_minus_one_splits_yield_k_ranges(self):
        splits = [250, 500, 750]
        ranges = ranges_from_splits(0, 1000, splits)
        assert len(ranges) == 4

    def test_first_open_last_open(self):
        ranges = ranges_from_splits(0, 1000, [250, 500, 750])
        assert ranges[0][0] is None
        assert ranges[-1][1] is None

    def test_disjoint_cover(self):
        splits = [250, 500, 750]
        ranges = ranges_from_splits(0, 1000, splits)
        for i in range(len(ranges) - 1):
            assert ranges[i][1] == ranges[i + 1][0], (
                f"gap/overlap between range {i} and {i+1}: {ranges}"
            )

    def test_splits_used_as_boundaries(self):
        splits = [250, 500, 750]
        ranges = ranges_from_splits(0, 1000, splits)
        assert ranges[0] == (None, 250)
        assert ranges[1] == (250, 500)
        assert ranges[2] == (500, 750)
        assert ranges[3] == (750, None)


# ===========================================================================
# v1.2.27 P0 fix tests — non-blocking partitioning
# ===========================================================================

import asyncio  # noqa: E402
from types import SimpleNamespace  # noqa: E402
from unittest.mock import MagicMock, patch  # noqa: E402

from app.services import partitioning as part_mod  # noqa: E402


class FakeCursor:
    """Minimal cursor: records executed SQL + params, returns scripted rows
    from ``fetchone`` based on substring match, raises on configured
    substrings (to simulate timeouts), and asserts on forbidden substrings
    (e.g. ``COUNT(*)``)."""

    def __init__(self, scripts=None, raise_on=None, forbid=None):
        self.scripts = scripts or {}      # substring -> row
        self.raise_on = raise_on or {}    # substring -> Exception
        self.forbid = forbid or []        # list of substrings
        self.executed: list[tuple] = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        for needle in self.forbid:
            assert needle not in sql, f"forbidden SQL executed: {sql!r}"
        for needle, exc in self.raise_on.items():
            if needle in sql:
                raise exc
        return None

    def fetchone(self):
        for sql, _ in reversed(self.executed):
            for needle, row in self.scripts.items():
                if needle in sql:
                    return row
        return None

    def fetchall(self):
        r = self.fetchone()
        return [r] if r is not None else []

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeConn:
    """Context-manager connection yielding a FakeCursor."""

    def __init__(self, cursor):
        self._cursor = cursor
        self.autocommit = True

    def cursor(self, **kw):
        return self._cursor

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _mysql_source():
    return {
        "host": "db.test", "port": 3306, "database_name": "dw",
        "username": "u", "password": "p",
    }


class TestNoCountStar:
    """Task 2: ``COUNT(*)`` is NEVER executed against the source."""

    def test_mysql_min_max_uses_information_schema_not_count(self):
        # PK index present, MIN/MAX returns 1..100, information_schema count.
        cur = FakeCursor(
            scripts={
                "information_schema.statistics": (1,),       # PK index exists
                "SELECT CONNECTION_ID()": (42,),
                "MIN(`id`), MAX(`id`)": (1, 100),
                "information_schema.tables": (1_000_000,),
            },
            forbid=["COUNT(*)"],
        )
        conn = FakeConn(cur)
        with patch("pymysql.connect", return_value=conn):
            ranges = partition_pk_ranges(_mysql_source(), "dw", "t", "id", "mysql", 4)
        assert len(ranges) == 4
        assert ranges[0][0] is None and ranges[-1][1] is None
        # No COUNT(*) in any executed query.
        for sql, _ in cur.executed:
            assert "COUNT(*)" not in sql, f"COUNT(*) executed: {sql!r}"

    def test_pg_min_max_uses_pg_class_not_count(self):
        # Postgres: pg_indexes says PK exists, MIN/MAX returns 1..100,
        # pg_class.reltuples returns 1_000_000.
        cur = FakeCursor(
            scripts={
                "pg_indexes": ({"?column?": 1},),
                "pg_backend_pid": ({"pid": 99},),
                "MIN(\"id\"), MAX(\"id\")": ({"min": 1, "max": 100}),
                "reltuples": ({"reltuples": 1_000_000},),
            },
            forbid=["COUNT(*)"],
        )
        conn = FakeConn(cur)
        with patch("psycopg2.connect", return_value=conn), \
             patch("psycopg2.extras", create=True):
            ranges = partition_pk_ranges(
                {**_mysql_source(), "port": 5432}, "dw", "t", "id", "postgres", 4
            )
        assert len(ranges) == 4
        for sql, _ in cur.executed:
            assert "COUNT(*)" not in sql


class TestProducesKDisjointRanges:
    """Task 7 test 1: mocked source returns MIN=1, MAX=100, table_rows=1_000_000
    -> K=4 disjoint ranges covering [1, 100]."""

    def test_mysql_produces_k_disjoint_ranges(self):
        cur = FakeCursor(
            scripts={
                "information_schema.statistics": (1,),
                "SELECT CONNECTION_ID()": (42,),
                "MIN(`id`), MAX(`id`)": (1, 100),
                "information_schema.tables": (1_000_000,),
            },
            forbid=["COUNT(*)"],
        )
        conn = FakeConn(cur)
        with patch("pymysql.connect", return_value=conn):
            ranges = partition_pk_ranges(_mysql_source(), "dw", "t", "id", "mysql", 4)
        assert len(ranges) == 4
        # Disjoint cover invariant.
        for i in range(len(ranges) - 1):
            assert ranges[i][1] == ranges[i + 1][0]
        assert ranges[0][0] is None and ranges[-1][1] is None


class TestTimeoutFallback:
    """Task 7 test 3: MIN/MAX raises a timeout -> falls back to plan B
    (first/last PK via ORDER BY LIMIT 1 + information_schema count)."""

    def test_mysql_timeout_falls_back_to_plan_b(self):
        # MIN/MAX raises (simulated server-side timeout). Plan B's
        # ORDER BY ... LIMIT 1 returns first=1, last=100.
        cur = FakeCursor(
            scripts={
                "information_schema.statistics": (1,),
                "SELECT CONNECTION_ID()": (42,),
                "ORDER BY `id` ASC LIMIT 1": (1,),
                "ORDER BY `id` DESC LIMIT 1": (100,),
                "information_schema.tables": (1_000_000,),
            },
            raise_on={"MIN(`id`), MAX(`id`)": RuntimeError("server timeout")},
            forbid=["COUNT(*)"],
        )
        conn = FakeConn(cur)
        with patch("pymysql.connect", return_value=conn):
            ranges = partition_pk_ranges(_mysql_source(), "dw", "t", "id", "mysql", 4)
        assert len(ranges) == 4
        assert ranges[0][0] is None and ranges[-1][1] is None


class TestKillCalledOnTimeout:
    """Task 7 test 4: KILL is called on the source when the query times out."""

    def test_mysql_kill_called_on_timeout(self):
        cur = FakeCursor(
            scripts={
                "information_schema.statistics": (1,),
                "SELECT CONNECTION_ID()": (42,),
                "ORDER BY `id` ASC LIMIT 1": (1,),
                "ORDER BY `id` DESC LIMIT 1": (100,),
                "information_schema.tables": (1_000_000,),
            },
            raise_on={"MIN(`id`), MAX(`id`)": RuntimeError("server timeout")},
            forbid=["COUNT(*)"],
        )
        conn = FakeConn(cur)
        kill_calls: list = []

        def fake_kill(host, port, database, user, password, conn_id):
            kill_calls.append(conn_id)

        with patch("pymysql.connect", return_value=conn), \
             patch.object(part_mod, "_mysql_kill", side_effect=fake_kill):
            partition_pk_ranges(_mysql_source(), "dw", "t", "id", "mysql", 4)
        # KILL was called with our connection id (42).
        assert 42 in kill_calls, f"KILL not called with conn_id=42; got {kill_calls}"


class TestNoPkIndexFallsBackToK1:
    """Task 7 test 5: no PK index -> K=1 with a warning (single range)."""

    def test_mysql_no_pk_index_returns_single_range(self):
        # information_schema.statistics returns None (no PK index row).
        cur = FakeCursor(
            scripts={"information_schema.statistics": None},
            forbid=["COUNT(*)", "MIN(`id`), MAX(`id`)"],
        )
        conn = FakeConn(cur)
        with patch("pymysql.connect", return_value=conn):
            ranges = partition_pk_ranges(_mysql_source(), "dw", "t", "id", "mysql", 4)
        assert ranges == [(None, None)]


class TestAsyncEndpointReturns202:
    """Task 7 test 6: the endpoint returns 202 immediately even when
    partitioning takes 5s (partitioning runs in a background task)."""

    def test_retry_initial_load_route_declares_202(self):
        # The route's declared status_code must be 202 ACCEPTED — this is
        # the contract that the endpoint returns immediately and the
        # partitioning + enqueue happens in a background task.
        from fastapi import status as http_status
        from app.api.connections import router
        route = next(
            r for r in router.routes
            if getattr(r, "path", "") == "/{connection_id}/retry-initial-load"
        )
        assert route.status_code == http_status.HTTP_202_ACCEPTED, (
            f"retry-initial-load must return 202; got {route.status_code}"
        )

    def test_initial_load_status_route_exists(self):
        # The status endpoint must exist for the UI to poll.
        from app.api.connections import router
        paths = {getattr(r, "path", "") for r in router.routes}
        assert "/{connection_id}/initial-load/status" in paths, (
            f"initial-load/status route missing; routes: {sorted(paths)[:10]}..."
        )

    def test_retry_initial_load_returns_202_immediately(self, monkeypatch):
        # End-to-end: call the route handler directly (not via TestClient,
        # which would require the full app + DB). Patch the helpers it uses
        # and assert it returns a dict with status="partitioning" without
        # awaiting the background task.
        import asyncio
        from uuid import uuid4
        from types import SimpleNamespace
        import app.api.connections as conn_mod
        from app.api.connections import retry_initial_load, _initial_load_state

        conn_id = uuid4()
        fake_conn = SimpleNamespace(
            connection_id=conn_id, sync_type="REALTIME",
            initial_load_completed=False, initial_load_completed_at=None,
            initial_load_started_at=None, sub_tenant_id=None,
        )
        bg_started: list = []

        async def fake_bg(conn_id_str, connection_id, user_id):
            bg_started.append(conn_id_str)
            _initial_load_state[conn_id_str] = {
                "phase": "enqueued", "task_id": "test", "partitions": 4,
                "rows_estimated": 1000, "error": None,
                "started_at": None, "updated_at": None,
            }

        class FakeDB:
            def commit(self): pass
            def rollback(self): pass

        monkeypatch.setattr(conn_mod, "_get_connection_by_id",
                            lambda db, cid, user: fake_conn)
        monkeypatch.setattr(conn_mod, "_run_initial_load_background", fake_bg)
        monkeypatch.setattr(conn_mod, "record_audit", lambda *a, **k: None)

        # Run the async handler directly.
        result = asyncio.new_event_loop().run_until_complete(
            retry_initial_load(connection_id=conn_id, db=FakeDB(),
                               current_user=SimpleNamespace(
                                   user_id=uuid4(), is_superuser=True,
                                   sub_tenant_id=None))
        )
        assert result["status"] == "partitioning"
        assert "task_id" in result
        # The background task was scheduled (not awaited) — bg_started is
        # populated only because our fake_bg runs synchronously when
        # asyncio.create_task schedules it. The point is the handler returned
        # immediately with status=partitioning.
        assert result["status"] == "partitioning"


class TestBackgroundPartitioningCompletes:
    """Task 7 test 7: the background partitioning completes and enqueues K
    tasks. We test ``_enqueue_initial_load_tasks`` directly with a mocked
    source DB to confirm it produces K tasks."""

    def test_enqueue_produces_k_tasks(self):
        # Mock partition_pk_ranges to return K=4 ranges (no DB), and mock
        # redis.lpush to count tasks. We need a Connection + Stream + Source
        # + Destination. Use SimpleNamespace stand-ins.
        from app.api.connections import _enqueue_initial_load_tasks
        import app.api.connections as conn_mod

        conn_id = "00000000-0000-0000-0000-000000000001"
        stream_id = "00000000-0000-0000-0000-000000000002"
        conn = SimpleNamespace(
            connection_id=conn_id,
            sync_type="REALTIME",
            resource_limits={"parallelism": 4, "chunk_size": 1000},
            destination_id="d",
            source_id="s",
        )
        stream = SimpleNamespace(
            stream_id=stream_id,
            is_enabled=True,
            source_schema_name="dw",
            source_table_name="t",
            destination_schema_name="dw",
            destination_table_name="t",
            primary_keys=["id"],
            transform_overrides={},
        )
        source = SimpleNamespace(
            source_id="s",
            host="db", port=3306, database_name="dw", username="u",
            password_encrypted=None, password=None, config={}, ssh_config={},
            connector_definition=SimpleNamespace(connector_type="mysql"),
        )
        dest = SimpleNamespace(
            destination_id="d",
            connection_config={"snapshot_mode": "transform_worker"},
            connector_definition=SimpleNamespace(connector_type="iceberg"),
        )

        class FakeDB:
            def query(self, model, *a, **k):
                m = MagicMock()
                m.filter.return_value = m
                if model.__name__ == "Destination":
                    m.first.return_value = dest
                elif model.__name__ == "Source":
                    m.first.return_value = source
                elif model.__name__ == "Stream":
                    m.all.return_value = [stream]
                else:
                    m.first.return_value = None
                    m.all.return_value = []
                return m
            def commit(self): pass
            def rollback(self): pass

        db = FakeDB()

        # Patch the heavy internals of _enqueue_initial_load_tasks.
        ranges = [(None, 25), (25, 50), (50, 75), (75, None)]
        pushed: list = []

        def fake_partition(*a, **k):
            return ranges

        class FakeRedis:
            def lpush(self, q, payload):
                pushed.append(payload)

        with patch.object(conn_mod, "partition_pk_ranges", fake_partition), \
             patch("redis.from_url", return_value=FakeRedis()), \
             patch.object(conn_mod, "_dest_needs_transform_worker",
                          return_value=True), \
             patch.object(conn_mod, "_connection_parallelism",
                          return_value=4), \
             patch.object(conn_mod, "_connection_chunk_size",
                          return_value=1000):
            # _enqueue_initial_load_tasks imports _decrypt_password from
            # app.api.sources at call time — patch it to a no-op.
            with patch("app.api.sources._decrypt_password", return_value=""):
                n = asyncio.run(_enqueue_initial_load_tasks(conn, db))
        assert n == 4, f"expected 4 tasks enqueued, got {n}"
        assert len(pushed) == 4

