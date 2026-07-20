"""
Integration tests for PostgresWriter (spark-consumer).

Requires postgres-dest container:
    docker compose -f docker/docker-compose.dev.yml up -d postgres-dest

Run with:
    pytest tests/integration/test_postgres_writer_integration.py -m integration
"""
from __future__ import annotations

import os
import sys
import time
import pytest
from pyspark.sql.types import StructType, StructField, StringType

PG_DSN = os.getenv(
    "PG_DEST_DSN",
    "host=127.0.0.1 port=5433 dbname=fusion_dw user=dw_user password=dw_password",
)

pytestmark = pytest.mark.integration

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="module")
def pg_conn():
    try:
        import psycopg2
    except ImportError:
        pytest.skip("psycopg2 not installed")

    try:
        conn = psycopg2.connect(PG_DSN, connect_timeout=5)
        conn.autocommit = True
    except Exception as exc:
        pytest.skip(f"postgres-dest not available — {exc}")

    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS int_test_orders (
                id      TEXT PRIMARY KEY,
                name    TEXT,
                amount  TEXT
            )
            """
        )
        cur.execute("DELETE FROM int_test_orders")
    yield conn
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS int_test_orders")
    conn.close()


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    _py = sys.executable
    session = (
        SparkSession.builder.master("local[1]")
        .appName("postgres-writer-integration-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.pyspark.python", _py)
        .config("spark.pyspark.driver.python", _py)
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


_SCHEMA = StructType([
    StructField("op", StringType()),
    StructField("id", StringType()),
    StructField("name", StringType()),
    StructField("amount", StringType()),
])


class TestPostgresWriterIntegration:
    def _query_all(self, pg_conn, table: str = "int_test_orders") -> list:
        with pg_conn.cursor() as cur:
            cur.execute(f"SELECT id, name, amount FROM {table} ORDER BY id")
            return cur.fetchall()

    def test_insert_row_appears_in_db(self, spark, pg_conn):
        from writers.postgres_writer import PostgresWriter

        writer = PostgresWriter(
            pg_dsn=PG_DSN,
            table="int_test_orders",
            pk_columns=["id"],
        )
        df = spark.createDataFrame(
            [("c", "1", "Alice", "99.99")],
            schema=_SCHEMA,
        )
        writer.upsert_batch(df, batch_id=0)

        rows = self._query_all(pg_conn)
        assert len(rows) == 1
        assert rows[0] == ("1", "Alice", "99.99")

    def test_update_row_via_upsert(self, spark, pg_conn):
        from writers.postgres_writer import PostgresWriter

        writer = PostgresWriter(
            pg_dsn=PG_DSN,
            table="int_test_orders",
            pk_columns=["id"],
        )
        # Insert then update
        df_insert = spark.createDataFrame([("c", "2", "Bob", "10.00")], schema=_SCHEMA)
        writer.upsert_batch(df_insert, batch_id=1)

        df_update = spark.createDataFrame([("u", "2", "Bob", "20.00")], schema=_SCHEMA)
        writer.upsert_batch(df_update, batch_id=2)

        rows = {r[0]: r for r in self._query_all(pg_conn)}
        assert rows["2"][2] == "20.00"

    def test_delete_row_removed_from_db(self, spark, pg_conn):
        from writers.postgres_writer import PostgresWriter

        writer = PostgresWriter(
            pg_dsn=PG_DSN,
            table="int_test_orders",
            pk_columns=["id"],
        )
        df_insert = spark.createDataFrame([("c", "3", "Eve", "5.00")], schema=_SCHEMA)
        writer.upsert_batch(df_insert, batch_id=3)

        df_delete = spark.createDataFrame([("d", "3", None, None)], schema=_SCHEMA)
        writer.upsert_batch(df_delete, batch_id=4)

        rows = {r[0]: r for r in self._query_all(pg_conn)}
        assert "3" not in rows
