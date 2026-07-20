"""
MongoDBSource — production-grade batch extractor for MongoDB → Iceberg / Postgres.

Uses the MongoDB Spark Connector (spark.read.format("mongodb")).
Requires: org.mongodb.spark:mongo-spark-connector_2.12 JAR on the Spark classpath.

Load strategy:
  ┌───────────────────────────────────────────────────────────────────────────┐
  │ Destination table does NOT exist (or cursor_field is None)               │
  │   → FULL LOAD: read entire collection                                    │
  │   → Write: writeTo(dest).createOrReplace()                               │
  ├───────────────────────────────────────────────────────────────────────────┤
  │ Destination table EXISTS + cursor_field configured                        │
  │   → INCREMENTAL: aggregation pipeline $match cursor_field > max_date     │
  │   → Write: staging-merge pattern (delete-by-PK then insert)              │
  └───────────────────────────────────────────────────────────────────────────┘

After fetching:
  - ObjectId / nested _id struct flattened to string.
  - Null bytes (\x00) stripped from string columns.
  - Nested structs / arrays serialized to JSON strings.
  - Duplicate column names (case-insensitive) deduplicated.
  - audit_updated_date (IST) added.
  - All column names lowercased.
  - Schema reconciled against the Iceberg destination.
"""

from __future__ import annotations

import logging
import urllib.parse
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# Datetime format strings tried in order when parsing the stored watermark
_DATE_FORMATS = [
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M",
    "%Y/%m/%d %H:%M:%S.%f",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d",
]


def _parse_watermark(watermark_str: str) -> Optional[datetime]:
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(watermark_str, fmt)
        except ValueError:
            pass
    logger.warning("Could not parse watermark %r with any known format", watermark_str)
    return None


class MongoDBSource:
    """
    Reads a MongoDB collection and loads it into an Iceberg or Postgres destination.

    Args:
        host:        MongoDB host.
        port:        MongoDB port (default 27017).
        username:    DB username (optional).
        password:    DB password (optional).
        auth_source: Authentication database (default 'admin').
    """

    def __init__(
        self,
        host: str,
        port: int = 27017,
        username: str = "",
        password: str = "",
        auth_source: str = "admin",
    ) -> None:
        self.host = host
        self.port = int(port)
        self.username = username
        self.password = password
        self.auth_source = auth_source

    # ------------------------------------------------------------------
    # Connection URI
    # ------------------------------------------------------------------

    @property
    def connection_uri(self) -> str:
        if self.username:
            encoded_pw = urllib.parse.quote_plus(self.password)
            return (
                f"mongodb://{self.username}:{encoded_pw}@{self.host}:{self.port}"
                f"/?authSource={self.auth_source}&readPreference=secondaryPreferred"
            )
        return f"mongodb://{self.host}:{self.port}/?readPreference=secondaryPreferred"

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def ingest(
        self,
        spark,
        source_db: str,
        source_collection: str,
        dest_table: str,
        cursor_field: Optional[str],
        primary_key: str,
        catalog: str,
        namespace: str,
        writer_type: str = "iceberg",
        pg_dsn: Optional[str] = None,
        pg_schema: str = "public",
    ) -> None:
        """
        Full ingest cycle: extract → clean → reconcile schema → write.

        Args:
            source_db:          MongoDB database name.
            source_collection:  MongoDB collection name.
            dest_table:         Target table name in the destination.
            cursor_field:       Incremental watermark field (e.g. 'updated_at').
                                Pass None to always perform a full refresh.
            primary_key:        Primary key field for staging-merge dedup.
            catalog:            Iceberg catalog name (e.g. 'vp_terra').
            namespace:          Iceberg namespace (e.g. 'raw_bank').
            writer_type:        'iceberg' (default) or 'postgres'.
            pg_dsn:             Postgres DSN string (required when writer_type='postgres').
            pg_schema:          Target Postgres schema (default 'public').
        """
        from sources.iceberg_utils import (
            ensure_namespace,
            get_max_watermark,
            reconcile_schema,
            table_exists,
        )
        from writers.batch_writer import IcebergBatchWriter, PostgresBatchWriter

        start = datetime.now()
        logger.info(
            "MongoDBSource.ingest: %s.%s → %s.%s.%s [writer=%s]",
            source_db, source_collection, catalog, namespace, dest_table, writer_type,
        )

        # 1. Determine load mode
        table_present = table_exists(spark, catalog, namespace, dest_table)
        max_run_date: Optional[str] = None

        if cursor_field and table_present:
            max_run_date = get_max_watermark(
                spark, catalog, namespace, dest_table, cursor_field
            )
            logger.info("Incremental load — watermark: %s", max_run_date)
        else:
            logger.info(
                "Full load (table_present=%s, cursor_field=%s)",
                table_present, cursor_field,
            )

        # 2. Verify collection exists
        if not self._collection_exists(source_db, source_collection):
            logger.warning(
                "Collection %s.%s not found — skipping.",
                source_db, source_collection,
            )
            return

        # 3. Extract
        df = self._fetch(spark, source_db, source_collection, cursor_field, max_run_date)
        count = df.count()
        logger.info(
            "Fetched %d documents from %s.%s",
            count, source_db, source_collection,
        )

        if count == 0:
            logger.info("No new data for %s — skipping write.", dest_table)
            return

        # 4. Clean + flatten + audit timestamp + lowercase columns
        df = self._clean(df)

        # 5. Schema reconciliation (only against existing Iceberg tables)
        if writer_type == "iceberg":
            ensure_namespace(spark, catalog, namespace)
            if table_present:
                df = reconcile_schema(spark, df, catalog, namespace, dest_table)

        # 6. Write
        if writer_type == "iceberg":
            IcebergBatchWriter(spark, catalog, namespace, dest_table).write(
                df, primary_key, cursor_field, max_run_date, table_present
            )
        else:
            if not pg_dsn:
                raise ValueError("pg_dsn is required when writer_type='postgres'")
            PostgresBatchWriter(pg_dsn, dest_table, primary_key, pg_schema).write(df)

        elapsed = (datetime.now() - start).total_seconds() / 60
        logger.info("MongoDBSource.ingest complete in %.2f min", elapsed)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _collection_exists(self, db_name: str, collection_name: str) -> bool:
        try:
            import pymongo
            client = pymongo.MongoClient(self.connection_uri, serverSelectionTimeoutMS=5000)
            exists = collection_name in client[db_name].list_collection_names()
            client.close()
            return exists
        except Exception as exc:
            logger.error("Error checking MongoDB collection existence: %s", exc)
            return False

    def _fetch(
        self,
        spark,
        db_name: str,
        collection_name: str,
        cursor_field: Optional[str],
        max_run_date: Optional[str],
    ):
        uri = self.connection_uri

        base_reader = (
            spark.read.format("mongodb")
            .option("spark.mongodb.read.connection.uri", uri)
            .option("spark.mongodb.read.database", db_name)
            .option("spark.mongodb.read.collection", collection_name)
        )

        if cursor_field and max_run_date:
            dt = _parse_watermark(max_run_date)
            if dt is not None:
                iso_date = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                pipeline = [{"$match": {cursor_field: {"$gt": {"$date": iso_date}}}}]
                logger.debug("MongoDB aggregation pipeline: %s", pipeline)
                return (
                    base_reader
                    .option("aggregation.pipeline", str(pipeline))
                    .load()
                )
            logger.warning(
                "Could not parse watermark %r — falling back to full load.", max_run_date
            )

        return base_reader.load()

    @staticmethod
    def _deduplicate_columns(df):
        """Rename duplicate column names (case-insensitive) by appending a counter."""
        seen: dict = {}
        new_names = []
        for col in df.columns:
            key = col.lower()
            if key in seen:
                seen[key] += 1
                new_names.append(f"{col}_{seen[key]}")
            else:
                seen[key] = 0
                new_names.append(col)
        return df.toDF(*new_names)

    @staticmethod
    def _clean(df):
        from pyspark.sql.functions import (
            col, expr, from_utc_timestamp, regexp_replace, to_json,
            when,
        )
        from pyspark.sql.types import ArrayType, StringType, StructType

        # Deduplicate columns before any other processing
        df = MongoDBSource._deduplicate_columns(df)

        # Flatten ObjectId stored as nested _id struct / non-string
        if "_id" in df.columns:
            id_type = df.schema["_id"].dataType
            if isinstance(id_type, StructType):
                # e.g. {oid: "abc123"} — extract the oid field
                df = df.withColumn("_id", expr("get_json_object(to_json(_id), '$.oid')"))
            else:
                df = df.withColumn("_id", col("_id").cast("string"))

        # Remove null bytes from string columns
        for column in df.columns:
            if isinstance(df.schema[column].dataType, StringType):
                df = df.withColumn(column, regexp_replace(df[column], "\x00", ""))
                df = df.withColumn(column, regexp_replace(df[column], "\u0000", ""))

        # Serialize nested structs and arrays to JSON strings
        for column in df.columns:
            dtype = df.schema[column].dataType
            if isinstance(dtype, StructType):
                df = df.withColumn(column, to_json(col(column)))
                df = df.fillna({column: "{}"})
                df = df.withColumn(
                    column, when(col(column) == "", None).otherwise(col(column))
                )
            elif isinstance(dtype, ArrayType):
                df = df.withColumn(column, to_json(col(column)))
                df = df.fillna({column: "[]"})
                df = df.withColumn(
                    column, when(col(column) == "", None).otherwise(col(column))
                )

        # Audit timestamp in IST
        df = df.withColumn(
            "audit_updated_date",
            from_utc_timestamp(expr("current_timestamp()"), "Asia/Kolkata"),
        )

        # Lowercase all column names
        df = df.toDF(*[c.lower() for c in df.columns])

        return df
