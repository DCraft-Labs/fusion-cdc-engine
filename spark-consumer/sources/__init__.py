"""
fusion-cdc-engine batch source connectors.

Provides env-configurable Spark session + initial-load / incremental
extractors for MySQL, PostgreSQL, and MongoDB.

Destinations:
  - Apache Iceberg via Nessie catalog (primary)
  - PostgreSQL via JDBC (secondary / fallback)

Usage:
    from sources.spark_session import get_spark_session
    from sources.mysql_source import MySQLSource

    spark = get_spark_session("my-job")
    src = MySQLSource(host="...", port=3306, username="u", password="p", database="db")
    src.ingest(spark, source_table="orders", dest_table="raw_orders",
               cursor_field="updated_at", primary_key="id",
               catalog="vp_terra", namespace="raw_bank")
"""
