"""
Unit tests for app/utils/db_tester.py (P1.16 — Real DB connection tester).

All DB calls are patched with unittest.mock so no live databases are required.
"""

from unittest.mock import patch, MagicMock
import pytest


class TestPostgresConnection:
    def test_successful_connection(self):
        from app.utils.db_tester import test_postgres_connection

        mock_conn = MagicMock()
        with patch("psycopg2.connect", return_value=mock_conn) as mock_connect:
            success, message, latency_ms = test_postgres_connection(
                host="localhost", port=5432,
                database_name="mydb", username="user", password="pass",
            )

        assert success is True
        assert message == "Connection successful"
        assert latency_ms is not None and latency_ms >= 0
        mock_conn.close.assert_called_once()

    def test_connection_failure_returns_false(self):
        from app.utils.db_tester import test_postgres_connection

        with patch("psycopg2.connect", side_effect=Exception("Connection refused")):
            success, message, latency_ms = test_postgres_connection(
                host="bad-host", port=5432,
                database_name="mydb", username="user", password="wrong",
            )

        assert success is False
        assert "PostgreSQL connection failed" in message
        assert latency_ms is None

    def test_ssl_flag_passed_to_driver(self):
        from app.utils.db_tester import test_postgres_connection

        mock_conn = MagicMock()
        with patch("psycopg2.connect", return_value=mock_conn) as mock_connect:
            test_postgres_connection(
                host="localhost", port=5432,
                database_name="mydb", username="user", password="pass",
                ssl_enabled=True,
            )

        call_kwargs = mock_connect.call_args.kwargs
        assert call_kwargs.get("sslmode") == "require"


class TestMySQLConnection:
    def test_successful_connection(self):
        from app.utils.db_tester import test_mysql_connection

        mock_conn = MagicMock()
        with patch("pymysql.connect", return_value=mock_conn):
            success, message, latency_ms = test_mysql_connection(
                host="localhost", port=3306,
                database_name="mydb", username="root", password="pass",
            )

        assert success is True
        assert message == "Connection successful"
        assert latency_ms is not None
        mock_conn.close.assert_called_once()

    def test_connection_failure_returns_false(self):
        from app.utils.db_tester import test_mysql_connection

        with patch("pymysql.connect", side_effect=Exception("Access denied")):
            success, message, latency_ms = test_mysql_connection(
                host="localhost", port=3306,
                database_name="mydb", username="root", password="wrong",
            )

        assert success is False
        assert "MySQL connection failed" in message


class TestMongoDBConnection:
    def test_successful_connection(self):
        from app.utils.db_tester import test_mongodb_connection

        mock_client = MagicMock()
        mock_client.__getitem__.return_value.command.return_value = {"ok": 1}
        with patch("pymongo.MongoClient", return_value=mock_client):
            success, message, latency_ms = test_mongodb_connection(
                host="localhost", port=27017,
                database_name="mydb", username="user", password="pass",
            )

        assert success is True
        assert message == "Connection successful"
        mock_client.close.assert_called_once()

    def test_connection_failure_returns_false(self):
        from app.utils.db_tester import test_mongodb_connection

        with patch("pymongo.MongoClient", side_effect=Exception("Server not reachable")):
            success, message, latency_ms = test_mongodb_connection(
                host="bad-host", port=27017,
                database_name="mydb",
            )

        assert success is False
        assert "MongoDB connection failed" in message


class TestConnectionDispatch:
    def test_dispatches_to_postgres(self):
        from app.utils.db_tester import test_connection

        with patch("app.utils.db_tester.test_postgres_connection", return_value=(True, "ok", 5)) as mock_fn:
            result = test_connection("postgresql", "h", 5432, "db", "u", "p")

        assert result == (True, "ok", 5)
        mock_fn.assert_called_once()

    def test_dispatches_to_mysql(self):
        from app.utils.db_tester import test_connection

        with patch("app.utils.db_tester.test_mysql_connection", return_value=(True, "ok", 3)) as mock_fn:
            result = test_connection("mysql", "h", 3306, "db", "u", "p")

        assert result == (True, "ok", 3)
        mock_fn.assert_called_once()

    def test_dispatches_to_mongodb(self):
        from app.utils.db_tester import test_connection

        with patch("app.utils.db_tester.test_mongodb_connection", return_value=(True, "ok", 10)) as mock_fn:
            result = test_connection("mongodb", "h", 27017, "db", "u", "p")

        assert result == (True, "ok", 10)
        mock_fn.assert_called_once()

    def test_unsupported_connector_returns_false(self):
        from app.utils.db_tester import test_connection

        success, message, latency_ms = test_connection("oracle", "h", 1521, "db", "u", "p")

        assert success is False
        assert "Unsupported connector type" in message
        assert latency_ms is None
