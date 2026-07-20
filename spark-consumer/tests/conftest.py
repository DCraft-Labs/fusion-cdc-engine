"""
Shared pytest fixtures for spark-consumer tests.

SparkSession is session-scoped to avoid the 10-15s JVM startup cost per test.
"""
import os
import sys

import pytest
from pyspark.sql import SparkSession

# Ensure Spark workers use the same Python interpreter as the driver (the venv).
# On macOS the default `python` in PATH may be a different version; force venv.
_PYTHON_EXEC = sys.executable
os.environ["PYSPARK_PYTHON"] = _PYTHON_EXEC
os.environ["PYSPARK_DRIVER_PYTHON"] = _PYTHON_EXEC


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("fusion-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "512m")
        .config("spark.ui.enabled", "false")
        .config("spark.python.worker.reuse", "true")
        # Pin the worker Python to the same interpreter as the driver
        .config("spark.pyspark.python", _PYTHON_EXEC)
        .config("spark.pyspark.driver.python", _PYTHON_EXEC)
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
