import os
import sys
from pathlib import Path

import pytest

# Make Spark's worker processes use the same interpreter as the tests. Without
# this, Windows often fails with "Python worker failed to connect back".
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def spark():
    """One small local Spark session shared by the whole test run."""
    # On Windows, Spark cannot list a folder without Hadoop's native libraries
    # (hadoop.dll and winutils.exe, located through HADOOP_HOME). Without them
    # every read fails with UnsatisfiedLinkError, so skip instead. CI runs on
    # Linux and always runs these tests.
    if sys.platform == "win32" and not os.environ.get("HADOOP_HOME"):
        pytest.skip("Spark needs Hadoop's native Windows libraries (HADOOP_HOME is not set); runs in CI")

    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("microstructure-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def raw_trades_dir() -> Path:
    return FIXTURES / "trades"
