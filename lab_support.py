"""Small laptop defaults shared by the checker and notebooks."""

import os
from pathlib import Path
import sys


def configure_local_environment():
    """Use this interpreter and its Conda Java without shell configuration."""
    prefix = Path(sys.prefix)
    executable = "java.exe" if os.name == "nt" else "java"
    candidates = [
        prefix / "Library",  # conda-forge, Windows
        prefix / "lib" / "jvm",  # conda-forge, Unix
        prefix,
    ]
    for home in candidates:
        if (home / "bin" / executable).is_file():
            os.environ["JAVA_HOME"] = str(home)
            break
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("OMP_NUM_THREADS", "1")
    os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")


def local_builder(app_name):
    """Two workers, two shuffle partitions and a modest JVM heap."""
    configure_local_environment()
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.driver.memory", "768m")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
    )

