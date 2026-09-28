"""Run before class: python check_environment.py (no services required)."""

from contextlib import contextmanager
import json
import math
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import tempfile

from lab_support import configure_local_environment, local_builder

ROOT = Path(__file__).resolve().parent
ACTIVATE = "conda activate data-stream-spark"


def report(message, ok=True):
    # Windows consoles do not necessarily have a Unicode output encoding.
    symbol = "✓" if ok else "✗"
    try:
        symbol.encode(sys.stdout.encoding or "utf-8")
    except UnicodeEncodeError:
        symbol = "OK" if ok else "FAIL"
    print(f"{symbol} {message}", flush=True)


@contextmanager
def engine_log(path):
    """Capture JVM diagnostics too, while keeping the checklist on stdout."""
    saved = os.dup(2)
    try:
        with path.open("w", encoding="utf-8") as log:
            sys.stderr.flush()
            os.dup2(log.fileno(), 2)
            yield
    finally:
        sys.stderr.flush()
        os.dup2(saved, 2)
        os.close(saved)


def check_spark():
    from pyspark.ml.recommendation import ALS
    from pyspark.sql import functions as F

    spark = None
    query = None
    stage = "SparkSession creation"
    try:
        spark = local_builder("spark-environment-check").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        report("SparkSession created: local[2]")
        stage = "DataFrame operations"
        # Exercise Python workers as well as the JVM DataFrame engine.
        df = spark.createDataFrame([(1, 2), (2, 3)], "id int, value int")
        assert df.agg(F.sum("value")).first()[0] == 5
        report("DataFrame operations work")
        stage = "MLlib (ALS)"
        ratings = spark.createDataFrame(
            [(u, i, float(1 + (u + i) % 5)) for u in range(4) for i in range(4)],
            "userId int, movieId int, rating float",
        )
        model = ALS(
            userCol="userId", itemCol="movieId", ratingCol="rating",
            rank=2, maxIter=1, seed=42, numUserBlocks=2, numItemBlocks=2,
            coldStartStrategy="drop",
        ).fit(ratings)
        predictions = model.transform(ratings).select("prediction").collect()
        assert len(predictions) == 16 and all(math.isfinite(r[0]) for r in predictions)
        report("MLlib works (ALS fit and prediction)")
        stage = "Structured Streaming (local files and checkpoint)"
        with tempfile.TemporaryDirectory(prefix="spark-check-") as directory:
            base = Path(directory)
            incoming = base / "input"
            incoming.mkdir()
            (incoming / "batch.json").write_text(
                json.dumps({"movieId": 1}) + "\n" + json.dumps({"movieId": 1}) + "\n",
                encoding="utf-8",
            )
            stream = spark.readStream.schema("movieId int").json(incoming.as_uri())
            query = (
                stream.groupBy("movieId").count().writeStream
                .format("memory").queryName("environment_views")
                .outputMode("complete")
                .option("checkpointLocation", (base / "checkpoint").as_uri())
                .trigger(availableNow=True).start()
            )
            try:
                if not query.awaitTermination(60):
                    raise TimeoutError("The streaming probe exceeded 60 seconds.")
                assert spark.table("environment_views").first()["count"] == 2
                report("Structured Streaming works")
            finally:
                query.stop()
                query = None
        return True
    except Exception as exc:
        report(f"{stage} failed ({type(exc).__name__}).", ok=False)
        print(f"\nActivate the lab environment with: {ACTIVATE}")
        print("Close other notebook kernels using Spark, then retry.")
        if os.name == "nt":
            print("Use Miniforge Prompt. If the log mentions winutils or NativeIO,")
            print("send the log to your instructor; native Windows validation is pending.")
        # Full details are available to the instructor without flooding the terminal.
        print(f"{stage}: {exc}", file=sys.stderr)
        return False
    finally:
        if query is not None:
            query.stop()
        if spark is not None:
            spark.stop()


def main():
    print("Spark environment check\n=======================\n", flush=True)
    if sys.version_info[:2] != (3, 11):
        report(f"Python {platform.python_version()}; this lab requires Python 3.11.", False)
        print(f"Run: {ACTIVATE}\nInterpreter: {sys.executable}")
        return 1
    report(f"Python: {platform.python_version()}")
    configure_local_environment()
    java_home = os.environ.get("JAVA_HOME")
    java = str(Path(java_home) / "bin" / ("java.exe" if os.name == "nt" else "java")) if java_home else shutil.which("java")
    try:
        result = subprocess.run([java or "java", "-version"], capture_output=True, text=True, timeout=15)
        version = result.stderr + result.stdout
        match = re.search(r'version "(\d+)', version)
        if result.returncode or not match or int(match[1]) != 17:
            raise ValueError("Java 17 was not found")
    except (OSError, ValueError, subprocess.TimeoutExpired):
        report("Java 17 could not be started.", False)
        print(f"Run: {ACTIVATE}\nThen retry. Java is installed by environment.yml.")
        return 1
    report(f"Java: {version.splitlines()[0]} ({java})")
    try:
        import pyspark
    except ImportError:
        report("PySpark is missing.", False)
        print(f"Run: {ACTIVATE}\nCreate the environment first if it does not exist.")
        return 1
    if not pyspark.__version__.startswith("4.0.") or pyspark.__version__ == "4.0.0":
        report(f"PySpark {pyspark.__version__}; expected 4.0.1 or a newer 4.0 patch.", False)
        print(f"Run: {ACTIVATE}")
        return 1
    report(f"PySpark: {pyspark.__version__}")
    log_dir = ROOT / "validation-results"
    log_dir.mkdir(exist_ok=True)
    log_path = log_dir / "environment-spark.log"
    try:
        with engine_log(log_path):
            ready = check_spark()
    except KeyboardInterrupt:
        report("Check interrupted; Spark resources were stopped.", False)
        return 130
    if not ready:
        print(f"Diagnostics: {log_path}")
        return 1
    print("\nEnvironment ready.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
