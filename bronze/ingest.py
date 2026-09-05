"""Ingest GitHub Archive JSON.gz files into the Bronze Delta table.

Only files NOT already recorded in <raw_dir>/.ingested.txt are processed.
After a successful write, each file's name is appended to that file, so a
re-run never re-ingests the same file (idempotent ingest).

Usage:
    python bronze/ingest.py --raw-dir ~/scratch/github-lakehouse/raw \
                            --bronze-dir ~/scratch/github-lakehouse/bronze
"""
import argparse
import logging
from pathlib import Path

import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, LongType, StringType,
                               StructField, StructType, TimestampType)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DEFAULT_RAW = Path.home() / "scratch" / "github-lakehouse" / "raw"
DEFAULT_BRONZE = Path.home() / "scratch" / "github-lakehouse" / "bronze"
DONE_FILE_NAME = ".ingested.txt"

RAW_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("type", StringType()),
    StructField("actor", StructType([
        StructField("id", LongType()),
        StructField("login", StringType()),
    ])),
    StructField("repo", StructType([
        StructField("id", LongType()),
        StructField("name", StringType()),
    ])),
    StructField("payload", StringType()),
    StructField("public", BooleanType()),
    StructField("org", StructType([
        StructField("id", LongType()),
        StructField("login", StringType()),
    ])),
    StructField("created_at", TimestampType()),
])


def build_spark() -> pyspark.sql.SparkSession:
    builder = (
        pyspark.sql.SparkSession.builder
        .appName("bronze-ingest")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.local.dir",
                str(Path.home() / "scratch" / "github-lakehouse" / "spark-local"))
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def ingest(raw_dir: Path, bronze_dir: Path) -> None:
    spark = build_spark()
    done_file = raw_dir / DONE_FILE_NAME
    done = set(done_file.read_text().splitlines()) if done_file.exists() else set()

    # Only files in raw_dir that are not yet recorded as ingested.
    candidates = [p for p in raw_dir.glob("*.json.gz") if p.is_file()]
    files = [p for p in candidates if p.name not in done]
    if not files:
        log.info("No new files to ingest (all already recorded)")
        spark.stop()
        return

    log.info("Ingesting %d new file(s)", len(files))
    df = spark.read.schema(RAW_SCHEMA).json([str(f) for f in files])
    row_count = df.count()

    bronze = (
        df
        .withColumn("event_type", F.col("type"))
        .withColumn("year",  F.year("created_at"))
        .withColumn("month", F.month("created_at"))
        .withColumn("day",   F.dayofmonth("created_at"))
        .withColumn("hour",  F.hour("created_at"))
    )

    bronze.write \
        .mode("append") \
        .format("delta") \
        .partitionBy("event_type", "year", "month", "day", "hour") \
        .save(str(bronze_dir))

    # Record files as ingested (append after successful write).
    with done_file.open("a") as f:
        for p in files:
            f.write(p.name + "\n")
    log.info("Wrote %s rows and recorded %d file(s)", row_count, len(files))
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest GH Archive into Bronze")
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW)
    parser.add_argument("--bronze-dir", type=Path, default=DEFAULT_BRONZE)
    args = parser.parse_args()
    ingest(args.raw_dir, args.bronze_dir)
