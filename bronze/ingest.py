"""Ingest GitHub Archive JSON.gz files into the Bronze Delta table.

Usage:
    python bronze/ingest.py --raw-dir ~/scratch/github-lakehouse/raw \
                            --bronze-dir ~/scratch/github-lakehouse/bronze
"""
import argparse
import logging
from pathlib import Path

import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql.types import (BooleanType, LongType, StringType,
                               StructField, StructType, TimestampType)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DEFAULT_RAW = Path.home() / "scratch" / "github-lakehouse" / "raw"
DEFAULT_BRONZE = Path.home() / "scratch" / "github-lakehouse" / "bronze"

# Fixed schema: payload is stored as a raw JSON string so the Bronze schema
# never drifts when GitHub changes nested payload structures.
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
        # Belt-and-suspenders; fixed schema above already prevents drift.
        .config("spark.databricks.delta.schemaAutoMerge.enabled", "true")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def ingest(raw_dir: Path, bronze_dir: Path) -> None:
    spark = build_spark()
    raw_path = str(raw_dir / "*.json.gz")
    log.info("Reading raw files from %s", raw_path)

    df = spark.read.schema(RAW_SCHEMA).json(raw_path)
    row_count = df.count()  # count source once, before the write action

    bronze = (
        df
        .withColumn("event_type", pyspark.sql.functions.col("type"))
        .withColumn("year",  pyspark.sql.functions.year("created_at"))
        .withColumn("month", pyspark.sql.functions.month("created_at"))
        .withColumn("day",   pyspark.sql.functions.dayofmonth("created_at"))
        .withColumn("hour",  pyspark.sql.functions.hour("created_at"))
    )

    bronze.write \
        .mode("append") \
        .format("delta") \
        .partitionBy("event_type", "year", "month", "day", "hour") \
        .save(str(bronze_dir))

    log.info("Wrote %s rows to %s", row_count, bronze_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest GH Archive into Bronze")
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW)
    parser.add_argument("--bronze-dir", type=Path, default=DEFAULT_BRONZE)
    args = parser.parse_args()
    ingest(args.raw_dir, args.bronze_dir)
