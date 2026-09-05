"""Build Gold table: repo_daily_stats (repo x day grain).

Separated into:
  build_gold(spark, silver_dir) -> DataFrame   (pure transform, no writes)
  write_gold(spark, gold, gold_dir) -> None    (idempotent writer)

Grouped by (repo_id, event_date) only: repo_name may change when a repo is
renamed, so it must not be part of the merge key. repo_name is kept as a
display name (max is fine for display purposes).

Usage:
    python gold/repo_daily.py
"""
import argparse
import logging
from pathlib import Path

import pyspark
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SILVER_ROOT = Path.home() / "scratch" / "github-lakehouse" / "silver"
GOLD_ROOT = Path.home() / "scratch" / "github-lakehouse" / "gold"


def build_spark() -> pyspark.sql.SparkSession:
    builder = (
        pyspark.sql.SparkSession.builder
        .appName("gold-repo-daily")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.local.dir", "/users/hguo55/scratch/github-lakehouse/spark-local")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def build_gold(spark: pyspark.sql.SparkSession, silver_dir: Path) -> pyspark.sql.DataFrame:
    """Pure transform: Silver events -> aggregated Gold DataFrame. No writes."""
    df = spark.read.format("delta").load(str(silver_dir))
    daily = df.withColumn("event_date", F.to_date("event_time"))

    gold = (
        daily
        .groupBy("repo_id", "event_date")
        .agg(
            F.max("repo_name").alias("repo_name"),
            F.sum(F.when((F.col("event_type") == "WatchEvent") &
                         (F.col("action") == "started"), 1).otherwise(0)).alias("star_delta"),
            F.sum(F.when(F.col("event_type") == "ForkEvent", 1).otherwise(0)).alias("fork_delta"),
            F.sum(F.when(F.col("event_type") == "PushEvent", 1).otherwise(0)).alias("push_count"),
            F.sum(F.when(F.col("event_type") == "PullRequestEvent", 1).otherwise(0)).alias("pr_count"),
            F.sum(F.when((F.col("event_type") == "PullRequestEvent") &
                         (F.col("action") == "closed"), 1).otherwise(0)).alias("pr_closed_count"),
            F.sum(F.when(F.col("event_type") == "IssuesEvent", 1).otherwise(0)).alias("issue_count"),
            F.sum(F.when(F.col("event_type") == "ReleaseEvent", 1).otherwise(0)).alias("release_count"),
            F.countDistinct("actor_id").alias("active_contributors"),
        )
    )

    # Merge key must be unique and non-null.
    return gold.filter(F.col("repo_id").isNotNull()) \
               .dropDuplicates(["repo_id", "event_date"])


def write_gold(spark: pyspark.sql.SparkSession, gold: pyspark.sql.DataFrame, gold_dir: Path) -> None:
    """Idempotent writer: create if missing, MERGE upsert otherwise."""
    if DeltaTable.isDeltaTable(spark, str(gold_dir)):
        table = DeltaTable.forPath(spark, str(gold_dir))
        result = (table.alias("t")
                  .merge(gold.alias("g"),
                         "t.repo_id = g.repo_id AND t.event_date = g.event_date")
                  .whenMatchedUpdateAll()
                  .whenNotMatchedInsertAll()
                  .execute())
        try:
            m = result.select("numTargetRowsInserted", "numTargetRowsUpdated").collect()[0]
            log.info("Gold upsert done: inserted=%s updated=%s",
                     m.numTargetRowsInserted, m.numTargetRowsUpdated)
        except Exception:
            log.info("Gold upsert done")
    else:
        gold.write.format("delta").save(str(gold_dir))
        log.info("Created new Gold table")


def main(silver_dir: Path, gold_dir: Path) -> None:
    spark = build_spark()
    gold = build_gold(spark, silver_dir)   # pure transform
    write_gold(spark, gold, gold_dir)      # idempotent write
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build repo_daily_stats Gold table")
    parser.add_argument("--silver-dir", type=Path, default=SILVER_ROOT)
    parser.add_argument("--gold-dir", type=Path, default=GOLD_ROOT)
    args = parser.parse_args()
    main(args.silver_dir, args.gold_dir)
