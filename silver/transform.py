"""Transform Bronze into Silver: flatten, normalize, deduplicate.

Fields are limited to what the post-Oct-2025 GitHub Events API actually
provides. pr_merged / push_commits / pr_state were removed upstream (the
pull_request object is trimmed to id/url/number/head/base, and push commit
counts were removed), so they are not extracted.

Usage:
    python silver/transform.py
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

BRONZE_ROOT = Path.home() / "scratch" / "github-lakehouse" / "bronze"
SILVER_ROOT = Path.home() / "scratch" / "github-lakehouse" / "silver"

# Extracted from a frequency scan of real Bronze data, then verified against
# the post-Oct-2025 Events API. Only fields confirmed to be present are kept.
EVENT_FIELDS = {
    "action":       "$.action",
    "push_ref":     "$.ref",
    "pr_number":    "$.number",
    "issue_number": "$.issue.number",
    "issue_state":  "$.issue.state",
    "ref_type":     "$.ref_type",
    "ref_name":     "$.ref",
    "release_tag":  "$.release.tag_name",
}


def build_spark() -> pyspark.sql.SparkSession:
    builder = (
        pyspark.sql.SparkSession.builder
        .appName("silver-transform")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.databricks.delta.schemaAutoMerge.enabled", "true")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def transform(bronze_dir: Path, silver_dir: Path) -> None:
    spark = build_spark()
    df = spark.read.format("delta").load(str(bronze_dir))
    source_count = df.count()

    silver = df.select(
        F.col("id").alias("event_id"),
        F.col("event_type"),
        F.to_timestamp("created_at").alias("event_time"),
        F.col("actor.id").alias("actor_id"),
        F.col("actor.login").alias("actor_login"),
        F.col("repo.id").alias("repo_id"),
        F.col("repo.name").alias("repo_name"),
        F.col("public"),
        *[F.get_json_object("payload", path).alias(name)
          for name, path in EVENT_FIELDS.items()],
    )

    if DeltaTable.isDeltaTable(spark, str(silver_dir)):
        table = DeltaTable.forPath(spark, str(silver_dir))
        result = (table.alias("t")
                  .merge(silver.alias("s"), "t.event_id = s.event_id")
                  .whenMatchedUpdateAll()
                  .whenNotMatchedInsertAll()
                  .execute())
        try:
            m = result.select("numTargetRowsInserted", "numTargetRowsUpdated").collect()[0]
            log.info("source=%s inserted=%s updated=%s",
                     source_count, m.numTargetRowsInserted, m.numTargetRowsUpdated)
        except Exception:
            log.info("Merged %s source rows into Silver", source_count)
    else:
        silver.write.format("delta").save(str(silver_dir))
        log.info("Created new Silver table with %s rows", source_count)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform Bronze to Silver")
    parser.add_argument("--bronze-dir", type=Path, default=BRONZE_ROOT)
    parser.add_argument("--silver-dir", type=Path, default=SILVER_ROOT)
    args = parser.parse_args()
    transform(args.bronze_dir, args.silver_dir)
