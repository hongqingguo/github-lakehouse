#!/bin/bash
#SBATCH --job-name=check
#SBATCH --output=/users/hguo55/scratch/github-lakehouse/logs/check-%j.log
#SBATCH --error=/users/hguo55/scratch/github-lakehouse/logs/check-%j.log
#SBATCH --time=01:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=4
#SBATCH --mem=32G

module load openjdk/17.0.11_9-7sqr
export PYSPARK_DRIVER_MEMORY=24g
cd /users/hguo55/projects/github-lakehouse
python3 - <<'PYEOF'
from delta import configure_spark_with_delta_pip
import pyspark
from pyspark.sql import functions as F
b = (pyspark.sql.SparkSession.builder.appName("check-dup")
     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
     .config("spark.local.dir", "/users/hguo55/scratch/github-lakehouse/spark-local"))
s = configure_spark_with_delta_pip(b).getOrCreate()
df = s.read.format("delta").load("/users/hguo55/scratch/github-lakehouse/bronze")
print("total rows:", df.count())
print("distinct event ids:", df.select("id").distinct().count())
s.stop()
PYEOF
