"""Assignment #6: RideCity batch job. It runs unchanged on AWS Glue and on your machine.

The job is given complete: you do not write Spark code in this assignment. Read it, because
the report asks what it does.

On AWS Glue (job parameters):      --input s3://<your-bucket>/raw   --output s3://<your-bucket>/processed
On your machine (optional):        python3 glue_job.py --input data --output output

It reads trips_history.csv and zones.csv from <input>, and writes two Parquet datasets:

  <output>/trips/        one row per trip, enriched, partitioned by pickup_date
  <output>/zone_daily/   one row per day and pickup zone: trips, revenue, averages

This is a plain PySpark script: no Glue-specific API, so it is the same code you would give
to spark-submit. Glue passes its own arguments (--JOB_NAME and others) next to yours; the
parser below ignores the ones it does not know.
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--output", required=True)
args, _ = parser.parse_known_args()
src, dst = args.input.rstrip("/"), args.output.rstrip("/")

spark = SparkSession.builder.appName("ridecity-a6").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

TRIPS_SCHEMA = StructType([
    StructField("trip_id", StringType()), StructField("driver_id", StringType()),
    StructField("pickup_zone", IntegerType()), StructField("dropoff_zone", IntegerType()),
    StructField("pickup_ts", TimestampType()), StructField("distance_km", DoubleType()),
    StructField("duration_min", DoubleType()), StructField("payment_type", StringType()),
    StructField("fare", DoubleType()),
])
ZONES_SCHEMA = StructType([
    StructField("zone_id", IntegerType()), StructField("zone_name", StringType()),
    StructField("borough", StringType()),
])


def enrich(trips, zones):
    """One row per trip, with the pickup zone's name and borough and three derived columns."""
    z = F.broadcast(zones.withColumnRenamed("zone_id", "pickup_zone"))
    return (trips.join(z, "pickup_zone", "left")
            .withColumn("pickup_date", F.to_date("pickup_ts"))
            .withColumn("pickup_hour", F.hour("pickup_ts"))
            .withColumn("fare_per_km", F.round(F.col("fare") / F.col("distance_km"), 2)))


def zone_daily(enriched):
    """One row per pickup_date and pickup zone."""
    return (enriched.groupBy("pickup_date", "pickup_zone", "zone_name", "borough")
            .agg(F.count("*").alias("trips"),
                 F.round(F.sum("fare"), 2).alias("revenue"),
                 F.round(F.avg("fare"), 2).alias("avg_fare"),
                 F.round(F.avg("distance_km"), 2).alias("avg_distance_km")))


trips = spark.read.csv(f"{src}/trips_history.csv", header=True, schema=TRIPS_SCHEMA,
                       timestampFormat="yyyy-MM-dd HH:mm:ss")
zones = spark.read.csv(f"{src}/zones.csv", header=True, schema=ZONES_SCHEMA)

enriched = enrich(trips, zones).cache()
daily = zone_daily(enriched)

# partitionBy puts each day in its own folder, pickup_date=2026-09-01/ and so on; a query
# that filters on pickup_date then reads only the folders it needs.
enriched.repartition("pickup_date").write.mode("overwrite").partitionBy("pickup_date").parquet(f"{dst}/trips")
daily.coalesce(1).write.mode("overwrite").parquet(f"{dst}/zone_daily")
print(f"trips written: {enriched.count()}   zone_daily rows: {daily.count()}")
print(f"total revenue: {enriched.agg(F.round(F.sum('fare'), 2)).first()[0]}")

spark.stop()
