"""Assignment #6: RideCity batch job, written to run unchanged on AWS Glue and on your machine.

On AWS Glue (job parameters):      --input s3://<your-bucket>/raw   --output s3://<your-bucket>/processed
On your machine, to test first:    python glue_job.py --input data --output output

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
    """TODO 1. One row per trip, with the pickup zone's name and borough and three derived columns.

    Return the trips joined with the zones on the PICKUP zone (left join; the zones table is
    tiny, so broadcast it), keeping every column of trips and adding:

      zone_name, borough   of the pickup zone
      pickup_date          the date of pickup_ts (a date, not a string)
      pickup_hour          the hour of pickup_ts, 0 to 23 (an integer)
      fare_per_km          fare / distance_km, rounded to 2 decimals

    The result must not contain a zone_id column.
    """
    return None


def zone_daily(enriched):
    """TODO 2. One row per pickup_date and pickup zone.

    Group the enriched trips by pickup_date, pickup_zone, zone_name and borough, and compute:

      trips             number of trips
      revenue           sum of fare, rounded to 2 decimals
      avg_fare          average fare, rounded to 2 decimals
      avg_distance_km   average distance_km, rounded to 2 decimals
    """
    return None


trips = spark.read.csv(f"{src}/trips_history.csv", header=True, schema=TRIPS_SCHEMA,
                       timestampFormat="yyyy-MM-dd HH:mm:ss")
zones = spark.read.csv(f"{src}/zones.csv", header=True, schema=ZONES_SCHEMA)

enriched = enrich(trips, zones)
daily = zone_daily(enriched) if enriched is not None else None

if enriched is None or daily is None:
    print("glue_job: not implemented yet (enrich or zone_daily returned None)")
else:
    enriched.cache()
    # partitionBy puts each day in its own folder, pickup_date=2026-09-01/ and so on; a query
    # that filters on pickup_date then reads only the folders it needs.
    enriched.repartition("pickup_date").write.mode("overwrite").partitionBy("pickup_date").parquet(f"{dst}/trips")
    daily.coalesce(1).write.mode("overwrite").parquet(f"{dst}/zone_daily")
    print(f"trips written: {enriched.count()}   zone_daily rows: {daily.count()}")
    print(f"total revenue: {enriched.agg(F.round(F.sum('fare'), 2)).first()[0]}")

spark.stop()
