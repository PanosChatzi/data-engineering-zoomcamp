#!/usr/bin/env python
# coding: utf-8

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, StringType, TimestampType
)

parser = argparse.ArgumentParser()
parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)
args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output

spark = SparkSession.builder.appName('test').getOrCreate()

# (Optional but recommended) stability settings – BEFORE reads
spark.conf.set("spark.sql.parquet.mergeSchema", "false")
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")

# --- Explicit schemas for only the columns you actually use ---
green_schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("lpep_pickup_datetime", TimestampType(), True),
    StructField("lpep_dropoff_datetime", TimestampType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("RatecodeID", IntegerType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("congestion_surcharge", DoubleType(), True),  # some files may miss it -> will be NULL
])

yellow_schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("RatecodeID", IntegerType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
])

# Read with explicit schemas — no inference/merging
df_green = spark.read.schema(green_schema).parquet(input_green)
df_yellow = spark.read.schema(yellow_schema).parquet(input_yellow)

# Normalize datetime column names
df_green = df_green.withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
                   .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

df_yellow = df_yellow.withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
                     .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

common_columns = [
    'VendorID',
    'pickup_datetime',
    'dropoff_datetime',
    'store_and_fwd_flag',
    'RatecodeID',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'fare_amount',
    'extra',
    'mta_tax',
    'tip_amount',
    'tolls_amount',
    'improvement_surcharge',
    'total_amount',
    'payment_type',
    'congestion_surcharge'
]

df_green_sel = df_green.select(common_columns).withColumn('service_type', F.lit('green'))
df_yellow_sel = df_yellow.select(common_columns).withColumn('service_type', F.lit('yellow'))

# Use unionByName for safety (column order can vary across datasets)
df_trips_data = df_green_sel.unionByName(df_yellow_sel)

# Aggregate
df_trips_data.createOrReplaceTempView('trips_data')
df_result = spark.sql("""
SELECT 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance
FROM trips_data
GROUP BY 1, 2, 3
""")

# Before the write:
spark.conf.set("spark.sql.adaptive.enabled", "true")  # AQE helps
spark.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")  # safer, fewer renames on GCS

# Option A: Let Spark decide the number of files (simplest)
df_result.write.mode("overwrite").parquet(output)