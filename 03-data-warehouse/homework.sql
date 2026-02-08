-- Question 1: Counting records
CREATE OR REPLACE EXTERNAL TABLE `ny-taxi-trips-485416.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://my-nyc-taxi-data/yellow_tripdata_2024-*.parquet']
);

SELECT COUNT(1) FROM ny-taxi-trips-485416.nytaxi.external_yellow_tripdata;

-- Question 2: Data read estimation
CREATE OR REPLACE TABLE ny-taxi-trips-485416.nytaxi.yellow_tripdata_non_partitioned AS
SELECT * FROM ny-taxi-trips-485416.nytaxi.external_yellow_tripdata;

SELECT DISTINCT(PULocationID) FROM nytaxi.yellow_tripdata_non_partitioned;

SELECT DISTINCT(PULocationID) FROM nytaxi.external_yellow_tripdata;

-- Question 3: Understanding columnar storage
SELECT PULocationID 
FROM nytaxi.yellow_tripdata_non_partitioned;

SELECT 
  PULocationID, 
  DOLocationID,
FROM nytaxi.yellow_tripdata_non_partitioned;

-- Question 4: Counting zero fare trips
SELECT COUNT(*)
FROM nytaxi.yellow_tripdata_non_partitioned
WHERE fare_amount = 0;

-- Question 5: Partitioning and clustering
CREATE OR REPLACE TABLE `ny-taxi-trips-485416.nytaxi.yellow_trips_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT *
FROM `ny-taxi-trips-485416.nytaxi.yellow_tripdata_non_partitioned`;

-- Question 6: Partition benefits
SELECT DISTINCT(VendorID)
FROM `nytaxi.yellow_trips_optimized`
WHERE 
  tpep_dropoff_datetime >= '2024-03-01' 
  AND tpep_dropoff_datetime <= '2024-03-15';

SELECT DISTINCT(VendorID)
FROM `nytaxi.yellow_tripdata_non_partitioned`
WHERE 
  tpep_dropoff_datetime >= '2024-03-01' 
  AND tpep_dropoff_datetime <= '2024-03-15';

-- Question 9: Understanding table scans
SELECT *
FROM `ny-taxi-trips-485416.nytaxi.yellow_tripdata_non_partitioned`;
