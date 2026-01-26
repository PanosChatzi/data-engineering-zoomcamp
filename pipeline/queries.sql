SELECT
	*
FROM
	PUBLIC.YELLOW_TAXI_DATA
LIMIT
	1;

SELECT
	*
FROM
	PUBLIC.ZONES;

SELECT
	TPEP_PICKUP_DATETIME,
	TPEP_DROPOFF_DATETIME,
	TOTAL_AMOUNT,
	CONCAT(ZPU."Borough", ' / ', ZPU."Zone") AS "pickup_location",
	CONCAT(ZPU."Borough", ' / ', ZDO."Zone") AS "dropoff_location"
FROM
	PUBLIC.YELLOW_TAXI_DATA T,
	ZONES ZPU,
	ZONES ZDO
WHERE
	T."PULocationID" = ZPU."LocationID"
	AND T."DOLocationID" = ZDO."LocationID"
LIMIT
	100;

SELECT
	TPEP_PICKUP_DATETIME,
	TPEP_DROPOFF_DATETIME,
	TOTAL_AMOUNT,
	CONCAT(ZPU."Borough", ' / ', ZPU."Zone") AS "pickup_location",
	CONCAT(ZPU."Borough", ' / ', ZDO."Zone") AS "dropoff_location"
FROM
	YELLOW_TAXI_DATA T
	JOIN ZONES ZPU ON T."PULocationID" = ZPU."LocationID"
	JOIN ZONES ZDO ON T."DOLocationID" = ZDO."LocationID"
LIMIT
	100;

SELECT
	CAST(DATE_TRUNC('DAY', TPEP_DROPOFF_DATETIME) AS DATE) AS "day",
	"DOLocationID",
	COUNT(1) as "count",
	MAX(total_amount),
	MAX(passenger_count)
FROM
	YELLOW_TAXI_DATA T
GROUP BY
	1, 2
ORDER BY 
	"day" ASC,
	"DOLocationID" ASC;

SELECT
	CAST(DATE_TRUNC('DAY', TPEP_DROPOFF_DATETIME) AS DATE) AS "day",
	COUNT(1) as "count",
	MAX(total_amount),
	MAX(passenger_co),
	MAX(total_amount),
FROM
	YELLOW_TAXI_DATA T
GROUP BY
	CAST(DATE_TRUNC('DAY', TPEP_DROPOFF_DATETIME) AS DATE)
ORDER BY "count" DESC;

-- Homework
-- Trips in November 2025 (inclusive lower bound, exclusive upper bound)
SELECT
	COUNT(*)
FROM
	PUBLIC.GREEN_TAXI_DATA
WHERE
	LPEP_PICKUP_DATETIME >= TIMESTAMP '2025-11-01'
	AND LPEP_PICKUP_DATETIME < TIMESTAMP '2025-12-01'
	AND TRIP_DISTANCE <= 1;

-- 8007
-- Which was the pick up day with the longest trip distance? Only consider trips with trip_distance less than 100 miles (to exclude data errors).
SELECT
	CAST(DATE_TRUNC('DAY', LPEP_PICKUP_DATETIME) AS DATE) AS "day",
	TRIP_DISTANCE
FROM
	PUBLIC.GREEN_TAXI_DATA
WHERE
	TRIP_DISTANCE < 100
ORDER BY
	TRIP_DISTANCE DESC
LIMIT
	5;

-- 2025-11-14


-- Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?
SELECT
  zpu."Zone" AS pickup_zone,
  SUM(t.total_amount) AS sum_total_amount
FROM public.green_taxi_data t
JOIN public.zones zpu
  ON t."PULocationID" = zpu."LocationID"
WHERE t.lpep_pickup_datetime >= TIMESTAMP '2025-11-18'
  AND t.lpep_pickup_datetime <  TIMESTAMP '2025-11-19'
GROUP BY zpu."Zone"
ORDER BY sum_total_amount DESC
LIMIT 1; -- East Harlem North

--For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?


SELECT
    zdo."Zone" AS dropoff_zone,
    t.tip_amount,
    t.lpep_pickup_datetime
FROM public.green_taxi_data t
JOIN public.zones zpu
    ON t."PULocationID" = zpu."LocationID"
JOIN public.zones zdo
    ON t."DOLocationID" = zdo."LocationID"
WHERE zpu."Zone" = 'East Harlem North'
  AND t.lpep_pickup_datetime >= TIMESTAMP '2025-11-01'
  AND t.lpep_pickup_datetime <  TIMESTAMP '2git 025-12-01'
ORDER BY t.tip_amount DESC
LIMIT 1; --Yorkville West

