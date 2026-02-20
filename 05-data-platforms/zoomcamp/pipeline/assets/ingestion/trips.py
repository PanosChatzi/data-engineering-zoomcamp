"""@bruin
name: ingestion.trips
type: python
image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: vendorid
    type: integer
    description: Vendor id
  - name: tpep_pickup_datetime
    type: timestamp
    description: Pickup datetime
  - name: tpep_dropoff_datetime
    type: timestamp
    description: Dropoff datetime
  - name: passenger_count
    type: integer
    description: Number of passengers
  - name: trip_distance
    type: float
    description: Trip distance (miles)
  - name: payment_type
    type: text
    description: Payment type
  - name: extracted_at
    type: timestamp
    description: Extraction timestamp (UTC)

@bruin"""

import os
import json
import logging
import datetime as _dt
from typing import List, Tuple

import pandas as pd

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def _months_between(start_date: _dt.date, end_date: _dt.date) -> List[Tuple[int, int]]:
    months = []
    cur = _dt.date(start_date.year, start_date.month, 1)
    end = _dt.date(end_date.year, end_date.month, 1)
    while cur <= end:
        months.append((cur.year, cur.month))
        if cur.month == 12:
            cur = _dt.date(cur.year + 1, 1, 1)
        else:
            cur = _dt.date(cur.year, cur.month + 1, 1)
    return months


def _parquet_url(taxi_type: str, year: int, month: int) -> str:
    # Public CDN for NYC TLC monthly parquet files used in many tutorials
    return f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month:02d}.parquet"


def materialize():
    """Fetch trip-month parquet files for each taxi type/month in the run window.

    Reads BRUIN_START_DATE and BRUIN_END_DATE from the environment (YYYY-MM-DD).
    Optionally reads BRUIN_VARS JSON and looks for a `taxi_types` list (e.g. ["yellow","green"]).

    Returns a pandas.DataFrame to be consumed by Bruin's Python materialization.
    """
    start = os.environ.get("BRUIN_START_DATE")
    end = os.environ.get("BRUIN_END_DATE")
    vars_json = os.environ.get("BRUIN_VARS")

    if vars_json:
        try:
            bruin_vars = json.loads(vars_json)
        except Exception:
            bruin_vars = {}
    else:
        bruin_vars = {}

    taxi_types = bruin_vars.get("taxi_types", ["yellow"]) or ["yellow"]

    if not start or not end:
        today = _dt.date.today()
        start_date = today
        end_date = today
    else:
        start_date = _dt.date.fromisoformat(start)
        end_date = _dt.date.fromisoformat(end)

    months = _months_between(start_date, end_date)

    dfs = []
    for taxi in taxi_types:
        for y, m in months:
            url = _parquet_url(taxi, y, m)
            try:
                df = pd.read_parquet(url, engine="pyarrow")
            except Exception as e:
                logger.warning("parquet read failed for %s: %s", url, e)
                # try csv fallback
                csv_url = url.replace(".parquet", ".csv")
                try:
                    df = pd.read_csv(csv_url)
                except Exception as e2:
                    logger.warning("csv fallback failed for %s: %s", csv_url, e2)
                    continue

            if df is None or df.shape[0] == 0:
                continue

            df["extracted_at"] = _dt.datetime.utcnow().isoformat()
            # tag which taxi dataset this row came from (yellow, green, etc.)
            df["taxi_type"] = taxi
            dfs.append(df)

    if not dfs:
        # return empty DataFrame with expected columns if nothing fetched
        cols = [
            "vendorid",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "payment_type",
            "extracted_at",
        ]
        return pd.DataFrame(columns=cols)

    final_df = pd.concat(dfs, ignore_index=True, copy=False)

    # normalize common column names (best-effort) to match staging expectations
    rename_map = {}

    # pickup / dropoff datetime
    if "tpep_pickup_datetime" in final_df.columns:
        rename_map["tpep_pickup_datetime"] = "pickup_datetime"
    if "tpep_dropoff_datetime" in final_df.columns:
        rename_map["tpep_dropoff_datetime"] = "dropoff_datetime"
    if "pickup_datetime" in final_df.columns and "pickup_datetime" not in rename_map.values():
        # already correct name
        pass
    # location ids (different files use different names)
    for col in ("PULocationID", "pulocationid", "PUlocationID", "pickup_location_id"):
        if col in final_df.columns:
            rename_map[col] = "pickup_location_id"
            break
    for col in ("DOLocationID", "dolocationid", "DOlocationID", "dropoff_location_id"):
        if col in final_df.columns:
            rename_map[col] = "dropoff_location_id"
            break

    # fare / total
    for col in ("fare_amount", "fare", "total_amount", "total"):
        if col in final_df.columns:
            rename_map[col] = "fare_amount"
            break

    # payment type (keep numeric code as `payment_type` to join with lookup)
    for col in ("payment_type", "payment_type_id", "paymenttype", "payment"):
        if col in final_df.columns:
            rename_map[col] = "payment_type"
            break

    # apply renames
    if rename_map:
        final_df = final_df.rename(columns=rename_map)

    # ensure essential columns exist (create empty/NULL-safe columns when missing)
    for c in ("pickup_datetime", "dropoff_datetime", "pickup_location_id", "dropoff_location_id", "fare_amount", "payment_type", "taxi_type"):
        if c not in final_df.columns:
            final_df[c] = pd.NA

    # coerce datetimes
    final_df["pickup_datetime"] = pd.to_datetime(final_df["pickup_datetime"], errors="coerce")
    final_df["dropoff_datetime"] = pd.to_datetime(final_df["dropoff_datetime"], errors="coerce")

    return final_df


