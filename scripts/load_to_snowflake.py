#!/usr/bin/env python3
"""Load raw Olist CSV extracts and Open-Meteo weather data into Snowflake.

This script intentionally writes directly through the Snowflake connector and does not
use Snowpipe, S3, GCS, or any staging object store.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Sequence
from urllib import parse, request
from urllib.error import HTTPError, URLError

from dotenv import load_dotenv
import snowflake.connector


LOGGER = logging.getLogger("snowflake_loader")

SNOWFLAKE_ENV_VARS = (
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
)

OLIST_TABLES = {
    "RAW_OLIST_ORDERS": {
        "file_names": ["orders.csv", "olist_orders.csv"],
        "columns": [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
    },
    "RAW_OLIST_ORDER_ITEMS": {
        "file_names": ["order_items.csv", "olist_order_items.csv"],
        "columns": [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
        ],
    },
    "RAW_OLIST_CUSTOMERS": {
        "file_names": ["customers.csv", "olist_customers.csv"],
        "columns": [
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ],
    },
    "RAW_OLIST_SELLERS": {
        "file_names": ["sellers.csv", "olist_sellers.csv"],
        "columns": [
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
        ],
    },
    "RAW_OLIST_GEOLOCATION": {
        "file_names": ["geolocation.csv", "olist_geolocation.csv"],
        "columns": [
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng",
            "geolocation_city",
            "geolocation_state",
        ],
    },
}

REQUIRED_COLUMNS = {
    "RAW_OLIST_ORDERS": {
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
    }
}

CREATE_TABLE_SQL = {
    "RAW_OLIST_ORDERS": """
        CREATE TABLE IF NOT EXISTS RAW_OLIST_ORDERS (
            order_id VARCHAR,
            customer_id VARCHAR,
            order_status VARCHAR,
            order_purchase_timestamp VARCHAR,
            order_approved_at VARCHAR,
            order_delivered_carrier_date VARCHAR,
            order_delivered_customer_date VARCHAR,
            order_estimated_delivery_date VARCHAR
        )
    """,
    "RAW_OLIST_ORDER_ITEMS": """
        CREATE TABLE IF NOT EXISTS RAW_OLIST_ORDER_ITEMS (
            order_id VARCHAR,
            order_item_id VARCHAR,
            product_id VARCHAR,
            seller_id VARCHAR,
            shipping_limit_date VARCHAR,
            price VARCHAR,
            freight_value VARCHAR
        )
    """,
    "RAW_OLIST_CUSTOMERS": """
        CREATE TABLE IF NOT EXISTS RAW_OLIST_CUSTOMERS (
            customer_id VARCHAR,
            customer_unique_id VARCHAR,
            customer_zip_code_prefix VARCHAR,
            customer_city VARCHAR,
            customer_state VARCHAR
        )
    """,
    "RAW_OLIST_SELLERS": """
        CREATE TABLE IF NOT EXISTS RAW_OLIST_SELLERS (
            seller_id VARCHAR,
            seller_zip_code_prefix VARCHAR,
            seller_city VARCHAR,
            seller_state VARCHAR
        )
    """,
    "RAW_OLIST_GEOLOCATION": """
        CREATE TABLE IF NOT EXISTS RAW_OLIST_GEOLOCATION (
            geolocation_zip_code_prefix VARCHAR,
            geolocation_lat VARCHAR,
            geolocation_lng VARCHAR,
            geolocation_city VARCHAR,
            geolocation_state VARCHAR
        )
    """,
    "RAW_HISTORICAL_WEATHER_INGEST": """
        CREATE TABLE IF NOT EXISTS RAW_HISTORICAL_WEATHER_INGEST (
            hub_identifier VARCHAR,
            latitude NUMBER(10, 6),
            longitude NUMBER(10, 6),
            logistics_date DATE,
            temp_max_celsius NUMBER(10, 2),
            temp_min_celsius NUMBER(10, 2),
            precipitation_sum_mm NUMBER(10, 2),
            wind_speed_max_kmh NUMBER(10, 2)
        )
    """,
}

INSERT_SQL = {
    "RAW_OLIST_ORDERS": (
        "INSERT INTO RAW_OLIST_ORDERS ("
        "order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, "
        "order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    ),
    "RAW_OLIST_ORDER_ITEMS": (
        "INSERT INTO RAW_OLIST_ORDER_ITEMS ("
        "order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    ),
    "RAW_OLIST_CUSTOMERS": (
        "INSERT INTO RAW_OLIST_CUSTOMERS ("
        "customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state) "
        "VALUES (%s, %s, %s, %s, %s)"
    ),
    "RAW_OLIST_SELLERS": (
        "INSERT INTO RAW_OLIST_SELLERS ("
        "seller_id, seller_zip_code_prefix, seller_city, seller_state) VALUES (%s, %s, %s, %s)"
    ),
    "RAW_OLIST_GEOLOCATION": (
        "INSERT INTO RAW_OLIST_GEOLOCATION ("
        "geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state) "
        "VALUES (%s, %s, %s, %s, %s)"
    ),
    "RAW_HISTORICAL_WEATHER_INGEST": (
        "INSERT INTO RAW_HISTORICAL_WEATHER_INGEST ("
        "hub_identifier, latitude, longitude, logistics_date, temp_max_celsius, temp_min_celsius, "
        "precipitation_sum_mm, wind_speed_max_kmh) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    ),
}


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def require_env() -> dict[str, str]:
    values: dict[str, str] = {}
    missing = []
    for env_var in SNOWFLAKE_ENV_VARS:
        value = os.getenv(env_var)
        if not value:
            missing.append(env_var)
        else:
            values[env_var] = value
    if missing:
        raise RuntimeError(
            "Missing required Snowflake environment variables: " + ", ".join(missing)
        )
    return values


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def resolve_csv_path(table_name: str, file_names: Sequence[str]) -> Path:
    project_root = get_project_root()
    search_roots = [
        project_root / "data" / "olist",
        project_root / "data",
        project_root / "raw_data" / "olist",
        project_root,
    ]
    for root in search_roots:
        for file_name in file_names:
            candidate = root / file_name
            if candidate.exists():
                return candidate
    raise FileNotFoundError(
        f"Could not locate an Olist CSV file for {table_name}. Looked for: "
        + ", ".join(f"{root / name}" for root in search_roots for name in file_names)
    )


def normalize_name(value: str) -> str:
    return (value or "").strip().lower().replace(" ", "_")


def clean_string(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    if cleaned in {"", "nan", "NaN", "null", "NULL", "None", "N/A"}:
        return None
    return cleaned


def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    env_values = require_env()
    return snowflake.connector.connect(
        account=env_values["SNOWFLAKE_ACCOUNT"],
        user=env_values["SNOWFLAKE_USER"],
        password=env_values["SNOWFLAKE_PASSWORD"],
        warehouse=env_values["SNOWFLAKE_WAREHOUSE"],
        database=env_values["SNOWFLAKE_DATABASE"],
        schema=env_values["SNOWFLAKE_SCHEMA"],
        autocommit=False,
    )


def create_raw_tables(cur) -> None:
    for table_name, ddl in CREATE_TABLE_SQL.items():
        cur.execute(ddl)
        LOGGER.info("Ensured existence of %s", table_name)


def insert_rows_transactionally(cur, table_name: str, rows: Sequence[tuple]) -> None:
    if not rows:
        LOGGER.info("No rows to insert into %s", table_name)
        return

    sql = INSERT_SQL[table_name]
    try:
        cur.execute("BEGIN")
        cur.executemany(sql, rows)
        cur.execute("COMMIT")
    except Exception:
        cur.execute("ROLLBACK")
        raise

    LOGGER.info("Loaded %s rows into %s", len(rows), table_name)


def get_table_row_count(cur, table_name: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    result = cur.fetchone()
    return int(result[0]) if result else 0


def load_olist_csv_table(cur, table_name: str) -> int:
    table_config = OLIST_TABLES[table_name]
    existing_rows = get_table_row_count(cur, table_name)
    if existing_rows:
        LOGGER.info("Skipping %s; target table already contains %s rows", table_name, existing_rows)
        return 0

    csv_path = resolve_csv_path(table_name, table_config["file_names"])
    LOGGER.info("Loading Olist CSV for %s from %s", table_name, csv_path)

    expected_columns = table_config["columns"]
    required_columns = REQUIRED_COLUMNS.get(table_name, set())
    rows: list[tuple] = []
    skipped_rows = 0
    loaded_rows = 0

    with csv_path.open("r", newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        if reader.fieldnames is None:
            raise ValueError(f"CSV file {csv_path} does not contain a header row.")

        field_lookup = {normalize_name(name): name for name in reader.fieldnames if name is not None}
        missing_fields = [column for column in expected_columns if normalize_name(column) not in field_lookup]
        if missing_fields:
            raise ValueError(
                f"CSV file {csv_path} is missing expected columns: {', '.join(missing_fields)}"
            )

        for row in reader:
            values: list[str | None] = []
            for column in expected_columns:
                source_name = field_lookup[normalize_name(column)]
                value = clean_string(row.get(source_name))
                values.append(value)

            required_missing = any(
                values[expected_columns.index(column)] is None
                for column in required_columns
                if column in expected_columns
            )
            if required_missing:
                skipped_rows += 1
                continue

            rows.append(tuple(values))
            if len(rows) >= 1000:
                insert_rows_transactionally(cur, table_name, rows)
                loaded_rows += len(rows)
                rows = []

    if rows:
        insert_rows_transactionally(cur, table_name, rows)
        loaded_rows += len(rows)

    LOGGER.info("Finished %s: loaded=%s, skipped=%s", table_name, loaded_rows, skipped_rows)
    return loaded_rows


def read_geolocation_hubs(cur, csv_path: Path) -> list[tuple[str, float, float]]:
    """
    Read geolocation data and create weather hub identifiers, filtering to only
    zip prefixes that appear in actual Olist orders.
    
    First queries Snowflake to get the set of zip_code_prefix values that appear in:
      - RAW_OLIST_CUSTOMERS.customer_zip_code_prefix
      - RAW_OLIST_SELLERS.seller_zip_code_prefix
    
    Then computes ONE representative coordinate per relevant zip_code_prefix
    (average of all valid pings for that prefix), rounds to 2 decimal places
    (~1.1km precision) for cross-prefix deduplication. This guarantees at most
    one hub per zip_code_prefix before coordinate-based merging happens.
    
    Args:
        cur: Snowflake cursor for querying relevant zip prefixes.
        csv_path: Path to the geolocation CSV file.
    
    Returns:
        list of (zip_code_prefix, rounded_lat, rounded_lon) tuples.
        Multiple zip prefixes may map to the same rounded-coordinate hub.
    """
    # Query Snowflake for all zip prefixes that appear in customer or seller records
    cur.execute(
        "SELECT DISTINCT customer_zip_code_prefix FROM RAW_OLIST_CUSTOMERS "
        "WHERE customer_zip_code_prefix IS NOT NULL "
        "UNION "
        "SELECT DISTINCT seller_zip_code_prefix FROM RAW_OLIST_SELLERS "
        "WHERE seller_zip_code_prefix IS NOT NULL"
    )
    relevant_zip_prefixes: set[str] = {row[0] for row in cur.fetchall()}
    
    # Build representative coordinate per relevant zip prefix (average of all pings)
    prefix_coords: dict[str, tuple[list[float], list[float]]] = {}  # prefix -> ([lats], [lons])
    all_zip_prefixes: set[str] = set()
    
    with csv_path.open("r", newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        if reader.fieldnames is None:
            raise ValueError(f"CSV file {csv_path} does not contain a header row.")

        lookup = {normalize_name(name): name for name in reader.fieldnames if name is not None}
        required = ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"]
        missing = [column for column in required if normalize_name(column) not in lookup]
        if missing:
            raise ValueError(
                f"Geolocation CSV is missing required columns: {', '.join(missing)}"
            )

        for row in reader:
            zip_prefix = clean_string(row.get(lookup["geolocation_zip_code_prefix"]))
            lat_raw = clean_string(row.get(lookup["geolocation_lat"]))
            lon_raw = clean_string(row.get(lookup["geolocation_lng"]))
            if zip_prefix is None or lat_raw is None or lon_raw is None:
                continue
            
            all_zip_prefixes.add(zip_prefix)
            
            # Only process zip prefixes that actually appear in orders
            if zip_prefix not in relevant_zip_prefixes:
                continue
            
            try:
                lat = float(lat_raw)
                lon = float(lon_raw)
            except ValueError:
                continue
            
            # Accumulate all pings for this prefix
            if zip_prefix not in prefix_coords:
                prefix_coords[zip_prefix] = ([], [])
            prefix_coords[zip_prefix][0].append(lat)
            prefix_coords[zip_prefix][1].append(lon)
    
    # Compute representative coordinate per prefix and deduplicate by rounded coordinates
    hubs: list[tuple[str, float, float]] = []
    seen_coordinates: dict[tuple[float, float], str] = {}  # (rounded_lat, rounded_lon) -> representative zip_prefix
    
    for zip_prefix in sorted(relevant_zip_prefixes):  # Sort for deterministic ordering
        if zip_prefix not in prefix_coords:
            # No valid coordinates found for this relevant prefix
            continue
        
        lats, lons = prefix_coords[zip_prefix]
        if not lats or not lons:
            continue
        
        # Compute average coordinate for this prefix
        avg_lat = sum(lats) / len(lats)
        avg_lon = sum(lons) / len(lons)
        
        # Round to 2 decimal places for deduplication (~1.1km precision)
        rounded_lat = round(avg_lat, 2)
        rounded_lon = round(avg_lon, 2)
        coord_key = (rounded_lat, rounded_lon)
        
        if coord_key not in seen_coordinates:
            seen_coordinates[coord_key] = zip_prefix
            hubs.append((zip_prefix, rounded_lat, rounded_lon))
    
    num_hubs = len(hubs)
    num_relevant = len(relevant_zip_prefixes)
    num_total = len(all_zip_prefixes)
    
    # Sanity check: hubs should never exceed relevant zip prefixes
    if num_hubs > num_relevant:
        LOGGER.error(
            "REGRESSION: Generated %s hubs from %s relevant zip prefixes! "
            "Hub count should never exceed relevant prefix count. "
            "This indicates deduplication failed.",
            num_hubs,
            num_relevant,
        )
        raise ValueError(
            f"Hub deduplication failed: {num_hubs} hubs > {num_relevant} relevant zip prefixes"
        )
    
    LOGGER.info(
        "Restricting weather hubs to %s order-relevant zip prefixes (out of %s total in geolocation data)",
        num_relevant,
        num_total,
    )
    LOGGER.info(
        "Reduced %s relevant zip prefixes to %s unique weather grid hubs (%.1f%% reduction via coordinate rounding)",
        num_relevant,
        num_hubs,
        (num_relevant - num_hubs) / num_relevant * 100 if num_relevant > 0 else 0,
    )

    return hubs


def fetch_open_meteo_response(url: str, attempts: int = 6) -> dict | list:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            with request.urlopen(url, timeout=90) as response:
                payload = json.loads(response.read().decode("utf-8"))
                if isinstance(payload, (dict, list)):
                    return payload
                raise ValueError("Open-Meteo response was neither a JSON object nor a JSON array.")
        except HTTPError as exc:
            last_error = exc
            if attempt == attempts:
                raise
            if exc.code == 429:
                retry_after = exc.headers.get("Retry-After") if exc.headers else None
                try:
                    delay_seconds = max(0, float(retry_after)) if retry_after else None
                except ValueError:
                    delay_seconds = None
                if delay_seconds is None:
                    delay_seconds = 30 * (2 ** (attempt - 1))
                    LOGGER.warning(
                        "Open-Meteo HTTP 429 retry using fallback backoff sequence: %s seconds",
                        delay_seconds,
                    )
                else:
                    LOGGER.warning(
                        "Open-Meteo HTTP 429 retry using Retry-After header: %s seconds",
                        delay_seconds,
                    )
            else:
                delay_seconds = 2 ** (attempt - 1)
            LOGGER.warning(
                "Open-Meteo request failed (attempt %s/%s). Retrying in %s seconds.",
                attempt,
                attempts,
                delay_seconds,
            )
            time.sleep(delay_seconds)
        except (URLError, TimeoutError, OSError, ValueError) as exc:
            last_error = exc
            if attempt == attempts:
                raise
            delay_seconds = 2 ** (attempt - 1)
            LOGGER.warning(
                "Open-Meteo request failed (attempt %s/%s). Retrying in %s seconds.",
                attempt,
                attempts,
                delay_seconds,
            )
            time.sleep(delay_seconds)
    if last_error is not None:
        raise last_error
    raise RuntimeError("Open-Meteo request did not return a response.")


def open_meteo_weather_rows(
    hubs: Sequence[tuple[str, float, float]],
    start_date: date,
    end_date: date,
) -> tuple[list[tuple], int]:
    if not hubs:
        return [], 0

    latitudes = ",".join(str(lat) for _, lat, _ in hubs)
    longitudes = ",".join(str(lon) for _, _, lon in hubs)
    params = {
        "latitude": latitudes,
        "longitude": longitudes,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max",
        "timezone": "auto",
        "temperature_unit": "celsius",
        "wind_speed_unit": "kmh",
    }
    url = "https://archive-api.open-meteo.com/v1/archive?" + parse.urlencode(params)
    payload = fetch_open_meteo_response(url)

    rows: list[tuple] = []
    skipped_rows = 0

    # Open-Meteo multi-location responses are top-level arrays, one object per submitted location, per the API docs.
    if isinstance(payload, list):
        for location_index, (zip_code_prefix, latitude, longitude) in enumerate(hubs):
            hub_identifier = f"hub_{latitude}_{longitude}"
            location_payload = payload[location_index] if location_index < len(payload) else {}
            daily = location_payload.get("daily") or {}
            times = daily.get("time") or []
            max_temps = daily.get("temperature_2m_max") or []
            min_temps = daily.get("temperature_2m_min") or []
            precipitation = daily.get("precipitation_sum") or []
            wind_speeds = daily.get("wind_speed_10m_max") or []

            for day_index, time_value in enumerate(times):
                try:
                    logistics_date = date.fromisoformat(time_value)
                except ValueError:
                    skipped_rows += 1
                    continue

                try:
                    temp_max = float(max_temps[day_index]) if day_index < len(max_temps) and max_temps[day_index] is not None else None
                    temp_min = float(min_temps[day_index]) if day_index < len(min_temps) and min_temps[day_index] is not None else None
                    precip = float(precipitation[day_index]) if day_index < len(precipitation) and precipitation[day_index] is not None else None
                    wind_speed = float(wind_speeds[day_index]) if day_index < len(wind_speeds) and wind_speeds[day_index] is not None else None
                except (TypeError, ValueError):
                    skipped_rows += 1
                    continue

                if any(value is None for value in (temp_max, temp_min, precip, wind_speed)):
                    skipped_rows += 1
                    continue

                rows.append(
                    (
                        hub_identifier,
                        float(latitude),
                        float(longitude),
                        logistics_date,
                        temp_max,
                        temp_min,
                        precip,
                        wind_speed,
                    )
                )
    else:
        daily = payload.get("daily") or {}
        times = daily.get("time") or []
        max_temps = daily.get("temperature_2m_max") or []
        min_temps = daily.get("temperature_2m_min") or []
        precipitation = daily.get("precipitation_sum") or []
        wind_speeds = daily.get("wind_speed_10m_max") or []

        for location_index, (zip_code_prefix, latitude, longitude) in enumerate(hubs[:1]):
            hub_identifier = f"hub_{latitude}_{longitude}"
            for day_index, time_value in enumerate(times):
                try:
                    logistics_date = date.fromisoformat(time_value)
                except ValueError:
                    skipped_rows += 1
                    continue

                try:
                    temp_max = float(max_temps[day_index]) if day_index < len(max_temps) and max_temps[day_index] is not None else None
                    temp_min = float(min_temps[day_index]) if day_index < len(min_temps) and min_temps[day_index] is not None else None
                    precip = float(precipitation[day_index]) if day_index < len(precipitation) and precipitation[day_index] is not None else None
                    wind_speed = float(wind_speeds[day_index]) if day_index < len(wind_speeds) and wind_speeds[day_index] is not None else None
                except (TypeError, ValueError):
                    skipped_rows += 1
                    continue

                if any(value is None for value in (temp_max, temp_min, precip, wind_speed)):
                    skipped_rows += 1
                    continue

                rows.append(
                    (
                        hub_identifier,
                        float(latitude),
                        float(longitude),
                        logistics_date,
                        temp_max,
                        temp_min,
                        precip,
                        wind_speed,
                    )
                )

    return rows, skipped_rows


def load_historical_weather(cur) -> tuple[int, int]:
    geo_path = resolve_csv_path("RAW_OLIST_GEOLOCATION", OLIST_TABLES["RAW_OLIST_GEOLOCATION"]["file_names"])
    hubs = read_geolocation_hubs(cur, geo_path)
    if not hubs:
        raise ValueError(f"No valid geolocation hubs were found in {geo_path}.")

    default_start = "2016-09-01"
    default_end = "2018-10-31"
    start_date = date.fromisoformat(os.getenv("OPEN_METEO_START_DATE", default_start))
    end_date = date.fromisoformat(os.getenv("OPEN_METEO_END_DATE", default_end))
    if end_date < start_date:
        raise ValueError("OPEN_METEO_END_DATE must be on or after OPEN_METEO_START_DATE.")

    all_rows: list[tuple] = []
    skipped_rows = 0
    loaded_rows = 0

    for index in range(0, len(hubs), 100):
        batch = hubs[index : index + 100]
        # Build WHERE clause matching on rounded coordinates, not hub_identifier format.
        # This ensures the resumability check works for both old (zip_prefix-based) and new
        # (coordinate-based) hub_identifier formats, since latitude/longitude are always numeric.
        # Uses EXISTS with a VALUES subquery since Snowflake doesn't support tuple IN (VALUES ...) syntax.
        coord_values = ", ".join([f"({lat}, {lon})" for _, lat, lon in batch])
        cur.execute(
            f"SELECT COUNT(*) FROM RAW_HISTORICAL_WEATHER_INGEST "
            f"WHERE EXISTS ("
            f"  SELECT 1 FROM (VALUES {coord_values}) AS v(vlat, vlon) "
            f"  WHERE ROUND(latitude, 2) = v.vlat AND ROUND(longitude, 2) = v.vlon"
            f") "
            "AND logistics_date BETWEEN %s AND %s",
            (start_date, end_date),
        )
        existing_rows = cur.fetchone()
        if existing_rows and existing_rows[0]:
            LOGGER.info(
                "Skipping weather API batch: locations=%s, existing_rows=%s",
                len(batch),
                existing_rows[0],
            )
            continue

        if index:
            LOGGER.info("Pausing 5 seconds before the next weather API batch")
            time.sleep(5)
        rows, malformed_count = open_meteo_weather_rows(batch, start_date, end_date)
        skipped_rows += malformed_count
        all_rows.extend(rows)
        LOGGER.info(
            "Weather API batch: locations=%s, fetched=%s rows, skipped=%s",
            len(batch),
            len(rows),
            malformed_count,
        )

        if len(all_rows) >= 1000:
            insert_rows_transactionally(cur, "RAW_HISTORICAL_WEATHER_INGEST", all_rows)
            loaded_rows += len(all_rows)
            all_rows = []

    if all_rows:
        insert_rows_transactionally(cur, "RAW_HISTORICAL_WEATHER_INGEST", all_rows)
        loaded_rows += len(all_rows)

    LOGGER.info(
        "Finished RAW_HISTORICAL_WEATHER_INGEST: loaded=%s, skipped=%s",
        loaded_rows,
        skipped_rows,
    )
    return loaded_rows, skipped_rows


def main() -> int:
    load_dotenv()
    setup_logging()
    logger = LOGGER

    try:
        conn = get_snowflake_connection()
    except Exception:
        logger.exception("Snowflake connection failed. Check the environment variables.")
        return 1

    try:
        with conn.cursor() as cur:
            create_raw_tables(cur)

            for table_name in OLIST_TABLES:
                load_olist_csv_table(cur, table_name)

            load_historical_weather(cur)

        conn.close()
        logger.info("Snowflake raw load completed successfully.")
        return 0
    except Exception:
        logger.exception("Raw load failed; transaction was rolled back for the failed table load.")
        return 1
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    sys.exit(main())
