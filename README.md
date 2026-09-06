# Olist Weather-Delay Analysis

A historical audit pipeline joining Brazilian e-commerce delivery data (Olist, 2016–2018) with real historical weather (Open-Meteo ERA5 reanalysis) to investigate whether severe weather is associated with late deliveries.

Built with **dbt Core + Snowflake**, using a layered staging → transform → mart architecture, including a genuine **Type 2 Slowly Changing Dimension** and real data-quality reporting throughout.

## Why historical audit, not live simulation

Olist's dataset is a static 2016–2018 extract. Rather than relabeling old orders as "live" data (which would mean fabricating timestamps to simulate a real-time pipeline that doesn't actually exist), this project pulls the *actual* historical weather for those same dates and asks whether real storms correlate with real recorded delays. This is a deliberate choice: a live-simulation demo would look more "modern," but it would require faking data provenance, which undermines the one thing a data pipeline is supposed to guarantee — that you can trust where the numbers came from.

## Key finding

Across ~99,441 Olist orders, the overall late-delivery rate was **7.9%**.

| Weather location | No severe weather | Severe weather | Ratio |
|---|---|---|---|
| Destination (customer) | 5.5% late | 14.8% late | 2.7x |
| Origin (seller) | 5.7% late | 15.2% late | 2.65x |

*Severe weather = precipitation > 20mm or wind speed > 40 km/h on any day during the order's purchase-to-delivery window.*

The similar effect size on both ends of the delivery journey suggests this isn't a purely last-mile phenomenon — severe weather shows a comparable association with delay whether it occurs at the shipping origin or the delivery destination.

**This is an observed association, not a causal claim.** The analysis does not control for confounds such as regional remoteness (areas with more severe weather may also have inherently longer baseline delivery times independent of weather) or seasonality. A natural next step would be a regression controlling for shipping distance and season to isolate weather's independent effect.

## Architecture

```
Raw ingestion (Python)          Staging (dbt views)       Transform (dbt tables)      Marts (dbt tables)
──────────────────────          ────────────────────      ──────────────────────      ───────────────────
Olist CSVs          ──┐
                       ├──►  RAW_* tables  ──►  stg_*  ──►  geo_zip_centroid      ──┐
Open-Meteo API ────────┘                                    geo_weather_station_bridge├──► fact_delivery_weather
                                                                                         │    dim_customer_geography
```

- **Ingestion** (`scripts/load_to_snowflake.py`): loads Olist CSVs and historical weather directly into Snowflake via `snowflake-connector-python`, no intermediate object storage. Weather is fetched from [Open-Meteo's Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api) (ERA5 reanalysis, gap-free global grid — see *Data source notes* below).
- **Staging**: light typing/renaming only, one view per raw source.
- **Transform**: resolves the Olist-to-weather geocoding gap — zip-code-prefix centroids computed via median lat/lng, then bridged to the nearest weather grid cell by geodesic distance (`ST_DISTANCE`), with a 50km QC tolerance.
- **Marts**:
  - `fact_delivery_weather` — one row per order, with independently-joined origin and destination weather aggregated across each order's delivery window.
  - `dim_customer_geography` — a genuine Type 2 SCD built from Olist's real repeat-customer signal (`customer_unique_id`), not simulated change events. See *SCD2 design* below.

## Data source notes

**Weather: Open-Meteo Historical Weather API (ERA5 reanalysis).** This is reanalysis data — a model+observation blend providing gap-free global coverage — not raw thermometer readings from physical stations. This tradeoff was chosen deliberately over station-based sources (e.g. NOAA GSOD) because Brazil's real weather station network is sparse outside major metros; a gap-free grid means every order's location resolves to *some* weather value instead of silently dropping interior/rural orders. The honest cost: values are interpolated, not directly measured.

**Weather station coverage QC:** `geo_weather_station_bridge` flags any zip-prefix centroid whose nearest weather grid cell is more than 50km away (`is_within_tolerance = false`). This is a real, reportable data-quality finding, not an error to hide — some interior Brazilian regions are genuinely farther from a resolvable grid cell than dense coastal metros.

**Ingestion status:** weather ingestion is rate-limited by Open-Meteo's fair-use throttle on their historical archive endpoint (undocumented exact threshold; empirically slower to clear than their published 600/min limit suggests for bulk multi-location historical requests). The ingestion script is fully resumable — it checks existing coverage before each batch and only fetches what's missing — so it has run across multiple sessions over several days rather than one sitting. As of the last check, ~2.87M of an estimated ~9M weather rows were loaded, covering 11,394 order-relevant grid cells (reduced from 19,015 raw zip prefixes via coordinate-based deduplication and restricting to zip prefixes actually used by real orders).

## SCD2 design: `dim_customer_geography`

Olist's dataset has no live "today vs. yesterday" signal to track — it's a static historical extract. Rather than fabricate synthetic change events to demonstrate SCD2 mechanics, this model uses a real signal already present in the data: `customer_unique_id` identifies the same person across multiple orders (distinct from `customer_id`, which Olist generates fresh per order). For repeat customers, address changes between consecutive orders are detected via `LAG()` + `IS DISTINCT FROM`, and versioned with standard `valid_from`/`valid_to`/`is_current` columns.

**Real result:** 252 of 96,096 unique customers (0.26%) show 2+ distinct address versions. This is expected — most Olist customers are one-time buyers — and is reported as a genuine data characteristic rather than treated as a shortfall.

## Setup

**Requirements:** Python 3.11+, a Snowflake account (free trial works — no credit card required, $400 credit / 30 days), dbt Core + dbt-snowflake.

> **Note on environment:** this project uses a **conda** environment, not a plain `venv`. `cryptography` (a `snowflake-connector-python` dependency) ships a compiled Rust extension that failed to load on Windows under Python 3.9 in development (`ImportError: DLL load failed`); conda-forge's pre-built binaries on Python 3.11 resolved this cleanly. If you hit the same DLL error on Windows, this is why.

```bash
conda create -n olist_pipeline python=3.11 -y
conda activate olist_pipeline
conda install -c conda-forge cryptography snowflake-connector-python -y
pip install -r requirements.txt
```

1. Download the [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) into `data/olist/` (rename files to `orders.csv`, `order_items.csv`, `customers.csv`, `sellers.csv`, `geolocation.csv`).
2. Create `.env` in the project root with your Snowflake credentials (see `.env` variable names in `scripts/load_to_snowflake.py`).
3. Create `~/.dbt/profiles.yml` from `profiles.yml.template` with the same credentials.
4. In Snowsight, run:
   ```sql
   CREATE DATABASE IF NOT EXISTS OLIST_WEATHER_DB;
   CREATE SCHEMA IF NOT EXISTS OLIST_WEATHER_DB.RAW;
   ```
5. Run ingestion (resumable — safe to re-run if interrupted or rate-limited):
   ```bash
   python scripts/load_to_snowflake.py
   ```
6. Build and test the dbt project:
   ```bash
   dbt run
   dbt test
   ```

## Known limitations

- **Weather ingestion is ongoing** (see status above). Orders whose relevant zip prefix's nearest weather station has no data yet for the needed date range show `NULL` weather aggregates in `fact_delivery_weather`, not zero or an error.
- **Multi-seller orders** in `fact_delivery_weather` reflect only the first seller's origin location (by `order_item_id`), to preserve one-row-per-order grain. A small simplification, documented in the model itself.
- **Snowflake trial account**: this project was built and demonstrated on Snowflake's free trial, which is time-boxed (30 days / $400 credit). A screen recording of the working pipeline is included as a permanent record, since the live warehouse may not remain queryable indefinitely.
- **ERA5 reanalysis, not station observations** — see *Data source notes* above.

## Tech stack

Python · Snowflake · dbt Core · Open-Meteo Historical Weather API · GitHub Actions (planned)
