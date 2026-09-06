# Change Summary — Ingestion Optimization & SCD2 Mart

This supersedes the earlier version of this document, which only covered the
first (and since-corrected) grid-rounding pass. Sections below reflect the
current, verified state of the codebase.

## 1. Weather Hub Reduction — Final, Verified Approach

**Goal:** reduce total Open-Meteo API calls needed, both by avoiding
redundant nearby-coordinate fetches and by skipping locations irrelevant to
the analysis entirely.

**Final logic in `read_geolocation_hubs()` (scripts/load_to_snowflake.py):**
1. Query Snowflake once for the set of zip prefixes that actually appear as
   either a `customer_zip_code_prefix` or `seller_zip_code_prefix` in real
   Olist orders (15,078 of the geolocation file's 19,015 total prefixes are
   actually relevant).
2. For each relevant prefix, average all of its raw GPS pings into a single
   representative coordinate (one point per prefix, by construction).
3. Round each prefix's representative coordinate to 2 decimal places
   (~1.1km precision, well within ERA5's ~9-28km native grid) to merge
   prefixes that land on the same effective grid cell.
4. A safety check (`if num_hubs > num_relevant: raise ValueError`) guards
   against a regression we hit once during development, where an earlier,
   incorrect version of this logic deduplicated raw ping rows instead of
   per-prefix averages and produced MORE hubs than input prefixes.

**Verified real result:** 15,078 relevant prefixes → 11,394 unique hubs
(24.4% reduction), for a combined ~40% reduction from the original
19,015-hub baseline.

**`hub_identifier` format:** `hub_<lat>_<lon>`, derived from each hub's
rounded representative coordinate — not tied to any single zip prefix,
since multiple prefixes may share a hub.

## 2. Resumability Check — Coordinate-Based, Not String-Based

Early versions of the resumability check compared `hub_identifier` strings
directly. This broke once `hub_identifier`'s format changed (from
zip-prefix-based to coordinate-based), since already-committed rows used
the old format and would never match new-format lookups — risking silent
re-fetching of already-paid-for data against a scarce API quota.

**Fix:** the check in `load_historical_weather()` now matches on rounded
`latitude`/`longitude` values directly (both stored as real numeric columns
on every row regardless of what `hub_identifier` string was used at
insertion time), via:
```sql
WHERE EXISTS (
    SELECT 1 FROM (VALUES (lat1, lon1), ...) AS v(vlat, vlon)
    WHERE ROUND(latitude, 2) = v.vlat AND ROUND(longitude, 2) = v.vlon
)
```
This exact syntax was verified directly in Snowsight before use — an
earlier attempt using a bare `(a, b) IN (VALUES (...))` tuple form is not
valid Snowflake syntax and was corrected to the `EXISTS`-based pattern above.

## 3. Rate Limiting — Open-Meteo 429 Handling

Open-Meteo's archive endpoint enforces a fair-use throttle stricter and
slower-to-clear than its published 600/min, 5000/hour limits suggest for
this request pattern. Current handling in `fetch_open_meteo_response()`:
- 6 retry attempts (up from an initial 3)
- Exponential backoff on 429s: 30s, 60s, 120s, 240s, 480s
- Uses the `Retry-After` header when Open-Meteo provides one, falling back
  to the fixed sequence otherwise
- Batch size: 100 locations per request (reverted after briefly testing 10,
  which increased total request count without reducing 429 frequency)

**Known limitation:** the throttle does not reliably clear within a single
day; ingestion has proceeded across multiple sessions over several days,
relying entirely on the resumability logic to avoid redundant work.

## 4. Transform Layer — Unaffected by Hub Format Changes

`geo_zip_centroid.sql` and `geo_weather_station_bridge.sql` require no
changes from any of the above. Confirmed reasoning:
- `geo_zip_centroid.sql` computes centroids from `stg_olist_geolocation`
  directly and has no dependency on weather hub identifiers at all.
- `geo_weather_station_bridge.sql` joins zip centroids to weather stations
  via `ST_DISTANCE()` geodesic distance, never by matching `station_id`
  strings — so it is structurally robust to any `hub_identifier` format
  change.

## 5. New: `dim_customer_geography` — Type 2 SCD Mart

Built to genuinely demonstrate SCD2 mechanics using real data, not
simulated or fabricated change events — a deliberate design decision given
Olist's dataset is a static historical extract with no live "today vs
yesterday" signal.

**Approach:** uses Olist's real repeat-customer signal.
`customer_unique_id` identifies the same person across multiple orders
(unlike `customer_id`, which is generated per-order). For each
`customer_unique_id`, orders are replayed in purchase-date order; a new
SCD2 version begins whenever the customer's zip/city/state differs from
their immediately preceding order (via `LAG()` + `IS DISTINCT FROM`,
Snowflake's null-safe inequality — not `<=>`, which is MySQL syntax and is
invalid in Snowflake).

**Schema:** `customer_unique_id`, `customer_zip_code_prefix`,
`customer_city`, `customer_state`, `valid_from`, `valid_to`, `is_current`
(derived directly from `valid_to IS NULL`), `version_number`.

**Verified real result (via `dbt run` against live Snowflake data):**
252 of 96,096 unique customers (0.26%) show 2+ distinct address versions.
This is expected and reported honestly — most Olist customers are one-time
buyers, so genuine SCD2 activity is a small but real slice of the data,
not a flaw in the design.

**Tests, all passing against real data:**
- `not_null` on `customer_unique_id`, `valid_from`, `is_current`
- Composite `unique` test on `(customer_unique_id, version_number)`
  (verified as a valid dbt built-in `unique` test expression by inspecting
  dbt's installed macro source directly, not assumed)

## Current Overall State

| Layer | Status |
|---|---|
| Staging (6 models) | Built, tested, verified against live Snowflake |
| Transform (`geo_zip_centroid`, `geo_weather_station_bridge`) | Built, tested; weather coverage improves as ingestion continues |
| Mart (`dim_customer_geography`) | Built, tested, real finding documented |
| Weather ingestion | In progress — resumable, ~2.87M rows committed as of last check, hub count reduced to 11,394 |
| `fact_delivery_weather` | Not yet built |
| README / demo video | Not yet started |