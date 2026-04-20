# bixi dbt project

Transforms raw [Bixi](https://bixi.com) bike-share data into analytics-ready tables in BigQuery.

## Architecture

```text
RAW LAYER (BigQuery dataset: raw)
├── trips_2023
├── trips_2024
└── trips_2025
        ↓
STAGING LAYER (views)
└── stg_trips       — cleans, renames columns, unions all years
        ↓
MART LAYER (tables)
├── fct_trips       — one row per trip, with time dimensions and duration buckets
├── agg_stations    — one row per station per year, with departure/arrival net flow
└── fct_winter      — fct_trips filtered to winter months (Dec–Mar), winters 2024 & 2025
```

**Staging** models are materialized as views (no storage cost, always reflects source).  
**Mart** models are materialized as tables (pre-computed for BI tool performance).

## Models

### `stg_trips`

Cleans and unions raw trip tables across all years using a Jinja loop — adding a year
means adding one entry to the `years` list and declaring the source in `sources.yml`.
Filters out null timestamps and trips where end time is before start time.

### `fct_trips`

Base fact table for trip-level analysis. Adds time dimensions (month, day of week, hour)
and duration bucketing (short / medium / long) on top of `stg_trips`.

### `agg_stations`

Station-level supply/demand analysis. Computes departures, arrivals, and net flow per
station per year. Excludes low-volume stations (< 100 trips) and extreme outliers.

### `fct_winter`

Winter season analysis built on top of `fct_trips`. Covers December–March for winters
2024 and 2025. Uses a fiscal-year convention: December 2023 belongs to winter 2024.

## Setup

**Requirements:** dbt-bigquery, a GCP project with OAuth credentials configured.

```bash
# Install dependencies
pip install dbt-bigquery

# Configure your profile (once)
# Edit ~/.dbt/profiles.yml — see profiles.yml structure below

# Run all models
dbt run

# Run tests
dbt test

# Generate and serve documentation
dbt docs generate
dbt docs serve
```

**Profile** (`~/.dbt/profiles.yml`):

```yaml
bixi:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: <your-gcp-project>
      dataset: analytics
      location: northamerica-northeast1
      threads: 4
```

## Adding a new year

1. Load the raw data into BigQuery as `raw.trips_YYYY`
2. Add `YYYY` to the `years` list in `models/staging/stg_trips.sql`
3. Declare the new source in `models/staging/sources.yml`
4. Update `accepted_values` tests for the `year` column in both `schema.yml` files
5. Run `dbt run && dbt test`
