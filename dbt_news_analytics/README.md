# dbt_news_analytics

dbt project for transforming raw PH news articles from MotherDuck.

## Stack

- **Adapter**: `dbt-duckdb` (connects to MotherDuck)
- **Source**: `ph_news.main.articles_raw` (written by the Scrapy crawler)
- **Target schemas**: `ph_news.staging`, `ph_news.intermediate`, `ph_news.marts`

## Setup

### 1. Install dependencies

```bash
pip install dbt-duckdb
dbt deps
```

### 2. Set your MotherDuck token

```bash
# Add to your .env or export directly
export MOTHERDUCK_TOKEN=your_token_here
```

### 3. Place profiles.yml

`profiles.yml` must be in one of two places:
- **Recommended**: `~/.dbt/profiles.yml` (dbt's default location)
- **Alternative**: in the project root, run dbt with `--profiles-dir .`

```bash
# Copy to dbt's default location
cp profiles.yml ~/.dbt/profiles.yml

# Or run with local profiles.yml
dbt run --profiles-dir .
```

### 4. Verify connection

```bash
dbt debug --profiles-dir .
```

You should see `Connection test: OK`

## Running

```bash
# Run all models
dbt run --profiles-dir .

# Run only staging models
dbt run --select staging --profiles-dir .

# Run tests
dbt test --profiles-dir .

# Run and test together
dbt build --profiles-dir .
```

## Development vs Production

The `profiles.yml` has two targets:

- **`prod`** (default): connects to `md:ph_news` on MotherDuck
- **`dev`**: connects to your local `articles_raw.duckdb` file

```bash
# Run against local DuckDB for development
dbt run --target dev --profiles-dir .

# Run against MotherDuck (default)
dbt run --profiles-dir .
```

## Model Layers

```
ph_news.main.articles_raw        ← source (written by crawler)
        ↓
ph_news.staging.stg_articles     ← cleaned, tags split into list
        ↓
ph_news.intermediate.*           ← enriched (TBD)
        ↓
ph_news.marts.*                  ← analytics-ready (TBD)
```

## BigQuery → MotherDuck Changes

| | BigQuery | DuckDB/MotherDuck |
|---|---|---|
| Exclude columns | `SELECT * EXCEPT (col)` | `SELECT * EXCLUDE (col)` |
| Split string | `SPLIT(str, ',')` | `STRING_SPLIT(str, ',')` |
| Auth | service account JSON | `MOTHERDUCK_TOKEN` env var |
| Schema location | dataset in GCP project | schema inside MotherDuck DB |