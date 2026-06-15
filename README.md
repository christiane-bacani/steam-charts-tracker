# Steam Charts Tracker — ETL Pipeline

A production-grade Python ETL pipeline that extracts, transforms, validates, and loads game statistics from [Steam Charts](https://steamcharts.com) into a structured data warehouse using a three-layer medallion architecture.

---

## Overview

Steam Charts Tracker automates the full data lifecycle from raw HTML extraction to analytics-ready dimensional tables — tracking trending games, top 100 games by current players, and all-time peak records.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                  DATA SOURCE                        │
│              steamcharts.com                        │
│     (Requests + BeautifulSoup scraping)             │
└────────────────────┬────────────────────────────────┘
                     │ Ingest + Load
                     ▼
┌─────────────────────────────────────────────────────┐
│                  RAW LAYER                          │
│         Raw HTML → Structured Tables                │
│   top5_trending_games_raw                           │
│   top100_games_raw                                  │
│   top10_records_raw                                 │
└────────────────────┬────────────────────────────────┘
                     │  Transform + Validate + Load
                     ▼
┌─────────────────────────────────────────────────────┐
│                STAGING LAYER (stg)                  │
│         Cleaned + Validated DataFrames              │
│   top5_trending_games_stg                           │
│   top100_games_stg                                  │
│   top10_records_stg                                 │
└────────────────────┬────────────────────────────────┘
                     │  Integrate + Dimension Build + Fact Build + Load
                     ▼
┌─────────────────────────────────────────────────────┐
│                  MART LAYER                         │
│         Analytics-Ready Dimensional Model           │
│   dim_rank_number         fact_trending_games       │
│   dim_timestamp           fact_top_games            │
│   dim_peak_year                                     │
│   dim_steam_game                                    │
│   dim_peak_month                                    │
└─────────────────────────────────────────────────────┘
```

---

## What Gets Tracked

| Dataset | Description |
|---|---|
| **Top 5 Trending Games** | Current trending games with player concurrency data |
| **Top 100 Games** | Ranked by current active players across 4 paginated pages |
| **Top 10 Records** | All-time peak player records per game |

---

## Tech Stack

| Layer | Tools |
|---|---|
| Extraction | Python, Requests, BeautifulSoup |
| Transformation | Pandas, SQLAlchemy |
| Validation | Custom validation modules per dataset |
| Storage | PostgreSQL (local), Snowflake (cloud) |
| Logging | Python structured logging |

---

## Project Structure

```
steam-charts-tracker/
│
├── etl/
│   ├── extract/
│   │   ├── extract.py       # Scraping logic per dataset
│   │   └── integrate.py     # Dimension integration from stg layer
│   ├── transform/
│   │   ├── transform.py     # Transformation logic per dataset
│   │   └── validate.py      # Validation logic per dataset
│   └── load/
│       └── load.py          # Load to raw / stg / mart schemas
│
├── utils/
│   ├── database/
│   │   ├── database.py      # Database creation
│   │   ├── schema.py        # Schema creation (raw, stg, mart)
│   │   └── table.py         # Table creation for raw layer
│   ├── extract/
│   │   └── parse.py         # BeautifulSoup parser utility
│   └── fact.py              # Fact table utilities
│
├── data/                    # Raw and processed data storage
├── logs/                    # Structured pipeline run logs
├── run.py                   # Pipeline entry point
├── .gitignore
└── README.md
```

---

## Pipeline Flow

```python
# 1. Initialize database, schemas, and raw tables
create_database("steam_charts")
create_schema("raw") → create_schema("stg") → create_schema("mart")

# 2. Extract → Load to raw layer
scrape_top5_trending_games()   → load to raw
scrape_top100_games()          → load to raw  (4 paginated pages)
scrape_top10_records()         → load to raw

# 3. Transform → Validate → Load to stg layer
transform → validate → load to stg  (per dataset)

# 4. Integrate dimensions → Transform → Validate → Load to mart layer
dim_rank_number / dim_steam_game / dim_timestamp /
dim_peak_month / dim_peak_year → load to mart
```

---

## Setup

### Prerequisites
- Python 3.8+
- PostgreSQL running locally
- Snowflake account (for cloud load)

### Installation

```bash
git clone https://github.com/christiane-bacani/steam-charts-tracker
cd steam-charts-tracker
pip install -r requirements.txt
```

### Configuration

Create a `.env` file in the root directory:

```env
DB_HOST=localhost
DB_PORT=5432
DB_USER=your_user
DB_PASSWORD=your_password
DB_NAME=steam_charts

SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=steam_charts
SNOWFLAKE_WAREHOUSE=your_warehouse
```

### Run

```bash
python run.py
```

---

## Roadmap

- [ ] Wrap pipeline in Apache Airflow DAGs for scheduled orchestration
- [ ] Add fact table construction for mart layer
- [ ] Power BI / dashboard integration for player trend visualization
- [ ] Add app_id and game_name to trending_games scraper
- [ ] Migrate raw data storage from dict to local JSON
- [ ] Add Snowflake cloud load integration

---

## Author

**Christiane Rhely Joselle A. Bacani**
Data Engineer | ETL & Analytics Pipeline Developer
[GitHub](https://github.com/christiane-bacani) · [LinkedIn](https://www.linkedin.com/in/christianebacani)
