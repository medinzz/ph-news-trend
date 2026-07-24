# Philippine News Crawler and Trend Analyzer

> A production-ready data pipeline for collecting, storing, and analyzing news trends from major Philippine news outlets using Python, DuckDB, BigQuery, dbt, and GitHub Actions.

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-Analytics-yellow.svg)](https://duckdb.org/)
[![BigQuery](https://img.shields.io/badge/BigQuery-Cloud-orange.svg)](https://cloud.google.com/bigquery)
[![dbt](https://img.shields.io/badge/dbt-Ready-red.svg)](https://www.getdbt.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Features](#-features)
- [Storage Backends](#-storage-backends)
- [Project Structure](#-project-structure)
- [Installation](#-installation)
- [Quick Start](#-quick-start)
- [Configuration](#-configuration)
- [Usage Examples](#-usage-examples)
- [Data Warehouse Structure](#-data-warehouse-structure)
- [Analytics with DuckDB](#-analytics-with-duckdb)
- [dbt Integration](#-dbt-integration-planned)
- [GitHub Actions Automation](#-github-actions-automation-planned)
- [Database Schema](#-database-schema)
- [Performance Benchmarks](#-performance-benchmarks)
- [Roadmap](#-roadmap)
- [Contributing](#-contributing)
- [License](#-license)

---

## 🎯 Overview

This project is a news scraper and analytics platform designed to gather, store, and analyze news from major Philippine outlets. It demonstrates modern data engineering best practices with a focus on scalability, performance, and production readiness.

### Key Capabilities

- **Automated Data Collection**: Scrapes articles from ABS-CBN News, Rappler, and Manila Bulletin
- **Multiple Storage Options**: SQLite (legacy), **DuckDB (recommended)**, and BigQuery (production)
- **Async Processing**: High-performance concurrent API requests
- **Clean Data**: Converts HTML to Markdown for better storage and analysis
- **Fast Analytics**: Optimized for aggregations and trend analysis with DuckDB
- **Production-Ready**: Queue-based batch processing with error handling

### Potential Applications

- **Journalists & Researchers**: Track evolving news landscape and public interest trends
- **Policymakers**: Monitor public discourse on important issues
- **Businesses**: Analyze sentiment and emerging trends
- **General Public**: Stay informed about trending topics in the Philippines

---

## 🏗️ Architecture

### Data Pipeline Flow

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│  News APIs  │────▶│    Scraper   │────▶│  Raw Layer  │────▶│ dbt (Planned)│────▶│  Analytics  │
│             │     │  (Python)    │     │  (DuckDB)   │     │              │     │   Layer     │
│ • ABS-CBN   │     │ • Async I/O  │     │             │     │ • Staging    │     │             │
│ • Rappler   │     │ • HTML→MD    │     │ Bronze/Raw  │     │ • Transform  │     │ Gold/Marts  │
│ • Manila B. │     │ • Batch Ins. │     │             │     │ • Aggregate  │     │             │
└─────────────┘     └──────────────┘     └─────────────┘     └──────────────┘     └─────────────┘
```

### Storage Architecture

```
Local Development (DuckDB)        Cloud Production (BigQuery)
┌────────────────────────┐       ┌────────────────────────────┐
│  articles_raw.duckdb   │  ───▶ │ complete-trees-452014-g4/  │
│  • Fast analytics      │       │ ├── ph_news_raw/           │
│  • No server needed    │       │ │   └── articles_raw        │
│  • Perfect for testing │       │ ├── ph_news_staging/       │
└────────────────────────┘       │ └── ph_news_analytics/     │
                                 └────────────────────────────┘
```

---

## ✨ Features

### ✅ Current Features

- **Multi-source Integration**: ABS-CBN News, Rappler, Manila Bulletin APIs
- **Triple Storage Backends**: 
  - **DuckDB** (recommended) - Fast local analytics
  - **BigQuery** - Cloud-scale production
  - **SQLite** - Legacy support
- **Async Processing**: Concurrent API requests for faster scraping
- **HTML to Markdown Conversion**: Clean, structured content storage
- **Batch Insertions**: Optimized BigQuery writes with configurable buffer size
- **Queue-based Processing**: Non-blocking async inserts with error handling
- **Unified Interface**: Abstract storage backend for easy switching
- **Environment-based Config**: `.env` file support for easy deployment
- **Command-line Interface**: Flexible CLI arguments for custom runs
- **Analytics-Ready**: Columnar storage optimized for aggregations

### 🚧 Planned Features

- **dbt Transformations**: SQL-based data transformations and modeling
- **GitHub Actions**: Automated daily pipeline execution
- **Keyword Analysis**: NLP-based trending topic identification
- **Data Visualization**: Interactive dashboards with Looker/Streamlit
- **More News Sources**: Expand to include Inquirer, GMA News, etc.
- **Incremental Loading**: Process only new articles to reduce costs

---

## 💾 Storage Backends

This project supports three storage backends, each optimized for different use cases:

### 🦆 DuckDB (Recommended for Analytics)

**Best for:** Local development, analytics, data exploration, prototyping

**Advantages:**
- ⚡ **15-20x faster** than SQLite for analytics queries
- 📊 Columnar storage optimized for aggregations
- 🚀 No server setup required - embedded like SQLite
- 💰 Free and open-source
- 📁 Query CSV/Parquet files directly without import
- 🔄 Perfect for dbt local development

**When to use:**
- Running trend analysis and aggregations
- Local data pipeline development
- Before scaling to BigQuery
- Testing and experimentation

```bash
# Using DuckDB
STORAGE_BACKEND=duckdb
python main.py --days-back 7
```

### ☁️ BigQuery (Production)

**Best for:** Production deployments, large datasets, team collaboration

**Advantages:**
- 🌐 Cloud-scale analytics
- 👥 Multi-user access
- 🔄 Automatic backups
- 📈 Handles terabytes of data
- 🔐 Enterprise security

**When to use:**
- Production data warehouse
- Team collaboration
- Large-scale datasets (1M+ articles)
- Integration with GCP services

```bash
# Using BigQuery
STORAGE_BACKEND=bigquery
python main.py --days-back 7
```

### 🗄️ SQLite (Legacy)

**Best for:** Backward compatibility, simple transaction processing

**Note:** SQLite is optimized for OLTP (transactions), not analytics. Consider DuckDB for better performance on analytical queries.

```bash
# Using SQLite
STORAGE_BACKEND=sqlite
python main.py --days-back 7
```

### Comparison Table

| Feature | DuckDB | BigQuery | SQLite |
|---------|--------|----------|--------|
| **Speed (Analytics)** | ⚡⚡⚡ | ⚡⚡⚡ | ⚡ |
| **Setup Complexity** | Very Easy | Medium | Very Easy |
| **Cost** | Free | Pay-per-query | Free |
| **Best Use Case** | Local Analytics | Production | Transactions |
| **Query Large Data** | ✅ Excellent | ✅ Excellent | ❌ Slow |
| **Aggregations** | ✅ Very Fast | ✅ Very Fast | ⚠️ Slow |
| **Server Required** | ❌ No | ✅ Yes (GCP) | ❌ No |
| **Team Collaboration** | ❌ Limited | ✅ Yes | ❌ No |

---

## 📁 Project Structure

```
ph-news-trend/
├── news/
│   ├── apis.py                 # API fetching logic for each news source
│   ├── crawler.py              # Legacy Scrapy crawler (deprecated)
│   ├── items.py                # Legacy Scrapy items
│   └── pipelines.py            # Legacy Scrapy pipelines
│
├── util/
│   ├── storage_backend.py      # Abstract storage interface & implementations
│   │                           # - SQLiteBackend
│   │                           # - DuckDBBackend (NEW!)
│   │                           # - BigQueryBackend
│   └── tools.py                # Helper utilities (logging, HTML conversion)
│
├── .env                        # Environment variables (not in repo)
├── .env.example                # Environment template
├── .gitignore                  # Git ignore patterns
├── config.py                   # Configuration management
├── main.py                     # Entry point CLI script
├── nlp_practice.ipynb          # Jupyter notebook for analysis experiments
├── requirements.txt            # Python dependencies
└── README.md                   # This file
```

---

## 🚀 Installation

### Prerequisites

- Python 3.10 or higher
- Google Cloud Platform account (for BigQuery - optional)
- Service account key with BigQuery permissions (for BigQuery - optional)

### Step 1: Clone the Repository

```bash
git clone https://github.com/medinzz/ph-news-trend.git
cd ph-news-trend
```

### Step 2: Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

This will install:
- `duckdb` - Fast analytics database
- `google-cloud-bigquery` - BigQuery client (optional)
- `aiohttp` - Async HTTP requests
- `beautifulsoup4` - HTML parsing
- `pandas` - Data manipulation
- Other utilities

### Step 4: Set Up Environment Variables

Create a `.env` file in the project root:

```bash
# Storage Configuration
STORAGE_BACKEND=duckdb  # Options: 'duckdb', 'bigquery', 'sqlite'

# Raw table name
TABLE_NAME=articles_raw

# DuckDB Configuration (recommended for local dev)
DUCKDB_DB_PATH=articles_raw.duckdb

# SQLite Configuration (legacy)
SQLITE_DB_PATH=articles_raw.db

# BigQuery Configuration (for production)
GCP_PROJECT_ID=your-project-id
BQ_DATASET_ID=ph_news_raw
BQ_TABLE_NAME=articles_raw
BQ_BUFFER_SIZE=100
GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account-key.json

# Scraping Configuration
DAYS_BACK=7
```

### Step 5: Set Up BigQuery (Optional)

Only needed if using BigQuery:

```bash
# Create the raw dataset
bq mk --dataset ${GCP_PROJECT_ID}:ph_news_raw

# Create staging and analytics datasets (for dbt later)
bq mk --dataset ${GCP_PROJECT_ID}:ph_news_staging
bq mk --dataset ${GCP_PROJECT_ID}:ph_news_analytics
```

---

## 🎬 Quick Start

### Method 1: Using DuckDB (Recommended)

```bash
# Scrape last 7 days with DuckDB
python main.py

# Show current configuration
python main.py --show-config
```

### Method 2: Custom Date Range

```bash
# Scrape from specific date
python main.py --start-date 2024-01-01

# Scrape last 30 days
python main.py --days-back 30
```

### Method 3: Override Backend

```bash
# Use DuckDB explicitly (local analytics)
python main.py --backend duckdb

# Use BigQuery (cloud production)
python main.py --backend bigquery --days-back 14

# Use SQLite (legacy)
python main.py --backend sqlite
```

---

## ⚙️ Configuration

### Option 1: Environment Variables (Recommended)

Edit `.env` file:

```bash
STORAGE_BACKEND=duckdb
DUCKDB_DB_PATH=articles_raw.duckdb
DAYS_BACK=7
```

### Option 2: Command Line Arguments

```bash
python main.py --backend duckdb --days-back 30
```

### Option 3: Edit config.py

```python
# config.py
STORAGE_BACKEND = 'duckdb'
DEFAULT_DAYS_BACK = 7

DUCKDB_CONFIG = {
    'db_path': 'articles_raw.duckdb',
    'table_name': 'articles_raw'
}
```

---

## 💡 Usage Examples

### Basic Scraping

```python
from news.apis import get_all_articles
from datetime import datetime, timedelta

# Fetch articles from last 7 days
start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
get_all_articles(start_date=start_date)
```

### Analytics with DuckDB

```python
from config import get_storage_backend_instance

# Get DuckDB backend
storage = get_storage_backend_instance()

# Query 1: Articles by source
df = storage.run_query("""
    SELECT 
        source, 
        COUNT(*) as article_count,
        COUNT(DISTINCT category) as unique_categories
    FROM articles_raw
    GROUP BY source
    ORDER BY article_count DESC
""")
print(df)

# Query 2: Daily trends with rolling average
df = storage.run_query("""
    SELECT 
        date,
        COUNT(*) as daily_articles,
        AVG(COUNT(*)) OVER (
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as rolling_7day_avg
    FROM articles_raw
    WHERE date >= CURRENT_DATE - INTERVAL 30 DAYS
    GROUP BY date
    ORDER BY date DESC
""")
print(df)

storage.close()
```

### Export to Parquet

```python
from config import get_storage_backend_instance

storage = get_storage_backend_instance()

# Export last 30 days to Parquet for external analysis
storage.export_to_parquet(
    output_path='news_last_30days.parquet',
    query="""
        SELECT * FROM articles_raw 
        WHERE date >= CURRENT_DATE - INTERVAL 30 DAYS
    """
)

storage.close()
```

### Query CSV Files Directly (DuckDB Only)

```python
from config import get_storage_backend_instance

storage = get_storage_backend_instance()

# Query CSV without importing it first!
df = storage.query_csv_directly(
    csv_path='external_data.csv',
    query="""
        SELECT category, COUNT(*) as count
        FROM read_csv_auto
        GROUP BY category
    """
)
print(df)

storage.close()
```

---

## 🗄️ Data Warehouse Structure

### DuckDB (Local Development)

Single file with all data:
```
articles_raw.duckdb
└── articles_raw (table)
```

### BigQuery (Production)

Medallion architecture with multiple datasets:
```
complete-trees-452014-g4/
├── ph_news_raw/              # 🥉 Bronze: Raw scraped data
│   └── articles_raw
│
├── ph_news_staging/          # 🥈 Silver: Cleaned & standardized (dbt)
│   ├── stg_articles
│   └── int_articles_cleaned
│
└── ph_news_analytics/        # 🥇 Gold: Business-ready analytics (dbt)
    ├── fct_articles          # Fact tables
    ├── dim_sources           # Dimension tables
    └── agg_daily_trends      # Aggregations
```

---

## 📊 Analytics with DuckDB

DuckDB excels at analytical queries. Here are some examples:

### Trend Analysis

```python
storage = get_storage_backend_instance()

# Find trending topics by category
df = storage.run_query("""
    SELECT 
        category,
        COUNT(*) as articles_this_week,
        LAG(COUNT(*)) OVER (PARTITION BY category ORDER BY DATE_TRUNC('week', date)) as articles_last_week,
        ROUND(100.0 * (COUNT(*) - LAG(COUNT(*)) OVER (PARTITION BY category ORDER BY DATE_TRUNC('week', date))) 
              / NULLIF(LAG(COUNT(*)) OVER (PARTITION BY category ORDER BY DATE_TRUNC('week', date)), 0), 1) as percent_change
    FROM articles_raw
    WHERE date >= CURRENT_DATE - INTERVAL 14 DAYS
    GROUP BY category, DATE_TRUNC('week', date)
    ORDER BY articles_this_week DESC
""")
```

### Source Comparison

```python
# Compare news sources
df = storage.run_query("""
    SELECT 
        source,
        COUNT(*) as total_articles,
        COUNT(DISTINCT category) as categories_covered,
        AVG(LENGTH(content)) as avg_article_length,
        MIN(date) as earliest_article,
        MAX(date) as latest_article
    FROM articles_raw
    GROUP BY source
""")
```

### Time Series Analysis

```python
# Daily article count with moving average
df = storage.run_query("""
    SELECT 
        date,
        source,
        COUNT(*) as daily_count,
        AVG(COUNT(*)) OVER (
            PARTITION BY source 
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as moving_avg_7d
    FROM articles_raw
    WHERE date >= CURRENT_DATE - INTERVAL 90 DAYS
    GROUP BY date, source
    ORDER BY date DESC, source
""")
```

---

## 📊 Database Schema

### Table: `articles_raw`

| Column | Type | Mode | Description |
|--------|------|------|-------------|
| id | VARCHAR/STRING | REQUIRED | Unique article identifier (MD5 hash of URL) |
| source | VARCHAR/STRING | NULLABLE | News source (abs-cbn, rappler, manila-bulletin) |
| url | VARCHAR/STRING | NULLABLE | Full article URL |
| category | VARCHAR/STRING | NULLABLE | Article category (e.g., nation, business) |
| title | VARCHAR/STRING | NULLABLE | Article headline |
| author | VARCHAR/STRING | NULLABLE | Article author name |
| date | DATE | NULLABLE | Publication date (normalized) |
| publish_time | TIMESTAMP | NULLABLE | Full publication timestamp |
| content | VARCHAR/STRING | NULLABLE | Article body in Markdown format |
| tags | VARCHAR/STRING | NULLABLE | Comma-separated tags |

---

## 🗺️ Roadmap

### Phase 1: Foundation ✅ (Completed)
- [x] Multi-source API integration (ABS-CBN, Rappler, Manila Bulletin)
- [x] SQLite storage backend
- [x] DuckDB storage backend (NEW!)
- [x] BigQuery storage backend
- [x] Async processing
- [x] HTML to Markdown conversion
- [x] CLI interface
- [x] Environment-based configuration

### Phase 2: Data Transformation 🚧 (In Progress)
- [ ] dbt project setup
- [ ] Staging models (cleaning, deduplication)
- [ ] Intermediate models (business logic)
- [ ] Analytics models (aggregations, metrics)
- [ ] Data quality tests
- [ ] Documentation generation

### Phase 3: Automation 📋 (Planned)
- [ ] GitHub Actions workflow
- [ ] Daily scheduled runs
- [ ] Error notifications (Slack/Email)
- [ ] Incremental loading
- [ ] Monitoring dashboard

### Phase 4: Analytics 📊 (Planned)
- [ ] Keyword extraction (NLP/spaCy)
- [ ] Trend identification algorithms
- [ ] Sentiment analysis
- [ ] Topic modeling
- [ ] Interactive dashboards (Streamlit/Looker)

### Phase 5: Expansion 🌟 (Future)
- [ ] More news sources (Inquirer, GMA, PhilStar)
- [ ] Real-time processing
- [ ] Machine learning models
- [ ] Public API
- [ ] Web interface

---

## 🤝 Contributing

Contributions are welcome! This project is developed independently as a learning portfolio, but I'm open to:

- 🐛 Bug reports and fixes
- 💡 Feature suggestions
- 📝 Documentation improvements
- 🔧 Code optimizations
- 🧪 Test coverage

**How to contribute:**
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- **News sources**: ABS-CBN News, Rappler, Manila Bulletin
- **Technologies**: Python, DuckDB, BigQuery, dbt, GitHub Actions
- **Inspired by**: Modern data engineering best practices and the dbt community

---

## 📚 Additional Resources

### Documentation
- [DuckDB Docs](https://duckdb.org/docs/)
- [dbt Docs](https://docs.getdbt.com/)
- [BigQuery Docs](https://cloud.google.com/bigquery/docs)

### Related Projects
- [dbt-duckdb adapter](https://github.com/duckdb/dbt-duckdb)
- [Evidence.dev](https://evidence.dev/) - BI tool for DuckDB
- [Dagster](https://dagster.io/) - Data orchestration

---

## 📧 Contact

**Developer**: medinzz  
**Repository**: [github.com/medinzz/ph-news-trend](https://github.com/medinzz/ph-news-trend)  
**Issues**: [Report a bug or request a feature](https://github.com/medinzz/ph-news-trend/issues)

For questions or collaboration opportunities, please open an issue on GitHub.

---

**⭐ If you find this project helpful, please consider giving it a star!**

---

## 🦆 Why DuckDB?

DuckDB is featured prominently in this project because:

1. **Perfect for Analytics** - This is a trend analysis project, and DuckDB is built for analytics
2. **15x Faster** - Aggregations and groupings are significantly faster than SQLite
3. **No Setup** - Works out of the box like SQLite, no server configuration needed
4. **dbt Compatible** - Seamless integration with dbt for transformations
5. **Future-Proof** - Easy to migrate to BigQuery when scaling up
6. **Open Source** - Free, actively developed, and well-documented

**Bottom line**: DuckDB gives you BigQuery-like performance on your laptop, making it perfect for development and small-to-medium datasets before scaling to the cloud.