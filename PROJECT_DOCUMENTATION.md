# SuperStore Sales & Business Performance Analytics System
## Project Documentation

**Version:** 1.0
**Date:** March 2026
**Platform:** Azure Databricks · Apache Airflow · Python · PySpark · Delta Lake

---

## Table of Contents
1. [Project Objective](#1-project-objective)
2. [System Architecture](#2-system-architecture)
3. [Raw Data — Source, Structure & Handling](#3-raw-data--source-structure--handling)
4. [Assumptions](#4-assumptions)
5. [Implementation](#5-implementation)
6. [Azure Cloud Integration](#6-azure-cloud-integration)
7. [Orchestration](#7-orchestration)
8. [Analytics & KPIs](#8-analytics--kpis)
9. [Visualization & Dashboard](#9-visualization--dashboard)
10. [Testing](#10-testing)
11. [How to Run](#11-how-to-run)

---

## 1. Project Objective

### Business Problem
A retail company "SuperStore" operates across 4 US regions (East, West, Central, South) selling Furniture, Office Supplies, and Technology products. Leadership needs:
- Real-time visibility into sales performance and profitability
- Identification of loss-making products and discount impact
- Regional and category-level trend analysis for strategic decisions
- Automated, repeatable data pipeline that eliminates manual reporting

### Technical Objective
Build a **production-grade, end-to-end analytics system** that:
1. Ingests raw transactional data with schema validation
2. Cleans, transforms, and enriches the data with business metrics
3. Runs multi-dimensional analytics using both Pandas and PySpark
4. Stores data in a cloud-native Medallion architecture on Azure
5. Orchestrates the full pipeline via Apache Airflow and Databricks Workflows
6. Delivers interactive dashboards through Databricks AI/BI (Lakeview)

---

## 2. System Architecture

### Medallion Architecture (Bronze → Silver → Gold)

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                 │
│   Synthetic CSV (9,980 rows)  ←  data/generate_dataset.py          │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER — Raw Data (Azure ADLS Gen2 / adls:bronze/)          │
│  Format: CSV  |  No transformation  |  Append-only                 │
└──────────────────────────┬──────────────────────────────────────────┘
                           │  DataLoader (schema validation)
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER — Cleaned Data (Azure ADLS Gen2 / adls:silver/)      │
│  Format: Parquet  |  Typed columns  |  8 derived metrics           │
│  DataCleaner: null handling, dedup, type casting, enrichment        │
└──────────────────────────┬──────────────────────────────────────────┘
                           │  Analytics Modules
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER — Analytics (Azure ADLS Gen2 / adls:gold/)             │
│  Format: CSV + Delta Lake  |  Aggregated KPIs  |  Business-ready   │
│  monthly_trend · category_performance · regional_performance        │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│  PRESENTATION LAYER                                                 │
│  • 14 Matplotlib/Seaborn charts  (data/output/*.png)               │
│  • Databricks AI/BI Dashboard    (Lakeview — 8 widgets)            │
└─────────────────────────────────────────────────────────────────────┘
```

### Component Interaction
```
run_pipeline.py (local)
    │
    ├── DataLoader      → validates schema, loads CSV
    ├── DataCleaner     → cleans + derives 8 metrics → Parquet
    ├── SalesAnalytics  → Pandas KPIs, trends, segments
    ├── ProfitabilityAnalytics → YoY, Return-on-Sales
    ├── SparkAnalytics  → PySpark groupBy, window functions
    ├── Dashboard       → 14 charts saved as PNG
    └── AzurePipeline   → uploads to Blob, ADLS, DBFS

Airflow DAG (airflow/dags/superstore_etl_dag.py)
    └── 8 tasks → same modules, orchestrated with retries + XCom

Databricks Workflows (Job ID: 105927601063399)
    └── 5 notebook tasks → Bronze→Silver→Gold→Delta→Dashboard
```

---

## 3. Raw Data — Source, Structure & Handling

### Data Source
The dataset is a **synthetic replica** of the classic Tableau SuperStore Sales dataset, generated programmatically via `data/generate_dataset.py` using Python's `random` and `numpy` libraries with `seed=42` for reproducibility.

**Why synthetic?** The original SuperStore dataset is proprietary. The synthetic generator mirrors its schema, distributions, and business logic exactly — allowing full pipeline development without licensing constraints.

### Dataset Dimensions
| Attribute | Value |
|-----------|-------|
| Total Rows | 9,980 transactions |
| Time Period | 4 years (2021–2024) |
| Regions | 4 (East, West, Central, South) |
| States | 24 US states |
| Categories | 3 (Furniture, Office Supplies, Technology) |
| Sub-Categories | 17 |
| Unique Products | ~200 |
| Customer Segments | 3 (Consumer, Corporate, Home Office) |
| Ship Modes | 4 (Standard, Second, First Class, Same Day) |

### Raw Schema (21 columns)
| Column | Type | Description |
|--------|------|-------------|
| Row ID | Integer | Unique row identifier |
| Order ID | String | Order identifier (e.g., CA-2021-152156) |
| Order Date | Date | Date order was placed |
| Ship Date | Date | Date order was shipped |
| Ship Mode | String | Shipping method chosen |
| Customer ID | String | Unique customer identifier |
| Customer Name | String | Full customer name |
| Segment | String | Customer segment |
| Country | String | Always "United States" |
| City | String | City of delivery |
| State | String | State of delivery |
| Postal Code | String | ZIP code |
| Region | String | Geographic region |
| Product ID | String | Unique product identifier |
| Category | String | Product category |
| Sub-Category | String | Product sub-category |
| Product Name | String | Full product name |
| Sales | Float | Revenue from sale |
| Quantity | Integer | Units sold |
| Discount | Float | Discount rate applied (0.0–0.8) |
| Profit | Float | Net profit (can be negative) |

### How Raw Data is Handled

**Step 1 — Schema Validation (`src/ingestion/data_loader.py`)**
```
Raw CSV → Check required columns → Validate data types
       → Detect nulls above threshold → Raise on schema mismatch
       → Return validated DataFrame
```
- 21 required columns are checked; missing columns raise `ValueError`
- Data types validated: dates parsed, numerics coerced
- Null threshold: warn if any column exceeds 10% nulls
- Duplicate Row IDs flagged and logged

**Step 2 — Cleaning (`src/cleaning/data_cleaner.py`)**
```
Validated DF → Drop full duplicates → Fill nulls
             → Cast types → Filter outliers
             → Add 8 derived columns → Write Parquet
```

**Null Handling Strategy:**
| Column | Strategy |
|--------|----------|
| Postal Code | Fill with `"00000"` |
| Sales / Profit | Fill with `0.0` |
| Order Date / Ship Date | Drop rows (critical field) |
| Quantity | Fill with `1` |

**Outlier Handling:**
- Sales > $20,000 → flagged but retained (legitimate bulk orders)
- Discount > 0.8 → capped at 0.8 (business rule: max 80% discount)
- Negative Profit → retained (loss-making orders are valid business data)

---

## 4. Assumptions

### Data Assumptions
1. **Country is always USA** — no international transactions; country column kept for schema completeness
2. **Profit can be negative** — high discounts cause losses; these are intentional and analytically important
3. **One order can have multiple rows** — each row is a line item; Order ID is not unique
4. **Date range is 2021–2024** — 4 complete fiscal years for YoY comparison
5. **Discount is a rate (0.0–0.8), not a dollar amount** — discount_amount = Sales × Discount/(1-Discount)
6. **Shipping days = Ship Date − Order Date** — negative shipping days (data error) are set to 0
7. **Postal Code is a string** — preserves leading zeros (e.g., "01234")

### Infrastructure Assumptions
8. **Azure ADLS Gen2 account `sachinxcube`** with filesystem `superstore` is pre-provisioned
9. **Databricks cluster `1222-063728-zf9e66jm`** is available and running
10. **Databricks Secret Scope `superstore-secrets`** exists with `adls-account-key` stored
11. **SQL Warehouse `92e1f064c1ae75a6`** is accessible for dashboard queries
12. **Python 3.12** is required for Airflow 2.9.3 (system Python 3.14 not supported)

### Pipeline Assumptions
13. **Idempotent runs** — pipeline can be re-run safely; Parquet and Delta tables use `overwrite` mode
14. **Airflow runs in SequentialExecutor** — single-machine setup; production would use CeleryExecutor
15. **Schema evolution** — column names with spaces are sanitized (spaces → underscores) before Delta writes
16. **Managed tables for SQL Warehouse** — `dash_*` copies stored on DBFS (not ADLS) to avoid credential issues with the SQL Serverless Warehouse

---

## 5. Implementation

### 5.1 ETL Pipeline

#### DataLoader (`src/ingestion/data_loader.py`)
- Reads CSV with `pandas.read_csv()`
- Validates 21 required columns against expected schema
- Parses `Order Date` and `Ship Date` as datetime
- Logs row count, null summary, duplicate count
- Returns clean, validated `DataFrame`

#### DataCleaner (`src/cleaning/data_cleaner.py`)
Applies transformations in sequence:

```python
# 8 Derived Metrics Added
profit_margin    = Profit / Sales                      # profitability ratio
discount_amount  = Sales * Discount / (1 - Discount)  # dollar value of discount
shipping_days    = (Ship Date - Order Date).days       # fulfilment speed
order_year       = Order Date.year                     # for YoY analysis
order_month      = Order Date.month                    # for seasonality
order_quarter    = Order Date.quarter                  # for quarterly trends
order_day_of_week = Order Date.day_name()             # for day-of-week patterns
is_loss          = Profit < 0                          # loss flag (boolean)
```

Output: `data/processed/superstore_clean.parquet`

### 5.2 Analytics Modules

#### SalesAnalytics (`src/analytics/sales_analytics.py`)
Pure Pandas analytics covering:
- **KPI Summary**: total sales, total profit, avg order value, profit margin, discount rate, loss %
- **Monthly/Quarterly/Yearly Trends**: time-series aggregations
- **Category & Sub-Category Performance**: sales, profit, margin by product group
- **Regional Analysis**: performance by region and state
- **Segment Analysis**: Consumer vs Corporate vs Home Office
- **Discount Impact**: profit margin by discount bucket (0%, 0–10%, 10–20%, >20%)
- **Shipping Analysis**: avg days and order share by ship mode
- **Loss-Making Products**: ranked list of products with negative profit

#### ProfitabilityAnalytics (`src/analytics/profitability_analytics.py`)
- **Year-over-Year Growth**: sales and profit % change vs prior year
- **Return on Sales (RoS)**: by category and sub-category
- **Profitability by State**: geographic heatmap data
- **High-Discount Loss Analysis**: orders where discount > 30%

#### SparkAnalytics (`src/analytics/spark_analytics.py`)
PySpark operations demonstrating distributed computing:
- **GroupBy Aggregations**: sales/profit by category, region, segment
- **Window Functions**: running totals, rank by sales within region
- **Joins**: product dimension join for enriched reporting
- **Broadcast Join**: small dimension table broadcast for efficiency

### 5.3 Project File Structure
```
Databrick-Assignment/
├── data/
│   ├── generate_dataset.py     ← Synthetic data generator (seed=42)
│   ├── raw/                    ← Bronze: raw CSV
│   ├── processed/              ← Silver: cleaned Parquet
│   └── output/                 ← Gold: CSVs + 14 PNG charts
├── src/
│   ├── ingestion/data_loader.py
│   ├── cleaning/data_cleaner.py
│   ├── analytics/
│   │   ├── sales_analytics.py
│   │   ├── profitability_analytics.py
│   │   └── spark_analytics.py
│   ├── storage/
│   │   ├── azure_blob.py       ← Blob Storage client
│   │   ├── adls_client.py      ← ADLS Gen2 client
│   │   └── databricks_client.py ← DBFS upload client
│   └── visualization/dashboard.py
├── airflow/dags/superstore_etl_dag.py
├── notebooks/
│   ├── 01_data_exploration.ipynb
│   ├── 02_etl_pipeline.ipynb
│   ├── 03_analytics.ipynb
│   ├── 04_dashboard.ipynb
│   ├── 05_azure_databricks_setup.ipynb  ← Delta Lake tables
│   └── 06_airflow_etl_pipeline.ipynb   ← Airflow demo
├── tests/                      ← 62 pytest tests
├── run_pipeline.py             ← Local end-to-end runner
└── config.yaml                 ← All configuration
```

---

## 6. Azure Cloud Integration

### Storage Architecture
```
Azure Storage (xcubeassignment)     Azure ADLS Gen2 (sachinxcube)
┌──────────────────────────┐       ┌────────────────────────────────┐
│ Blob Container:          │       │ Filesystem: superstore          │
│ superstore-analytics     │       │                                 │
│  • raw CSV               │       │  bronze/  ← raw CSV            │
│  • cleaned Parquet       │       │  silver/  ← cleaned Parquet    │
│  • analytics CSVs        │       │  gold/    ← analytics CSVs     │
│  • dashboard PNGs        │       │  delta/   ← Delta Lake tables  │
└──────────────────────────┘       └────────────────────────────────┘
                                           ↑
                                   Databricks DBFS
                                   /dbfs/superstore/
                                    ← notebooks
                                    ← pipeline code
```

### Delta Lake Tables (Gold Layer)
| Table | Rows | Partitioned By | Purpose |
|-------|------|----------------|---------|
| `superstore_clean` | 9,748 | `order_year`, `Category` | Full cleaned dataset |
| `monthly_trend` | 48 | — | Monthly sales/profit aggregation |
| `category_performance` | 3 | — | Per-category KPIs |
| `regional_performance` | 4 | — | Per-region KPIs |

### Managed Dashboard Tables (DBFS)
Copies of Gold tables stored in DBFS managed storage, accessible by the SQL Serverless Warehouse without external ADLS credentials:

| Table | Source | Rows |
|-------|--------|------|
| `dash_monthly_trend` | `delta/monthly_trend/` | 48 |
| `dash_category_performance` | `delta/category_performance/` | 3 |
| `dash_regional_performance` | `delta/regional_performance/` | 4 |
| `dash_superstore` | `delta/superstore_clean/` | 9,748 |

### Security
- Credentials stored in **Databricks Secret Scope** (`superstore-secrets`)
- `.env` file is **gitignored** — never committed to source control
- ADLS accessed via **Account Key** auth (production: use Managed Identity)
- SQL Warehouse blocked from direct ADLS key config → managed table workaround

---

## 7. Orchestration

### Apache Airflow DAG (`airflow/dags/superstore_etl_dag.py`)
```
DAG: superstore_etl_pipeline
Schedule: @daily  |  Executor: SequentialExecutor  |  Retries: 2

validate_raw_data
      ↓
ingest_data  ←──────────── XCom: raw_path
      ↓
clean_transform_data  ←─── XCom: raw_path
      ↓
run_pandas_analytics  ←─── XCom: clean_path
      ↓
run_spark_analytics   ←─── XCom: clean_path
      ↓
generate_dashboard    ←─── XCom: clean_path
      ↓
upload_to_azure       ←─── XCom: output_dir
      ↓
data_quality_checks   ←─── XCom: clean_path
```

**Key Airflow Features Used:**
- `PythonOperator` for all tasks
- `XCom push/pull` for inter-task data passing (file paths)
- `retries=2`, `retry_delay=timedelta(minutes=5)`
- `depends_on_past=False` — each run is independent
- `catchup=False` — no backfill on scheduler start

### Databricks Workflow (Job ID: 105927601063399)
```
Task 1: bronze_ingestion   → 01_data_exploration.ipynb
Task 2: silver_transform   → 02_etl_pipeline.ipynb
Task 3: gold_analytics     → 03_analytics.ipynb
Task 4: gold_delta_tables  → 05_azure_databricks_setup.ipynb
Task 5: dashboard          → 04_dashboard.ipynb
(All tasks: retries=1, timeout=600s, existing cluster)
```

---

## 8. Analytics & KPIs

### Key Performance Indicators
| KPI | Formula | Significance |
|-----|---------|--------------|
| Total Sales | `SUM(Sales)` | Top-line revenue |
| Total Profit | `SUM(Profit)` | Bottom-line health |
| Profit Margin % | `SUM(Profit)/SUM(Sales) × 100` | Operational efficiency |
| Avg Order Value | `SUM(Sales) / COUNT(DISTINCT Order ID)` | Customer spend level |
| Loss Line Items % | `COUNT(is_loss=True) / COUNT(*) × 100` | Risk indicator |
| Overall Discount Rate | `AVG(Discount) × 100` | Pricing strategy health |
| YoY Sales Growth | `(Current - Prior) / Prior × 100` | Business trajectory |
| Return on Sales | `Profit / Sales` by sub-category | Product profitability |

### Analytical Dimensions
- **Time**: Month, Quarter, Year, Day-of-Week
- **Geography**: Region, State, City
- **Product**: Category, Sub-Category, Product Name
- **Customer**: Segment (Consumer / Corporate / Home Office)
- **Operational**: Ship Mode, Discount Bucket, Shipping Days

---

## 9. Visualization & Dashboard

### Local Charts (14 PNG files — `data/output/`)
| Chart | Type | Insight |
|-------|------|---------|
| Monthly Sales Trend | Line | Revenue seasonality |
| Quarterly Trend | Bar + Line | Quarterly growth |
| Yearly Trend | Bar | Annual comparison |
| Category Performance | Grouped Bar | Category sales vs profit |
| Sub-Category Performance | Horizontal Bar | Best/worst sub-categories |
| Sub-Category RoS | Bar | Return on Sales ranking |
| Regional Heatmap | Seaborn Heatmap | Region × Category matrix |
| Profitability by State | Choropleth-style Bar | State-level profit |
| Segment Performance | Pie + Bar | Segment revenue share |
| Discount Impact | Line | Margin erosion by discount level |
| Shipping Analysis | Bar | Ship mode preferences |
| Loss Products | Horizontal Bar | Top loss-making SKUs |
| YoY Growth | Bar | Year-over-year change |
| KPI Summary Card | Text/Table | Executive summary |

### Databricks AI/BI Dashboard (Lakeview)
**Dashboard ID:** `01f11c93745e16bc8a7f8953affeeafb`

8 interactive widgets:
- 4 KPI Counter cards (Sales, Profit, Orders, Margin %)
- Monthly Sales & Profit trend (Line chart)
- Sales by Category (Bar chart)
- Sales by Region (Bar chart)
- Top 15 Products by Sales (Table)

Data source: `spark_catalog.default.dash_*` managed tables

---

## 10. Testing

### Test Coverage (62 tests across 3 modules)
```
tests/
├── test_ingestion.py    ← DataLoader: schema validation, type checking, null handling
├── test_cleaning.py     ← DataCleaner: derived metrics, null fills, outlier caps
└── test_analytics.py   ← SalesAnalytics, ProfitabilityAnalytics: KPI values, trends
```

**Run tests:**
```bash
pytest tests/ -v --tb=short
# Result: 62 passed in ~8s
```

**Key test scenarios:**
- Missing required column → `ValueError` raised
- All-null column → handled gracefully, not crashed
- `profit_margin` calculated correctly on known values
- `shipping_days` never negative
- `is_loss = True` when `Profit < 0`
- YoY growth returns correct percentage change

---

## 11. How to Run

### Local (No Cloud Required)
```bash
# 1. Clone and install
git clone https://github.com/sachintulla/Databrick-Assignment.git
cd Databrick-Assignment
pip install -r requirements.txt

# 2. Generate dataset
python data/generate_dataset.py

# 3. Run full pipeline (Spark optional)
python run_pipeline.py --no-spark      # Pandas only (~10s)
python run_pipeline.py                 # With PySpark (~45s)

# 4. Run tests
pytest tests/ -v
```

### With Azure (Full Cloud Pipeline)
```bash
# 1. Copy and fill credentials
cp .env.example .env
# Fill: DATABRICKS_HOST, DATABRICKS_TOKEN, AZURE_ADLS_ACCOUNT_KEY, etc.

# 2. Run pipeline with cloud upload
python run_pipeline.py --upload

# 3. Run Airflow DAG (requires Python 3.12 venv)
cd airflow
python3.12 -m venv .venv
.venv/bin/pip install apache-airflow==2.9.3
airflow db init
airflow dags trigger superstore_etl_pipeline
```

### In Databricks (Notebook by Notebook)
```
1. Run 01_data_exploration   → understand raw data
2. Run 02_etl_pipeline       → clean + transform
3. Run 03_analytics          → KPIs + trends
4. Run 04_dashboard          → generate charts
5. Run 05_azure_databricks_setup → create Delta tables
6. Run 06_airflow_etl_pipeline   → demonstrate Airflow
```

### Via Databricks Workflow (Automated)
```
Databricks UI → Workflows → Job 105927601063399
              → "Run now" button
              → Monitor 5 tasks completing in ~6 minutes
```

---

## Summary

| Component | Technology | Status |
|-----------|-----------|--------|
| Data Generation | Python / NumPy | ✅ 9,980 rows |
| Schema Validation | Pandas / Custom | ✅ 21 columns validated |
| ETL Cleaning | Pandas DataCleaner | ✅ 8 derived metrics |
| Pandas Analytics | SalesAnalytics / ProfitabilityAnalytics | ✅ 15+ KPIs |
| Spark Analytics | PySpark groupBy / Window | ✅ Distributed compute |
| Airflow Orchestration | Airflow 2.9.3 / 8-task DAG | ✅ All tasks pass |
| Azure Blob Storage | azure-storage-blob | ✅ Uploaded |
| ADLS Gen2 | azure-storage-file-datalake | ✅ Medallion layers |
| Databricks DBFS | Databricks SDK v0.97 | ✅ Notebooks + data |
| Delta Lake | 4 Delta tables | ✅ OPTIMIZE + Time Travel |
| Databricks Workflow | 5-task job | ✅ SUCCESS in ~6 min |
| Dashboard | Lakeview AI/BI — 8 widgets | ✅ Live |
| Unit Tests | pytest | ✅ 62/62 passing |
| Source Control | GitHub | ✅ Public repo |
