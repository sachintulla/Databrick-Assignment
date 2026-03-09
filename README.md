# SuperStore Sales & Business Performance Analytics System

An end-to-end retail analytics solution built with **Python, Pandas, PySpark, Apache Airflow, and Azure Databricks**.

## Project Structure

```
superstore-analytics/
├── data/
│   ├── generate_dataset.py       ← Synthetic dataset generator
│   ├── raw/                      ← Raw CSV input
│   ├── processed/                ← Cleaned Parquet output
│   └── output/                   ← Analytics CSVs + Dashboard PNGs
├── src/
│   ├── ingestion/
│   │   └── data_loader.py        ← Load + schema validate raw CSV
│   ├── cleaning/
│   │   └── data_cleaner.py       ← Clean, transform, derive metrics
│   ├── analytics/
│   │   ├── sales_analytics.py    ← Pandas KPIs, trends, category/region
│   │   ├── profitability_analytics.py ← Profit deep-dive, YoY, RoS
│   │   └── spark_analytics.py    ← PySpark groupBy, joins, windows
│   ├── visualization/
│   │   └── dashboard.py          ← 14 Matplotlib/Seaborn charts
│   └── utils/
│       ├── logger.py             ← Centralized logging
│       └── config.py             ← YAML config loader
├── airflow/
│   └── dags/
│       └── superstore_etl_dag.py ← 8-task Airflow DAG
├── notebooks/
│   ├── 01_data_exploration.ipynb ← EDA + schema validation
│   ├── 02_etl_pipeline.ipynb     ← Cleaning + derived metrics demo
│   ├── 03_analytics.ipynb        ← Pandas + PySpark analytics
│   └── 04_dashboard.ipynb        ← Chart generation + Databricks layout
├── tests/
│   ├── conftest.py               ← Shared fixtures
│   ├── test_ingestion.py         ← DataLoader tests
│   ├── test_cleaning.py          ← DataCleaner tests
│   └── test_analytics.py        ← Analytics module tests
├── run_pipeline.py               ← Local pipeline runner (no Airflow needed)
├── config.yaml                   ← All configuration
├── requirements.txt
└── pytest.ini
```

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Generate Dataset
```bash
python data/generate_dataset.py
```

### 3. Run Full Pipeline
```bash
# Full pipeline (Pandas + Spark + Dashboard)
python run_pipeline.py

# Skip PySpark if not installed locally
python run_pipeline.py --no-spark

# Skip dashboard (faster)
python run_pipeline.py --no-spark --no-dashboard
```

### 4. Run Tests
```bash
pytest
# With coverage
pytest --cov=src --cov-report=term-missing
```

### 5. Run Notebooks
```bash
jupyter notebook notebooks/
```

## Technology Stack

| Layer | Technology |
|-------|-----------|
| Data Processing | Python 3.11, Pandas, NumPy |
| Big Data | Databricks, PySpark 3.5 |
| Orchestration | Apache Airflow 2.8 |
| Storage | CSV (raw), Parquet (processed) |
| Visualization | Matplotlib, Seaborn, Plotly |
| Dashboard | Azure Databricks Dashboard |
| Version Control | Git & GitHub |

## Pipeline Stages

```
[1] Generate/Ingest Raw CSV
        ↓
[2] Schema & Data Validation
        ↓
[3] Clean & Transform
    - Remove duplicates
    - Handle nulls
    - Standardize categoricals
    - Create derived metrics
        ↓
[4] Pandas Analytics          [5] PySpark Analytics
    - KPI Summary                 - Distributed groupBy
    - Trend analysis              - Join operations
    - Category/Region             - Window functions
    - Loss detection              - Ranking
        ↓
[6] Dashboard Generation (14 charts)
        ↓
[7] Post-ETL Data Quality Checks
        ↓
[8] Success Notification
```

## Airflow DAG

The Airflow DAG (`airflow/dags/superstore_etl_dag.py`) runs daily and implements:
- 8 tasks with full dependency management
- 3 retries with 5-minute delay on each task
- XCom for inter-task data passing
- Post-ETL quality gate checks

```bash
# Copy DAG to Airflow DAGs folder
cp airflow/dags/superstore_etl_dag.py ~/airflow/dags/

# Test DAG locally
python airflow/dags/superstore_etl_dag.py --run-local
```

## Databricks Deployment

1. Upload notebooks from `notebooks/` to your Databricks workspace
2. Upload `src/` directory to DBFS: `/dbfs/superstore/src/`
3. Upload cleaned Parquet to: `/dbfs/superstore/processed/`
4. Run notebooks in order: `01 → 02 → 03 → 04`
5. In Notebook 04, click **Add to Dashboard** for each chart cell
6. Configure dashboard auto-refresh aligned with your DAG schedule

## Evaluation Coverage

| Criterion | Implementation | Weight |
|-----------|---------------|--------|
| Data Processing & ETL | `data_loader.py`, `data_cleaner.py`, Airflow DAG | 30% |
| Analytics & Business Insights | `sales_analytics.py`, `profitability_analytics.py`, `spark_analytics.py` | 25% |
| Workflow Automation | `superstore_etl_dag.py` (8 tasks, retries, logging, XCom) | 20% |
| Visualization & Reporting | `dashboard.py` (14 charts), Notebook 04 | 15% |
| Documentation & Presentation | README, docstrings, notebooks, config | 10% |

## Key Business Insights Generated

- **KPI Summary**: Total Sales, Profit, Orders, Customers, AOV, Margin
- **Sales Trends**: Monthly, Quarterly, Yearly with profit margin overlay
- **Category Analysis**: Revenue and profit by Category and Sub-Category
- **Regional Analysis**: Sales distribution and profitability by region
- **Loss Detection**: Products and sub-categories with negative profit
- **Discount Impact**: How discount depth correlates with margin erosion
- **Risk Analysis**: High-discount / low-profit SKUs requiring attention
- **YoY Growth**: Year-over-year profit growth per category
