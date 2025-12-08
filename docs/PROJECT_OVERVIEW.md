# NSF Grants Cancellation Prediction Project

## Project Goal

**Objective:** Predict whether National Science Foundation (NSF) grants will be cancelled or terminated by the Trump administration.

### Business Problem Statement

The Trump administration has shown interest in reviewing and potentially terminating certain federally-funded research grants, particularly those deemed politically sensitive or ideologically opposed to the administration's agenda. This project aims to build a predictive model to identify NSF grants at risk of cancellation, enabling researchers and institutions to:

- Proactively identify vulnerable grants
- Understand factors that contribute to grant cancellation risk
- Support strategic planning and funding diversification
- Provide early warning systems for research institutions

### Target Variable

**Binary Classification:** `is_cancelled` (0 = Active/Completed, 1 = Cancelled/Terminated)

---

## Datasets

### 1. NSF Grants Dataset (Primary - JSON)
- **Source:** NSF Public API (`https://api.nsf.gov/services/v1/awards.json`)
- **Period:** 2015-2025
- **Format:** JSON
- **Location:** `datalake/landing/nsf_grants/`
- **Description:** Comprehensive dataset of all NSF grant awards including:
  - Award ID, Title, Abstract
  - Principal Investigator information
  - Institution details
  - Funding amount and duration
  - Research fields/programs
  - Award date and status

### 2. US Legislators Dataset (JSON)
- **Source:** United States Congress Legislators Historical Data
- **URL:** `https://unitedstates.github.io/congress-legislators/legislators-historical.json`
- **Format:** JSON
- **Location:** `datalake/landing/legislators.json`
- **Description:** Historical information about US Congress members including:
  - Names, party affiliation, terms
  - State representation
  - Committee memberships
- **Use Case:** Cross-reference with grant locations, analyze political climate impact

### 3. Terminated Grants Dataset (CSV)
- **Source:** TidyTuesday NSF Terminations
- **URL:** `https://raw.githubusercontent.com/rfordatascience/tidytuesday/main/data/2025/2025-05-06/nsf_terminations.csv`
- **Format:** CSV
- **Location:** `datalake/landing/terminated_data.csv`
- **Description:** List of NSF grants that were officially terminated
- **Use Case:** Ground truth labels for model training

### 4. Cruz List Dataset (CSV)
- **Source:** Senate Commerce Committee
- **URL:** Senate Commerce Committee file server
- **Format:** Excel/CSV
- **Location:** `datalake/landing/cruz_list.csv`
- **Description:** List of grants flagged by Senator Ted Cruz's office for potential review/termination
- **Use Case:** Important predictor feature - grants on this list have higher cancellation risk

---

## Data Lake Architecture

```
datalake/
├── landing/              # Raw data as ingested
│   ├── nsf_grants/
│   │   ├── 2015.json
│   │   ├── 2016.json
│   │   ├── ...
│   │   └── nsf_awards_spark/  # Spark partitioned by year
│   ├── legislators.json
│   ├── legislators/           # Parquet format
│   ├── terminated_data.csv
│   ├── terminated_data/       # Parquet format
│   ├── cruz_list.csv
│   └── cruz_list/             # Parquet format
│
├── formatted/            # MongoDB collections (standardized)
│   └── [To be implemented]
│
└── exploitation/         # Delta tables (ML-ready)
    └── [To be implemented]
```

---

## Task Progress Review

### ✅ A.1 - Business Problem Definition (COMPLETED)
**Status:** ✅ Complete

**Datasets Selected:**
1. ✅ NSF Grants (2015-2025) - JSON format
2. ✅ US Legislators - JSON format
3. ✅ Terminated Grants - CSV format
4. ✅ Cruz List - CSV format (Excel converted)

**Analysis Defined:**
- **Target:** Predict grant cancellation/termination
- **Features:** Grant metadata, funding details, research topics, Cruz list membership, geographic/political factors
- **Justification:** 
  - ≥3 datasets requirement met (4 datasets)
  - ≥2 JSON datasets requirement met (NSF Grants, Legislators)
  - Real-world relevance with policy implications
  - Sufficient data volume (10+ years of grants)

---

### ✅ A.2 - Data Lake Architecture Design (COMPLETED)
**Status:** ✅ Complete

**Architecture Implemented:**
- Medallion architecture with 3 zones (Landing, Formatted, Exploitation)
- Clear data flow: Sources → Landing → Formatted (MongoDB) → Exploitation (Delta)
- Directory structure established in local file system

**Documentation:** See "Data Lake Architecture" section above

---

### ✅ A.3 - Data Collection Pipelines (COMPLETED)
**Status:** ✅ Complete

**Implementation:** `scripts/data_collection.py`

**Features Implemented:**
1. ✅ **NSF Grants Collector:**
   - Fetches data from NSF Public API
   - Paginated requests with retry logic (max 3 retries)
   - Year-by-year collection (2015-2025)
   - Saves both JSON (per year) and Spark-partitioned format
   - Rate limiting (0.2s pause between requests)
   - Configurable periods (supports daily/weekly/monthly runs via parameters)

2. ✅ **Terminated Grants Collector:**
   - Downloads CSV from TidyTuesday repository
   - Converts to Parquet for Spark compatibility

3. ✅ **Legislators Collector:**
   - Downloads JSON from GitHub repository
   - Converts to Parquet format

4. ✅ **Cruz List Collector:**
   - Downloads Excel file from Senate website
   - Converts to CSV and Parquet formats

**Periodic Execution Support:**
- ✅ Year-based collection allows for incremental updates
- ✅ `overwrite` parameter for re-running specific periods
- ✅ Designed to be scheduled via Airflow

**Technologies:**
- ✅ PySpark for data processing
- ✅ Requests library for API calls
- ✅ Pandas for Excel/CSV handling
- ✅ Retry logic and error handling implemented

---

### ⚠️ A.4 - Data Formatting Pipelines (NOT STARTED)
**Status:** ❌ Not Started

**File:** `scripts/data_formatter.py` (empty)

**Required Implementation:**
- [ ] Read raw data from Landing Zone (4 datasets)
- [ ] Apply minimal, common cleaning rules
- [ ] Standardize data models (syntactic homogenization)
- [ ] Reconcile data for future integration (e.g., award IDs, geographic codes)
- [ ] Write to MongoDB with one collection per dataset:
  - `nsf_grants` collection
  - `legislators` collection
  - `terminated_grants` collection
  - `cruz_list` collection
- [ ] Add metadata and enrichment where applicable

**Next Steps:**
1. Set up MongoDB connection
2. Define schema for each collection
3. Implement Spark → MongoDB write operations
4. Add data validation and quality checks

---

### ⚠️ A.5 - Move Data to Exploitation Zone (NOT STARTED)
**Status:** ❌ Not Started

**File:** `scripts/data_transformer.py` (empty)

**Required Implementation:**
- [ ] Read from MongoDB Formatted Zone
- [ ] Apply analysis-specific transformations:
  - Feature engineering (text analysis of abstracts, funding patterns)
  - Data integration (join grants with Cruz list, terminated list)
  - Label creation (`is_cancelled` target variable)
  - Geographic/political feature engineering from legislators data
- [ ] Data cleaning specific to ML requirements:
  - Handle missing values
  - Remove duplicates
  - Filter relevant time periods
- [ ] Write to Delta table in Exploitation Zone
- [ ] Implement appropriate partitioning (e.g., by year, cancellation status)

**Expected Output:**
- Delta table with ML-ready features and target variable
- Partitioned for efficient querying

---

### ❌ B.1 - Model Training and Validation (NOT STARTED)
**Status:** ❌ Not Started

**Required Implementation:**
- [ ] Load data from Exploitation Zone (Delta table)
- [ ] Split into training/validation sets (e.g., 80/20)
- [ ] Train ≥2 classification models using Spark MLlib:
  - Suggested: Logistic Regression, Random Forest, Gradient Boosted Trees
- [ ] Store hyperparameters for each model
- [ ] Evaluate on validation set with metrics:
  - Accuracy
  - Precision/Recall (important for imbalanced classes)
  - F1-score
  - AUC-ROC
- [ ] Rank models by performance
- [ ] Automatically deploy best model

**Considerations:**
- Class imbalance (few cancelled grants vs. many active)
- May need SMOTE or class weighting
- Feature importance analysis

---

### ❌ B.2 - Model Management (NOT STARTED)
**Status:** ❌ Not Started

**Required Implementation:**
- [ ] Install and configure MLflow
- [ ] Log experiments:
  - Models (serialized)
  - Hyperparameters
  - Metrics (accuracy, recall, etc.)
- [ ] Register models in MLflow Model Registry
- [ ] Tag best-performing model for deployment
- [ ] Version tracking for model iterations

**MLflow Setup Needed:**
- [ ] Install: `pip install mlflow`
- [ ] Initialize tracking server
- [ ] Configure storage backend
- [ ] Create model registry

---

### ❌ B.3 - ML Results Documentation (NOT STARTED)
**Status:** ❌ Not Started

**Required:**
- [ ] Create visualizations:
  - Confusion matrices
  - ROC curves
  - Feature importance plots
  - Precision-recall curves
- [ ] Generate comparison tables for models
- [ ] Document insights and findings
- [ ] Include in final PDF deliverable

---

### ❌ C.1 - Scheduling (NOT STARTED)
**Status:** ❌ Not Started

**Required Implementation:**
- [ ] Install Apache Airflow (script exists: `install_airflow.sh`)
- [ ] Create DAG for complete pipeline
- [ ] Define schedule intervals:
  - Data collection: Weekly/Monthly (NSF API updates)
  - Formatting: After collection completes
  - Transformation: After formatting completes
  - Model training: Monthly/On-demand
- [ ] Configure `start_date`, `schedule_interval`, `catchup`

**Airflow Setup:**
- ✅ `AIRFLOW_HOME` configured in `setup_env.sh`
- ⚠️ Airflow installed but no DAGs created yet

---

### ❌ C.2 - Dependency Management (NOT STARTED)
**Status:** ❌ Not Started

**Required Implementation:**
- [ ] Create DAG with task dependencies:
  ```
  collect_nsf → collect_other_datasets → format_data → 
  transform_data → train_models → evaluate_models
  ```
- [ ] Configure retries for each task (e.g., 3 retries with exponential backoff)
- [ ] Set up alerting:
  - Email notifications on failure
  - Success confirmations
- [ ] Implement idempotency checks
- [ ] Add conditional logic (e.g., skip training if no new data)
- [ ] Enable backfill capability

**Design Patterns to Implement:**
- Task decomposition
- Parametrized execution
- Fast feedback loops
- Concurrency where appropriate

---

## Overall Progress Summary

### Completed (20%)
- ✅ Business problem definition
- ✅ Architecture design
- ✅ Data collection pipelines (fully implemented)
- ✅ Landing Zone populated with data

### In Progress (0%)
- None

### Not Started (80%)
- ❌ Data formatting to MongoDB
- ❌ Data transformation to Exploitation Zone
- ❌ ML model training
- ❌ MLflow integration
- ❌ Airflow DAG creation
- ❌ Results documentation

---

## Technical Stack

### Currently Implemented:
- ✅ Python 3.x
- ✅ PySpark 4.0.1
- ✅ Pandas 2.3.3
- ✅ Requests library
- ✅ Local file system (simulating DFS)

### To Be Implemented:
- ⏳ MongoDB (Formatted Zone storage)
- ⏳ Delta Lake (Exploitation Zone)
- ⏳ Spark MLlib (Model training)
- ⏳ MLflow (Model management)
- ⏳ Apache Airflow (Orchestration)

---

## Next Steps Priority

1. **High Priority:**
   - [ ] Set up MongoDB instance
   - [ ] Implement `data_formatter.py` (Task A.4)
   - [ ] Implement `data_transformer.py` (Task A.5)

2. **Medium Priority:**
   - [ ] Create ML training pipeline (Task B.1)
   - [ ] Set up MLflow (Task B.2)
   - [ ] Create Airflow DAG (Tasks C.1, C.2)

3. **Low Priority:**
   - [ ] Generate visualizations (Task B.3)
   - [ ] Write final PDF documentation
   - [ ] Code cleanup and optimization

---

## Deliverables Checklist

### Code (ZIP file)
- [x] `data_collection.py` - Task A.3 ✅
- [ ] `data_formatter.py` - Task A.4 ❌
- [ ] `data_transformer.py` - Task A.5 ❌
- [ ] ML training script - Task B.1 ❌
- [ ] MLflow integration - Task B.2 ❌
- [ ] Airflow DAG - Tasks C.1, C.2 ❌
- [ ] Comments and documentation ⚠️ (partial)
- [ ] Config files for paths/credentials ⚠️ (partially done)

### PDF Document (Max 4 pages)
- [x] A.1: Dataset selection and analysis justification ✅
- [ ] A.2: Architecture diagram ⚠️ (needs proper diagram)
- [ ] Assumptions and justifications ❌
- [ ] B.3: Model results discussion ❌

---

## Assessment Readiness

### Code Quality
- ✅ **Correctness:** Data collection works without errors
- ✅ **Understandability:** Variables well-named, some comments present
- ✅ **No hardcoding:** Uses `Path(__file__)` for relative paths
- ⚠️ **Efficiency:** Could be improved (sequential API calls)

### PDF Quality
- ⏳ Not yet started

---

## Data Characteristics

### Volume
- **NSF Grants:** ~50,000+ awards (2015-2025)
- **Legislators:** ~12,000+ historical records
- **Terminated Grants:** ~100-500 records (estimated)
- **Cruz List:** ~50-200 records (estimated)

### Format Compliance
- ✅ 4 datasets (exceeds minimum of 3)
- ✅ 2 JSON datasets (NSF Grants, Legislators)
- ✅ Real-world data with sufficient volume

### Data Quality Considerations
- Class imbalance (few cancelled vs. many active grants)
- Missing values in grant abstracts/descriptions
- Temporal nature (older grants completed, newer grants active)
- Need for text processing (abstracts, titles)

---

## Project Timeline Estimate

Based on current progress:

- **Week 1:** ✅ Data Collection (DONE)
- **Week 2:** Data Formatting + Transformation (IN PROGRESS)
- **Week 3:** ML Training + MLflow Integration
- **Week 4:** Airflow Orchestration + Documentation

---

## Contact & Resources

- **NSF API Documentation:** https://www.nsf.gov/developer/
- **MongoDB Documentation:** https://docs.mongodb.com/
- **Delta Lake Documentation:** https://docs.delta.io/
- **Spark MLlib Guide:** https://spark.apache.org/docs/latest/ml-guide.html
- **MLflow Documentation:** https://mlflow.org/docs/latest/index.html
- **Airflow Documentation:** https://airflow.apache.org/docs/

---

*Last Updated: 8 December 2025*
