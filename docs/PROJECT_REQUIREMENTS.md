# BDA Data Engineering Project - Requirements Summary

**Project Title:** Data Engineering for Predictive Analytics  
**Course:** Bases de Dades Avançades  
**Date Extracted:** 8 December 2025

---

## Project Overview

Develop an end-to-end data-driven architecture with two critical backbones:
1. **Data Engineering Backbone** - Medallion architecture data lake with structured zones
2. **Data Analysis Backbone** - Predictive analytics with ML model management
3. **Orchestration Framework** - Coordinate and manage all data flows

---

## A. Data Engineering Backbone

### Landing Zone
- **Description:** Stores raw data from source systems in structured/semi-structured format
- **Implementation:** Local file system (simulates distributed file system)
- **Requirements:** Ingest ≥3 datasets (at least 2 must be JSON format)

### Formatted Zone
- **Description:** Standardized data model with uniform structure; minimal cleaning (only common rules applicable to any future analysis)
- **Implementation:** MongoDB collections (one per dataset)
- **Features:** Data can be enriched with metadata, reconciled for integration

### Exploitation Zone
- **Description:** Processed, refined data optimized for specific analysis (features, KPIs, cleaned, enriched/joined)
- **Implementation:** Delta files in local file system
- **Purpose:** Custom views/datasets over Formatted Zone data, ready for analysis

### Data Sources
Provided datasets from Open Data BCN portal (folder: `datasets/`):
- Income data
- Idealista data (apartment listings)
- Incidence data
- Unemployment data
- Cultural sites data
- Prices data
- Lookup data for reconciliation (Idealista/Income)

**Allowed:** Additional datasets from BCN portal or other sources if needed

---

## Tasks - Data Engineering Backbone

### **A.1 - Business Problem Definition (Exploration)**
- Study and explore provided datasets
- Choose 3 datasets for Landing Zone (≥2 JSON)
- Define the predictive analysis target/class variable
- Provide overall explanation and justification

**Example Business Problems:**
- Estimate rental price for apartments in Barcelona
- Estimate average rental price per neighborhood/year
- Estimate neighborhood income
- Estimate domicile changes based on rent price and income

### **A.2 - Data Lake Architecture Design**
- Design high-level architecture showing interaction between zones
- Illustrate data pipelines (sequence of transformations)
- Use simplified notation of choice

### **A.3 - Data Collection Pipelines**
- Create Landing Zone directory in local file system
- Develop Spark pipelines to collect raw data from sources (API, FTP, etc.)
- **Critical:** Data Collectors must allow periodic executions (daily/weekly/monthly)

### **A.4 - Data Formatting Pipelines**
- Create Formatted Zone directory in local file system
- Develop Spark pipelines to:
  - Read raw data from Landing Zone (3 datasets)
  - Perform necessary transformations
  - Write data to MongoDB in Formatted Zone

### **A.5 - Move Data to Exploitation Zone**
- Create Exploitation Zone directory in local file system
- Develop Spark pipeline to:
  - Perform analysis-specific transformations and cleaning
  - Write data to Delta table
  - Ensure proper partitioning for efficient querying

---

## B. Data Analysis Backbone

### ML Model Training and Validation
- **Description:** Train ML models on engineered features, validate performance
- **Implementation:** 
  - Use Spark MLlib (DataFrame-based API)
  - Split data into training/validation sets
  - Use relevant metrics (precision, recall, accuracy)

### ML Model Management
- **Description:** Version, deploy, and manage models in production
- **Implementation:**
  - Use MLflow to version models, track changes
  - Manage deployment of different model versions
  - Maintain model registry with metadata and deployment status

---

## Tasks - Data Analysis Backbone

### **B.1 - Model Training and Validation**
Use `spark.ml` (DataFrame-based API) with data from Exploitation Zone:

1. **Dataset Creation:**
   - Create training and validation datasets
   - Format according to classifier algorithm requirements

2. **Model Training:**
   - Create ≥2 classification models
   - Store models with hyperparameters and evaluation metrics
   - Use metrics: predictive accuracy, recall

3. **Model Selection:**
   - Rank models by validation set performance
   - Automatically deploy best-performing model (highest accuracy)

### **B.2 - Model Management**
- Use MLflow to:
  - Store/track models, hyperparameters, evaluation metrics
  - Tag the best performing model for deployment

### **B.3 - ML Results**
- Use graphs/charts/tables to discuss results from Model Training and Validation

---

## C. Pipeline Orchestration

### Pipeline Orchestration and Management
- **Description:** Manage and orchestrate both Data Engineering and Analysis tasks
- **Implementation:** Apache Airflow to:
  - Automate and schedule data tasks/pipelines
  - Set up task dependencies (control flow)
  - Ensure tasks run in correct order

---

## Tasks - Pipeline Orchestration

### **C.1 - Scheduling**
- Specify when pipelines should run using Airflow
- Automate pipelines at regular intervals (minute/hourly/daily/weekly)
- Example: Run pipeline at midnight daily to process previous day's data

### **C.2 - Dependency Management (Control Flow)**
- Define task dependencies using Directed Acyclic Graphs (DAGs)
- Specify execution order (Task B runs only after Task A completes)
- Configure automatic retries for transient failures
- Set up alerts/notifications for failures or task completion

---

## Deliverables

### 1. Python Scripts/Notebooks (ZIP file)
Must implement:
- **A.3:** PySpark Data Collector Pipelines
- **A.4:** PySpark Data Formatter Pipelines
- **A.5:** PySpark pipeline for Formatted → Exploitation Zone
- **B.1:** PySpark Model Training and Validation (≥2 ML algorithms)
- **B.2:** MLflow Model Management code
- **C.1:** Airflow scheduling code
- **C.2:** Airflow task dependency code

**Notes:**
- Not all tasks need separate scripts; some can be combined
- Include comments referring to implemented tasks
- Organize code following best practices
- Include comments for understanding
- Do NOT hardcode absolute paths or credentials (use config files or user input)

### 2. PDF Document (Max 4 A4 pages)
Must include:
- **A.1:** Selected datasets and analysis explanation with justification
- **A.2:** Pipeline sketch at high abstraction level (grouped operations)
- **Assumptions/Justifications:** Elaborate on any assumptions and decisions (reference tasks: A.2, A.3, etc.)
- **B.3:** Discussion of Model Training and Validation results

---

## Assessment Criteria

### a) Python Code
1. **Correctness:** Executable without errors, no changes needed by lecturer
2. **Understandability:** Readable variable names, commented complex blocks
3. **Efficiency:** Execution time optimization
4. **No hardcoding:** Avoid hardcoded data/paths

### b) PDF Files
1. **Conciseness:** Clear, concise explanations
2. **Coherence:** Logical flow and consistency
3. **Soundness:** Well-justified decisions

---

## Evaluation

- **60%** Deliverables (Python code + PDF file)
- **40%** Individual exercises related to project (final exam day)

---

## Key Technologies

- **Data Processing:** Apache Spark (PySpark) - **MANDATORY**
- **Data Storage:**
  - Landing Zone: Local file system (raw formats)
  - Formatted Zone: MongoDB
  - Exploitation Zone: Delta Tables (local file system)
- **ML Framework:** Spark MLlib (DataFrame-based API)
- **Model Management:** MLflow
- **Orchestration:** Apache Airflow

---

## Design Patterns & Best Practices

### Data Transformation Patterns
- Enrichment, joining, filtering, structuring, conversion
- Aggregation, anonymization, splitting, deduplication
- Missing value imputation

### Data Update Patterns
- Overwrite, Insert, Upsert, Delete (soft/hard)

### Best Practices
- **Staging:** Protect against data loss, ensure low time to recovery
- **Idempotency:** Multiple runs yield consistent results (reproducibility)
- **Normalization/Denormalization:** Balance uniqueness vs. performance
- **Incrementality:** Define INSERT vs. OVERWRITE vs. UPSERT

### Orchestration Best Practices
- **Backfills:** Repeatable processes for recreating data
- **Event-driven:** Trigger on events for up-to-date data
- **Conditional logic:** Handle varying inputs with if-then-else flows
- **Concurrency:** Fan out tasks for parallel execution
- **Fast feedback loops:** Fail fast, identify errors quickly
- **Parametrized execution:** Accept variables for reusable pipelines
- **Lineage tracking:** Monitor data's journey through lifecycle
- **Pipeline decomposition:** Break into manageable tasks

### Spark Optimizations
- **Lazy evaluation:** Cache/persist, unpersist, checkpoint
- **Parallelism:** repartition, coalesce

---

## Example Business Use Cases

1. **Rental Price Prediction:** Given apartment characteristics (Idealista), income data, density/immigration → estimate rental price
2. **Average Price per Neighborhood:** Given incidents, population, income → estimate average rental price
3. **Neighborhood Income Estimation:** Given rental price, train stations → estimate income/family income index
4. **Domicile Changes:** Given rent price, income → estimate number of domicile changes

**Note:** Synthetic data generation is allowed if specific attributes or volume are insufficient

---

## Important Reminders

- Work with at least **3 different datasets** in Landing Zone
- At least **2 datasets must be in JSON format**
- Use **Spark for all data processing** (mandatory)
- Implement **periodic execution capability** for data collectors
- Create **at least 2 classification models**
- **Automatically deploy** the best-performing model
- Use **MLflow** for model versioning and registry
- Ensure code is **executable without modifications**
- **No hardcoded paths or credentials**
- PDF document: **maximum 4 pages**

---

## Project Workflow Summary

```
Data Sources
    ↓
[Landing Zone] ← Data Collectors (Spark, periodic)
    ↓
[Formatted Zone] ← Data Formatters (Spark → MongoDB)
    ↓
[Exploitation Zone] ← Data Transformers (Spark → Delta Tables)
    ↓
ML Training & Validation (Spark MLlib)
    ↓
Model Management (MLflow)
    ↓
Deployment (Best Model)

[Apache Airflow] → Orchestrates all pipelines with scheduling & dependencies
```

---

## Resources

- **Open Data BCN Portal:** https://opendata-ajuntament.barcelona.cat/data/en/dataset
- **MLflow Documentation:** https://mlflow.org
- **Apache Airflow:** https://airflow.apache.org
- **Spark API Reference:** https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html
