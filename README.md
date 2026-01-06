# NSF Grants Cancellation Prediction Pipeline

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![PySpark](https://img.shields.io/badge/PySpark-4.0.1-orange.svg)](https://spark.apache.org/)
[![MLflow](https://img.shields.io/badge/MLflow-3.7.0-blue.svg)](https://mlflow.org/)
[![Airflow](https://img.shields.io/badge/Airflow-2.10.0-green.svg)](https://airflow.apache.org/)

A complete data engineering and predictive analytics pipeline for predicting NSF grant cancellations using a Medallion architecture.

## 🎯 Project Overview

**Objective:** Predict whether National Science Foundation (NSF) grants will be cancelled or terminated based on various features including political context, funding patterns, and textual analysis.

**Course:** Bases de Dades Avançades (BDA) - UPC  
**Authors:** Gerard & Eloi

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA LAKE (Medallion Architecture)                │
├─────────────────┬─────────────────────┬─────────────────────────────────────┤
│  LANDING ZONE   │   FORMATTED ZONE    │        EXPLOITATION ZONE            │
│  (Raw Files)    │     (MongoDB)       │         (Delta Tables)              │
├─────────────────┼─────────────────────┼─────────────────────────────────────┤
│ • NSF Awards    │ • nsf_grants        │ • grants_train_delta                │
│ • Terminated    │ • terminated_grants │ • grants_test_delta                 │
│ • Cruz List     │ • cruz_list         │                                     │
│ • Legislators   │ • legislators       │   ┌─────────────────────────────┐   │
│ • Flagged Words │ • flagged_words     │   │  ML MODELS (MLflow)         │   │
│                 │                     │   │  • Logistic Regression      │   │
│                 │                     │   │  • Random Forest            │   │
│                 │                     │   │  • Gradient Boosted Trees   │   │
│                 │                     │   └─────────────────────────────┘   │
└─────────────────┴─────────────────────┴─────────────────────────────────────┘
                              │
                              ▼
                    ┌─────────────────────┐
                    │   Apache Airflow    │
                    │   (Orchestration)   │
                    └─────────────────────┘
```

---

## 📋 Prerequisites

- **Python** 3.10 or higher
- **Java** 11 or 17 (required for Spark)
- **MongoDB** 6.0+ (running locally or remote)

---

## 🚀 Installation

### 1. Extract the ZIP File

```bash
unzip Entrega_BDA_Gerard_Eloi.zip
cd Entrega_BDA_Gerard_Eloi
```

### 2. Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
.\venv\Scripts\activate   # Windows
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Start MongoDB

```bash
# Start your local MongoDB service
sudo systemctl start mongod

# Verify MongoDB is running
mongosh --eval "db.runCommand({ ping: 1 })"
```

### 5. Configure Environment Variables (Optional)

Create a `.env` file or export variables:

```bash
export MONGO_URI="mongodb://localhost:27017/"
export MONGO_DB="nsf_grants_formatted"
export LOG_LEVEL="INFO"
```

> **Note:** Default values are provided in `scripts/config.py` if environment variables are not set.

---

## 📁 Project Structure

```
Entrega_BDA_Gerard_Eloi/
│
├── Documentation-BDA-Gerard-Grau-Eloi-Pagès.pdf  # Project report
├── README.md                  # This file
├── requirements.txt           # Python dependencies
│
├── scripts/                   # Python scripts for all pipeline stages
│   ├── config.py              # Centralized configuration
│   │
│   ├── collect_awards.py      # Task A.3: NSF API data collector
│   ├── collect_terminated.py  # Task A.3: Terminated grants collector
│   ├── collect_cruz_list.py   # Task A.3: Cruz list collector
│   ├── collect_legislators.py # Task A.3: Legislators collector
│   ├── collect_flagged_words.py # Task A.3: Flagged words collector
│   │
│   ├── format_awards.py       # Task A.4: NSF grants formatter
│   ├── format_terminated.py   # Task A.4: Terminated grants formatter
│   ├── format_cruz_list.py    # Task A.4: Cruz list formatter
│   ├── format_legislators.py  # Task A.4: Legislators formatter
│   ├── format_flagged_words.py # Task A.4: Flagged words formatter
│   ├── formatter_utils.py     # Task A.4: Shared formatting utilities
│   │
│   ├── data_transformer.py    # Task A.5: MongoDB → Delta transformation
│   │
│   ├── model_training_utils.py # Tasks B.1 & B.2: ML training + MLflow
│   ├── model_training.ipynb   # Interactive model training notebook
│   ├── mlflow_visualization.ipynb # MLflow results visualization
│   └── *.sh                   # Shell scripts for execution
│
├── airflow/
│   └── dags/
│       └── airflow_dag.py     # Tasks C.1 & C.2: Pipeline orchestration
│
└── raw-data/
    └── flagged_words_trump_admin.csv  # Source data for flagged words
```

> **Note:** The `datalake/` folder will be created automatically when running the pipeline.

---

## ▶️ Execution Guide

### Option A: Run Individual Scripts (Manual)

Execute each stage of the pipeline manually:

```bash
cd scripts

# Stage 1: Data Collection (Landing Zone)
python collect_awards.py
python collect_terminated.py
python collect_cruz_list.py
python collect_legislators.py
python collect_flagged_words.py

# Stage 2: Data Formatting (Formatted Zone → MongoDB)
python format_awards.py
python format_terminated.py
python format_cruz_list.py
python format_legislators.py
python format_flagged_words.py

# Stage 3: Data Transformation (Exploitation Zone → Delta)
python data_transformer.py

# Stage 4: Model Training (use the notebook)
# Open model_training.ipynb in Jupyter
```

### Option B: Run with Airflow (Automated)

#### 1. Initialize Airflow

```bash
export AIRFLOW_HOME=$(pwd)/airflow

# Initialize the database
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

#### 2. Start Airflow Services

```bash
# Terminal 1: Start the scheduler
airflow scheduler

# Terminal 2: Start the webserver
airflow webserver --port 8080
```

#### 3. Access Airflow UI

Open http://localhost:8080 in your browser and:
1. Login with `admin` / `admin`
2. Enable the `nsf_pipeline` DAG
3. Trigger the DAG manually or wait for scheduled execution

---

## 🧪 Model Training

### Using the Notebook

1. Ensure the Exploitation Zone has data (run stages 1-3 first)
2. Open `scripts/model_training.ipynb` in Jupyter
3. Run all cells to train and evaluate models

### MLflow Tracking

View experiment results in MLflow UI:

```bash
cd scripts
mlflow ui --port 5000
```

Open http://localhost:5000 to see:
- Model metrics (accuracy, precision, recall, F1, AUC)
- Hyperparameters for each run
- Model artifacts

---

## 📊 Task Reference

| Task | Description | Script(s) |
|------|-------------|-----------|
| **A.3** | Data Collection Pipelines | `collect_*.py` |
| **A.4** | Data Formatting Pipelines | `format_*.py`, `formatter_utils.py` |
| **A.5** | Formatted → Exploitation | `data_transformer.py` |
| **B.1** | Model Training & Validation | `model_training_utils.py`, `model_training.ipynb` |
| **B.2** | MLflow Model Management | `model_training_utils.py`, `mlflow_visualization.ipynb` |
| **C.1** | Airflow Scheduling | `airflow_dag.py` |
| **C.2** | Airflow Task Dependencies | `airflow_dag.py` |

---

## 🔧 Configuration

All configuration is centralized in `scripts/config.py`:

| Variable | Description | Default |
|----------|-------------|---------|
| `MONGO_URI` | MongoDB connection string | `mongodb://localhost:27017/` |
| `MONGO_DB` | Database name | `nsf_grants_formatted` |
| `LANDING_DIR` | Landing zone path | `datalake/landing/` |
| `EXPLOITATION_DIR` | Exploitation zone path | `datalake/exploitation/` |
| `BATCH_SIZE` | MongoDB batch write size | `1000` |

Override defaults using environment variables:

```bash
export MONGO_URI="mongodb://user:pass@remote-host:27017/"
```

---

## 📈 Pipeline DAG

```
collect_awards ─────────► format_awards ─────────┐
collect_terminated ─────► format_terminated ─────┤
collect_cruz_list ──────► format_cruz_list ──────┼──► mongo_to_delta
collect_legislators ────► format_legislators ────┤
collect_flagged_words ──► format_flagged_words ──┘
```

- **Collectors** run in parallel (independent data sources)
- **Formatters** wait for their respective collector
- **Transformer** waits for all formatters to complete

---

## 🛠️ Troubleshooting

### MongoDB Connection Error

```bash
# Check if MongoDB is running
mongosh --eval "db.runCommand({ ping: 1 })"

# Start MongoDB if not running
sudo systemctl start mongod
```

### Spark Java Error

Ensure Java 11 or 17 is installed:

```bash
java -version
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
```

### Airflow DAG Not Visible

```bash
# Check for syntax errors
python airflow/dags/airflow_dag.py

# Refresh DAGs
airflow dags list
```

---

## 📝 License

This project was developed for educational purposes as part of the BDA course at UPC.

---

## 👥 Authors

- **Gerard Grau**
- **Eloi Pagès**

Course: Bases de Dades Avançades (BDA) - Universitat Politècnica de Catalunya
