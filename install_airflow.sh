#!/bin/bash

# 1. Set Airflow to live inside your current project folder
source setup_env.sh

# 2. Install Airflow (using constraints for Python 3.12)
pip install "apache-airflow[celery]==2.10.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.0/constraints-3.12.txt"

# 3. Initialize the database
airflow db init

# 4. Create the Admin User
airflow users create \
    --username admin \
    --firstname Gerard \
    --lastname Admin \
    --role Admin \
    --email admin@example.com

echo "Done! Airflow is installed in: $(pwd)/airflow"
