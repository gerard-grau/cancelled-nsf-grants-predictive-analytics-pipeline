"""
Configuration module for MongoDB connection and project settings.
Supports environment variables for flexible deployment.
"""
import os
from pathlib import Path

# MongoDB Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://172.27.160.1:27017/")
MONGO_DB = os.getenv("MONGO_DB", "nsf_grants_formatted")

# Collections
COLLECTION_NSF_GRANTS = "nsf_grants"
COLLECTION_TERMINATED_GRANTS = "terminated_grants"
COLLECTION_CRUZ_LIST = "cruz_list"
COLLECTION_LEGISLATORS = "legislators"
COLLECTION_FLAGGED_WORDS = "flagged_words"

# Project Paths
BASE_DIR = Path(__file__).parent.parent
DATALAKE_DIR = BASE_DIR / "datalake"
LANDING_DIR = DATALAKE_DIR / "landing"
FORMATTED_DIR = DATALAKE_DIR / "formatted"
EXPLOITATION_DIR = DATALAKE_DIR / "exploitation"

# Landing Zone Paths
LANDING_NSF_GRANTS = LANDING_DIR / "nsf_grants" / "nsf_awards_spark"
LANDING_TERMINATED = LANDING_DIR / "terminated_data"
LANDING_CRUZ_LIST = LANDING_DIR / "cruz_list"
LANDING_LEGISLATORS = LANDING_DIR / "legislators"
LANDING_FLAGGED_WORDS = LANDING_DIR / "flagged_words"

# Spark Configuration
SPARK_APP_NAME = "NSFDataFormatter"
BATCH_SIZE = 1000  # Documents per batch write to MongoDB

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
