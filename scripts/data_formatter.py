"""
Task A.4 - Data Formatting Pipelines

This script implements Spark-based data formatters to transform raw data from
the Landing Zone into standardized MongoDB collections in the Formatted Zone.

Transformations applied:
- Minimal, common cleaning (applicable to any future analysis)
- Field standardization (snake_case naming, type conversions)
- Data reconciliation (ID matching, geographic codes)
- Metadata enrichment
- Syntactic homogenization

Technologies: PySpark, MongoDB (pymongo)
"""

import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, TimestampType
)
from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure, BulkWriteError

# Import configuration
from config import (
    MONGO_URI, MONGO_DB, SPARK_APP_NAME, BATCH_SIZE,
    COLLECTION_NSF_GRANTS, COLLECTION_TERMINATED_GRANTS,
    COLLECTION_CRUZ_LIST, COLLECTION_LEGISLATORS,
    LANDING_NSF_GRANTS, LANDING_TERMINATED, LANDING_CRUZ_LIST, LANDING_LEGISLATORS
)


# ============================================================================
# MongoDB Connection Management
# ============================================================================

class MongoDBManager:
    """Manages MongoDB connections and operations."""
    
    def __init__(self, uri: str = MONGO_URI, db_name: str = MONGO_DB):
        self.uri = uri
        self.db_name = db_name
        self.client: Optional[MongoClient] = None
        self.db = None
    
    def connect(self):
        """Establish MongoDB connection."""
        try:
            self.client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
            # Test connection
            self.client.admin.command('ping')
            self.db = self.client[self.db_name]
            print(f"✅ Connected to MongoDB: {self.db_name}")
            return True
        except ConnectionFailure as e:
            print(f"❌ Failed to connect to MongoDB: {e}")
            return False
    
    def get_collection(self, collection_name: str):
        """Get a MongoDB collection."""
        if self.db is None:
            raise RuntimeError("Not connected to MongoDB. Call connect() first.")
        return self.db[collection_name]
    
    def create_indexes(self, collection_name: str, indexes: List[tuple]):
        """
        Create indexes on a collection.
        
        Args:
            collection_name: Name of the collection
            indexes: List of (field_name, index_type) tuples
        """
        collection = self.get_collection(collection_name)
        for field, index_type in indexes:
            try:
                collection.create_index([(field, index_type)])
                print(f"   ✓ Created index on {collection_name}.{field}")
            except Exception as e:
                print(f"   ⚠️  Index creation warning for {field}: {e}")
    
    def write_batch(self, collection_name: str, documents: List[Dict]):
        """Write a batch of documents to MongoDB."""
        if not documents:
            return 0
        
        collection = self.get_collection(collection_name)
        try:
            result = collection.insert_many(documents, ordered=False)
            return len(result.inserted_ids)
        except BulkWriteError as e:
            # Handle partial writes
            inserted = e.details.get('nInserted', 0)
            print(f"   ⚠️  Bulk write partial success: {inserted} inserted")
            return inserted
    
    def clear_collection(self, collection_name: str):
        """Clear all documents from a collection."""
        collection = self.get_collection(collection_name)
        result = collection.delete_many({})
        print(f"   🧹 Cleared {result.deleted_count} documents from {collection_name}")
    
    def get_collection_count(self, collection_name: str) -> int:
        """Get document count in a collection."""
        collection = self.get_collection(collection_name)
        return collection.count_documents({})
    
    def close(self):
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            print("✅ MongoDB connection closed")


# ============================================================================
# Spark Session Management
# ============================================================================

def create_spark_session(app_name: str = SPARK_APP_NAME) -> SparkSession:
    """Create and configure Spark session."""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    return spark


# ============================================================================
# Data Type Conversion Utilities
# ============================================================================

def parse_date_column(df: DataFrame, col_name: str, new_col_name: str = None) -> DataFrame:
    """
    Parse a date string column to timestamp type.
    
    Handles multiple date formats commonly found in NSF data.
    Uses try_to_timestamp to handle invalid dates gracefully (returns NULL).
    """
    if new_col_name is None:
        new_col_name = col_name
    
    # Use try_to_timestamp which returns NULL for invalid dates instead of throwing error
    # Note: Format strings need to be wrapped in F.lit()
    return df.withColumn(
        new_col_name,
        F.coalesce(
            F.expr(f"try_to_timestamp({col_name}, 'MM/dd/yyyy')"),
            F.expr(f"try_to_timestamp({col_name}, 'yyyy-MM-dd')"),
            F.expr(f"try_to_timestamp({col_name}, 'yyyy-MM-dd\\'T\\'HH:mm:ss')"),
            F.expr(f"try_to_timestamp({col_name})")
        )
    )


def clean_numeric_id(df: DataFrame, col_name: str) -> DataFrame:
    """
    Clean numeric IDs that may have .0 suffix (float conversion artifacts).
    
    Example: 2211032.0 -> "2211032"
    """
    return df.withColumn(
        col_name,
        F.regexp_replace(F.col(col_name).cast("string"), r"\.0$", "")
    )


def standardize_column_names(df: DataFrame) -> DataFrame:
    """
    Standardize column names to snake_case.
    
    Example: "Award ID" -> "award_id", "ProjectTitle" -> "project_title"
    """
    for col in df.columns:
        # Convert to snake_case
        new_col = col.strip().lower().replace(" ", "_").replace("-", "_")
        # Remove special characters
        new_col = "".join(c if c.isalnum() or c == "_" else "_" for c in new_col)
        # Remove duplicate underscores
        new_col = "_".join(filter(None, new_col.split("_")))
        
        if new_col != col:
            df = df.withColumnRenamed(col, new_col)
    
    return df


# ============================================================================
# Dataset-Specific Formatters
# ============================================================================

def format_nsf_grants(spark: SparkSession, mongo: MongoDBManager) -> int:
    """
    Format NSF grants dataset from Landing to Formatted Zone.
    
    Task A.4 - NSF Grants Formatter
    
    Transformations:
    - Read from Parquet partitions (by year)
    - Standardize field names to snake_case
    - Parse date fields
    - Cast numeric fields
    - Remove duplicates by award_id
    - Add metadata (formatted_at timestamp)
    - Write to MongoDB collection: nsf_grants
    
    Returns:
        Number of documents written
    """
    print("\n" + "="*80)
    print("📊 Formatting NSF Grants Dataset")
    print("="*80)
    
    # Read from Landing Zone (JSON format partitioned by year)
    print(f"⬇️  Reading from: {LANDING_NSF_GRANTS}")
    df = spark.read.json(str(LANDING_NSF_GRANTS))
    
    initial_count = df.count()
    print(f"   ✓ Loaded {initial_count:,} records")
    
    # Standardize column names
    print("🔧 Standardizing column names...")
    df = standardize_column_names(df)
    
    # Parse date fields (if they exist)
    date_fields = [col for col in df.columns if 'date' in col.lower() or 'effective' in col.lower()]
    for date_field in date_fields:
        print(f"   📅 Parsing date field: {date_field}")
        df = parse_date_column(df, date_field)
    
    # Cast numeric fields (use try_cast to handle malformed data)
    if 'estimatedtotalamt' in df.columns:
        df = df.withColumn('estimated_total_amt', 
                          F.expr('try_cast(estimatedtotalamt as double)'))
    if 'fundsoblignatedamt' in df.columns:
        df = df.withColumn('funds_obligated_amt', 
                          F.expr('try_cast(fundsoblignatedamt as double)'))
    
    # Ensure award ID is string and clean
    if 'id' in df.columns:
        df = df.withColumnRenamed('id', 'award_id')
    
    if 'award_id' in df.columns:
        df = clean_numeric_id(df, 'award_id')
    
    # Remove duplicates by award_id
    print("🔍 Removing duplicates...")
    df = df.dropDuplicates(['award_id'])
    deduped_count = df.count()
    print(f"   ✓ {initial_count - deduped_count:,} duplicates removed")
    print(f"   ✓ {deduped_count:,} unique records")
    
    # Add metadata
    df = df.withColumn('formatted_at', F.lit(datetime.now()))
    
    # Clear existing collection
    print(f"🧹 Clearing collection: {COLLECTION_NSF_GRANTS}")
    mongo.clear_collection(COLLECTION_NSF_GRANTS)
    
    # Convert to documents and write to MongoDB in batches
    print(f"💾 Writing to MongoDB collection: {COLLECTION_NSF_GRANTS}")
    
    documents_written = 0
    
    # Use toPandas for more efficient conversion, then iterate
    print("   Converting to documents...")
    pandas_df = df.toPandas()
    total_rows = len(pandas_df)
    print(f"   ✓ {total_rows:,} documents to write")
    
    batch = []
    for idx, row in pandas_df.iterrows():
        doc = row.to_dict()
        # Convert NaN/NaT to None for MongoDB
        doc = {k: (None if pd.isna(v) else v) for k, v in doc.items()}
        batch.append(doc)
        
        if len(batch) >= BATCH_SIZE:
            written = mongo.write_batch(COLLECTION_NSF_GRANTS, batch)
            documents_written += written
            print(f"   ✓ Progress: {documents_written:,}/{total_rows:,} documents ({100*documents_written/total_rows:.1f}%)")
            batch = []
    
    # Write remaining documents
    if batch:
        written = mongo.write_batch(COLLECTION_NSF_GRANTS, batch)
        documents_written += written
    
    # Create indexes
    print("🔍 Creating indexes...")
    mongo.create_indexes(COLLECTION_NSF_GRANTS, [
        ('award_id', ASCENDING),
        ('year', ASCENDING)
    ])
    
    print(f"✅ NSF Grants formatting complete: {documents_written:,} documents written")
    return documents_written


def format_terminated_grants(spark: SparkSession, mongo: MongoDBManager) -> int:
    """
    Format terminated grants dataset.
    
    Task A.4 - Terminated Grants Formatter
    
    Transformations:
    - Read from Parquet
    - Standardize field names
    - Parse dates
    - Convert boolean fields
    - Cast numeric fields
    - Create nested organization structure
    - Write to MongoDB collection: terminated_grants
    """
    print("\n" + "="*80)
    print("📊 Formatting Terminated Grants Dataset")
    print("="*80)
    
    print(f"⬇️  Reading from: {LANDING_TERMINATED}")
    df = spark.read.parquet(str(LANDING_TERMINATED))
    
    initial_count = df.count()
    print(f"   ✓ Loaded {initial_count:,} records")
    
    # Standardize column names
    df = standardize_column_names(df)
    
    # Parse date fields
    date_fields = [col for col in df.columns if 'date' in col.lower()]
    for date_field in date_fields:
        print(f"   📅 Parsing date field: {date_field}")
        df = parse_date_column(df, date_field)
    
    # Convert boolean field - handle cases where it might be string "True"/"False" or actual boolean
    if 'in_cruz_list' in df.columns:
        df = df.withColumn('in_cruz_list', 
                          F.when(F.col('in_cruz_list').cast('string').isin(['True', 'true', '1', 'yes']), True)
                           .when(F.col('in_cruz_list').cast('string').isin(['False', 'false', '0', 'no', '']), False)
                           .otherwise(None).cast('boolean'))
    
    # Cast numeric fields (use try_cast to handle malformed data gracefully)
    if 'usaspending_obligated' in df.columns:
        df = df.withColumn('usaspending_obligated', 
                          F.expr('try_cast(usaspending_obligated as double)'))
    
    # Add metadata
    df = df.withColumn('formatted_at', F.lit(datetime.now()))
    
    # Clear and write
    print(f"🧹 Clearing collection: {COLLECTION_TERMINATED_GRANTS}")
    mongo.clear_collection(COLLECTION_TERMINATED_GRANTS)
    
    print(f"💾 Writing to MongoDB collection: {COLLECTION_TERMINATED_GRANTS}")
    documents_written = 0
    
    pandas_df = df.toPandas()
    total_rows = len(pandas_df)
    print(f"   ✓ {total_rows:,} documents to write")
    
    batch = []
    for idx, row in pandas_df.iterrows():
        doc = row.to_dict()
        doc = {k: (None if pd.isna(v) else v) for k, v in doc.items()}
        batch.append(doc)
        
        if len(batch) >= BATCH_SIZE:
            written = mongo.write_batch(COLLECTION_TERMINATED_GRANTS, batch)
            documents_written += written
            batch = []
    
    if batch:
        written = mongo.write_batch(COLLECTION_TERMINATED_GRANTS, batch)
        documents_written += written
    
    # Create indexes
    print("🔍 Creating indexes...")
    mongo.create_indexes(COLLECTION_TERMINATED_GRANTS, [
        ('grant_number', ASCENDING),
        ('in_cruz_list', ASCENDING)
    ])
    
    print(f"✅ Terminated Grants formatting complete: {documents_written:,} documents")
    return documents_written


def format_cruz_list(spark: SparkSession, mongo: MongoDBManager) -> int:
    """
    Format Cruz list dataset.
    
    Task A.4 - Cruz List Formatter
    
    Transformations:
    - Read from Parquet
    - Standardize field names
    - Clean award ID (remove .0 suffix)
    - Parse date fields
    - Cast numeric fields
    - Standardize empty category fields to null
    - Write to MongoDB collection: cruz_list
    """
    print("\n" + "="*80)
    print("📊 Formatting Cruz List Dataset")
    print("="*80)
    
    print(f"⬇️  Reading from: {LANDING_CRUZ_LIST}")
    df = spark.read.parquet(str(LANDING_CRUZ_LIST))
    
    initial_count = df.count()
    print(f"   ✓ Loaded {initial_count:,} records")
    
    # Standardize column names
    df = standardize_column_names(df)
    
    # Clean award ID
    if 'award_id' in df.columns:
        print("🧹 Cleaning award IDs...")
        df = clean_numeric_id(df, 'award_id')
    
    # Parse date fields
    date_fields = [col for col in df.columns if 'date' in col.lower()]
    for date_field in date_fields:
        print(f"   📅 Parsing date field: {date_field}")
        df = parse_date_column(df, date_field)
    
    # Cast numeric fields (use try_cast to handle malformed data)
    if 'total_award_funding_amount' in df.columns:
        df = df.withColumn('total_award_funding_amount', 
                          F.expr('try_cast(total_award_funding_amount as double)'))
    
    # Standardize empty strings to null in category fields
    category_fields = [col for col in df.columns if 'category' in col.lower()]
    for cat_field in category_fields:
        df = df.withColumn(cat_field, 
                          F.when(F.trim(F.col(cat_field)) == "", None)
                           .otherwise(F.col(cat_field)))
    
    # Add metadata
    df = df.withColumn('formatted_at', F.lit(datetime.now()))
    
    # Clear and write
    print(f"🧹 Clearing collection: {COLLECTION_CRUZ_LIST}")
    mongo.clear_collection(COLLECTION_CRUZ_LIST)
    
    print(f"💾 Writing to MongoDB collection: {COLLECTION_CRUZ_LIST}")
    documents_written = 0
    
    pandas_df = df.toPandas()
    total_rows = len(pandas_df)
    print(f"   ✓ {total_rows:,} documents to write")
    
    batch = []
    for idx, row in pandas_df.iterrows():
        doc = row.to_dict()
        doc = {k: (None if pd.isna(v) else v) for k, v in doc.items()}
        batch.append(doc)
        
        if len(batch) >= BATCH_SIZE:
            written = mongo.write_batch(COLLECTION_CRUZ_LIST, batch)
            documents_written += written
            batch = []
    
    if batch:
        written = mongo.write_batch(COLLECTION_CRUZ_LIST, batch)
        documents_written += written
    
    # Create indexes
    print("🔍 Creating indexes...")
    mongo.create_indexes(COLLECTION_CRUZ_LIST, [
        ('award_id', ASCENDING)
    ])
    
    print(f"✅ Cruz List formatting complete: {documents_written:,} documents")
    return documents_written


def format_legislators(spark: SparkSession, mongo: MongoDBManager) -> int:
    """
    Format legislators dataset.
    
    Task A.4 - Legislators Formatter
    
    Transformations:
    - Read from Parquet
    - Standardize field names
    - Handle nested structures (terms, bio)
    - Parse date fields
    - Write to MongoDB collection: legislators
    """
    print("\n" + "="*80)
    print("📊 Formatting Legislators Dataset")
    print("="*80)
    
    print(f"⬇️  Reading from: {LANDING_LEGISLATORS}")
    df = spark.read.parquet(str(LANDING_LEGISLATORS))
    
    initial_count = df.count()
    print(f"   ✓ Loaded {initial_count:,} records")
    
    # Standardize column names (nested structures preserved)
    df = standardize_column_names(df)
    
    # Add metadata
    df = df.withColumn('formatted_at', F.lit(datetime.now()))
    
    # Clear and write
    print(f"🧹 Clearing collection: {COLLECTION_LEGISLATORS}")
    mongo.clear_collection(COLLECTION_LEGISLATORS)
    
    print(f"💾 Writing to MongoDB collection: {COLLECTION_LEGISLATORS}")
    documents_written = 0
    
    pandas_df = df.toPandas()
    total_rows = len(pandas_df)
    print(f"   ✓ {total_rows:,} documents to write")
    
    batch = []
    for idx, row in pandas_df.iterrows():
        doc = row.to_dict()
        # Keep nested structures for legislators (bio, terms, etc.)
        doc = {k: (None if pd.isna(v) else v) for k, v in doc.items()}
        batch.append(doc)
        
        if len(batch) >= BATCH_SIZE:
            written = mongo.write_batch(COLLECTION_LEGISLATORS, batch)
            documents_written += written
            batch = []
    
    if batch:
        written = mongo.write_batch(COLLECTION_LEGISLATORS, batch)
        documents_written += written
    
    # Create indexes
    print("🔍 Creating indexes...")
    mongo.create_indexes(COLLECTION_LEGISLATORS, [
        ('id.bioguide', ASCENDING)
    ])
    
    print(f"✅ Legislators formatting complete: {documents_written:,} documents")
    return documents_written


# ============================================================================
# Validation and Statistics
# ============================================================================

def validate_formatted_zone(mongo: MongoDBManager):
    """
    Validate the Formatted Zone after data loading.
    
    Task A.4 - Validation
    
    Checks:
    - Document counts per collection
    - Sample documents
    - Index presence
    """
    print("\n" + "="*80)
    print("✅ FORMATTED ZONE VALIDATION")
    print("="*80)
    
    collections = [
        COLLECTION_NSF_GRANTS,
        COLLECTION_TERMINATED_GRANTS,
        COLLECTION_CRUZ_LIST,
        COLLECTION_LEGISLATORS
    ]
    
    total_docs = 0
    
    for coll_name in collections:
        count = mongo.get_collection_count(coll_name)
        total_docs += count
        print(f"\n📊 Collection: {coll_name}")
        print(f"   Documents: {count:,}")
        
        # Show sample document
        collection = mongo.get_collection(coll_name)
        sample = collection.find_one()
        if sample:
            print(f"   Sample fields: {list(sample.keys())[:10]}...")
        
        # Show indexes
        indexes = collection.list_indexes()
        index_names = [idx['name'] for idx in indexes]
        print(f"   Indexes: {', '.join(index_names)}")
    
    print(f"\n{'='*80}")
    print(f"✅ Total documents in Formatted Zone: {total_docs:,}")
    print(f"{'='*80}\n")


# ============================================================================
# Main Execution
# ============================================================================

def main():
    """
    Main execution function for Task A.4 - Data Formatting Pipelines.
    
    Orchestrates the formatting of all datasets from Landing to Formatted Zone.
    """
    print("\n" + "="*80)
    print("🚀 TASK A.4 - DATA FORMATTING PIPELINES")
    print("="*80)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"MongoDB URI: {MONGO_URI}")
    print(f"MongoDB Database: {MONGO_DB}")
    print("="*80)
    
    # Initialize MongoDB connection
    mongo = MongoDBManager()
    if not mongo.connect():
        print("❌ Failed to connect to MongoDB. Exiting.")
        sys.exit(1)
    
    # Initialize Spark session
    print("\n🔥 Initializing Spark session...")
    spark = create_spark_session()
    print(f"   ✓ Spark version: {spark.version}")
    
    try:
        # Format each dataset
        stats = {}
        
        # 1. NSF Grants (largest dataset)
        stats['nsf_grants'] = format_nsf_grants(spark, mongo)
        
        # 2. Terminated Grants
        stats['terminated_grants'] = format_terminated_grants(spark, mongo)
        
        # 3. Cruz List
        stats['cruz_list'] = format_cruz_list(spark, mongo)
        
        # 4. Legislators
        stats['legislators'] = format_legislators(spark, mongo)
        
        # Validation
        validate_formatted_zone(mongo)
        
        # Summary
        print("\n" + "="*80)
        print("📈 FORMATTING SUMMARY")
        print("="*80)
        for dataset, count in stats.items():
            print(f"   {dataset:.<30} {count:>10,} documents")
        print(f"   {'TOTAL':.<30} {sum(stats.values()):>10,} documents")
        print("="*80)
        
        print(f"\n✅ Task A.4 completed successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"\n❌ Error during formatting: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        # Cleanup
        spark.stop()
        mongo.close()
        print("\n🏁 Resources released. Goodbye!")


if __name__ == "__main__":
    main()
