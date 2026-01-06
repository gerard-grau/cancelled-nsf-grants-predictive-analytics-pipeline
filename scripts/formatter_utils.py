# formatter_utils.py
"""
Task A.4 - Data Formatter Utilities

Shared utilities for all formatter pipelines including:
- MongoDBManager: Connection management and batch operations
- FormatterConfig: Configuration dataclass for formatters
- generic_formatter: Reusable formatting pipeline template
- Spark session creation with MongoDB connector

This module follows DRY principles to avoid code duplication
across individual formatter scripts.
"""

import sys
from datetime import datetime
from typing import Optional, Callable
from dataclasses import dataclass, field

import numpy as np
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, BulkWriteError
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from config import MONGO_URI, MONGO_DB, SPARK_APP_NAME, BATCH_SIZE


@dataclass
class FormatterConfig:
    """Configuration for dataset formatting."""
    name: str
    landing_path: str
    collection_name: str
    file_format: str = "parquet"  # "parquet" or "json"
    column_mapping: dict[str, str] = field(default_factory=dict)
    date_columns: dict[str, str] = field(default_factory=dict)
    numeric_columns: list[str] = field(default_factory=list)
    id_column: Optional[str] = None
    dedupe_columns: Optional[list[str]] = None
    indexes: list[tuple[str, int]] = field(default_factory=list)
    custom_transform: Optional[Callable[[DataFrame], DataFrame]] = None


class MongoDBManager:
    def __init__(self, uri: str = MONGO_URI, db_name: str = MONGO_DB):
        self.uri = uri
        self.db_name = db_name
        self.client: Optional[MongoClient] = None
        self.db = None

    def connect(self) -> bool:
        try:
            self.client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
            self.client.admin.command("ping")
            self.db = self.client[self.db_name]
            print(f"✅ Connected to MongoDB: {self.db_name}")
            return True
        except ConnectionFailure as e:
            print(f"❌ Failed to connect to MongoDB: {e}")
            return False

    def get_collection(self, collection_name: str):
        if self.db is None:
            raise RuntimeError("Not connected to MongoDB. Call connect() first.")
        return self.db[collection_name]

    def create_indexes(self, collection_name: str, indexes: list[tuple]):
        collection = self.get_collection(collection_name)
        for field, index_type in indexes:
            try:
                collection.create_index([(field, index_type)])
                print(f"   ✓ Created index on {collection_name}.{field}")
            except Exception as e:
                print(f"   ⚠️  Index creation warning for {field}: {e}")

    def write_batch(self, collection_name: str, documents: list[dict]) -> int:
        if not documents:
            return 0
        collection = self.get_collection(collection_name)
        try:
            result = collection.insert_many(documents, ordered=False)
            return len(result.inserted_ids)
        except BulkWriteError as e:
            inserted = e.details.get("nInserted", 0)
            print(f"   ⚠️  Bulk write partial success: {inserted} inserted")
            return inserted

    def clear_collection(self, collection_name: str):
        collection = self.get_collection(collection_name)
        result = collection.delete_many({})
        print(f"   🧹 Cleared {result.deleted_count} documents from {collection_name}")

    def get_collection_count(self, collection_name: str) -> int:
        collection = self.get_collection(collection_name)
        return collection.count_documents({})

    def close(self):
        if self.client:
            self.client.close()
            print("✅ MongoDB connection closed")


def create_spark_session(app_name: str = SPARK_APP_NAME) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    return spark


def parse_date_column(df: DataFrame, col_name: str, new_col_name: Optional[str] = None) -> DataFrame:
    if new_col_name is None:
        new_col_name = col_name
    return df.withColumn(
        new_col_name,
        F.coalesce(
            F.expr(f"try_to_timestamp({col_name}, 'MM/dd/yyyy')"),
            F.expr(f"try_to_timestamp({col_name}, 'yyyy-MM-dd')"),
            F.expr(f"try_to_timestamp({col_name}, 'yyyy-MM-dd\\'T\\'HH:mm:ss')"),
            F.expr(f"try_to_timestamp({col_name})"),
        ),
    )


def clean_numeric_id(df: DataFrame, col_name: str) -> DataFrame:
    return df.withColumn(
        col_name,
        F.regexp_replace(F.col(col_name).cast("string"), r"\.0$", ""),
    )


def standardize_column_names(df: DataFrame, apply: bool = False) -> DataFrame:
    """
    Generic column name standardization - now opt-in via apply parameter.
    Prefer using explicit column mappings in format_*.py files.
    """
    if not apply:
        return df
    
    for col in df.columns:
        new_col = col.strip().lower().replace(" ", "_").replace("-", "_")
        new_col = "".join(c if c.isalnum() or c == "_" else "_" for c in new_col)
        new_col = "_".join(filter(None, new_col.split("_")))
        if new_col != col:
            df = df.withColumnRenamed(col, new_col)
    return df


def apply_column_mapping(df: DataFrame, column_mapping: dict[str, str]) -> DataFrame:
    """
    Apply explicit column mapping to DataFrame.
    Only renames columns that exist in the mapping.
    """
    for old_name, new_name in column_mapping.items():
        if old_name in df.columns and old_name != new_name:
            df = df.withColumnRenamed(old_name, new_name)
    return df


def parse_date_with_format(df: DataFrame, col_name: str, date_format: str, new_col_name: Optional[str] = None) -> DataFrame:
    """
    Parse date column with a specific format.
    """
    if new_col_name is None:
        new_col_name = col_name
    return df.withColumn(
        new_col_name,
        F.expr(f"try_to_timestamp({col_name}, '{date_format}')"),
    )


def cast_numeric_columns(df: DataFrame, numeric_columns: list[str], data_type: str = "double") -> DataFrame:
    """
    Cast multiple columns to numeric type.
    """
    for col_name in numeric_columns:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.expr(f"try_cast({col_name} as {data_type})"),
            )
    return df


def row_to_mongo_doc(row: pd.Series) -> dict[str, any]:
    doc: dict[str, any] = {}
    for k, v in row.items():
        if isinstance(v, (list, dict, np.ndarray, pd.Series)):
            doc[k] = v
            continue
        try:
            if pd.isna(v):
                doc[k] = None
            else:
                doc[k] = v
        except TypeError:
            doc[k] = v
    return doc


def write_df_to_mongo(
    df: DataFrame,
    collection_name: str,
    mongo: MongoDBManager,
    batch_size: int = BATCH_SIZE,
    show_progress: bool = True,
) -> int:
    """
    Converteix un DataFrame Spark a pandas i l'escriu a MongoDB en batches.
    Retorna el nombre total de documents escrits.
    """
    pandas_df = df.toPandas()
    total_rows = len(pandas_df)
    print(f"   ✓ {total_rows:,} documents to write")

    documents_written = 0
    batch: list[dict[str, any]] = []

    for idx, row in pandas_df.iterrows():
        doc = row_to_mongo_doc(row)
        batch.append(doc)
        if len(batch) >= batch_size:
            written = mongo.write_batch(collection_name, batch)
            documents_written += written
            if show_progress:
                print(
                    f"   ✓ Progress: {documents_written:,}/{total_rows:,} "
                    f"({100 * documents_written / max(total_rows, 1):.1f}%)"
                )
            batch = []

    if batch:
        written = mongo.write_batch(collection_name, batch)
        documents_written += written

    return documents_written


def generic_formatter(spark: SparkSession, mongo: MongoDBManager, config: FormatterConfig) -> int:
    """
    Generic formatter function following DRY principle.
    Handles common formatting workflow for all datasets.
    """
    print("\n" + "=" * 80)
    print(f"📊 Formatting {config.name} Dataset")
    print("=" * 80)

    # Read data
    print(f"⬇️  Reading from: {config.landing_path}")
    if config.file_format == "json":
        df = spark.read.json(str(config.landing_path))
    else:
        df = spark.read.parquet(str(config.landing_path))

    initial_count = df.count()
    print(f"   ✓ Loaded {initial_count:,} records")

    # Apply column mappings
    if config.column_mapping:
        print("🔄 Applying column mappings...")
        df = apply_column_mapping(df, config.column_mapping)

    # Clean ID column
    if config.id_column and config.id_column in df.columns:
        print(f"🧹 Cleaning {config.id_column}...")
        df = clean_numeric_id(df, config.id_column)

    # Cast numeric columns
    if config.numeric_columns:
        print("🔢 Casting numeric columns...")
        df = cast_numeric_columns(df, config.numeric_columns, "double")

    # Parse date columns
    if config.date_columns:
        print("📅 Parsing date columns...")
        for col_name, date_format in config.date_columns.items():
            if col_name in df.columns:
                print(f"   ✓ Parsing {col_name} with format {date_format}")
                df = parse_date_with_format(df, col_name, date_format)

    # Apply custom transformations
    if config.custom_transform:
        print("🔧 Applying custom transformations...")
        df = config.custom_transform(df)

    # Validate required fields
    if config.id_column:
        print("✅ Validating required fields...")
        if config.id_column in df.columns:
            null_count = df.filter(F.col(config.id_column).isNull()).count()
            if null_count > 0:
                print(f"   ⚠️  Warning: {null_count} records with null {config.id_column}")

    # Remove duplicates
    if config.dedupe_columns:
        print("🔍 Removing duplicates...")
        df = df.dropDuplicates(config.dedupe_columns)
        deduped_count = df.count()
        print(f"   ✓ {initial_count - deduped_count:,} duplicates removed")
        print(f"   ✓ {deduped_count:,} unique records")

    # Add metadata
    df = df.withColumn("formatted_at", F.lit(datetime.now()))

    # Write to MongoDB
    print(f"🧹 Clearing collection: {config.collection_name}")
    mongo.clear_collection(config.collection_name)

    print(f"💾 Writing to MongoDB collection: {config.collection_name}")
    documents_written = write_df_to_mongo(df, config.collection_name, mongo)

    # Create indexes
    if config.indexes:
        print("🔍 Creating indexes...")
        mongo.create_indexes(config.collection_name, config.indexes)

    print(f"✅ {config.name} formatting complete: {documents_written:,} documents written")
    return documents_written


def validate_formatted_zone(mongo: MongoDBManager, collections: list[str]):
    """
    Versió genèrica del validatore: rep una llista de noms de col·lecció.
    """
    print("\n" + "=" * 80)
    print("✅ FORMATTED ZONE VALIDATION")
    print("=" * 80)

    total_docs = 0

    for coll_name in collections:
        count = mongo.get_collection_count(coll_name)
        total_docs += count
        print(f"\n📊 Collection: {coll_name}")
        print(f"   Documents: {count:,}")

        collection = mongo.get_collection(coll_name)
        sample = collection.find_one()
        if sample:
            print(f"   Sample fields: {list(sample.keys())[:10]}...")

        indexes = collection.list_indexes()
        index_names = [idx["name"] for idx in indexes]
        print(f"   Indexes: {', '.join(index_names)}")

    print("\n" + "=" * 80)
    print(f"✅ Total documents in Formatted Zone: {total_docs:,}")
    print("=" * 80 + "\n")
