import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure, BulkWriteError

from config import (
    MONGO_URI, MONGO_DB, SPARK_APP_NAME, BATCH_SIZE,
    COLLECTION_NSF_GRANTS, COLLECTION_TERMINATED_GRANTS,
    COLLECTION_CRUZ_LIST, COLLECTION_LEGISLATORS,
    LANDING_NSF_GRANTS, LANDING_TERMINATED, LANDING_CRUZ_LIST, LANDING_LEGISLATORS
)


class MongoDBManager:
    def __init__(self, uri: str = MONGO_URI, db_name: str = MONGO_DB):
        self.uri = uri
        self.db_name = db_name
        self.client: Optional[MongoClient] = None
        self.db = None
    
    def connect(self):
        try:
            self.client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping')
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
    
    def create_indexes(self, collection_name: str, indexes: List[tuple]):
        collection = self.get_collection(collection_name)
        for field, index_type in indexes:
            try:
                collection.create_index([(field, index_type)])
                print(f"   ✓ Created index on {collection_name}.{field}")
            except Exception as e:
                print(f"   ⚠️  Index creation warning for {field}: {e}")
    
    def write_batch(self, collection_name: str, documents: List[Dict]):
        if not documents:
            return 0
        collection = self.get_collection(collection_name)
        try:
            result = collection.insert_many(documents, ordered=False)
            return len(result.inserted_ids)
        except BulkWriteError as e:
            inserted = e.details.get('nInserted', 0)
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


def parse_date_column(df: DataFrame, col_name: str, new_col_name: str = None) -> DataFrame:
    if new_col_name is None:
        new_col_name = col_name
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
    return df.withColumn(
        col_name,
        F.regexp_replace(F.col(col_name).cast("string"), r"\.0$", "")
    )


def standardize_column_names(df: DataFrame) -> DataFrame:
    for col in df.columns:
        new_col = col.strip().lower().replace(" ", "_").replace("-", "_")
        new_col = "".join(c if c.isalnum() or c == "_" else "_" for c in new_col)
        new_col = "_".join(filter(None, new_col.split("_")))
        if new_col != col:
            df = df.withColumnRenamed(col, new_col)
    return df


def row_to_mongo_doc(row: pd.Series) -> Dict[str, Any]:
    doc: Dict[str, Any] = {}
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


def format_nsf_grants(spark: SparkSession, mongo: MongoDBManager) -> int:
    print("\n" + "="*80)
    print("📊 Formatting NSF Grants Dataset")
    print("="*80)
    
    print(f"⬇️  Reading from: {LANDING_NSF_GRANTS}")
    df = spark.read.json(str(LANDING_NSF_GRANTS))
    
    initial_count = df.count()
    print(f"   ✓ Loaded {initial_count:,} records")
    
    df = standardize_column_names(df)
    
    if "id" in df.columns:
        df = df.withColumnRenamed("id", "award_id")
    
    date_fields = [c for c in df.columns if "date" in c.lower()]
    for date_field in date_fields:
        print(f"   📅 Parsing date field: {date_field}")
        df = parse_date_column(df, date_field)
    
    if "estimatedtotalamt" in df.columns:
        df = df.withColumn(
            "estimated_total_amt",
            F.expr("try_cast(estimatedtotalamt as double)")
        )
    if "fundsobligatedamt" in df.columns:
        df = df.withColumn(
            "funds_obligated_amt",
            F.expr("try_cast(fundsobligatedamt as double)")
        )
    
    if "award_id" in df.columns:
        df = clean_numeric_id(df, "award_id")
    
    print("🔍 Removing duplicates...")
    if "award_id" in df.columns:
        df = df.dropDuplicates(["award_id"])
    deduped_count = df.count()
    print(f"   ✓ {initial_count - deduped_count:,} duplicates removed")
    print(f"   ✓ {deduped_count:,} unique records")
    
    df = df.withColumn("formatted_at", F.lit(datetime.now()))
    
    print(f"🧹 Clearing collection: {COLLECTION_NSF_GRANTS}")
    mongo.clear_collection(COLLECTION_NSF_GRANTS)
    
    print(f"💾 Writing to MongoDB collection: {COLLECTION_NSF_GRANTS}")
    pandas_df = df.toPandas()
    total_rows = len(pandas_df)
    print(f"   ✓ {total_rows:,} documents to write")
    
    documents_written = 0
    batch: List[Dict[str, Any]] = []
    
    for idx, row in pandas_df.iterrows():
        doc = row_to_mongo_doc(row)
        batch.append(doc)
        if len(batch) >= BATCH_SIZE:
            written = mongo.write_batch(COLLECTION_NSF_GRANTS, batch)
            documents_written += written
            print(f"   ✓ Progress: {documents_written:,}/{total_rows:,} documents ({100*documents_written/total_rows:.1f}%)")
            batch = []
    
    if batch:
        written = mongo.write_batch(COLLECTION_NSF_GRANTS, batch)
        documents_written += written
    
    print("🔍 Creating indexes...")
    index_list = []
    if "award_id" in df.columns:
        index_list.append(("award_id", ASCENDING))
    if "year" in df.columns:
        index_list.append(("year", ASCENDING))
    if index_list:
        mongo.create_indexes(COLLECTION_NSF_GRANTS, index_list)
    
    print(f"✅ NSF Grants formatting complete: {documents_written:,} documents written")
    return documents_written


def format_terminated_grants(spark: SparkSession, mongo: MongoDBManager) -> int:
    print("\n" + "="*80)
    print("📊 Formatting Terminated Grants Dataset")
    print("="*80)
    
    print(f"⬇️  Reading from: {LANDING_TERMINATED}")
    df = spark.read.parquet(str(LANDING_TERMINATED))
    
    initial_count = df.count()
    print(f"   ✓ Loaded {initial_count:,} records")
    
    df = standardize_column_names(df)
    
    date_fields = [c for c in df.columns if "date" in c.lower()]
    for date_field in date_fields:
        print(f"   📅 Parsing date field: {date_field}")
        df = parse_date_column(df, date_field)
    
    if "in_cruz_list" in df.columns:
        df = df.withColumn(
            "in_cruz_list",
            F.when(F.col("in_cruz_list").cast("string").isin("True", "true", "1", "yes"), True)
             .when(F.col("in_cruz_list").cast("string").isin("False", "false", "0", "no", "", "None", "NULL", "null"), False)
             .otherwise(None).cast("boolean")
        )
    
    if "usaspending_obligated" in df.columns:
        df = df.withColumn(
            "usaspending_obligated",
            F.expr("try_cast(usaspending_obligated as double)")
        )
    
    df = df.withColumn("formatted_at", F.lit(datetime.now()))
    
    print(f"🧹 Clearing collection: {COLLECTION_TERMINATED_GRANTS}")
    mongo.clear_collection(COLLECTION_TERMINATED_GRANTS)
    
    print(f"💾 Writing to MongoDB collection: {COLLECTION_TERMINATED_GRANTS}")
    pandas_df = df.toPandas()
    total_rows = len(pandas_df)
    print(f"   ✓ {total_rows:,} documents to write")
    
    documents_written = 0
    batch: List[Dict[str, Any]] = []
    
    for idx, row in pandas_df.iterrows():
        doc = row_to_mongo_doc(row)
        batch.append(doc)
        if len(batch) >= BATCH_SIZE:
            written = mongo.write_batch(COLLECTION_TERMINATED_GRANTS, batch)
            documents_written += written
            batch = []
    
    if batch:
        written = mongo.write_batch(COLLECTION_TERMINATED_GRANTS, batch)
        documents_written += written
    
    print("🔍 Creating indexes...")
    index_list = []
    if "grant_number" in df.columns:
        index_list.append(("grant_number", ASCENDING))
    if "in_cruz_list" in df.columns:
        index_list.append(("in_cruz_list", ASCENDING))
    if index_list:
        mongo.create_indexes(COLLECTION_TERMINATED_GRANTS, index_list)
    
    print(f"✅ Terminated Grants formatting complete: {documents_written:,} documents")
    return documents_written


def format_cruz_list(spark: SparkSession, mongo: MongoDBManager) -> int:
    print("\n" + "="*80)
    print("📊 Formatting Cruz List Dataset")
    print("="*80)
    
    print(f"⬇️  Reading from: {LANDING_CRUZ_LIST}")
    df = spark.read.parquet(str(LANDING_CRUZ_LIST))
    
    initial_count = df.count()
    print(f"   ✓ Loaded {initial_count:,} records")
    
    df = standardize_column_names(df)
    
    if "award_id" in df.columns:
        print("🧹 Cleaning award IDs...")
        df = clean_numeric_id(df, "award_id")
    
    date_fields = [c for c in df.columns if "date" in c.lower()]
    for date_field in date_fields:
        print(f"   📅 Parsing date field: {date_field}")
        df = parse_date_column(df, date_field)
    
    if "total_award_funding_amount" in df.columns:
        df = df.withColumn(
            "total_award_funding_amount",
            F.expr("try_cast(total_award_funding_amount as double)")
        )
    
    category_fields = [c for c in df.columns if "category" in c.lower()]
    for cat_field in category_fields:
        df = df.withColumn(
            cat_field,
            F.when(F.trim(F.col(cat_field)).isin("", "NULL", "null", "None"), None)
             .otherwise(F.col(cat_field))
        )
    
    df = df.withColumn("formatted_at", F.lit(datetime.now()))
    
    print(f"🧹 Clearing collection: {COLLECTION_CRUZ_LIST}")
    mongo.clear_collection(COLLECTION_CRUZ_LIST)
    
    print(f"💾 Writing to MongoDB collection: {COLLECTION_CRUZ_LIST}")
    pandas_df = df.toPandas()
    total_rows = len(pandas_df)
    print(f"   ✓ {total_rows:,} documents to write")
    
    documents_written = 0
    batch: List[Dict[str, Any]] = []
    
    for idx, row in pandas_df.iterrows():
        doc = row_to_mongo_doc(row)
        batch.append(doc)
        if len(batch) >= BATCH_SIZE:
            written = mongo.write_batch(COLLECTION_CRUZ_LIST, batch)
            documents_written += written
            batch = []
    
    if batch:
        written = mongo.write_batch(COLLECTION_CRUZ_LIST, batch)
        documents_written += written
    
    print("🔍 Creating indexes...")
    index_list = []
    if "award_id" in df.columns:
        index_list.append(("award_id", ASCENDING))
    if index_list:
        mongo.create_indexes(COLLECTION_CRUZ_LIST, index_list)
    
    print(f"✅ Cruz List formatting complete: {documents_written:,} documents")
    return documents_written


def format_legislators(spark: SparkSession, mongo: MongoDBManager) -> int:
    print("\n" + "="*80)
    print("📊 Formatting Legislators Dataset")
    print("="*80)
    
    print(f"⬇️  Reading from: {LANDING_LEGISLATORS}")
    df = spark.read.parquet(str(LANDING_LEGISLATORS))
    
    initial_count = df.count()
    print(f"   ✓ Loaded {initial_count:,} records")
    
    df = standardize_column_names(df)
    df = df.withColumn("formatted_at", F.lit(datetime.now()))
    
    print(f"🧹 Clearing collection: {COLLECTION_LEGISLATORS}")
    mongo.clear_collection(COLLECTION_LEGISLATORS)
    
    print(f"💾 Writing to MongoDB collection: {COLLECTION_LEGISLATORS}")
    pandas_df = df.toPandas()
    total_rows = len(pandas_df)
    print(f"   ✓ {total_rows:,} documents to write")
    
    documents_written = 0
    batch: List[Dict[str, Any]] = []
    
    for idx, row in pandas_df.iterrows():
        doc = row_to_mongo_doc(row)
        batch.append(doc)
        if len(batch) >= BATCH_SIZE:
            written = mongo.write_batch(COLLECTION_LEGISLATORS, batch)
            documents_written += written
            batch = []
    
    if batch:
        written = mongo.write_batch(COLLECTION_LEGISLATORS, batch)
        documents_written += written
    
    print("🔍 Creating indexes...")
    mongo.create_indexes(COLLECTION_LEGISLATORS, [
        ("id.bioguide", ASCENDING)
    ])
    
    print(f"✅ Legislators formatting complete: {documents_written:,} documents")
    return documents_written


def validate_formatted_zone(mongo: MongoDBManager):
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
        
        collection = mongo.get_collection(coll_name)
        sample = collection.find_one()
        if sample:
            print(f"   Sample fields: {list(sample.keys())[:10]}...")
        
        indexes = collection.list_indexes()
        index_names = [idx['name'] for idx in indexes]
        print(f"   Indexes: {', '.join(index_names)}")
    
    print(f"\n{'='*80}")
    print(f"✅ Total documents in Formatted Zone: {total_docs:,}")
    print(f"{'='*80}\n")


def main():
    print("\n" + "="*80)
    print("🚀 TASK A.4 - DATA FORMATTING PIPELINES")
    print("="*80)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"MongoDB URI: {MONGO_URI}")
    print(f"MongoDB Database: {MONGO_DB}")
    print("="*80)
    
    mongo = MongoDBManager()
    if not mongo.connect():
        print("❌ Failed to connect to MongoDB. Exiting.")
        sys.exit(1)
    
    print("\n🔥 Initializing Spark session...")
    spark = create_spark_session()
    print(f"   ✓ Spark version: {spark.version}")
    
    try:
        stats = {}
        stats["nsf_grants"] = format_nsf_grants(spark, mongo)
        stats["terminated_grants"] = format_terminated_grants(spark, mongo)
        stats["cruz_list"] = format_cruz_list(spark, mongo)
        stats["legislators"] = format_legislators(spark, mongo)
        
        validate_formatted_zone(mongo)
        
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
        spark.stop()
        mongo.close()
        print("\n🏁 Resources released. Goodbye!")


if __name__ == "__main__":
    main()
