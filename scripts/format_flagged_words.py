# format_flagged_words.py

import sys
from datetime import datetime

from pymongo import ASCENDING
from pyspark.sql import DataFrame, functions as F

from config import (
    MONGO_URI, MONGO_DB,
    COLLECTION_FLAGGED_WORDS,
    LANDING_FLAGGED_WORDS,
)
from formatter_utils import (
    MongoDBManager,
    create_spark_session,
    FormatterConfig,
    generic_formatter,
)


def normalize_and_validate_words(df: DataFrame) -> DataFrame:
    """Custom transformation to normalize and validate flagged words."""
    if "flagged_word" in df.columns:
        # Normalize to lowercase
        df = df.withColumn("flagged_word", F.lower(F.trim(F.col("flagged_word"))))
        
        # Filter out null and empty values
        null_count = df.filter(F.col("flagged_word").isNull()).count()
        empty_count = df.filter(F.col("flagged_word") == "").count()
        
        if null_count > 0 or empty_count > 0:
            print(f"   ⚠️  Filtering out {null_count} null and {empty_count} empty flagged words")
            df = df.filter(F.col("flagged_word").isNotNull() & (F.col("flagged_word") != ""))
    
    return df


# Configuration for flagged words dataset
FLAGGED_WORDS_CONFIG = FormatterConfig(
    name="Flagged Words",
    landing_path=str(LANDING_FLAGGED_WORDS),
    collection_name=COLLECTION_FLAGGED_WORDS,
    file_format="parquet",
    column_mapping={},  # No mapping needed
    date_columns={},
    numeric_columns=[],
    id_column=None,
    dedupe_columns=["flagged_word"],
    indexes=[("flagged_word", ASCENDING)],
    custom_transform=normalize_and_validate_words,
)


def format_flagged_words(spark, mongo: MongoDBManager) -> int:
    """Format flagged words using generic formatter."""
    return generic_formatter(spark, mongo, FLAGGED_WORDS_CONFIG)
    print("\n" + "=" * 80)
    print("📊 Formatting Flagged Words Dataset")
    print("=" * 80)

    print(f"⬇️  Reading from: {LANDING_FLAGGED_WORDS}")
    df = spark.read.parquet(str(LANDING_FLAGGED_WORDS))

    initial_count = df.count()
    print(f"   ✓ Loaded {initial_count:,} flagged words")

    # Normalize flagged words to lowercase for case-insensitive matching
    if "flagged_word" in df.columns:
        print("🧹 Normalizing flagged words to lowercase...")
        df = df.withColumn("flagged_word", F.lower(F.trim(F.col("flagged_word"))))
    
    # Validate required fields
    print("✅ Validating required fields...")
    if "flagged_word" in df.columns:
        null_count = df.filter(F.col("flagged_word").isNull()).count()
        empty_count = df.filter(F.trim(F.col("flagged_word")) == "").count()
        if null_count > 0 or empty_count > 0:
            print(f"   ⚠️  Warning: {null_count} null and {empty_count} empty flagged words")
            df = df.filter(F.col("flagged_word").isNotNull() & (F.trim(F.col("flagged_word")) != ""))

    # Remove duplicates
    initial = df.count()
    df = df.dropDuplicates(["flagged_word"])
    final = df.count()
    if initial != final:
        print(f"   ✓ Removed {initial - final} duplicate words")

    # Add metadata
    df = df.withColumn("formatted_at", F.lit(datetime.now()))

    print(f"🧹 Clearing collection: {COLLECTION_FLAGGED_WORDS}")
    mongo.clear_collection(COLLECTION_FLAGGED_WORDS)

    print(f"💾 Writing to MongoDB collection: {COLLECTION_FLAGGED_WORDS}")
    documents_written = write_df_to_mongo(df, COLLECTION_FLAGGED_WORDS, mongo)

    print("🔍 Creating indexes...")
    mongo.create_indexes(COLLECTION_FLAGGED_WORDS, [("flagged_word", ASCENDING)])

    print(f"✅ Flagged Words formatting complete: {documents_written:,} documents")
    return documents_written


def main():
    print("\n" + "=" * 80)
    print("🚀 FLAGGED WORDS - DATA FORMATTING")
    print("=" * 80)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"MongoDB URI: {MONGO_URI}")
    print(f"MongoDB Database: {MONGO_DB}")
    print("=" * 80)

    mongo = MongoDBManager()
    if not mongo.connect():
        print("❌ Failed to connect to MongoDB. Exiting.")
        sys.exit(1)

    print("\n🔥 Initializing Spark session...")
    spark = create_spark_session()
    print(f"   ✓ Spark version: {spark.version}")

    try:
        documents_written = format_flagged_words(spark, mongo)
        print("\n" + "=" * 80)
        print(f"✅ SUCCESS: {documents_written:,} flagged words formatted")
        print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)

    except Exception as e:
        print(f"\n❌ ERROR during formatting: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        mongo.close()
        spark.stop()


if __name__ == "__main__":
    main()
