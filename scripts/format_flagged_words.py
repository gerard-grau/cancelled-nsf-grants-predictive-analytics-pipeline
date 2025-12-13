# format_flagged_words.py

import sys
from datetime import datetime

from pymongo import ASCENDING
from pyspark.sql import functions as F

from config import (
    MONGO_URI, MONGO_DB,
    COLLECTION_FLAGGED_WORDS,
    LANDING_FLAGGED_WORDS,
)
from formatter_utils import (
    MongoDBManager,
    create_spark_session,
    standardize_column_names,
    write_df_to_mongo,
)


def format_flagged_words(spark, mongo: MongoDBManager) -> int:
    print("\n" + "=" * 80)
    print("📊 Formatting Flagged Words Dataset")
    print("=" * 80)

    print(f"⬇️  Reading from: {LANDING_FLAGGED_WORDS}")
    df = spark.read.parquet(str(LANDING_FLAGGED_WORDS))

    initial_count = df.count()
    print(f"   ✓ Loaded {initial_count:,} flagged words")

    # Standardize column names
    df = standardize_column_names(df)

    # Normalize flagged words to lowercase for case-insensitive matching
    if "flagged_word" in df.columns:
        print("🧹 Normalizing flagged words to lowercase...")
        df = df.withColumn("flagged_word", F.lower(F.trim(F.col("flagged_word"))))

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
