# format_cruz_list.py

import sys
from datetime import datetime

from pymongo import ASCENDING
from pyspark.sql import functions as F

from config import (
    MONGO_URI, MONGO_DB,
    COLLECTION_CRUZ_LIST,
    LANDING_CRUZ_LIST,
)
from formatter_utils import (
    MongoDBManager,
    create_spark_session,
    parse_date_column,
    clean_numeric_id,
    standardize_column_names,
    write_df_to_mongo,
)


def format_cruz_list(spark, mongo: MongoDBManager) -> int:
    print("\n" + "=" * 80)
    print("📊 Formatting Cruz List Dataset")
    print("=" * 80)

    print(f"⬇️  Reading from: {LANDING_CRUZ_LIST}")
    df = spark.read.parquet(str(LANDING_CRUZ_LIST))

    initial_count = df.count()
    print(f"   ✓ Loaded {initial_count:,} records")

    df = standardize_column_names(df)

    if "award_id" in df.columns:
        print("🧹 Cleaning award IDs...")
        df = clean_numeric_id(df, "award_id")

    # parse dates
    date_fields = [c for c in df.columns if "date" in c.lower()]
    for date_field in date_fields:
        print(f"   📅 Parsing date field: {date_field}")
        df = parse_date_column(df, date_field)

    if "total_award_funding_amount" in df.columns:
        df = df.withColumn(
            "total_award_funding_amount",
            F.expr("try_cast(total_award_funding_amount as double)"),
        )

    category_fields = [c for c in df.columns if "category" in c.lower()]
    for cat_field in category_fields:
        df = df.withColumn(
            cat_field,
            F.when(
                F.trim(F.col(cat_field)).isin("", "NULL", "null", "None"),
                None,
            ).otherwise(F.col(cat_field)),
        )

    df = df.withColumn("formatted_at", F.lit(datetime.now()))

    print(f"🧹 Clearing collection: {COLLECTION_CRUZ_LIST}")
    mongo.clear_collection(COLLECTION_CRUZ_LIST)

    print(f"💾 Writing to MongoDB collection: {COLLECTION_CRUZ_LIST}")
    documents_written = write_df_to_mongo(df, COLLECTION_CRUZ_LIST, mongo)

    print("🔍 Creating indexes...")
    index_list = []
    if "award_id" in df.columns:
        index_list.append(("award_id", ASCENDING))
    if index_list:
        mongo.create_indexes(COLLECTION_CRUZ_LIST, index_list)

    print(f"✅ Cruz List formatting complete: {documents_written:,} documents")
    return documents_written


def main():
    print("\n" + "=" * 80)
    print("🚀 CRUZ LIST - DATA FORMATTING")
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
        count = format_cruz_list(spark, mongo)
        print("\n" + "=" * 80)
        print("📈 CRUZ LIST FORMATTING SUMMARY")
        print("=" * 80)
        print(f"   {'cruz_list':.<30} {count:>10,} documents")
        print("=" * 80)
        print(
            f"\n✅ Cruz List task completed at "
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
    except Exception as e:
        print(f"\n❌ Error during Cruz list formatting: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()
        mongo.close()
        print("\n🏁 Resources released. Goodbye!")


if __name__ == "__main__":
    main()
