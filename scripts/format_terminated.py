# format_terminated_grants.py

import sys
from datetime import datetime

from pymongo import ASCENDING
from pyspark.sql import functions as F

from config import (
    MONGO_URI, MONGO_DB,
    COLLECTION_TERMINATED_GRANTS,
    LANDING_TERMINATED,
)
from formatter_utils import (
    MongoDBManager,
    create_spark_session,
    parse_date_column,
    standardize_column_names,
    write_df_to_mongo,
)


def format_terminated_grants(spark, mongo: MongoDBManager) -> int:
    print("\n" + "=" * 80)
    print("📊 Formatting Terminated Grants Dataset")
    print("=" * 80)

    print(f"⬇️  Reading from: {LANDING_TERMINATED}")
    df = spark.read.parquet(str(LANDING_TERMINATED))

    initial_count = df.count()
    print(f"   ✓ Loaded {initial_count:,} records")

    df = standardize_column_names(df)

    # parse dates
    date_fields = [c for c in df.columns if "date" in c.lower()]
    for date_field in date_fields:
        print(f"   📅 Parsing date field: {date_field}")
        df = parse_date_column(df, date_field)

    # boolean in_cruz_list
    if "in_cruz_list" in df.columns:
        df = df.withColumn(
            "in_cruz_list",
            F.when(
                F.col("in_cruz_list").cast("string").isin(
                    "True", "true", "1", "yes"
                ),
                True,
            )
            .when(
                F.col("in_cruz_list").cast("string").isin(
                    "False", "false", "0", "no", "", "None", "NULL", "null"
                ),
                False,
            )
            .otherwise(None)
            .cast("boolean"),
        )

    # numeric field
    if "usaspending_obligated" in df.columns:
        df = df.withColumn(
            "usaspending_obligated",
            F.expr("try_cast(usaspending_obligated as double)"),
        )

    df = df.withColumn("formatted_at", F.lit(datetime.now()))

    print(f"🧹 Clearing collection: {COLLECTION_TERMINATED_GRANTS}")
    mongo.clear_collection(COLLECTION_TERMINATED_GRANTS)

    print(f"💾 Writing to MongoDB collection: {COLLECTION_TERMINATED_GRANTS}")
    documents_written = write_df_to_mongo(df, COLLECTION_TERMINATED_GRANTS, mongo)

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


def main():
    print("\n" + "=" * 80)
    print("🚀 TERMINATED GRANTS - DATA FORMATTING")
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
        count = format_terminated_grants(spark, mongo)
        print("\n" + "=" * 80)
        print("📈 TERMINATED GRANTS FORMATTING SUMMARY")
        print("=" * 80)
        print(f"   {'terminated_grants':.<30} {count:>10,} documents")
        print("=" * 80)
        print(
            f"\n✅ Terminated Grants task completed at "
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
    except Exception as e:
        print(f"\n❌ Error during terminated grants formatting: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()
        mongo.close()
        print("\n🏁 Resources released. Goodbye!")


if __name__ == "__main__":
    main()
