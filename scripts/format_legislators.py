# format_legislators.py

import sys
from datetime import datetime

from config import (
    MONGO_URI, MONGO_DB,
    COLLECTION_LEGISLATORS,
    LANDING_LEGISLATORS,
)
from formatter_utils import (
    MongoDBManager,
    create_spark_session,
    standardize_column_names,
    write_df_to_mongo,
)


def format_legislators(spark, mongo: MongoDBManager) -> int:
    print("\n" + "=" * 80)
    print("📊 Formatting Legislators Dataset")
    print("=" * 80)

    print(f"⬇️  Reading from: {LANDING_LEGISLATORS}")
    df = spark.read.parquet(str(LANDING_LEGISLATORS))

    initial_count = df.count()
    print(f"   ✓ Loaded {initial_count:,} records")

    df = standardize_column_names(df)
    df = df.withColumn("formatted_at", df.sparkSession.sql.functions.lit(datetime.now()))

    print(f"🧹 Clearing collection: {COLLECTION_LEGISLATORS}")
    mongo.clear_collection(COLLECTION_LEGISLATORS)

    print(f"💾 Writing to MongoDB collection: {COLLECTION_LEGISLATORS}")
    documents_written = write_df_to_mongo(df, COLLECTION_LEGISLATORS, mongo)

    # index d’exemple (el pots adaptar)
    mongo.create_indexes(COLLECTION_LEGISLATORS, [
        ("id.bioguide", 1),
    ])

    print(f"✅ Legislators formatting complete: {documents_written:,} documents")
    return documents_written


def main():
    print("\n" + "=" * 80)
    print("🚀 LEGISLATORS - DATA FORMATTING")
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
        count = format_legislators(spark, mongo)
        print("\n" + "=" * 80)
        print("📈 LEGISLATORS FORMATTING SUMMARY")
        print("=" * 80)
        print(f"   {'legislators':.<30} {count:>10,} documents")
        print("=" * 80)
        print(
            f"\n✅ Legislators task completed at "
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
    except Exception as e:
        print(f"\n❌ Error during legislators formatting: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()
        mongo.close()
        print("\n🏁 Resources released. Goodbye!")


if __name__ == "__main__":
    main()
