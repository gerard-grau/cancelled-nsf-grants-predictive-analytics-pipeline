# format_nsf_grants.py

import sys
from datetime import datetime

from pymongo import ASCENDING
from pyspark.sql import functions as F

from config import (
    MONGO_URI, MONGO_DB,
    COLLECTION_NSF_GRANTS,
    LANDING_NSF_GRANTS,
)
from formatter_utils import (
    MongoDBManager,
    create_spark_session,
    parse_date_column,
    clean_numeric_id,
    standardize_column_names,
    write_df_to_mongo,
)


def format_nsf_grants(spark, mongo: MongoDBManager) -> int:
    print("\n" + "=" * 80)
    print("📊 Formatting NSF Grants Dataset")
    print("=" * 80)

    print(f"⬇️  Reading from: {LANDING_NSF_GRANTS}")
    df = spark.read.json(str(LANDING_NSF_GRANTS))

    initial_count = df.count()
    print(f"   ✓ Loaded {initial_count:,} records")

    df = standardize_column_names(df)

    # id → award_id
    if "id" in df.columns:
        df = df.withColumnRenamed("id", "award_id")

    # parse dates
    date_fields = [c for c in df.columns if "date" in c.lower()]
    for date_field in date_fields:
        print(f"   📅 Parsing date field: {date_field}")
        df = parse_date_column(df, date_field)

    # numeric fields
    if "estimatedtotalamt" in df.columns:
        df = df.withColumn(
            "estimated_total_amt",
            F.expr("try_cast(estimatedtotalamt as double)"),
        )
    if "fundsobligatedamt" in df.columns:
        df = df.withColumn(
            "funds_obligated_amt",
            F.expr("try_cast(fundsobligatedamt as double)"),
        )

    # clean award_id
    if "award_id" in df.columns:
        df = clean_numeric_id(df, "award_id")

    # dedupe
    print("🔍 Removing duplicates...")
    if "award_id" in df.columns:
        df = df.dropDuplicates(["award_id"])
    deduped_count = df.count()
    print(f"   ✓ {initial_count - deduped_count:,} duplicates removed")
    print(f"   ✓ {deduped_count:,} unique records")

    df = df.withColumn("formatted_at", F.lit(datetime.now()))

    # write to mongo
    print(f"🧹 Clearing collection: {COLLECTION_NSF_GRANTS}")
    mongo.clear_collection(COLLECTION_NSF_GRANTS)

    print(f"💾 Writing to MongoDB collection: {COLLECTION_NSF_GRANTS}")
    documents_written = write_df_to_mongo(df, COLLECTION_NSF_GRANTS, mongo)

    # indexes
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


def main():
    print("\n" + "=" * 80)
    print("🚀 NSF GRANTS - DATA FORMATTING")
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
        count = format_nsf_grants(spark, mongo)
        print("\n" + "=" * 80)
        print("📈 NSF GRANTS FORMATTING SUMMARY")
        print("=" * 80)
        print(f"   {'nsf_grants':.<30} {count:>10,} documents")
        print("=" * 80)
        print(
            f"\n✅ NSF Grants task completed at "
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
    except Exception as e:
        print(f"\n❌ Error during NSF grants formatting: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()
        mongo.close()
        print("\n🏁 Resources released. Goodbye!")


if __name__ == "__main__":
    main()
