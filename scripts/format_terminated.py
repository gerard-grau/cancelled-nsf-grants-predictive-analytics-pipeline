# format_terminated_grants.py

import sys
from datetime import datetime

from pymongo import ASCENDING
from pyspark.sql import DataFrame, functions as F

from config import (
    MONGO_URI, MONGO_DB,
    COLLECTION_TERMINATED_GRANTS,
    LANDING_TERMINATED,
)
from formatter_utils import (
    MongoDBManager,
    create_spark_session,
    FormatterConfig,
    generic_formatter,
)


def parse_boolean_field(df: DataFrame) -> DataFrame:
    """Custom transformation for in_cruz_list boolean field."""
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
    return df


# Configuration for terminated grants dataset
TERMINATED_GRANTS_CONFIG = FormatterConfig(
    name="Terminated Grants",
    landing_path=str(LANDING_TERMINATED),
    collection_name=COLLECTION_TERMINATED_GRANTS,
    file_format="parquet",
    column_mapping={},  # Already snake_case
    date_columns={
        "termination_letter_date": "yyyy-MM-dd",
        "nsf_startdate": "yyyy-MM-dd",
        "nsf_expected_end_date": "yyyy-MM-dd",
    },
    numeric_columns=["usaspending_obligated"],
    id_column="grant_number",
    dedupe_columns=None,
    indexes=[("grant_number", ASCENDING), ("in_cruz_list", ASCENDING)],
    custom_transform=parse_boolean_field,
)


def format_terminated_grants(spark, mongo: MongoDBManager) -> int:
    """Format terminated grants using generic formatter."""
    return generic_formatter(spark, mongo, TERMINATED_GRANTS_CONFIG)


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
