# format_cruz_list.py
"""
Task A.4 - PySpark Data Formatter Pipeline: Cruz List

Reads raw Cruz list data from the Landing Zone and transforms it
into a standardized format for storage in MongoDB (Formatted Zone).

Transformations include:
- Award ID extraction and normalization
- Text field cleaning
- Metadata enrichment
"""

import sys
from datetime import datetime

from pymongo import ASCENDING
from pyspark.sql import DataFrame, functions as F

from config import (
    MONGO_URI, MONGO_DB,
    COLLECTION_CRUZ_LIST,
    LANDING_CRUZ_LIST,
)
from formatter_utils import (
    MongoDBManager,
    create_spark_session,
    FormatterConfig,
    generic_formatter,
)


def clean_category_fields(df: DataFrame) -> DataFrame:
    """Custom transformation to clean category fields."""
    category_fields = [c for c in df.columns if "category" in c.lower()]
    if category_fields:
        for cat_field in category_fields:
            df = df.withColumn(
                cat_field,
                F.when(
                    F.trim(F.col(cat_field)).isin("", "NULL", "null", "None"),
                    None,
                ).otherwise(F.col(cat_field)),
            )
    return df


# Configuration for Cruz list dataset
CRUZ_LIST_CONFIG = FormatterConfig(
    name="Cruz List",
    landing_path=str(LANDING_CRUZ_LIST),
    collection_name=COLLECTION_CRUZ_LIST,
    file_format="parquet",
    column_mapping={
        "AWARD ID": "award_id",
        "USASPENDING LINK": "usaspending_link",
        "TOTAL AWARD FUNDING AMOUNT": "total_award_funding_amount",
        "RECIPIENT TYPE": "recipient_type",
        "RECIPIENT NAME": "recipient_name",
        "RECIPIENT PARENT NAME": "recipient_parent_name",
        "RECIPIENT STATE": "recipient_state",
        "RECIPIENT STATE OF PERFORMANCE": "recipient_state_of_performance",
        "STATUS CATEGORY": "status_category",
        "SOCIAL JUSTICE CATEGORY": "social_justice_category",
        "RACE CATEGORY": "race_category",
        "GENDER CATEGORY": "gender_category",
        "ENVIRONMENTAL JUSTICE CATEGORY": "environmental_justice_category",
        "AWARD DESCRIPTIONS": "award_descriptions",
        "AWARD ACTION DATE": "award_action_date",
        "PERFORMANCE START DATE": "performance_start_date",
        "PERFORMANCE END DATE": "performance_end_date",
        "RECIPIENT CITY": "recipient_city",
        "RECIPIENT CITY OF PERFORMANCE": "recipient_city_of_performance",
        "RECIPIENT FOREIGN CITY": "recipient_foreign_city",
        "RECIPIENT FOREIGN CITY OF PERFORMANCE": "recipient_foreign_city_of_performance",
        "RECIPIENT COUNTRY": "recipient_country",
        "RECIPIENT COUNTRY OF PERFORMANCE": "recipient_country_of_performance",
        "NSF FUNDING OFFICE": "nsf_funding_office",
        "NSF AWARD CATEGORY": "nsf_award_category",
        "NSF AWARD TYPE": "nsf_award_type",
    },
    date_columns={
        "award_action_date": "MM/dd/yyyy",
        "performance_start_date": "MM/dd/yyyy",
        "performance_end_date": "MM/dd/yyyy",
    },
    numeric_columns=["total_award_funding_amount"],
    id_column="award_id",
    dedupe_columns=None,
    indexes=[("award_id", ASCENDING)],
    custom_transform=clean_category_fields,
)


def format_cruz_list(spark, mongo: MongoDBManager) -> int:
    """Format Cruz list using generic formatter."""
    return generic_formatter(spark, mongo, CRUZ_LIST_CONFIG)


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
