# format_nsf_grants.py

import sys
from datetime import datetime

from pymongo import ASCENDING

from config import (
    MONGO_URI, MONGO_DB,
    COLLECTION_NSF_GRANTS,
    LANDING_NSF_GRANTS,
)
from formatter_utils import (
    MongoDBManager,
    create_spark_session,
    FormatterConfig,
    generic_formatter,
)

# Configuration for NSF grants dataset
NSF_GRANTS_CONFIG = FormatterConfig(
    name="NSF Grants",
    landing_path=str(LANDING_NSF_GRANTS),
    collection_name=COLLECTION_NSF_GRANTS,
    file_format="json",
    column_mapping={
        "id": "award_id",
        "estimatedTotalAmt": "estimated_total_amt",
        "fundsObligatedAmt": "funds_obligated_amt",
        "abstractText": "abstract_text",
        "awardeeAddress": "awardee_address",
        "awardeeCity": "awardee_city",
        "awardeeCountryCode": "awardee_country_code",
        "awardeeDistrictCode": "awardee_district_code",
        "awardeeDistrict": "awardee_district",
        "awardeeName": "awardee_name",
        "awardeeStateCode": "awardee_state_code",
        "awardeeZipCode": "awardee_zip_code",
        "cfdaNumber": "cfda_number",
        "ueiNumber": "uei_number",
        "parentUeiNumber": "parent_uei_number",
        "pdPIName": "pd_pi_name",
        "piFirstName": "pi_first_name",
        "piLastName": "pi_last_name",
        "piEmail": "pi_email",
        "poName": "po_name",
        "poEmail": "po_email",
        "poPhone": "po_phone",
        "primaryProgram": "primary_program",
        "publicAccessMandate": "public_access_mandate",
        "projectOutComesReport": "project_outcomes_report",
        "publicationResearch": "publication_research",
        "startDate": "start_date",
        "expDate": "exp_date",
        "transType": "trans_type",
        "divAbbr": "div_abbr",
        "dirAbbr": "dir_abbr",
        "initAmendmentDate": "init_amendment_date",
        "latestAmendmentDate": "latest_amendment_date",
        "orgLongName": "org_long_name",
        "orgLongName2": "org_long_name_2",
        "progEleCode": "prog_ele_code",
        "progRefCode": "prog_ref_code",
        "fundsObligated": "funds_obligated",
        "histAwd": "hist_awd",
        "activeAwd": "active_awd",
        "managingPec": "managing_pec",
        "orgUrl": "org_url",
    },
    date_columns={
        "date": "MM/dd/yyyy",
        "start_date": "MM/dd/yyyy",
        "exp_date": "MM/dd/yyyy",
        "init_amendment_date": "MM/dd/yyyy",
        "latest_amendment_date": "MM/dd/yyyy",
    },
    numeric_columns=["estimated_total_amt", "funds_obligated_amt"],
    id_column="award_id",
    dedupe_columns=["award_id"],
    indexes=[("award_id", ASCENDING), ("year", ASCENDING)],
)


def format_nsf_grants(spark, mongo: MongoDBManager) -> int:
    """Format NSF grants using generic formatter."""
    return generic_formatter(spark, mongo, NSF_GRANTS_CONFIG)


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
