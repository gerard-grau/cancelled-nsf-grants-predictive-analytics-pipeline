import sys
from pathlib import Path

import pandas as pd
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

from config import (
    MONGO_URI,
    MONGO_DB,
    COLLECTION_NSF_GRANTS,
    COLLECTION_TERMINATED_GRANTS,
    COLLECTION_CRUZ_LIST,
)

BASE_DIR = Path(__file__).parent.parent
DATALAKE_DIR = BASE_DIR / "datalake"
EXPLOITATION_DIR = DATALAKE_DIR / "exploitation"
TRAIN_DELTA_DIR = EXPLOITATION_DIR / "grants_train_delta"
TEST_DELTA_DIR = EXPLOITATION_DIR / "grants_test_delta"


def create_spark_with_delta(app_name: str = "Task A.5 - Exploitation Dataset") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


def load_collection_as_pandas(collection_name: str) -> pd.DataFrame:
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    coll = db[collection_name]
    docs = list(coll.find({}, {"_id": False}))
    client.close()
    if not docs:
        return pd.DataFrame()
    return pd.DataFrame(docs)


def main(train_ratio: float = 0.8, seed: int = 42):
    EXPLOITATION_DIR.mkdir(parents=True, exist_ok=True)

    pdf_nsf = load_collection_as_pandas(COLLECTION_NSF_GRANTS)
    pdf_cruz = load_collection_as_pandas(COLLECTION_CRUZ_LIST)
    pdf_term = load_collection_as_pandas(COLLECTION_TERMINATED_GRANTS)

    if pdf_nsf.empty:
        print("NSF grants collection is empty, nothing to do.")
        sys.exit(1)

    pdf_nsf["award_id"] = pdf_nsf["award_id"].astype(str)

    cruz_ids = set()
    if not pdf_cruz.empty and "award_id" in pdf_cruz.columns:
        cruz_ids = set(pdf_cruz["award_id"].astype(str).tolist())

    term_ids = set()
    if not pdf_term.empty and "grant_number" in pdf_term.columns:
        term_ids = set(pdf_term["grant_number"].astype(str).tolist())

    pdf_nsf["in_cruz_list"] = pdf_nsf["award_id"].isin(cruz_ids)
    pdf_nsf["is_terminated"] = pdf_nsf["award_id"].isin(term_ids)

    candidate_cols = [
        "award_id",
        "year",
        "agency",
        "awardagencycode",
        "dirabbr",
        "divabbr",
        "fundagencycode",
        "fundprogramname",
        "orglongname",
        "orgcodedir",
        "orgcodediv",
        "orgurl",
        "perfcity",
        "perfstatecode",
        "perfcountrycode",
        "perfzipcode",
        "estimated_total_amt",
        "funds_obligated_amt",
        "startdate",
        "date",
        "expdate",
        "projectoutcomesreport",
        "title",
        "transtype",
        "formatted_at",
        "in_cruz_list",
        "is_terminated",
    ]

    cols_exist = [c for c in candidate_cols if c in pdf_nsf.columns]
    pdf_feat = pdf_nsf[cols_exist].copy()

    if "year" in pdf_feat.columns:
        pdf_feat = pdf_feat[pdf_feat["year"].notna()]
    pdf_feat = pdf_feat[pdf_feat["award_id"].notna()]

    for col in pdf_feat.columns:
        if col in ("in_cruz_list", "is_terminated"):
            pdf_feat[col] = pdf_feat[col].astype(bool)
        elif col == "year":
            pdf_feat[col] = pd.to_numeric(pdf_feat[col], errors="coerce").astype("Int64")
        elif col in ("estimated_total_amt", "funds_obligated_amt"):
            pdf_feat[col] = pd.to_numeric(pdf_feat[col], errors="coerce")

    spark = create_spark_with_delta()

    df_all = spark.createDataFrame(pdf_feat)

    if "year" in df_all.columns:
        df_all = df_all.filter(F.col("year").isNotNull())

    df_train, df_test = df_all.randomSplit([train_ratio, 1.0 - train_ratio], seed=seed)

    df_train.write.format("delta").mode("overwrite").partitionBy("year").save(str(TRAIN_DELTA_DIR))
    df_test.write.format("delta").mode("overwrite").partitionBy("year").save(str(TEST_DELTA_DIR))

    print(f"Train Delta saved at: {TRAIN_DELTA_DIR}")
    print(f"Test Delta saved at:  {TEST_DELTA_DIR}")

    spark.stop()


if __name__ == "__main__":
    main()
