import sys
from pathlib import Path
from typing import Optional

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
    COLLECTION_LEGISLATORS,  # 🔴 nou import
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


def build_terms_dataframe(pdf_legislators: pd.DataFrame) -> pd.DataFrame:
    """
    Construeix un DataFrame pla amb un registre per term:
    columns: ['type', 'state', 'start', 'end', 'party']
    Només té en compte els elements de 'terms' que siguin dicts.
    """
    if pdf_legislators.empty or "terms" not in pdf_legislators.columns:
        return pd.DataFrame(columns=["type", "state", "start", "end", "party"])

    records = []

    for _, row in pdf_legislators.iterrows():
        terms = row.get("terms") or []

        # Ens assegurem que 'terms' és una llista
        if not isinstance(terms, list):
            continue

        for t in terms:
            # Només processem si és un dict; si és una llista/altre, l'ignorem
            if not isinstance(t, dict):
                continue

            records.append(
                {
                    "type": t.get("type"),
                    "state": t.get("state"),
                    "start": t.get("start"),
                    "end": t.get("end"),
                    "party": t.get("party"),
                }
            )

    df_terms = pd.DataFrame(records)
    if df_terms.empty:
        return df_terms

    # Netegem i convertim a dates
    df_terms = df_terms.dropna(subset=["type", "state", "start", "end"])
    df_terms["start"] = pd.to_datetime(df_terms["start"], errors="coerce")
    df_terms["end"] = pd.to_datetime(df_terms["end"], errors="coerce")
    df_terms = df_terms.dropna(subset=["start", "end"])

    return df_terms


def make_party_lookup(df_terms: pd.DataFrame):
    """
    Retorna una funció que, donat (state, date, type='rep'/'sen'),
    retorna el partit o 'Unknown' si no hi ha cap term actiu.
    """
    if df_terms.empty:
        # Si no tenim dades, retornem directament Unknown
        def lookup(_state: Optional[str], _date, _typ: str) -> str:
            return "Unknown"

        return lookup

    # Per optimitzar, separem per tipus
    df_rep = df_terms[df_terms["type"] == "rep"].copy()
    df_sen = df_terms[df_terms["type"] == "sen"].copy()

    def _get_party_for_type(state: Optional[str], date, typ: str) -> str:
        if state is None or pd.isna(state) or date is None or pd.isna(date):
            return "Unknown"

        if typ == "rep":
            df = df_rep
        else:
            df = df_sen

        if df.empty:
            return "Unknown"

        mask = (df["state"] == state) & (df["start"] <= date) & (df["end"] >= date)
        matches = df.loc[mask]
        if matches.empty:
            return "Unknown"

        # Si hi ha múltiples, agafem un (per exemple el més recent per start)
        match = matches.sort_values("start").iloc[-1]
        return match.get("party") or "Unknown"

    return _get_party_for_type


def main(train_ratio: float = 0.8, seed: int = 42):
    EXPLOITATION_DIR.mkdir(parents=True, exist_ok=True)

    # -----------------------------------------------------------------
    # 🔹 Carreguem les col·leccions de Mongo
    # -----------------------------------------------------------------
    pdf_nsf = load_collection_as_pandas(COLLECTION_NSF_GRANTS)
    pdf_cruz = load_collection_as_pandas(COLLECTION_CRUZ_LIST)
    pdf_term = load_collection_as_pandas(COLLECTION_TERMINATED_GRANTS)
    pdf_leg = load_collection_as_pandas(COLLECTION_LEGISLATORS)  # 🔴 nova col·lecció

    if pdf_nsf.empty:
        print("NSF grants collection is empty, nothing to do.")
        sys.exit(1)

    pdf_nsf["award_id"] = pdf_nsf["award_id"].astype(str)

    # -----------------------------------------------------------------
    # 🔹 Cruz list & terminated flags
    # -----------------------------------------------------------------
    cruz_ids = set()
    if not pdf_cruz.empty and "award_id" in pdf_cruz.columns:
        cruz_ids = set(pdf_cruz["award_id"].astype(str).tolist())

    term_ids = set()
    if not pdf_term.empty and "grant_number" in pdf_term.columns:
        term_ids = set(pdf_term["grant_number"].astype(str).tolist())

    pdf_nsf["in_cruz_list"] = pdf_nsf["award_id"].isin(cruz_ids)
    pdf_nsf["is_terminated"] = pdf_nsf["award_id"].isin(term_ids)

    # -----------------------------------------------------------------
    # 🔹 Selecció de columnes de base
    # -----------------------------------------------------------------
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

    # -----------------------------------------------------------------
    # 🔹 Afegim la info de legislators (representant / senator)
    # -----------------------------------------------------------------
    # Preparem un DataFrame pla de termes i la funció de lookup
    df_terms = build_terms_dataframe(pdf_leg)
    lookup_party = make_party_lookup(df_terms)

    # Convertim startdate a datetime per poder fer el "entre start i end"
    pdf_feat["startdate_dt"] = pd.to_datetime(pdf_feat.get("startdate"), errors="coerce")

    # Columnes noves: representant (rep) i senator (sen)
    pdf_feat["representant"] = pdf_feat.apply(
        lambda row: lookup_party(
            row.get("perfstatecode"),
            row.get("startdate_dt"),
            "rep",
        ),
        axis=1,
    )

    pdf_feat["senator"] = pdf_feat.apply(
        lambda row: lookup_party(
            row.get("perfstatecode"),
            row.get("startdate_dt"),
            "sen",
        ),
        axis=1,
    )

    # -----------------------------------------------------------------
    # 🔹 Neteja i tipatge de columnes base
    # -----------------------------------------------------------------
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
        # Les columnes 'representant' i 'senator' es queden com a strings

    # -----------------------------------------------------------------
    # 🔹 Spark + Delta (train / test)
    # -----------------------------------------------------------------
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
