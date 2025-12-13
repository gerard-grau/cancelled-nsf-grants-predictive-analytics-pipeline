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
    COLLECTION_LEGISLATORS,
    COLLECTION_FLAGGED_WORDS,
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


def compute_flagged_word_features(pdf_nsf: pd.DataFrame, flagged_words_set: set) -> pd.DataFrame:
    """
    Compute text analysis features based on flagged words in abstract and title.
    Returns a DataFrame with 6 new columns:
    - flagged_word_count_abstract: Total occurrences in abstract
    - flagged_word_unique_abstract: Number of distinct flagged words in abstract
    - flagged_word_ratio_abstract: flagged_count / total_word_count in abstract
    - flagged_word_count_title: Total occurrences in title
    - has_flagged_words: Boolean indicator (any flagged word present)
    - flagged_word_diversity_score: unique / count (0 if count=0)
    """
    import re
    
    def analyze_text(text: str, flagged_words: set) -> tuple:
        """Returns (count, unique_count, ratio)"""
        if pd.isna(text) or not text:
            return (0, 0, 0.0)
        
        # Normalize to lowercase
        text_lower = text.lower()
        
        # Count total words
        words = re.findall(r'\b\w+\b', text_lower)
        total_words = len(words)
        
        if total_words == 0:
            return (0, 0, 0.0)
        
        # Find flagged words with word boundaries
        flagged_found = []
        for word in words:
            if word in flagged_words:
                flagged_found.append(word)
        
        count = len(flagged_found)
        unique_count = len(set(flagged_found))
        ratio = count / total_words if total_words > 0 else 0.0
        
        return (count, unique_count, ratio)
    
    # Extract abstract and title columns
    abstracts = pdf_nsf.get('abstract', pd.Series([None] * len(pdf_nsf)))
    titles = pdf_nsf.get('title', pd.Series([None] * len(pdf_nsf)))
    
    # Analyze abstract
    abstract_results = abstracts.apply(lambda x: analyze_text(x, flagged_words_set))
    pdf_nsf['flagged_word_count_abstract'] = abstract_results.apply(lambda x: x[0])
    pdf_nsf['flagged_word_unique_abstract'] = abstract_results.apply(lambda x: x[1])
    pdf_nsf['flagged_word_ratio_abstract'] = abstract_results.apply(lambda x: x[2])
    
    # Analyze title
    title_results = titles.apply(lambda x: analyze_text(x, flagged_words_set))
    pdf_nsf['flagged_word_count_title'] = title_results.apply(lambda x: x[0])
    
    # Boolean indicator
    pdf_nsf['has_flagged_words'] = (
        (pdf_nsf['flagged_word_count_abstract'] > 0) | 
        (pdf_nsf['flagged_word_count_title'] > 0)
    )
    
    # Diversity score
    total_count = pdf_nsf['flagged_word_count_abstract'] + pdf_nsf['flagged_word_count_title']
    total_unique = pdf_nsf['flagged_word_unique_abstract']  # We only track unique in abstract
    pdf_nsf['flagged_word_diversity_score'] = total_unique / total_count.replace(0, 1)
    pdf_nsf.loc[total_count == 0, 'flagged_word_diversity_score'] = 0.0
    
    return pdf_nsf


def main(train_ratio: float = 0.8, seed: int = 42):
    EXPLOITATION_DIR.mkdir(parents=True, exist_ok=True)

    # -----------------------------------------------------------------
    # 🔹 Carreguem les col·leccions de Mongo
    # -----------------------------------------------------------------
    pdf_nsf = load_collection_as_pandas(COLLECTION_NSF_GRANTS)
    pdf_cruz = load_collection_as_pandas(COLLECTION_CRUZ_LIST)
    pdf_term = load_collection_as_pandas(COLLECTION_TERMINATED_GRANTS)
    pdf_leg = load_collection_as_pandas(COLLECTION_LEGISLATORS)
    pdf_flagged = load_collection_as_pandas(COLLECTION_FLAGGED_WORDS)

    if pdf_nsf.empty:
        print("NSF grants collection is empty, nothing to do.")
        sys.exit(1)
    
    # -----------------------------------------------------------------
    # 🔹 Prepare flagged words set for text analysis
    # -----------------------------------------------------------------
    flagged_words_set = set()
    if not pdf_flagged.empty and "flagged_word" in pdf_flagged.columns:
        flagged_words_set = set(pdf_flagged["flagged_word"].dropna().tolist())
        print(f"✅ Loaded {len(flagged_words_set)} flagged words for text analysis")

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
    # 🔹 Compute flagged word features
    # -----------------------------------------------------------------
    if flagged_words_set:
        print("🔍 Computing flagged word features...")
        pdf_feat = compute_flagged_word_features(pdf_feat, flagged_words_set)
        print(f"   ✓ Added 6 flagged word features")
        print(f"   ✓ Grants with flagged words: {pdf_feat['has_flagged_words'].sum()} / {len(pdf_feat)}")
    else:
        print("⚠️  No flagged words loaded, skipping text analysis features")

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

    df_train.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("year").save(str(TRAIN_DELTA_DIR))
    df_test.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("year").save(str(TEST_DELTA_DIR))

    print(f"Train Delta saved at: {TRAIN_DELTA_DIR}")
    print(f"Test Delta saved at:  {TEST_DELTA_DIR}")

    spark.stop()


if __name__ == "__main__":
    main()
