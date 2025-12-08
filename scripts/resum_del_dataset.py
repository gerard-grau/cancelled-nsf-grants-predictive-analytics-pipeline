from pymongo import MongoClient
import pandas as pd
import numpy as np
from datetime import date, datetime

from config import (
    MONGO_URI,
    MONGO_DB,
    COLLECTION_NSF_GRANTS,
    COLLECTION_TERMINATED_GRANTS,
    COLLECTION_CRUZ_LIST,
    COLLECTION_LEGISLATORS,
)


def infer_object_type(series: pd.Series) -> str:
    non_null = series.dropna()

    if len(non_null) == 0:
        return "empty / null"

    sample = non_null.iloc[0]

    if isinstance(sample, str):
        return "string"
    if isinstance(sample, bool):
        return "bool"
    if isinstance(sample, int):
        return "int"
    if isinstance(sample, float):
        return "float"
    if isinstance(sample, datetime):
        return "datetime"
    if isinstance(sample, date):
        return "date"
    if isinstance(sample, list):
        if len(sample) == 0:
            return "list[empty]"
        inner = sample[0]
        if isinstance(inner, str):
            return "list[string]"
        if isinstance(inner, dict):
            return "list[dict]"
        return f"list[{type(inner).__name__}]"
    if isinstance(sample, dict):
        return "dict"

    return type(sample).__name__


def print_schema(collection_name):
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    coll = db[collection_name]

    docs = list(coll.find({}, {"_id": False}))
    client.close()

    print("=" * 100)
    print(f"COLLECTION: {collection_name}")

    if not docs:
        print("❌ Empty collection")
        return

    df = pd.DataFrame(docs)
    print(f"Shape: {df.shape}\n")

    max_len = max(len(col) for col in df.columns)

    print("Columns:")
    for col in df.columns:
        dtype = df[col].dtype

        if dtype == "object":
            real_type = infer_object_type(df[col])
            print(f"  {col.ljust(max_len)}  -> object ({real_type})")
        else:
            print(f"  {col.ljust(max_len)}  -> {dtype}")


if __name__ == "__main__":
    print_schema(COLLECTION_NSF_GRANTS)
    print_schema(COLLECTION_TERMINATED_GRANTS)
    print_schema(COLLECTION_CRUZ_LIST)
    print_schema(COLLECTION_LEGISLATORS)
