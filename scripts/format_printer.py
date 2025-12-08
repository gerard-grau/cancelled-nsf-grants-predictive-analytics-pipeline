from pyspark.sql import SparkSession
from config import (
    LANDING_NSF_GRANTS,
    LANDING_TERMINATED,
    LANDING_CRUZ_LIST,
    LANDING_LEGISLATORS,
)

spark = SparkSession.builder.appName("format_probe").getOrCreate()


def probe_df(name, df):
    print("\n" + "=" * 80)
    print(f"DATASET: {name}")
    print("=" * 80)

    print("\nSchema:")
    df.printSchema()

    print("\nSpark dtypes:")
    for c, t in df.dtypes:
        print(f"  {c:<40} {t}")

    print("\nSample rows:")
    df.show(3, truncate=False)

    print("\nNon-scalar columns (array / struct / map):")
    for field in df.schema.fields:
        if field.dataType.typeName() in ("array", "struct", "map"):
            print(f"  {field.name}: {field.dataType}")

    print("\nRow count:", df.count())


print("\n--- NSF GRANTS ---")
probe_df(
    "NSF_GRANTS",
    spark.read.json(str(LANDING_NSF_GRANTS))
)

print("\n--- TERMINATED GRANTS ---")
probe_df(
    "TERMINATED_GRANTS",
    spark.read.parquet(str(LANDING_TERMINATED))
)

print("\n--- CRUZ LIST ---")
probe_df(
    "CRUZ_LIST",
    spark.read.parquet(str(LANDING_CRUZ_LIST))
)

print("\n--- LEGISLATORS ---")
probe_df(
    "LEGISLATORS",
    spark.read.parquet(str(LANDING_LEGISLATORS))
)

spark.stop()
