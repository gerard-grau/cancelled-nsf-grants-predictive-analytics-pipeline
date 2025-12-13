# collect_flagged_words.py

import shutil
from pathlib import Path
from typing import Union

from pyspark.sql import SparkSession


def create_spark(app_name: str = "FlaggedWordsDownload") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    return spark


def collect_flagged_words(source_path, landing_path, spark: Union[SparkSession, None] = None):
    """
    Copy flagged words CSV from raw-data to landing zone
    and convert to Parquet if Spark is available.
    """
    source_path = Path(source_path)
    landing_path = Path(landing_path)
    landing_path.parent.mkdir(parents=True, exist_ok=True)

    # Copy CSV to landing zone
    shutil.copy2(source_path, landing_path)
    print(f"✓ Flagged words copied to {landing_path}")

    # Convert to Parquet for consistency with other datasets
    if spark is not None:
        df = spark.read.csv(str(landing_path), header=True, inferSchema=True)
        parquet_path = landing_path.with_suffix("")
        (
            df.write
            .mode("overwrite")
            .parquet(str(parquet_path))
        )
        print(f"✓ Flagged words also saved as Parquet to {parquet_path}")


def main():
    BASE_DIR = Path(__file__).parent.parent
    RAW_DATA_DIR = BASE_DIR / "raw-data"
    DATALAKE_DIR = BASE_DIR / "datalake"
    LANDING_DIR = DATALAKE_DIR / "landing"
    
    SOURCE_PATH = RAW_DATA_DIR / "flagged_words_trump_admin.csv"
    LANDING_PATH = LANDING_DIR / "flagged_words.csv"

    spark = create_spark()

    collect_flagged_words(SOURCE_PATH, LANDING_PATH, spark=spark)
    print("✅ Flagged words collected successfully")

    spark.stop()


if __name__ == "__main__":
    main()
