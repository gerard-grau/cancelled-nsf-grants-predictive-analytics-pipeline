# cruz_list.py

import requests
from pathlib import Path
from typing import Union
from io import BytesIO

import pandas as pd
from pyspark.sql import SparkSession


TIMEOUT = 40


def create_spark(app_name: str = "CruzListDownload") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    return spark


def download_cruz_list(file_path, spark: Union[SparkSession, None] = None):
    """
    Descarrega la 'Cruz list' (Excel XML), la converteix a CSV
    i opcionalment a Parquet si hi ha Spark.
    """
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    url = "https://www.commerce.senate.gov/index.cfm?a=files.serve&File_id=94060590-F32F-4944-8810-300E6766B1D6"
    print("⬇️  Downloading Cruz list (Excel XML)")
    r = requests.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    content = r.content

    df_pd = pd.read_excel(BytesIO(content))
    df_pd.to_csv(file_path, index=False)
    print(f"   ✓ Cruz list saved to {file_path}")

    if spark is not None:
        df = spark.read.csv(str(file_path), header=True, inferSchema=True)
        parquet_path = file_path.with_suffix("")
        (
            df.write
            .mode("overwrite")
            .parquet(str(parquet_path))
        )
        print(f"   ✓ Cruz list also saved as Parquet to {parquet_path}")


def main():
    BASE_DIR = Path(__file__).parent.parent
    DATALAKE_DIR = BASE_DIR / "datalake"
    LANDING_DIR = DATALAKE_DIR / "landing"
    PATH_CANCELLED = LANDING_DIR / "cruz_list.csv"

    spark = create_spark()

    download_cruz_list(PATH_CANCELLED, spark=spark)
    print("Downloaded Cruz list")

    spark.stop()


if __name__ == "__main__":
    main()
