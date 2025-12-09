# legislators.py

import requests
import json
from pathlib import Path
from typing import Union

from pyspark.sql import SparkSession


TIMEOUT = 40


def create_spark(app_name: str = "LegislatorsDownload") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    return spark


def download_legislators(file_path, spark: Union[SparkSession, None] = None):
    """
    Descarrega legislators-historical.json i el desa localment.
    Si hi ha Spark, també crea Parquet.
    """
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    url = "https://unitedstates.github.io/congress-legislators/legislators-historical.json"
    print("⬇️  Downloading legislators JSON")
    r = requests.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()
    with file_path.open("w", encoding="utf-8") as f_out:
        json.dump(data, f_out, ensure_ascii=False)
    print(f"   ✓ Legislators saved to {file_path}")

    if spark is not None:
        df = spark.read.json(str(file_path))
        parquet_path = file_path.with_suffix("")
        (
            df.write
            .mode("overwrite")
            .parquet(str(parquet_path))
        )
        print(f"   ✓ Legislators also saved as Parquet to {parquet_path}")


def main():
    BASE_DIR = Path(__file__).parent.parent
    DATALAKE_DIR = BASE_DIR / "datalake"
    LANDING_DIR = DATALAKE_DIR / "landing"
    PATH_LEGISLATORS = LANDING_DIR / "legislators.json"

    spark = create_spark()

    download_legislators(PATH_LEGISLATORS, spark=spark)
    print("Downloaded legislators")

    spark.stop()


if __name__ == "__main__":
    main()
