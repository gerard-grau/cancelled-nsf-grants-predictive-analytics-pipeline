# terminated_data.py

from pathlib import Path
from typing import Union

import pandas as pd
from pyspark.sql import SparkSession


def create_spark(app_name: str = "TerminatedData") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    return spark


def read_terminated(file_path, spark: Union[SparkSession, None] = None):
    """
    Descarrega el CSV de terminated data de TidyTuesday
    i el desa localment. Si hi ha Spark, també crea Parquet.
    """
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)

    nsf_terminations = pd.read_csv(
        "https://raw.githubusercontent.com/rfordatascience/tidytuesday/main/data/2025/2025-05-06/nsf_terminations.csv"
    )
    nsf_terminations.to_csv(file_path, index=False)
    print(f"✓ Terminated data CSV saved to {file_path}")

    if spark is not None:
        df = spark.read.csv(str(file_path), header=True, inferSchema=True)
        parquet_path = file_path.with_suffix("")
        (
            df.write
            .mode("overwrite")
            .parquet(str(parquet_path))
        )
        print(f"✓ Terminated data also saved as Parquet to {parquet_path}")


def main():
    BASE_DIR = Path(__file__).parent.parent
    DATALAKE_DIR = BASE_DIR / "datalake"
    LANDING_DIR = DATALAKE_DIR / "landing"
    PATH_TERMINATED_DATA = LANDING_DIR / "terminated_data.csv"

    spark = create_spark()

    read_terminated(PATH_TERMINATED_DATA, spark=spark)
    print("Terminated data downloaded")

    spark.stop()


if __name__ == "__main__":
    main()
