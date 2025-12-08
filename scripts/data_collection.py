import requests
import json
import time
import os
from pathlib import Path
from typing import Union, Dict, List
from io import BytesIO # Added missing import

import pandas as pd
from pyspark.sql import SparkSession, functions as F


BASE_URL = "https://api.nsf.gov/services/v1/awards.json"
MAX_RETRIES = 3
TIMEOUT = 40
DEFAULT_RPP = 25
DEFAULT_PAUSE = 0.05



def create_spark(app_name: str = "NSFDataCollectors") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    return spark



def fetch_awards_page(
    year: int,
    offset: int,
    rpp: int = DEFAULT_RPP,
    # Fix: Changed 'dict | None' to 'Union[Dict, None]' for Python < 3.10 compatibility
    extra_params: Union[Dict, None] = None,
    max_retries: int = MAX_RETRIES,
    timeout: int = TIMEOUT,
) -> Union[Dict, None]:
    """
    Crida una pàgina de l'API de l'NSF per a un any i offset concrets.
    Retorna el JSON o None si finalment falla.
    """
    params = {
        "rpp": rpp,
        "offset": offset,
        "startDateStart": f"01/01/{year}",
        "startDateEnd": f"12/31/{year}",
    }

    if extra_params:
        params.update(extra_params)

    for attempt in range(1, max_retries + 1):
        try:
            print(f"⬇️  [Year {year} offset {offset} attempt {attempt}] Fetching from API")
            r = requests.get(BASE_URL, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"⚠️  [Year {year} offset {offset} attempt {attempt}] {e}")
            time.sleep(2 * attempt)

    print(f"❌  Skipping [Year {year} offset {offset}] after {max_retries} retries.")
    return None


def get_time_data(
    start_year: int,
    end_year: int,
    output_dir,
    spark: SparkSession,
    rpp: int = DEFAULT_RPP,
    pause: float = DEFAULT_PAUSE,
    # Fix: Changed 'dict | None' to 'Union[Dict, None]'
    extra_params: Union[Dict, None] = None,
    overwrite: bool = False,
) -> int:
    """
    Baixa totes les concessions NSF entre start_year i end_year (inclosos),
    any per any, via API.

    Per a cada any:
      - Paginació via 'offset'.
      - Es recullen tots els 'award' en una llista plana.
      - Es desa un fitxer JSON: <output_dir>/<year>.json
        amb el format: [ {award1}, {award2}, ... ] (un objecte per award).
      - També s'escriu en un directori Spark particionat per any.
    """

    if end_year < start_year:
        raise ValueError("end_year must be >= start_year")

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    spark_output_dir = output_dir / "nsf_awards_spark"
    spark_output_dir.mkdir(parents=True, exist_ok=True)

    total_awards = 0

    for year in range(start_year, end_year + 1):
        year_file = output_dir / f"{year}.json"

        if overwrite and year_file.exists():
            os.remove(year_file)
            print(f"🧹 Deleted old file for year {year}: {year_file}")

        print(f"\n📆 Processing year {year}")

        # Fix: Changed 'list[dict]' to 'List[Dict]'
        awards_year: List[Dict] = []
        offset = 0

        while True:
            response_json = fetch_awards_page(
                year=year,
                offset=offset,
                rpp=rpp,
                extra_params=extra_params,
                max_retries=MAX_RETRIES,
                timeout=TIMEOUT,
            )

            if not response_json:
                print(f"✅ Finished / skipped year {year} (no response).")
                break

            resp = response_json.get("response", {})

            if "serviceNotification" in resp:
                print(f"⚠️  API error for year {year}, offset {offset}: {resp['serviceNotification']}")
                break

            awards_page = resp.get("award", [])
            if not awards_page:
                print(f"✅ Finished year {year} (no more awards).")
                break

            awards_year.extend(awards_page)

            count_page = len(awards_page)
            print(f"   ✓ Fetched page at offset {offset} with {count_page} awards")

            if count_page < rpp:
                print(f"🎯 End of results for year {year}.")
                break

            offset += rpp
            time.sleep(pause)

        if not awards_year:
            print(f"✅ No awards parsed for year {year}.")
            continue

        with year_file.open("w", encoding="utf-8") as f_out:
            json.dump(awards_year, f_out, ensure_ascii=False)

        df_year = spark.createDataFrame(awards_year)
        df_year = df_year.withColumn("year", F.lit(year))

        (
            df_year
            .write
            .mode("overwrite" if overwrite else "append")
            .partitionBy("year")
            .json(str(spark_output_dir))
        )

        count_year = len(awards_year)
        total_awards += count_year
        print(f"   ✓ Saved {count_year} awards for year {year} → {year_file}")
        print(f"   ✓ Written to Spark Landing (partition year={year})")

    print("\nCompleted.")
    print(f"Total awards saved across all years: {total_awards}")
    print(f"Local JSON directory (per year): {output_dir}")
    print(f"Spark JSON directory (Landing Zone): {spark_output_dir}")

    return total_awards



def read_terminated(file_path, spark: Union[SparkSession, None] = None): # Fix: Changed 'SparkSession | None' to 'Union[SparkSession, None]'
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


def download_legislators(file_path, spark: Union[SparkSession, None] = None): # Fix: Changed 'SparkSession | None' to 'Union[SparkSession, None]'
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    url = "https://unitedstates.github.io/congress-legislators/legislators-historical.json"
    print("⬇️  Downloading legislators JSON")
    r = requests.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()
    with file_path.open("w", encoding="utf-8") as f_out:
        json.dump(data, f_out, ensure_ascii=False)
    print(f"   ✓ Legislators saved to {file_path}")

    if spark is not None:
        df = spark.read.json(str(file_path))
        parquet_path = file_path.with_suffix("")
        (
            df.write
            .mode("overwrite")
            .parquet(str(parquet_path))
        )
        print(f"   ✓ Legislators also saved as Parquet to {parquet_path}")


def download_cruz_list(file_path, spark: Union[SparkSession, None] = None): # Fix: Changed 'SparkSession | None' to 'Union[SparkSession, None]'
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    url = "https://www.commerce.senate.gov/index.cfm?a=files.serve&File_id=94060590-F32F-4944-8810-300E6766B1D6"
    print("⬇️  Downloading Cruz list (Excel XML)")
    r = requests.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    content = r.content

    # The original code had the import inside the function: `from io import BytesIO`
    # I moved the import to the top of the file.

    df_pd = pd.read_excel(BytesIO(content))
    df_pd.to_csv(file_path, index=False)
    print(f"   ✓ Cruz list saved to {file_path}")

    if spark is not None:
        df = spark.read.csv(str(file_path), header=True, inferSchema=True)
        parquet_path = file_path.with_suffix("")
        (
            df.write
            .mode("overwrite")
            .parquet(str(parquet_path))
        )
        print(f"   ✓ Cruz list also saved as Parquet to {parquet_path}")



if __name__ == "__main__":
    BASE_DIR = Path(__file__).parent.parent
    DATALAKE_DIR = BASE_DIR / "datalake"
    LANDING_DIR = DATALAKE_DIR / "landing"
    NSF_DIR = LANDING_DIR / "nsf_grants"

    PATH_LEGISLATORS = LANDING_DIR / "legislators.json"
    PATH_CANCELLED = LANDING_DIR / "cruz_list.csv"
    PATH_TERMINATED_DATA = LANDING_DIR / "terminated_data.csv"

    start_year = 2022
    end_year = 2025

    spark = create_spark()

    read_terminated(PATH_TERMINATED_DATA, spark=spark)
    print("Terminated data downloaded")

    download_legislators(PATH_LEGISLATORS, spark=spark)
    print("Downloaded legislators")

    download_cruz_list(PATH_CANCELLED, spark=spark)
    print("Downloaded Cruz list")

    total = get_time_data(
        start_year=start_year,
        end_year=end_year,
        output_dir=NSF_DIR,
        spark=spark,
        overwrite=True,
    )
    print(f"Total awards downloaded: {total}")

    spark.stop()