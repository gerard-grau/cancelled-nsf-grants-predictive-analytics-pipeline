# nsf_awards.py

import requests
import json
import time
import os
from pathlib import Path
from typing import Union, Dict, List

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
            print(f"⬇️  [Year {year} offset {offset} attempt {attempt}] Fetching from API")
            r = requests.get(BASE_URL, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"⚠️  [Year {year} offset {offset} attempt {attempt}] {e}")
            time.sleep(2 * attempt)

    print(f"❌  Skipping [Year {year} offset {offset}] after {max_retries} retries.")
    return None


def get_time_data(
    start_year: int,
    end_year: int,
    output_dir,
    spark: SparkSession,
    rpp: int = DEFAULT_RPP,
    pause: float = DEFAULT_PAUSE,
    extra_params: Union[Dict, None] = None,
    overwrite: bool = False,
) -> int:
    """
    Baixa totes les concessions NSF entre start_year i end_year (inclosos),
    any per any, via API.
    Desa:
      - JSON per any: <output_dir>/<year>.json
      - Directori Spark particionat per any: <output_dir>/nsf_awards_spark
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
                print(f"⚠️  API error for year {year}, offset {offset}: {resp['serviceNotification']}")
                break

            awards_page = resp.get("award", [])
            if not awards_page:
                print(f"✅ Finished year {year} (no more awards).")
                break

            awards_year.extend(awards_page)

            count_page = len(awards_page)
            print(f"   ✓ Fetched page at offset {offset} with {count_page} awards")

            if count_page < rpp:
                print(f"🎯 End of results for year {year}.")
                break

            offset += rpp
            time.sleep(pause)

        if not awards_year:
            print(f"✅ No awards parsed for year {year}.")
            continue

        # JSON local per any
        with year_file.open("w", encoding="utf-8") as f_out:
            json.dump(awards_year, f_out, ensure_ascii=False)

        # Escriure a Spark (Landing) particionat per any
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
        print(f"   ✓ Saved {count_year} awards for year {year} → {year_file}")
        print(f"   ✓ Written to Spark Landing (partition year={year})")

    print("\nCompleted.")
    print(f"Total awards saved across all years: {total_awards}")
    print(f"Local JSON directory (per year): {output_dir}")
    print(f"Spark JSON directory (Landing Zone): {spark_output_dir}")

    return total_awards


def main():
    BASE_DIR = Path(__file__).parent.parent
    DATALAKE_DIR = BASE_DIR / "datalake"
    LANDING_DIR = DATALAKE_DIR / "landing"
    NSF_DIR = LANDING_DIR / "nsf_grants"

    start_year = 2022
    end_year = 2025

    spark = create_spark("NSF_Awards_Download")

    total = get_time_data(
        start_year=start_year,
        end_year=end_year,
        output_dir=NSF_DIR,
        spark=spark,
        overwrite=True,
    )
    print(f"Total awards downloaded: {total}")

    spark.stop()


if __name__ == "__main__":
    main()
