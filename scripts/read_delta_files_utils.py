from pathlib import Path
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).parent.parent
DATALAKE_DIR = BASE_DIR / "datalake"
EXPLOITATION_DIR = DATALAKE_DIR / "exploitation"
TRAIN_DELTA_DIR = EXPLOITATION_DIR / "grants_train_delta"
TEST_DELTA_DIR = EXPLOITATION_DIR / "grants_test_delta"


def create_spark_with_delta():
    builder = (
        SparkSession.builder.appName("ML Training")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def load_train(spark):
    df_train = spark.read.format("delta").load(str(TRAIN_DELTA_DIR))
    return df_train

def load_test(spark):
    df_test = spark.read.format("delta").load(str(TEST_DELTA_DIR))
    return df_test


if __name__ == "__main__":
    spark = create_spark_with_delta()
    df_train = load_train(spark)
    df_test = load_test(spark)
    print(df_train.columns)