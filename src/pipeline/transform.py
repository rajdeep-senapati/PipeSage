from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DateType,
    DoubleType,
)

BASE_DIR = Path(__file__).resolve().parents[2]
INPUT_FILE = BASE_DIR / "data" / "sales.csv"
OUTPUT_DIR = BASE_DIR / "data" / "processed_sales"

SALES_SCHEMA = StructType(
    [
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("order_date", DateType(), True),
        StructField("product", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("region", StringType(), True),
    ]
)


def run_pipeline():
    spark = (
        SparkSession.builder.master("local[*]").appName("PipeSage-ETL").getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("Starting sales ETL pipeline...")

        # 1. Extract
        sales_df = (
            spark.read.option("header", True)
            .option("dateFormat", "yyyy-MM-dd")
            .schema(SALES_SCHEMA)
            .csv(str(INPUT_FILE))
        )

        print(f"Extracted records: {sales_df.count()}")

        # 2. Transform
        transformed_df = sales_df.withColumn(
            "total_amount", col("quantity") * col("unit_price")
        )

        # 3. Load
        (transformed_df.write.mode("overwrite").parquet(str(OUTPUT_DIR)))

        print(f"Pipeline completed. Output: {OUTPUT_DIR}")

        transformed_df.show(truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    run_pipeline()
