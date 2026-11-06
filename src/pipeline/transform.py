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


def validate_data(df):
    """Validate incoming sales data before transformation."""

    print("\n========== DATA QUALITY CHECKS ==========")

    errors = []

    # 1. Check for null values
    critical_columns = [
        "order_id",
        "customer_id",
        "order_date",
        "product",
        "quantity",
        "unit_price",
        "region",
    ]

    print("\n1. Checking null values...")

    for column in critical_columns:
        null_count = df.filter(col(column).isNull()).count()

        if null_count > 0:
            errors.append(f"{column}: {null_count} null values found")
            print(f"   FAILED: {column} has {null_count} null values")
        else:
            print(f"   PASSED: {column}")

    # 2. Check duplicate order IDs
    print("\n2. Checking duplicate order IDs...")

    duplicate_count = df.groupBy("order_id").count().filter(col("count") > 1).count()

    if duplicate_count > 0:
        errors.append(f"{duplicate_count} duplicate order IDs found")
        print(f"   FAILED: {duplicate_count} duplicate order IDs")
    else:
        print("   PASSED: No duplicate order IDs")

    # 3. Check invalid quantities
    print("\n3. Checking quantities...")

    invalid_quantity = df.filter(col("quantity") <= 0).count()

    if invalid_quantity > 0:
        errors.append(f"{invalid_quantity} invalid quantities found")
        print(f"   FAILED: {invalid_quantity} invalid quantities")
    else:
        print("   PASSED: All quantities are valid")

    # 4. Check invalid prices
    print("\n4. Checking unit prices...")

    invalid_price = df.filter(col("unit_price") < 0).count()

    if invalid_price > 0:
        errors.append(f"{invalid_price} negative unit prices found")
        print(f"   FAILED: {invalid_price} negative prices")
    else:
        print("   PASSED: All unit prices are valid")

    # 5. Final validation result
    print("\n==========================================")

    if errors:
        print("\nDATA QUALITY VALIDATION FAILED!")

        for error in errors:
            print(f"- {error}")

        raise ValueError("Pipeline stopped due to data quality issues.")

    print("\nALL DATA QUALITY CHECKS PASSED!")
    print("==========================================\n")


def run_pipeline():

    spark = (
        SparkSession.builder.master("local[*]").appName("PipeSage-ETL").getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("Starting sales ETL pipeline...")

        # 1. EXTRACT
        sales_df = (
            spark.read.option("header", True)
            .option("dateFormat", "yyyy-MM-dd")
            .schema(SALES_SCHEMA)
            .csv(str(INPUT_FILE))
        )

        print(f"\nExtracted records: {sales_df.count()}")

        # 2. VALIDATE
        validate_data(sales_df)

        # 3. TRANSFORM
        print("Starting data transformation...")

        transformed_df = sales_df.withColumn(
            "total_amount", col("quantity") * col("unit_price")
        )

        # 4. LOAD
        print("Loading transformed data...")

        transformed_df.write.mode("overwrite").parquet(str(OUTPUT_DIR))

        print(f"\nPipeline completed successfully!")
        print(f"Output location: {OUTPUT_DIR}")

        transformed_df.show(truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    run_pipeline()
