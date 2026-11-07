import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as spark_sum,
    when,
)
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
SIMULATED_DIR = BASE_DIR / "data" / "simulated"

SCENARIOS = [
    "clean",
    "null_customer",
    "duplicate_order",
    "zero_quantity",
    "negative_price",
    "invalid_date",
]

SALES_SCHEMA = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", DateType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("region", StringType(), True),
])


def validate_data(df):
    """Validate incoming sales data using a single Spark aggregation."""

    print("\n========== DATA QUALITY CHECKS ==========")

    critical_columns = [
        "order_id", "customer_id", "order_date",
        "product", "quantity", "unit_price", "region",
    ]

    validation_expressions = []

    for column in critical_columns:
        validation_expressions.append(
            spark_sum(
                when(col(column).isNull(), 1).otherwise(0)
            ).alias(f"null_{column}")
        )

    validation_expressions.extend([
        count("order_id").alias("total_order_ids"),
        countDistinct("order_id").alias("distinct_order_ids"),
        spark_sum(
            when(col("quantity") <= 0, 1).otherwise(0)
        ).alias("invalid_quantities"),
        spark_sum(
            when(col("unit_price") < 0, 1).otherwise(0)
        ).alias("negative_prices"),
    ])

    metrics = df.agg(*validation_expressions).first()
    errors = []

    print("\n1. Checking null values...")

    for column in critical_columns:
        null_count = metrics[f"null_{column}"] or 0

        if null_count > 0:
            errors.append(f"{column}: {null_count} null values found")
            print(f"   FAILED: {column} has {null_count} null values")
        else:
            print(f"   PASSED: {column}")

    print("\n2. Checking duplicate order IDs...")

    duplicate_count = (
        metrics["total_order_ids"] - metrics["distinct_order_ids"]
    )

    if duplicate_count > 0:
        errors.append(f"{duplicate_count} duplicate order ID occurrences found")
        print(f"   FAILED: {duplicate_count} duplicate occurrences")
    else:
        print("   PASSED: No duplicate order IDs")

    print("\n3. Checking quantities...")

    invalid_quantity = metrics["invalid_quantities"] or 0

    if invalid_quantity > 0:
        errors.append(f"{invalid_quantity} invalid quantities found")
        print(f"   FAILED: {invalid_quantity} invalid quantities")
    else:
        print("   PASSED: All quantities are valid")

    print("\n4. Checking unit prices...")

    invalid_price = metrics["negative_prices"] or 0

    if invalid_price > 0:
        errors.append(f"{invalid_price} negative unit prices found")
        print(f"   FAILED: {invalid_price} negative prices")
    else:
        print("   PASSED: All unit prices are valid")

    print("\n==========================================")

    if errors:
        print("\nDATA QUALITY VALIDATION FAILED!")

        for error in errors:
            print(f"- {error}")

        raise ValueError("Pipeline stopped due to data quality issues.")

    print("\nALL DATA QUALITY CHECKS PASSED!")
    print("==========================================\n")


def run_pipeline(scenario="clean"):

    input_file = SIMULATED_DIR / scenario / "sales.csv"
    output_dir = OUTPUT_DIR / scenario

    if not input_file.exists():
        raise FileNotFoundError(f"Dataset not found: {input_file}")

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("PipeSage-ETL")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        print(f"Starting sales ETL pipeline | Scenario: {scenario}")

        # 1. EXTRACT
        sales_df = (
            spark.read.option("header", True)
            .option("dateFormat", "yyyy-MM-dd")
            .schema(SALES_SCHEMA)
            .csv(str(input_file))
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

        transformed_df.write.mode("overwrite").parquet(str(output_dir))

        print("\nPipeline completed successfully!")
        print(f"Output location: {output_dir}")

        transformed_df.show(truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PipeSage ETL Pipeline")
    parser.add_argument(
        "--scenario",
        choices=SCENARIOS,
        default="clean",
    )

    args = parser.parse_args()
    run_pipeline(args.scenario)