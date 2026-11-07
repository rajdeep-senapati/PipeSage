
import argparse
import logging
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
    "negative_quantity",
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

logger = logging.getLogger(__name__)


def configure_logging():
    """Configure application-wide logging."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def validate_data(df):
    """Validate incoming sales data using a single Spark aggregation."""

    logger.info("Starting data quality validation")

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

    logger.info("Checking null values")

    for column in critical_columns:
        null_count = metrics[f"null_{column}"] or 0

        if null_count > 0:
            errors.append(f"{column}: {null_count} null values found")
            logger.warning("%s has %s null values", column, null_count)
        else:
            logger.info("Null check passed: %s", column)

    logger.info("Checking duplicate order IDs")

    duplicate_count = (
        metrics["total_order_ids"] - metrics["distinct_order_ids"]
    )

    if duplicate_count > 0:
        errors.append(
            f"{duplicate_count} duplicate order ID occurrences found"
        )
        logger.warning("%s duplicate order ID occurrences found", duplicate_count)
    else:
        logger.info("Duplicate order ID check passed")

    logger.info("Checking quantities")

    invalid_quantity = metrics["invalid_quantities"] or 0

    if invalid_quantity > 0:
        errors.append(f"{invalid_quantity} invalid quantities found")
        logger.warning("%s invalid quantities found", invalid_quantity)
    else:
        logger.info("Quantity check passed")

    logger.info("Checking unit prices")

    invalid_price = metrics["negative_prices"] or 0

    if invalid_price > 0:
        errors.append(f"{invalid_price} negative unit prices found")
        logger.warning("%s negative unit prices found", invalid_price)
    else:
        logger.info("Unit price check passed")

    if errors:
        logger.error("Data quality validation failed")

        for error in errors:
            logger.error("Validation issue: %s", error)

        raise ValueError("Pipeline stopped due to data quality issues.")

    logger.info("All data quality checks passed")


def run_pipeline(scenario="clean", input_path=None, output_path=None):

    input_file = (
        Path(input_path)
        if input_path
        else SIMULATED_DIR / scenario / "sales.csv"
    )

    output_dir = (
        Path(output_path)
        if output_path
        else OUTPUT_DIR / scenario
    )

    logger.info("Starting PipeSage ETL pipeline | Scenario: %s", scenario)

    if not input_file.exists():
        logger.error("Dataset not found: %s", input_file)
        raise FileNotFoundError(f"Dataset not found: {input_file}")

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("PipeSage-ETL")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # 1. EXTRACT
        logger.info("Extracting data from %s", input_file)

        sales_df = (
            spark.read.option("header", True)
            .option("dateFormat", "yyyy-MM-dd")
            .schema(SALES_SCHEMA)
            .csv(str(input_file))
        )

        record_count = sales_df.count()
        logger.info("Extraction completed | Records: %s", record_count)

        # 2. VALIDATE
        validate_data(sales_df)

        # 3. TRANSFORM
        logger.info("Starting data transformation")

        transformed_df = sales_df.withColumn(
            "total_amount", col("quantity") * col("unit_price")
        )

        logger.info("Transformation completed")

        # 4. LOAD
        logger.info("Loading transformed data to %s", output_dir)

        transformed_df.write.mode("overwrite").parquet(str(output_dir))

        logger.info("Pipeline completed successfully")
        logger.info("Output location: %s", output_dir)

        transformed_df.show(truncate=False)

    except Exception:
        logger.exception("Pipeline execution failed")
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    configure_logging()

    parser = argparse.ArgumentParser(description="PipeSage ETL Pipeline")
    parser.add_argument(
        "--scenario",
        choices=SCENARIOS,
        default="clean",
    )

    parser.add_argument(
    "--input",
    type=Path,
    help="Custom input CSV file path",
    )

    parser.add_argument(
    "--output",
    type=Path,
    help="Custom output directory path",
    )

    args = parser.parse_args()
    run_pipeline(
    scenario=args.scenario,
    input_path=args.input,
    output_path=args.output,
    )
