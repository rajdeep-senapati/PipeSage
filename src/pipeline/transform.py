from datetime import datetime, timezone
import argparse
import logging
from pathlib import Path
import time
import json

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
VALIDATION_CONFIG = BASE_DIR / "config" / "validation_rules.json"

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


def evaluate_data_quality(df):
    """Evaluate configured data-quality rules and return structured results."""

    logger.info("Starting config-driven data quality validation")

    if not VALIDATION_CONFIG.exists():
        raise FileNotFoundError(
            f"Validation configuration not found: {VALIDATION_CONFIG}"
        )

    try:
        with open(VALIDATION_CONFIG, "r", encoding="utf-8") as file:
            config = json.load(file)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Invalid validation configuration: {exc}"
        ) from exc

    if not isinstance(config, dict):
        raise ValueError("Validation configuration must be a JSON object.")

    rules = config.get("rules")

    if not isinstance(rules, list):
        raise ValueError(
            "Validation configuration must contain a rules list."
        )

    supported_checks = {
        "not_null",
        "unique",
        "positive",
        "non_negative",
    }

    validated_rules = []

    # Validate the complete configuration before using Spark.
    for rule in rules:
        if not isinstance(rule, dict):
            raise ValueError(f"Invalid validation rule: {rule}")

        column = rule.get("column")
        checks = rule.get("checks")

        if not isinstance(column, str) or not column:
            raise ValueError(
                f"Invalid column in validation rule: {rule}"
            )

        if not isinstance(checks, list):
            raise ValueError(
                f"Invalid checks for column: {column}"
            )

        if column not in df.columns:
            raise ValueError(
                f"Validation column does not exist: {column}"
            )

        for check in checks:
            if not isinstance(check, str) or check not in supported_checks:
                raise ValueError(
                    f"Unsupported validation check: {check}"
                )

        validated_rules.append((column, checks))

    expressions = [
        count("*").alias("_total_records")
    ]
    rule_metadata = []

    for index, (column, checks) in enumerate(validated_rules):
        for check_index, check in enumerate(checks):
            alias = f"rule_{index}_{check_index}"
            column_expr = col(column)

            if check == "not_null":
                expressions.append(
                    count(
                        when(column_expr.isNull(), 1)
                    ).alias(alias)
                )

            elif check == "unique":
                expressions.extend([
                    count(column_expr).alias(f"{alias}_count"),
                    countDistinct(column_expr).alias(
                        f"{alias}_distinct"
                    ),
                ])

            elif check == "positive":
                expressions.append(
                    count(
                        when(column_expr <= 0, 1)
                    ).alias(alias)
                )

            elif check == "non_negative":
                expressions.append(
                    count(
                        when(column_expr < 0, 1)
                    ).alias(alias)
                )

            rule_metadata.append((alias, column, check))

    # Execute all checks in one Spark aggregation.
    results = df.agg(*expressions).first().asDict()

    rule_results = []
    passed_rules = []
    failed_rules = []

    for alias, column, check in rule_metadata:
        if check == "unique":
            invalid_count = (
                results[f"{alias}_count"]
                - results[f"{alias}_distinct"]
            )
        else:
            invalid_count = results[alias]

        rule_result = {
            "column": column,
            "check": check,
            "status": "passed" if invalid_count == 0 else "failed",
            "invalid_count": invalid_count,
        }

        rule_results.append(rule_result)

        if invalid_count > 0:
            failed_rules.append(rule_result)
            logger.warning(
                "Validation failed | Column: %s | Rule: %s | Count: %s",
                column,
                check,
                invalid_count,
            )
        else:
            passed_rules.append(rule_result)
            logger.info(
                "Validation passed | Column: %s | Rule: %s",
                column,
                check,
            )

    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_records": results["_total_records"],
        "overall_status": "passed" if not failed_rules else "failed",
        "total_rules": len(rule_results),
        "passed_rule_count": len(passed_rules),
        "failed_rule_count": len(failed_rules),
        "passed_rules": passed_rules,
        "failed_rules": failed_rules,
        "rule_results": rule_results,
    }

    logger.info(
        "Data quality evaluation completed | Status: %s | Passed: %s | Failed: %s",
        report["overall_status"],
        report["passed_rule_count"],
        report["failed_rule_count"],
    )

    return report


def write_quality_report(report, report_path):
    """Write a machine-readable data-quality report to JSON."""

    report_path = Path(report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    with open(report_path, "w", encoding="utf-8") as file:
        json.dump(report, file, indent=4)

    logger.info("Data quality report saved: %s", report_path)


def validate_data(df):
    """Preserve the existing validation interface."""

    report = evaluate_data_quality(df)

    if report["failed_rules"]:
        for rule in report["failed_rules"]:
            logger.error(
                "Validation issue: %s: %s failed for %s records",
                rule["column"],
                rule["check"],
                rule["invalid_count"],
            )

        raise ValueError(
            "Pipeline stopped due to data quality issues."
        )

    logger.info("All configured data quality checks passed")
    return report


def run_pipeline(scenario="clean", input_path=None, output_path=None):
    start_time = time.perf_counter()
    input_file = (
        Path(input_path).expanduser().resolve()
        if input_path
        else SIMULATED_DIR / scenario / "sales.csv"
    )

    output_dir = (
        Path(output_path).expanduser().resolve()
        if output_path
        else OUTPUT_DIR / scenario
    )

    logger.info("Starting PipeSage ETL pipeline | Scenario: %s", scenario)

    # Validate input before initializing Spark.
    if not input_file.exists():
        raise FileNotFoundError(f"Dataset not found: {input_file}")

    if not input_file.is_file():
        raise ValueError(f"Input path is not a file: {input_file}")

    if input_file.suffix.lower() != ".csv":
        raise ValueError(f"Unsupported input format: {input_file.suffix}")

    spark = None

    try:
        # Initialize Spark
        logger.info("Initializing Spark session")

        spark = (
            SparkSession.builder
            .master("local[*]")
            .appName("PipeSage-ETL")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("ERROR")

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

        if record_count == 0:
            raise ValueError("Input dataset is empty.")

        
        # 2. VALIDATE
        quality_report = evaluate_data_quality(sales_df)

        report_path = (
            output_dir.parent / f"{output_dir.name}_quality_report.json"
        )

        write_quality_report(quality_report, report_path)

        if quality_report["failed_rules"]:
            raise ValueError(
            "Pipeline stopped due to data quality issues. "
            f"See report: {report_path}"
            )


        # 3. TRANSFORM
        logger.info("Starting data transformation")

        transformed_df = sales_df.withColumn(
            "total_amount", col("quantity") * col("unit_price")
        )

        logger.info("Transformation completed")

        # 4. LOAD
        logger.info("Preparing output directory: %s", output_dir)

        output_dir.parent.mkdir(parents=True, exist_ok=True)

        logger.info("Loading transformed data")

        transformed_df.write.mode("overwrite").parquet(str(output_dir))

        logger.info("Pipeline completed successfully")
        logger.info("Records processed: %s", record_count)
        logger.info("Output location: %s", output_dir)

        transformed_df.show(truncate=False)

    except (FileNotFoundError, ValueError) as exc:
        logger.error("Pipeline execution failed: %s", exc)
        raise

    except Exception:
        logger.exception("Unexpected pipeline failure")
        raise

    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped")

        elapsed_time = time.perf_counter() - start_time
        logger.info("Total execution time: %.2f seconds", elapsed_time)


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

    try:
        run_pipeline(   
            scenario=args.scenario,
            input_path=args.input,
            output_path=args.output,
        )
    except (FileNotFoundError, ValueError) as exc:
        parser.error(str(exc))
