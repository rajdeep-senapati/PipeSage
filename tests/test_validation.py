import pytest
from src.simulator.generate_data import generate_sales_data
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
import subprocess
import sys
import json
from unittest.mock import Mock
import src.pipeline.transform as transform

from src.pipeline.transform import (
    validate_data,
    run_pipeline,
    SALES_SCHEMA,
    INPUT_FILE,
)


@pytest.fixture(scope="function")
def spark():
    session = (
        SparkSession.builder
        .master("local[*]")
        .appName("PipeSage-Tests")
        .getOrCreate()
    )

    yield session
    session.stop()


@pytest.fixture(scope="function")
def sales_df(spark):
    return (
        spark.read
        .schema(SALES_SCHEMA)
        .option("header", True)
        .option("dateFormat", "yyyy-MM-dd")
        .csv(str(INPUT_FILE))
    )


def test_valid_data_passes(sales_df):
    validate_data(sales_df)


def test_null_customer_id_is_rejected(sales_df):
    invalid_df = sales_df.withColumn(
        "customer_id",
        when(col("order_id") == 1, lit(None).cast("integer"))
        .otherwise(col("customer_id"))
    )

    with pytest.raises(ValueError, match="data quality issues"):
        validate_data(invalid_df)


def test_duplicate_order_id_is_rejected(sales_df):
    invalid_df = sales_df.unionByName(
        sales_df.filter(col("order_id") == 1)
    )

    with pytest.raises(ValueError, match="data quality issues"):
        validate_data(invalid_df)


def test_zero_quantity_is_rejected(sales_df):
    invalid_df = sales_df.withColumn(
        "quantity",
        when(col("order_id") == 1, lit(0))
        .otherwise(col("quantity"))
    )

    with pytest.raises(ValueError, match="data quality issues"):
        validate_data(invalid_df)


def test_negative_unit_price_is_rejected(sales_df):
    invalid_df = sales_df.withColumn(
        "unit_price",
        when(col("order_id") == 1, lit(-100.0))
        .otherwise(col("unit_price"))
    )

    with pytest.raises(ValueError, match="data quality issues"):
        validate_data(invalid_df)


def test_negative_quantity_is_rejected(sales_df):
    invalid_df = sales_df.withColumn(
        "quantity",
        when(col("order_id") == 1, lit(-5))
        .otherwise(col("quantity"))
    )

    with pytest.raises(ValueError, match="data quality issues"):
        validate_data(invalid_df)


def test_null_unit_price_is_rejected(sales_df):
    invalid_df = sales_df.withColumn(
        "unit_price",
        when(col("order_id") == 1, lit(None).cast("double"))
        .otherwise(col("unit_price"))
    )

    with pytest.raises(ValueError, match="data quality issues"):
        validate_data(invalid_df)


def test_invalid_date_is_rejected(spark, tmp_path):
    csv_file = tmp_path / "invalid_date.csv"

    csv_file.write_text(
        "order_id,customer_id,order_date,product,quantity,unit_price,region\n"
        "999,101,not-a-date,Laptop,1,55000.0,East\n"
    )

    invalid_df = (
        spark.read
        .option("header", True)
        .option("dateFormat", "yyyy-MM-dd")
        .schema(SALES_SCHEMA)
        .csv(str(csv_file))
    )

    with pytest.raises(ValueError, match="data quality issues"):
        validate_data(invalid_df)

@pytest.mark.parametrize(
    "scenario, should_pass",
    [
        ("clean", True),
        ("null_customer", False),
        ("duplicate_order", False),
        ("zero_quantity", False),
        ("negative_price", False),
        ("invalid_date", False),
        ("negative_quantity", False),
    ],
)
def test_simulated_scenarios(spark, tmp_path, scenario, should_pass):
    generate_sales_data(scenario, output_dir=tmp_path)

    input_file = tmp_path / scenario / "sales.csv"

    simulated_df = (
        spark.read
        .option("header", True)
        .option("dateFormat", "yyyy-MM-dd")
        .schema(SALES_SCHEMA)
        .csv(str(input_file))
    )

    if should_pass:
        validate_data(simulated_df)
    else:
        with pytest.raises(ValueError, match="data quality issues"):
            validate_data(simulated_df)


def test_missing_input_file_is_rejected(tmp_path):
    missing_file = tmp_path / "missing.csv"

    with pytest.raises(FileNotFoundError, match="Dataset not found"):
        run_pipeline(input_path=missing_file)


def test_input_directory_is_rejected(tmp_path):
    input_directory = tmp_path / "input.csv"
    input_directory.mkdir()

    with pytest.raises(ValueError, match="not a file"):
        run_pipeline(input_path=input_directory)


def test_unsupported_input_format_is_rejected(tmp_path):
    unsupported_file = tmp_path / "input.txt"
    unsupported_file.write_text("unsupported content")

    with pytest.raises(ValueError, match="Unsupported input format"):
        run_pipeline(input_path=unsupported_file)


def test_empty_csv_is_rejected(tmp_path):
    empty_file = tmp_path / "empty.csv"
    empty_file.write_text(
        "order_id,customer_id,order_date,product,quantity,unit_price,region\n"
    )

    with pytest.raises(ValueError, match="Input dataset is empty"):
        run_pipeline(
            input_path=empty_file,
            output_path=tmp_path / "output",
        )

@pytest.mark.parametrize(
    "filename, expected_error",
    [
        ("missing.csv", "Dataset not found"),
        ("unsupported.txt", "Unsupported input format"),
    ],
)
def test_cli_displays_clean_input_errors(tmp_path, filename, expected_error):
    input_file = tmp_path / filename

    if filename == "unsupported.txt":
        input_file.write_text("test")

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "src.pipeline.transform",
            "--input",
            str(input_file),
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert expected_error in result.stderr
    assert "Traceback" not in result.stderr

def test_unsupported_validation_rule_is_rejected(tmp_path, monkeypatch):
    config_path = tmp_path / "validation_rules.json"
    config_path.write_text(
        json.dumps({
            "rules": [{"column": "order_id", "checks": ["invalid_check"]}]
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr(transform, "VALIDATION_CONFIG", config_path)

    df = Mock()
    df.columns = ["order_id"]

    with pytest.raises(ValueError, match="Unsupported validation check"):
        transform.validate_data(df)


def test_missing_validation_column_is_rejected(tmp_path, monkeypatch):
    config_path = tmp_path / "validation_rules.json"
    config_path.write_text(
        json.dumps({
            "rules": [{"column": "missing_column", "checks": ["not_null"]}]
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr(transform, "VALIDATION_CONFIG", config_path)

    df = Mock()
    df.columns = ["order_id"]

    with pytest.raises(ValueError, match="column does not exist"):
        transform.validate_data(df)


def test_invalid_validation_json_is_rejected(tmp_path, monkeypatch):
    config_path = tmp_path / "validation_rules.json"
    config_path.write_text("{invalid json", encoding="utf-8")
    monkeypatch.setattr(transform, "VALIDATION_CONFIG", config_path)

    with pytest.raises(ValueError, match="Invalid validation configuration"):
        transform.validate_data(Mock())


def test_missing_validation_config_is_rejected(tmp_path, monkeypatch):
    config_path = tmp_path / "missing.json"
    monkeypatch.setattr(transform, "VALIDATION_CONFIG", config_path)

    with pytest.raises(FileNotFoundError, match="Validation configuration not found"):
        transform.validate_data(Mock())


def test_quality_report_passes_for_valid_data(spark, sales_df):
    report = transform.evaluate_data_quality(sales_df)

    assert report["overall_status"] == "passed"
    assert report["total_records"] == 10
    assert report["failed_rule_count"] == 0
    assert report["passed_rule_count"] == report["total_rules"]
    assert report["failed_rules"] == []


def test_quality_report_detects_invalid_records(spark, sales_df):
    invalid_df = sales_df.withColumn(
        "customer_id",
        when(
            col("order_id") == 1,
            lit(None)
        ).otherwise(col("customer_id"))
    )

    report = transform.evaluate_data_quality(invalid_df)

    assert report["overall_status"] == "failed"
    assert report["total_records"] == 10
    assert report["failed_rule_count"] == 1

    failed_rule = report["failed_rules"][0]

    assert failed_rule["column"] == "customer_id"
    assert failed_rule["check"] == "not_null"
    assert failed_rule["invalid_count"] == 1


def test_quality_report_is_written_as_json(tmp_path):
    report = {
        "overall_status": "passed",
        "total_records": 10,
        "passed_rule_count": 10,
        "failed_rule_count": 0,
    }

    report_path = tmp_path / "quality_report.json"

    transform.write_quality_report(report, report_path)

    assert report_path.exists()

    with open(report_path, "r", encoding="utf-8") as file:
        saved_report = json.load(file)

    assert saved_report == report
