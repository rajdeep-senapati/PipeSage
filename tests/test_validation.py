import pytest

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when

from src.pipeline.transform import validate_data, SALES_SCHEMA, INPUT_FILE


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[*]")
        .appName("PipeSage-Tests")
        .getOrCreate()
    )

    yield session
    session.stop()


@pytest.fixture(scope="session")
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
