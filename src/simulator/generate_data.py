import argparse
import csv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
OUTPUT_DIR = BASE_DIR / "data" / "simulated"

SALES_DATA = [
    (1, 101, "2026-10-01", "Laptop", 1, 55000.00, "East"),
    (2, 102, "2026-10-01", "Mouse", 2, 750.00, "West"),
    (3, 103, "2026-10-02", "Keyboard", 1, 1800.00, "North"),
    (4, 104, "2026-10-02", "Monitor", 2, 12000.00, "South"),
    (5, 105, "2026-10-03", "Laptop", 1, 62000.00, "East"),
    (6, 101, "2026-10-03", "Mouse", 3, 750.00, "East"),
    (7, 106, "2026-10-04", "Keyboard", 2, 1800.00, "West"),
    (8, 107, "2026-10-04", "Monitor", 1, 12000.00, "North"),
    (9, 108, "2026-10-05", "Laptop", 1, 58000.00, "South"),
    (10, 102, "2026-10-05", "Mouse", 1, 750.00, "West"),
]

COLUMNS = [
    "order_id", "customer_id", "order_date",
    "product", "quantity", "unit_price", "region",
]

def generate_sales_data(scenario="clean", output_dir=OUTPUT_DIR):
    data = [list(row) for row in SALES_DATA]

    if scenario == "null_customer":
        data[0][1] = ""
    elif scenario == "duplicate_order":
        data.append(data[0].copy())
    elif scenario == "zero_quantity":
        data[1][4] = 0
    elif scenario == "negative_price":
        data[2][5] = -1800.00
    elif scenario == "invalid_date":
        data[3][2] = "not-a-date"
    elif scenario == "negative_quantity":
        data[1][4] = -2

    output_file = output_dir / scenario / "sales.csv"
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with output_file.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(COLUMNS)
        writer.writerows(data)

    print(f"Generated {len(data)} records for '{scenario}': {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PipeSage failure simulator")
    parser.add_argument(
        "--scenario",
        choices=[
            "clean", "null_customer", "duplicate_order",
            "zero_quantity", "negative_price", "invalid_date","negative_quantity",
        ],
        default="clean",
    )
    args = parser.parse_args()
    generate_sales_data(args.scenario)
