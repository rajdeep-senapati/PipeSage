import csv
from pathlib import Path

OUTPUT_DIR = Path("data")
OUTPUT_FILE = OUTPUT_DIR / "sales.csv"

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
    "order_id",
    "customer_id",
    "order_date",
    "product",
    "quantity",
    "unit_price",
    "region",
]


def generate_sales_data():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(COLUMNS)
        writer.writerows(SALES_DATA)

    print(f"Generated {len(SALES_DATA)} sales records: {OUTPUT_FILE}")


if __name__ == "__main__":
    generate_sales_data()
