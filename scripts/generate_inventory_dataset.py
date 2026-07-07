import csv
import random
import argparse
from pathlib import Path
from datetime import datetime, timedelta

random.seed(42)

ROW_COUNT = 10
DUMP_FORMAT = "csv"
OUTPUT_DIR = Path("data")

CITIES = [
    "Kolkata", "Mumbai", "Bangalore", "Delhi",
    "Hyderabad", "Pune", "Chennai", "Jaipur", "Lucknow"
]

REGIONS = {
    "Kolkata": "East",
    "Mumbai": "West",
    "Bangalore": "South",
    "Delhi": "North",
    "Hyderabad": "South",
    "Pune": "West",
    "Chennai": "South",
    "Jaipur": "North",
    "Lucknow": "North"
}

WAREHOUSE_TYPES = ["FULFILLMENT_CENTER", "DARK_STORE", "REGIONAL_HUB"]

PRODUCT_CATEGORIES = [
    "Electronics", "Fashion", "Grocery",
    "Home", "Beauty", "Sports", "Automotive"
]

MOVEMENT_TYPES = [
    "STOCK_RECEIVED",
    "STOCK_SOLD",
    "STOCK_RETURNED",
    "STOCK_TRANSFERRED",
    "STOCK_DAMAGED",
    "STOCK_ADJUSTED"
]

SOURCE_SYSTEMS = [
    "WAREHOUSE_APP",
    "ERP",
    "POS",
    "SUPPLIER_FEED"
]


def random_date(start_date: datetime, days: int) -> datetime:
    return start_date + timedelta(days=random.randint(0, days))


def calculate_inventory_status(available_quantity: int) -> str:
    if available_quantity <= 0:
        return "OUT_OF_STOCK"
    if available_quantity < 20:
        return "LOW_STOCK"
    return "IN_STOCK"


def generate_warehouses():
    warehouses = []

    warehouse_id = 1

    for city in CITIES:
        for _ in range(3):
            warehouses.append({
                "warehouse_id": f"W{warehouse_id:04d}",
                "city": city,
                "region": REGIONS[city],
                "warehouse_type": random.choice(WAREHOUSE_TYPES)
            })
            warehouse_id += 1

    return warehouses


def generate_inventory(row_count: int, warehouses):
    rows = []
    start_date = datetime(2026, 1, 1)

    for i in range(1, row_count + 1):
        inventory_id = 500000 + i

        warehouse = random.choice(warehouses)
        city = warehouse["city"]
        warehouse_id = warehouse["warehouse_id"]

        product_id = f"P{random.randint(1, 5000):05d}"
        product_category = random.choice(PRODUCT_CATEGORIES)

        movement_date = random_date(start_date, 90)
        movement_type = random.choice(MOVEMENT_TYPES)

        quantity = random.randint(1, 500)

        available_quantity = random.randint(0, 1000)
        reserved_quantity = random.randint(0, 200)
        damaged_quantity = random.randint(0, 50)

        if movement_type == "STOCK_DAMAGED":
            damaged_quantity += random.randint(1, 30)

        if movement_type == "STOCK_SOLD":
            available_quantity = max(0, available_quantity - quantity)

        if movement_type == "STOCK_RECEIVED":
            available_quantity += quantity

        inventory_status = calculate_inventory_status(available_quantity)

        updated_at = movement_date + timedelta(
            hours=random.randint(1, 72),
            minutes=random.randint(0, 59)
        )

        rows.append({
            "inventory_id": inventory_id,
            "product_id": product_id,
            "warehouse_id": warehouse_id,
            "city": city,
            "movement_date": movement_date.strftime("%Y-%m-%d"),
            "product_category": product_category,
            "movement_type": movement_type,
            "quantity": quantity,
            "available_quantity": available_quantity,
            "reserved_quantity": reserved_quantity,
            "damaged_quantity": damaged_quantity,
            "inventory_status": inventory_status,
            "updated_at": updated_at.strftime("%Y-%m-%d %H:%M:%S"),
            "source_system": random.choice(SOURCE_SYSTEMS),
            "sync_version": 1
        })

    return rows


def write_csv(path: Path, rows):
    if not rows:
        return

    with open(path, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def write_parquet(path: Path, rows):
    if not rows:
        return

    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError(
            "Parquet output requires pandas and pyarrow. "
            "Install them with: pip install pandas pyarrow"
        ) from exc

    pd.DataFrame(rows).to_parquet(path, index=False)


def write_dataset(output_dir: Path, file_name: str, rows, dump_format: str):
    if dump_format == "csv":
        write_csv(output_dir / f"{file_name}.csv", rows)
        return

    if dump_format == "parquet":
        write_parquet(output_dir / f"{file_name}.parquet", rows)
        return

    raise ValueError("dump_format must be either 'csv' or 'parquet'")


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic inventory data for Iceberg capstone project."
    )

    parser.add_argument(
        "--output-dir",
        default=OUTPUT_DIR,
        type=Path,
        help="Location where data files should be created."
    )

    parser.add_argument(
        "--dump-format",
        choices=["csv", "parquet"],
        default=DUMP_FORMAT,
        help="Output file format."
    )

    parser.add_argument(
        "--row-count",
        type=int,
        default=ROW_COUNT,
        help="Number of inventory rows."
    )

    args = parser.parse_args()

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    dump_format = args.dump_format.lower()

    warehouses = generate_warehouses()
    inventory = generate_inventory(args.row_count, warehouses)

    write_dataset(output_dir, "inventory", inventory, dump_format)
    write_dataset(output_dir, "warehouse_dim", warehouses, dump_format)

    print(f"{dump_format.upper()} inventory file generated at:")
    print(output_dir.resolve() / f"inventory.{dump_format}")

    print(f"{dump_format.upper()} warehouse dimension file generated at:")
    print(output_dir.resolve() / f"warehouse_dim.{dump_format}")

    print(f"Inventory rows generated: {args.row_count}")
    print(f"Warehouse dimension rows generated: {len(warehouses)}")


if __name__ == "__main__":
    main()