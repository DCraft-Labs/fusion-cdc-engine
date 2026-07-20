#!/usr/bin/env python3
"""
Parameterized MySQL loader for Fusion CDC E2E.

Generates synthetic data into the `fusion_e2e` database (see mysql-init-schema.sql)
and supports a target data volume in GB. Designed for the 2GB initial-sync E2E
described in the release plan.

Usage:
  python scripts/e2e/mysql-load.py --target-gb 2 --dsn mysql+pymysql://fusion_user:fusion_password@localhost:3307/fusion_e2e

Defaults:
  --target-gb 2
  --batch-size 5000
  --dsn mysql+pymysql://fusion_user:fusion_password@localhost:3307/fusion_e2e

Notes:
  - Uses SQLAlchemy + PyMySQL for portable inserts.
  - Batches are committed every --batch-size rows.
  - The script is idempotent at the table level (it appends; truncate first if
    you want a clean run).
  - For 2GB on a laptop: expect ~15–40 MB/s → ~3–7.5 hours wall time. Use
    --target-gb 0.05 for a quick smoke test (~50 MB).
"""
from __future__ import annotations

import argparse
import random
import string
import sys
import time
from datetime import datetime, timedelta

try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import Session
except ImportError as e:
    print(f"ERROR: {e}. Install with: pip install sqlalchemy pymysql", file=sys.stderr)
    sys.exit(2)


COUNTRIES = ["IN", "US", "GB", "DE", "FR", "SG", "AE", "AU", "CA", "JP"]
CATEGORIES = ["general", "electronics", "books", "clothing", "grocery", "toys"]
STATUSES = ["pending", "paid", "shipped", "delivered", "refunded"]


def rand_str(n: int) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=n))


def rand_email() -> str:
    return f"user{rand_str(8)}@example.com"


def load_customers(session: Session, n: int) -> int:
    rows = []
    for i in range(n):
        rows.append({
            "email": rand_email(),
            "full_name": f"Customer {rand_str(6)}",
            "country": random.choice(COUNTRIES),
            "metadata_json": '{"tier": "%s", "score": %d}' % (random.choice(["bronze", "silver", "gold"]), random.randint(0, 1000)),
        })
    session.execute(text(
        "INSERT INTO customers (email, full_name, country, metadata_json) "
        "VALUES (:email, :full_name, :country, :metadata_json)"
    ), rows)
    return n


def load_products(session: Session, n: int) -> int:
    rows = []
    for i in range(n):
        rows.append({
            "sku": f"SKU-{rand_str(10)}",
            "name": f"Product {rand_str(8)}",
            "price": round(random.uniform(10, 5000), 4),
            "category": random.choice(CATEGORIES),
            "attributes": '{"color": "%s", "weight": %d}' % (random.choice(["red", "blue", "green"]), random.randint(1, 100)),
        })
    session.execute(text(
        "INSERT INTO products (sku, name, price, category, attributes) "
        "VALUES (:sku, :name, :price, :category, :attributes)"
    ), rows)
    return n


def load_orders(session: Session, n: int, customer_max: int, product_max: int) -> int:
    rows = []
    base = datetime(2024, 1, 1)
    for i in range(n):
        rows.append({
            "customer_id": random.randint(1, customer_max),
            "product_id": random.randint(1, product_max),
            "quantity": random.randint(1, 10),
            "amount": round(random.uniform(10, 5000), 4),
            "status": random.choice(STATUSES),
            "placed_at": (base + timedelta(seconds=random.randint(0, 366 * 86400))).strftime("%Y-%m-%d %H:%M:%S"),
            "notes": rand_str(64) if random.random() < 0.3 else None,
        })
    session.execute(text(
        "INSERT INTO orders (customer_id, product_id, quantity, amount, status, placed_at, notes) "
        "VALUES (:customer_id, :product_id, :quantity, :amount, :status, :placed_at, :notes)"
    ), rows)
    return n


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-gb", type=float, default=2.0)
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--dsn", default="mysql+pymysql://fusion_user:fusion_password@localhost:3307/fusion_e2e")
    parser.add_argument("--truncate", action="store_true", help="Truncate tables before loading")
    args = parser.parse_args()

    engine = create_engine(args.dsn, future=True)
    with Session(engine) as session:
        if args.truncate:
            print("Truncating tables ...")
            for t in ("orders", "customers", "products"):
                session.execute(text(f"TRUNCATE TABLE {t}"))
            session.commit()

        # Roughly: 1 order row ≈ 50 bytes; 1 customer ≈ 400 bytes; 1 product ≈ 500 bytes.
        # For target_gb GB we want ~target_gb*1e9/50 order rows (the bulk).
        target_bytes = int(args.target_gb * 1e9)
        n_orders = max(1, target_bytes // 50)
        n_customers = max(1000, n_orders // 200)
        n_products = max(500, n_orders // 4000)
        print(f"Target {args.target_gb} GB → orders={n_orders:,} customers={n_customers:,} products={n_products:,}")

        t0 = time.time()
        # Products first (orders reference them)
        for off in range(0, n_products, args.batch_size):
            n = min(args.batch_size, n_products - off)
            load_products(session, n)
            session.commit()
        # Customers
        for off in range(0, n_customers, args.batch_size):
            n = min(args.batch_size, n_customers - off)
            load_customers(session, n)
            session.commit()
        # Orders (bulk)
        written = 0
        for off in range(0, n_orders, args.batch_size):
            n = min(args.batch_size, n_orders - off)
            written += load_orders(session, n, n_customers, n_products)
            session.commit()
            if (off // args.batch_size) % 20 == 0:
                rate = written / max(1e-9, time.time() - t0)
                print(f"  orders: {written:,} rows | rate {rate:,.0f} rows/s | elapsed {time.time() - t0:.0f}s")

        elapsed = time.time() - t0
        print(f"Done: {written:,} orders in {elapsed:.0f}s ({written / max(1e-9, elapsed):,.0f} rows/s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
