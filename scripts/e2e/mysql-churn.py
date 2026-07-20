#!/usr/bin/env python3
"""
Apply 500MB of churn (INSERT/UPDATE/DELETE) to the MySQL source for CDC E2E.

Usage:
  python scripts/e2e/mysql-churn.py --target-mb 500 --dsn mysql+pymysql://fusion_user:fusion_password@localhost:3307/fusion_e2e

Generates a mix of:
  - INSERT new orders (~60%)
  - UPDATE existing orders' status/amount (~30%)
  - DELETE old orders (~10%)

Each operation is committed individually so the binlog captures every change.
"""
from __future__ import annotations

import argparse
import random
import sys
import time
from datetime import datetime

try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import Session
except ImportError as e:
    print(f"ERROR: {e}. Install with: pip install sqlalchemy pymysql", file=sys.stderr)
    sys.exit(2)


STATUSES = ["pending", "paid", "shipped", "delivered", "refunded"]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-mb", type=float, default=500.0)
    parser.add_argument("--dsn", default="mysql+pymysql://fusion_user:fusion_password@localhost:3307/fusion_e2e")
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    engine = create_engine(args.dsn, future=True)
    target_bytes = int(args.target_mb * 1e6)
    # ~50 bytes per order row → ~target_bytes/50 ops total
    total_ops = max(100, target_bytes // 50)
    n_inserts = int(total_ops * 0.60)
    n_updates = int(total_ops * 0.30)
    n_deletes = total_ops - n_inserts - n_updates
    print(f"Target {args.target_mb} MB → inserts={n_inserts:,} updates={n_updates:,} deletes={n_deletes:,}")

    with Session(engine) as session:
        max_id = session.execute(text("SELECT COALESCE(MAX(id), 0) FROM orders")).scalar() or 0
        max_customer = session.execute(text("SELECT COALESCE(MAX(id), 0) FROM customers")).scalar() or 1
        max_product = session.execute(text("SELECT COALESCE(MAX(id), 0) FROM products")).scalar() or 1
        print(f"Current max order id = {max_id:,}")

        t0 = time.time()
        # Inserts
        for i in range(n_inserts):
            session.execute(text(
                "INSERT INTO orders (customer_id, product_id, quantity, amount, status, placed_at, notes) "
                "VALUES (:c, :p, :q, :a, :s, NOW(), NULL)"
            ), {
                "c": random.randint(1, max_customer),
                "p": random.randint(1, max_product),
                "q": random.randint(1, 10),
                "a": round(random.uniform(10, 5000), 4),
                "s": random.choice(STATUSES),
            })
            if i % 1000 == 0:
                session.commit()
        session.commit()
        new_max = session.execute(text("SELECT COALESCE(MAX(id), 0) FROM orders")).scalar() or max_id
        print(f"  inserts done in {time.time() - t0:.0f}s; new max id = {new_max:,}")

        # Updates
        t1 = time.time()
        for i in range(n_updates):
            oid = random.randint(max(1, max_id - n_inserts - 1000), new_max)
            session.execute(text(
                "UPDATE orders SET status = :s, amount = :a, updated_at = NOW() WHERE id = :id"
            ), {"s": random.choice(STATUSES), "a": round(random.uniform(10, 5000), 4), "id": oid})
            if i % 1000 == 0:
                session.commit()
        session.commit()
        print(f"  updates done in {time.time() - t1:.0f}s")

        # Deletes
        t2 = time.time()
        for i in range(n_deletes):
            oid = random.randint(1, max(1, max_id - 1000))
            session.execute(text("DELETE FROM orders WHERE id = :id"), {"id": oid})
            if i % 1000 == 0:
                session.commit()
        session.commit()
        print(f"  deletes done in {time.time() - t2:.0f}s")

        print(f"Churn complete in {time.time() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
