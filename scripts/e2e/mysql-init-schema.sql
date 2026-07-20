-- =============================================================================
-- MySQL source schema for Fusion CDC E2E.
-- Creates a `fusion_e2e` database with three tables sized for 2GB initial load
-- + 500MB churn: orders (high row count), customers (medium), products (small).
-- All tables have a primary key (required for CDC upsert) and an updated_at
-- column used as the cursor field for incremental syncs.
--
-- Run:
--   docker compose -f docker/docker-compose.dev.yml exec mysql-source \
--     mysql -u fusion_user -pfusion_password < scripts/e2e/mysql-init-schema.sql
-- =============================================================================

CREATE DATABASE IF NOT EXISTS fusion_e2e;
USE fusion_e2e;

-- Customers — ~200K rows (~80 MB)
CREATE TABLE IF NOT EXISTS customers (
  id            BIGINT NOT NULL AUTO_INCREMENT,
  email         VARCHAR(255) NOT NULL,
  full_name     VARCHAR(255) NOT NULL,
  country       VARCHAR(64)  NOT NULL DEFAULT 'IN',
  metadata_json JSON NULL,
  created_at    DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at    DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_customers_email (email),
  KEY ix_customers_updated (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Products — ~10K rows (~5 MB)
CREATE TABLE IF NOT EXISTS products (
  id           BIGINT NOT NULL AUTO_INCREMENT,
  sku          VARCHAR(64) NOT NULL,
  name         VARCHAR(255) NOT NULL,
  price        DECIMAL(18, 4) NOT NULL,
  category     VARCHAR(64) NOT NULL DEFAULT 'general',
  attributes   JSON NULL,
  created_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_products_sku (sku),
  KEY ix_products_updated (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Orders — ~40M rows (~1.9 GB) — the bulk of the 2GB load
CREATE TABLE IF NOT EXISTS orders (
  id           BIGINT NOT NULL AUTO_INCREMENT,
  customer_id  BIGINT NOT NULL,
  product_id   BIGINT NOT NULL,
  quantity     INT NOT NULL DEFAULT 1,
  amount       DECIMAL(18, 4) NOT NULL,
  status       VARCHAR(32) NOT NULL DEFAULT 'pending',
  placed_at    DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  notes        TEXT NULL,
  PRIMARY KEY (id),
  KEY ix_orders_customer (customer_id),
  KEY ix_orders_product (product_id),
  KEY ix_orders_status (status),
  KEY ix_orders_updated (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Enable row-level binlog (already set in docker-compose.dev.yml command:)
-- Verify with:  SHOW VARIABLES WHERE Variable_name IN
--   ('log_bin','binlog_format','binlog_row_image','server_id');
