-- =============================================================================
-- Fusion local seed
-- Seeds: superadmin role + admin user + connector definitions +
--        sample source (local pg-source) + sample destination (local pg-dest) +
--        sample connection (pg-source → pg-dest, REALTIME CDC)
--
-- Idempotent — safe to run multiple times.
--
-- Run:
--   psql -h localhost -U fusion_user -d fusion_cdc_metadata \
--        -f scripts/seed-admin.sql
--
-- Encrypted passwords use Fernet with ENCRYPTION_KEY=fusion-local-encrypt-key-32byt
-- (matches .env.local). To regenerate for a different key:
--   python3 -c "
--   import base64,hashlib; from cryptography.fernet import Fernet
--   f=Fernet(base64.urlsafe_b64encode(hashlib.sha256(b'YOUR_KEY').digest()))
--   print(f.encrypt(b'YOUR_PASSWORD').decode())"
-- =============================================================================

DO $$
DECLARE
  v_role_id           uuid;
  v_user_id           uuid;
  v_pg_src_def_id     uuid;
  v_mysql_src_def_id  uuid;
  v_mongo_src_def_id  uuid;
  v_pg_dst_def_id     uuid;  -- assigned from v_pg_src_def_id (same connector)
  v_source_id         uuid;
  v_dest_id           uuid;
BEGIN

  -- ──────────────────────────────────────────────────────────────────────────
  -- 1. RBAC: superadmin role
  -- ──────────────────────────────────────────────────────────────────────────
  INSERT INTO roles (role_name, display_name, role_level, description, is_active, is_system_role, created_at, updated_at)
  VALUES ('superadmin', 'Super Admin', 'superadmin', 'Full system access', true, true, NOW(), NOW())
  ON CONFLICT (role_name) DO NOTHING;

  SELECT role_id INTO v_role_id FROM roles WHERE role_name = 'superadmin';

  -- ──────────────────────────────────────────────────────────────────────────
  -- 2. Admin user  (password: Admin@123)
  -- ──────────────────────────────────────────────────────────────────────────
  INSERT INTO users (
    username, email, password_hash,
    first_name, last_name, full_name,
    is_active, is_superuser, is_email_verified,
    failed_login_attempts, preferences,
    created_at, updated_at
  )
  VALUES (
    'admin', 'admin@dcraftfusion.io',
    '$2b$12$DnILea2s7A9Jq7rPYNtdieWK3vWLQ4Aljlq2/SkJqQIuw17KKtdHO',
    'Fusion', 'Admin', 'Fusion Admin',
    true, true, true,
    '0', '{}'::jsonb,
    NOW(), NOW()
  )
  ON CONFLICT (username) DO NOTHING;

  SELECT user_id INTO v_user_id FROM users WHERE username = 'admin';

  INSERT INTO user_roles (user_id, role_id)
  VALUES (v_user_id, v_role_id)
  ON CONFLICT DO NOTHING;

  -- ──────────────────────────────────────────────────────────────────────────
  -- 3. Connector definitions
  --    connector_definitions.connector_name is UNIQUE across the whole table,
  --    so one entry per database type covers both source and destination use.
  --    We use ON CONFLICT ... DO UPDATE + RETURNING to reliably get the ID.
  -- ──────────────────────────────────────────────────────────────────────────

  -- PostgreSQL connector (used for both source CDC and destination DW)
  INSERT INTO connector_definitions (
    connector_name, connector_type, category, latest_version,
    default_config, required_fields, optional_fields, default_resource_limits,
    supports_cdc, supports_full_refresh, supports_incremental,
    documentation_url, is_active
  )
  VALUES (
    'PostgreSQL', 'postgres', 'source', '1.0.0',
    '{"replication_plugin": "pgoutput", "initial_waiting_seconds": 5}'::jsonb,
    '["host","port","database_name","username","password"]'::jsonb,
    '["ssl_enabled","replication_slot","publication","schema_name","batch_size"]'::jsonb,
    '{"cpu": "500m", "memory": "512Mi"}'::jsonb,
    true, true, true,
    'https://docs.dcraftfusion.io/connectors/postgres',
    true
  )
  ON CONFLICT (connector_name) DO UPDATE SET
    connector_type = EXCLUDED.connector_type,
    category = EXCLUDED.category
  RETURNING connector_id INTO v_pg_src_def_id;

  -- PostgreSQL DESTINATION connector (separate entry so UI can show it in destination list)
  INSERT INTO connector_definitions (
    connector_name, connector_type, category, latest_version,
    default_config, required_fields, optional_fields, default_resource_limits,
    supports_cdc, supports_full_refresh, supports_incremental,
    documentation_url, is_active
  )
  VALUES (
    'PostgreSQL Destination', 'postgresql', 'destination', '1.0.0',
    '{"schema": "public"}'::jsonb,
    '["host","port","database_name","username","password"]'::jsonb,
    '["ssl_enabled","schema_name"]'::jsonb,
    '{"cpu": "500m", "memory": "512Mi"}'::jsonb,
    false, true, true,
    'https://docs.dcraftfusion.io/connectors/postgres-destination',
    true
  )
  ON CONFLICT (connector_name) DO UPDATE SET
    connector_type = EXCLUDED.connector_type,
    category = EXCLUDED.category
  RETURNING connector_id INTO v_pg_dst_def_id;

  -- MySQL connector
  INSERT INTO connector_definitions (
    connector_name, connector_type, category, latest_version,
    default_config, required_fields, optional_fields, default_resource_limits,
    supports_cdc, supports_full_refresh, supports_incremental,
    documentation_url, is_active
  )
  VALUES (
    'MySQL', 'mysql', 'source', '1.0.0',
    '{"server_id": 1, "initial_waiting_seconds": 5}'::jsonb,
    '["host","port","database_name","username","password"]'::jsonb,
    '["ssl_enabled","server_id"]'::jsonb,
    '{"cpu": "500m", "memory": "512Mi"}'::jsonb,
    true, true, true,
    'https://docs.dcraftfusion.io/connectors/mysql',
    true
  )
  ON CONFLICT (connector_name) DO UPDATE SET
    connector_type = EXCLUDED.connector_type,
    category = EXCLUDED.category
  RETURNING connector_id INTO v_mysql_src_def_id;

  -- MongoDB connector
  INSERT INTO connector_definitions (
    connector_name, connector_type, category, latest_version,
    default_config, required_fields, optional_fields, default_resource_limits,
    supports_cdc, supports_full_refresh, supports_incremental,
    documentation_url, is_active
  )
  VALUES (
    'MongoDB', 'mongodb', 'source', '1.0.0',
    '{"replica_set": "rs0", "auth_source": "admin"}'::jsonb,
    '["host","port","database_name"]'::jsonb,
    '["username","password","replica_set","auth_source"]'::jsonb,
    '{"cpu": "500m", "memory": "512Mi"}'::jsonb,
    true, true, false,
    'https://docs.dcraftfusion.io/connectors/mongodb',
    true
  )
  ON CONFLICT (connector_name) DO UPDATE SET
    connector_type = EXCLUDED.connector_type,
    category = EXCLUDED.category
  RETURNING connector_id INTO v_mongo_src_def_id;

  -- ──────────────────────────────────────────────────────────────────────────
  -- 4. Sample source — local pg-source (wal_level=logical, port 5434)
  --    password 'cdc_password' encrypted with ENCRYPTION_KEY from .env.local
  -- ──────────────────────────────────────────────────────────────────────────
  INSERT INTO sources (
    source_name, connector_definition_id, connector_version,
    host, port, database_name, username, password_encrypted,
    ssl_enabled, ssl_config, config,
    status, created_at, updated_at
  )
  VALUES (
    'Local PostgreSQL Source',
    v_pg_src_def_id,
    '1.0.0',
    'pg-source',   -- Docker service name (resolvable inside compose network)
    5432,
    'source_db',
    'cdc_user',
    'gAAAAABp_L8ol3mkPIHpwaGnrvXuQemZ0d1GYNU1YiHE5p_x1DtN6epTYvd17N7qxyRmrMX7d35UBuEm8x0vGivHZcp_iKMzBg==',
    false,
    '{}'::jsonb,
    '{"replication_plugin": "pgoutput", "replication_slot": "fusion_slot", "publication": "fusion_pub"}'::jsonb,
    'active',
    NOW(), NOW()
  )
  ON CONFLICT DO NOTHING
  RETURNING source_id INTO v_source_id;

  IF v_source_id IS NULL THEN
    SELECT source_id INTO v_source_id FROM sources WHERE source_name = 'Local PostgreSQL Source';
  END IF;

  -- ──────────────────────────────────────────────────────────────────────────
  -- 5. Sample destination — local postgres-dest (port 5433)
  --    password 'dw_password' encrypted with ENCRYPTION_KEY from .env.local
  -- ──────────────────────────────────────────────────────────────────────────
  INSERT INTO destinations (
    destination_name, connector_definition_id, connector_version,
    host, port, database_name, schema_name, username, password_encrypted,
    ssl_enabled, ssl_config, config,
    status, created_at, updated_at
  )
  VALUES (
    'Local PostgreSQL Destination',
    v_pg_dst_def_id,
    '1.0.0',
    'postgres-dest',  -- Docker service name
    5432,
    'fusion_dw',
    'public',
    'dw_user',
    'gAAAAABp_L8o4jvV4fglnSnabhrkN5vcXJn3RjjSF3X_YFQ5YXkLrNIxZwe72Qj1SczIdTpQYib4usUoSkBxXuyJNjeqio-k6w==',
    false,
    '{}'::jsonb,
    '{"batch_size": 1000}'::jsonb,
    'active',
    NOW(), NOW()
  )
  ON CONFLICT DO NOTHING
  RETURNING destination_id INTO v_dest_id;

  IF v_dest_id IS NULL THEN
    SELECT destination_id INTO v_dest_id FROM destinations WHERE destination_name = 'Local PostgreSQL Destination';
  END IF;

  -- ──────────────────────────────────────────────────────────────────────────
  -- 6. Sample connection — pg-source → pg-dest, REALTIME CDC
  -- ──────────────────────────────────────────────────────────────────────────
  INSERT INTO connections (
    connection_name, source_id, destination_id,
    sync_mode, sync_type, status,
    sync_enabled, replication_slot, publication,
    namespace_definition, namespace_format, stream_prefix,
    resource_limits, config,
    schema_evolution_policy, initial_load_completed,
    created_by, created_at, updated_at
  )
  VALUES (
    'pg-source → pg-dest (REALTIME)',
    v_source_id,
    v_dest_id,
    'CDC',
    'REALTIME',
    'active',
    true,
    'fusion_slot',
    'fusion_pub',
    'DESTINATION_SCHEMA',
    'public',
    '',
    '{"cpu": "500m", "memory": "512Mi"}'::jsonb,
    '{}'::jsonb,
    'MANUAL_APPROVAL',
    false,
    v_user_id,
    NOW(), NOW()
  )
  ON CONFLICT DO NOTHING;

  RAISE NOTICE '=== Seed complete ===';
  RAISE NOTICE 'Admin login  : admin / Admin@123';
  RAISE NOTICE 'Connector defs: PostgreSQL SOURCE, MySQL SOURCE, MongoDB SOURCE, PostgreSQL DESTINATION';
  RAISE NOTICE 'Source       : Local PostgreSQL Source  (pg-source:5432/source_db)';
  RAISE NOTICE 'Destination  : Local PostgreSQL Destination (postgres-dest:5432/fusion_dw)';
  RAISE NOTICE 'Connection   : pg-source → pg-dest (REALTIME CDC)';

END
$$;
