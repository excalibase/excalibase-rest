-- Application user for excalibase-rest (non-superuser, required for RLS enforcement)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'appuser') THEN
        CREATE ROLE appuser LOGIN PASSWORD 'apppass';
    END IF;
END
$$;

-- Grant connect and usage
GRANT CONNECT ON DATABASE excalibase_rest TO appuser;
GRANT USAGE ON SCHEMA public TO appuser;

-- Grant table access
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO appuser;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO appuser;

-- Grant sequence access (for SERIAL / BIGSERIAL columns)
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO appuser;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO appuser;

-- Auth admin role (for excalibase-auth service)
-- Auth service creates its own schema + tables via migrations
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'auth_admin') THEN
        CREATE ROLE auth_admin LOGIN PASSWORD 'authpass';
    END IF;
END
$$;
GRANT CREATE ON DATABASE excalibase_rest TO auth_admin;
GRANT CREATE ON SCHEMA public TO auth_admin;
GRANT USAGE ON SCHEMA public TO auth_admin;

-- Grant RLS test tables to appuser
GRANT SELECT, INSERT, UPDATE, DELETE ON rls_orders TO appuser;
GRANT USAGE, SELECT ON SEQUENCE rls_orders_id_seq TO appuser;
GRANT SELECT, INSERT, UPDATE, DELETE ON rls_payments TO appuser;
GRANT USAGE, SELECT ON SEQUENCE rls_payments_id_seq TO appuser;
