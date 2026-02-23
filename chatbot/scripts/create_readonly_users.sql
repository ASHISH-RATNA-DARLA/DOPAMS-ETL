-- ==================================================
-- PostgreSQL: Create Read-Only User
-- ==================================================
-- Run this script:
-- psql -U dopamasdev_ur -d dopamasdev -h localhost -f create_readonly_users.sql

-- Create read-only user
CREATE USER readonly_user WITH PASSWORD 'changeme_readonly_pass_123';

-- Grant connection permission
GRANT CONNECT ON DATABASE dopamasdev TO readonly_user;

-- Grant usage on schema
GRANT USAGE ON SCHEMA public TO readonly_user;

-- Grant SELECT on all existing tables
GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly_user;

-- Grant SELECT on future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO readonly_user;

-- Set query timeout (30 seconds)
ALTER USER readonly_user SET statement_timeout = '30s';

-- Display created user
\du readonly_user

-- Display tables the user can access
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public' 
  AND table_type = 'BASE TABLE';

-- Success message
DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'PostgreSQL Read-Only User Created!';
    RAISE NOTICE 'User: readonly_user';
    RAISE NOTICE 'Password: changeme_readonly_pass_123';
    RAISE NOTICE 'Database: dopamasdev';
    RAISE NOTICE '';
    RAISE NOTICE 'IMPORTANT: Change the password!';
    RAISE NOTICE 'IMPORTANT: Update .env file with this password!';
    RAISE NOTICE '========================================';
END $$;

