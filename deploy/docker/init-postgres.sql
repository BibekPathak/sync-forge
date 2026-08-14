-- Creates SyncForge roles in the freshly-initialized PostgreSQL instance.
-- syncforge_app is subject to Row-Level Security (used by the API for
-- tenant-scoped queries). syncforge_engine bypasses RLS (internal workers and
-- cross-tenant administration).
CREATE ROLE syncforge_app LOGIN PASSWORD 'syncforge_app';
CREATE ROLE syncforge_engine LOGIN PASSWORD 'syncforge_engine' BYPASSRLS;

GRANT ALL PRIVILEGES ON DATABASE syncforge TO syncforge_app;
GRANT ALL PRIVILEGES ON DATABASE syncforge TO syncforge_engine;

-- PG15+ restricts CREATE on the public schema to the database owner; the app
-- role owns the tables it creates, so it needs schema CREATE.
GRANT CREATE ON SCHEMA public TO syncforge_app;
GRANT CREATE ON SCHEMA public TO syncforge_engine;
