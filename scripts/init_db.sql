-- ─────────────────────────────────────────────────────────────────────────────
--  QueryGuard – Database Bootstrap Script
--  Run once by Postgres on first container start (or manually against Neon).
--  100% idempotent: uses IF NOT EXISTS / ON CONFLICT DO NOTHING everywhere.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ── Tenants ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS tenants (
    id          UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    name        TEXT        NOT NULL,
    plan        TEXT        NOT NULL DEFAULT 'free',   -- free | pro | enterprise
    is_active   BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ
);

-- ── GitHub Installations ──────────────────────────────────────────────────────
-- KEY table: maps GitHub App installation_id → tenant
-- Both the webhook (asyncpg) and worker (SQLAlchemy) query this table.
CREATE TABLE IF NOT EXISTS github_installations (
    id              UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    installation_id TEXT        NOT NULL UNIQUE,   -- GitHub numeric ID stored as text
    tenant_id       TEXT        NOT NULL,           -- UUID string (compatible with both layers)
    schema_name     TEXT        NOT NULL,           -- e.g. schema_12345678
    account_login   TEXT,
    account_type    TEXT,
    is_active       BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_installations_installation_id
    ON github_installations(installation_id);

-- ── PR Analysis Results ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pr_analyses (
    id                      UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id               TEXT        NOT NULL,
    installation_id         TEXT        NOT NULL,
    repo_full_name          TEXT        NOT NULL,
    pr_number               INTEGER     NOT NULL,
    pr_title                TEXT,
    pr_url                  TEXT,
    branch_name             TEXT,
    author_name             TEXT,
    total_impacted_queries  INTEGER     DEFAULT 0,
    analysis_data           JSONB,
    status                  TEXT        NOT NULL DEFAULT 'pending',
    -- status values: pending | processing | done | failed
    idempotency_key         TEXT        UNIQUE,
    -- format: {installation_id}:{pr_number}:{delivery_id}
    processed_at            TIMESTAMPTZ,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_pr_analyses_tenant
    ON pr_analyses(tenant_id);

CREATE INDEX IF NOT EXISTS idx_pr_analyses_idempotency
    ON pr_analyses(idempotency_key);

CREATE INDEX IF NOT EXISTS idx_pr_analyses_status
    ON pr_analyses(status);

-- ── Seed Data (remove in production) ─────────────────────────────────────────
-- Replace 99999999 with your real GitHub App installation_id for local testing.
INSERT INTO github_installations
    (installation_id, tenant_id, schema_name, account_login, account_type, is_active)
VALUES
    ('99999999', 'tenant_99999999', 'schema_99999999', 'demo-org', 'Organization', TRUE)
ON CONFLICT DO NOTHING;
