-- =========================================================
-- Extensions
-- =========================================================
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- =========================================================
-- Enum types (idempotent via DO block with bare $ dollar-quoting)
-- =========================================================
DO
$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'user_role') THEN
            CREATE TYPE user_role AS ENUM ('admin','user','owner');
        END IF;

        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'priority_level') THEN
            CREATE TYPE priority_level AS ENUM ('low','medium','high','critical');
        END IF;

        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'item_status') THEN
            CREATE TYPE item_status AS ENUM ('active','archived');
        END IF;

        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'visibility') THEN
            CREATE TYPE visibility AS ENUM ('public','private');
        END IF;
    END
$;

-- =========================================================
-- Core: users, sessions, accounts, memberships
-- =========================================================
CREATE TABLE IF NOT EXISTS users
(
    user_id       UUID PRIMARY KEY     DEFAULT uuid_generate_v4(),
    email         TEXT        NOT NULL UNIQUE,
    password_hash TEXT        NOT NULL,
    active        BOOLEAN     NOT NULL DEFAULT TRUE,
    login_code    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sessions
(
    session_id UUID PRIMARY KEY     DEFAULT uuid_generate_v4(),
    user_id    UUID        NOT NULL REFERENCES users (user_id) ON DELETE CASCADE,
    token_hash BYTEA       NOT NULL UNIQUE,
    ip         INET,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS sessions_user_idx ON sessions (user_id);

-- =========================================================
-- Plans & accounts
-- =========================================================
CREATE TABLE IF NOT EXISTS plans
(
    key          TEXT PRIMARY KEY,
    name         TEXT           NOT NULL,
    monthly_cost NUMERIC(10, 2) NOT NULL,
    visibility   visibility     NOT NULL DEFAULT 'public',
    max_items    INT            NOT NULL,
    max_users    INT            NOT NULL DEFAULT 1,
    created_at   TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS accounts
(
    account_id UUID PRIMARY KEY     DEFAULT uuid_generate_v4(),
    name       TEXT        NOT NULL,
    plan_key   TEXT        NOT NULL DEFAULT 'free',
    active     BOOLEAN     NOT NULL DEFAULT TRUE,
    settings   JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT accounts_plan_fk FOREIGN KEY (plan_key) REFERENCES plans (key) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE IF NOT EXISTS account_users
(
    account_id UUID        NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
    user_id    UUID        NOT NULL REFERENCES users (user_id) ON DELETE CASCADE,
    role       user_role   NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, user_id)
);
CREATE INDEX IF NOT EXISTS account_users_user_idx ON account_users (user_id);

-- Validate the deferred FK now that plans exists
ALTER TABLE accounts
    VALIDATE CONSTRAINT accounts_plan_fk;

-- =========================================================
-- Items (generic entity with priority and status enums)
-- =========================================================
CREATE TABLE IF NOT EXISTS items
(
    item_id              UUID PRIMARY KEY     DEFAULT uuid_generate_v4(),
    account_id           UUID           NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
    name                 TEXT           NOT NULL,
    priority             priority_level NOT NULL DEFAULT 'medium',
    status               item_status    NOT NULL DEFAULT 'active',
    config               JSONB          NOT NULL DEFAULT '{}'::jsonb,
    score                INT            NOT NULL DEFAULT 0 CHECK (score BETWEEN 0 AND 100),
    seconds_after_minute INT            NOT NULL DEFAULT (floor(random() * 60))::INT,
    created_at           TIMESTAMPTZ    NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS items_account_idx ON items (account_id);
CREATE INDEX IF NOT EXISTS items_active_idx ON items (account_id, status) WHERE status = 'active';

-- =========================================================
-- Events (log table with BIGSERIAL, JSONB, composite PK usage)
-- =========================================================
CREATE TABLE IF NOT EXISTS events
(
    id          BIGSERIAL PRIMARY KEY,
    account_id  UUID        NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
    event_type  TEXT        NOT NULL,
    payload     JSONB       NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed   BOOLEAN     NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS events_account_idx ON events (account_id, received_at DESC);

-- =========================================================
-- Tags (many-to-many with items, composite PK)
-- =========================================================
CREATE TABLE IF NOT EXISTS tags
(
    tag_id     UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_id UUID        NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
    name       TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (account_id, name)
);

CREATE TABLE IF NOT EXISTS item_tags
(
    item_id UUID NOT NULL REFERENCES items (item_id) ON DELETE CASCADE,
    tag_id  UUID NOT NULL REFERENCES tags (tag_id) ON DELETE CASCADE,
    PRIMARY KEY (item_id, tag_id)
);

-- =========================================================
-- Notifications (INET, BYTEA, ON DELETE SET NULL)
-- =========================================================
CREATE TABLE IF NOT EXISTS notifications
(
    notification_id UUID PRIMARY KEY     DEFAULT uuid_generate_v4(),
    account_id      UUID        NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
    sent_by         UUID                 REFERENCES users (user_id) ON DELETE SET NULL,
    channel         TEXT        NOT NULL DEFAULT 'email',
    subject         TEXT,
    body            TEXT,
    metadata        JSONB,
    sent_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    success         BOOLEAN     NOT NULL DEFAULT TRUE
);
CREATE INDEX IF NOT EXISTS notifications_account_idx ON notifications (account_id);
CREATE INDEX IF NOT EXISTS notifications_sent_at_idx ON notifications (sent_at DESC);

-- =========================================================
-- Audit log (tracks changes)
-- =========================================================
CREATE TABLE IF NOT EXISTS audit_log
(
    id          BIGSERIAL PRIMARY KEY,
    account_id  UUID        NOT NULL,
    user_id     UUID,
    action      TEXT        NOT NULL,
    object_type TEXT        NOT NULL,
    object_id   TEXT,
    detail      JSONB,
    ip          INET,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS audit_log_account_idx ON audit_log (account_id, created_at DESC);

-- =========================================================
-- Invitations (ON DELETE SET NULL example)
-- =========================================================
CREATE TABLE IF NOT EXISTS invitations
(
    invitation_id UUID PRIMARY KEY     DEFAULT uuid_generate_v4(),
    account_id    UUID        NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
    invited_by    UUID                 REFERENCES users (user_id) ON DELETE SET NULL,
    email         TEXT        NOT NULL,
    status        TEXT        NOT NULL DEFAULT 'pending',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS invitations_account_idx ON invitations (account_id);
CREATE INDEX IF NOT EXISTS invitations_email_idx ON invitations (email);

-- =========================================================
-- Scheduled tasks (trigger + partial indexes)
-- =========================================================
CREATE TABLE IF NOT EXISTS scheduled_tasks
(
    task_id          UUID PRIMARY KEY     DEFAULT uuid_generate_v4(),
    account_id       UUID        NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
    name             TEXT        NOT NULL,
    interval_seconds INT         NOT NULL,
    retry_seconds    INT         NOT NULL,
    last_executed_at TIMESTAMPTZ,
    next_run_at      TIMESTAMPTZ,
    pending          BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS scheduled_tasks_account_idx ON scheduled_tasks (account_id);

-- Trigger function with bare $ dollar-quoting
CREATE OR REPLACE FUNCTION scheduled_tasks_compute_next_run() RETURNS trigger AS
$
BEGIN
  NEW.next_run_at := NEW.last_executed_at + (INTERVAL '1 second' * NEW.interval_seconds);
  RETURN NEW;
END;
$ LANGUAGE plpgsql;

CREATE TRIGGER scheduled_tasks_next_run_trg
    BEFORE INSERT OR UPDATE OF last_executed_at, interval_seconds, retry_seconds
    ON scheduled_tasks
    FOR EACH ROW
    EXECUTE FUNCTION scheduled_tasks_compute_next_run();

-- Partial indexes on scheduled_tasks
CREATE INDEX IF NOT EXISTS scheduled_tasks_due_idx
    ON scheduled_tasks (next_run_at)
    WHERE
        pending = false
            AND interval_seconds > 0
            AND last_executed_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS scheduled_tasks_pending_idx
    ON scheduled_tasks (task_id)
    WHERE
        pending = true;

-- =========================================================
-- Task runs (BIGSERIAL, INET)
-- =========================================================
CREATE TABLE IF NOT EXISTS task_runs
(
    id      BIGSERIAL PRIMARY KEY,
    task_id UUID        NOT NULL REFERENCES scheduled_tasks (task_id) ON DELETE CASCADE,
    at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    ip      INET,
    detail  TEXT
);
CREATE INDEX IF NOT EXISTS task_runs_task_at_idx ON task_runs (task_id, at DESC);

-- =========================================================
-- Network lookup cache (CIDR type)
-- =========================================================
CREATE TABLE IF NOT EXISTS network_cache
(
    cidr         CIDR        NOT NULL,
    data         TEXT        NOT NULL,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT network_cache_pkey PRIMARY KEY (cidr)
);
CREATE INDEX IF NOT EXISTS network_cache_refreshed_at_idx ON network_cache (refreshed_at);

-- =========================================================
-- Rate limits (generic KV-based window buckets)
-- =========================================================
CREATE TABLE IF NOT EXISTS rate_limits
(
    key            TEXT PRIMARY KEY,
    counter        INT         NOT NULL DEFAULT 0,
    window_start   TIMESTAMPTZ NOT NULL DEFAULT now(),
    window_seconds INT         NOT NULL DEFAULT 3600,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =========================================================
-- Config entries (key-value, VARCHAR PK)
-- =========================================================
CREATE TABLE IF NOT EXISTS config_entries
(
    key        VARCHAR     NOT NULL PRIMARY KEY,
    value      TEXT        NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =========================================================
-- Custom locks
-- =========================================================
CREATE TABLE custom_locks (
    lock_key    TEXT PRIMARY KEY,
    owner_id    TEXT NOT NULL,
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ NOT NULL
);

-- =========================================================
-- Duplicate CREATE INDEX IF NOT EXISTS (second should be silently ignored)
-- =========================================================
CREATE INDEX IF NOT EXISTS notifications_sent_at_idx ON notifications (sent_at);

-- =========================================================
-- Expression index with lower() and operator class (text_pattern_ops)
-- =========================================================
CREATE INDEX IF NOT EXISTS tags_name_pattern_idx ON tags (account_id, lower(name) text_pattern_ops);

-- =========================================================
-- View
-- =========================================================
CREATE OR REPLACE VIEW v_account_plan AS
SELECT a.account_id,
       a.name,
       a.plan_key,
       p.name AS plan_name,
       p.monthly_cost,
       p.max_items,
       p.max_users
FROM accounts a
         JOIN plans p ON p.key = a.plan_key;

-- =========================================================
-- Seed plans (ON CONFLICT DO UPDATE)
-- =========================================================
INSERT INTO plans(key, name, monthly_cost, visibility, max_items, max_users)
VALUES ('free', 'Plan 1', 0.00, 'public', 5, 1),
       ('plan2', 'Plan 2', 9.50, 'public', 50, 3),
       ('enterprise', 'Plan 3', 49.50, 'public', 500, 25)
ON CONFLICT (key) DO UPDATE
    SET name=EXCLUDED.name,
        monthly_cost=EXCLUDED.monthly_cost,
        visibility=EXCLUDED.visibility,
        max_items=EXCLUDED.max_items,
        max_users=EXCLUDED.max_users,
        updated_at=now();

-- =========================================================
-- Seed config_entries (ON CONFLICT DO NOTHING)
-- =========================================================
INSERT INTO config_entries (key, value)
VALUES ('theme', 'dark'),
       ('currency', 'USD'),
       ('page_size', '25')
ON CONFLICT (key) DO NOTHING;
