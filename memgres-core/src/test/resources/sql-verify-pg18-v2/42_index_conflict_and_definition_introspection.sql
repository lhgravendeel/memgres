\pset pager off
\pset format unaligned
\pset tuples_only off
\pset null <NULL>
\set VERBOSITY verbose
\set SHOW_CONTEXT always
\set ON_ERROR_STOP off

DROP SCHEMA IF EXISTS compat CASCADE;
CREATE SCHEMA compat;
SET search_path = compat, pg_catalog;
SET client_min_messages = notice;
SET extra_float_digits = 0;
SET DateStyle = 'ISO, YMD';
SET IntervalStyle = 'postgres';
SET TimeZone = 'UTC';

SELECT current_schema() AS current_schema,
       current_setting('TimeZone') AS timezone,
       current_setting('DateStyle') AS datestyle,
       current_setting('IntervalStyle') AS intervalstyle;

CREATE TABLE idx_t(
  id int PRIMARY KEY,
  tenant_id int NOT NULL,
  email text,
  status text NOT NULL DEFAULT 'active',
  note text
);

INSERT INTO idx_t VALUES
(1, 10, 'a@example.com', 'active', 'x'),
(2, 10, 'b@example.com', 'inactive', 'y'),
(3, 20, 'a@example.com', 'active', 'z');

CREATE UNIQUE INDEX idx_t_email_active_uq
  ON idx_t(lower(email))
  WHERE status = 'active';

CREATE INDEX idx_t_note_expr_idx
  ON idx_t ((coalesce(note, '')));

CREATE UNIQUE INDEX idx_t_tenant_email_uq
  ON idx_t(tenant_id, email);

SELECT pg_get_indexdef('idx_t_email_active_uq'::regclass);
SELECT pg_get_indexdef('idx_t_note_expr_idx'::regclass);
SELECT pg_get_indexdef('idx_t_tenant_email_uq'::regclass);

SELECT i.indexrelid::regclass, i.indisunique, i.indpred IS NOT NULL AS has_predicate
FROM pg_index i
WHERE i.indrelid = 'compat.idx_t'::regclass
ORDER BY i.indexrelid::regclass::text;

-- ON CONFLICT inference and edge cases
INSERT INTO idx_t(id, tenant_id, email, status, note)
VALUES (4, 10, 'b@example.com', 'active', 'n')
ON CONFLICT (tenant_id, email) DO UPDATE
SET note = EXCLUDED.note
RETURNING *;

INSERT INTO idx_t(id, tenant_id, email, status, note)
VALUES (5, 10, 'c@example.com', 'active', 'm')
ON CONFLICT (tenant_id, email) DO NOTHING
RETURNING *;

INSERT INTO idx_t(id, tenant_id, email, status, note)
VALUES (6, 30, 'a@example.com', 'active', 'p')
ON CONFLICT ((lower(email))) WHERE status = 'active' DO UPDATE
SET note = EXCLUDED.note
RETURNING *;

-- bad index / conflict cases
CREATE INDEX idx_bad_pred ON idx_t(id) WHERE nope > 0;
CREATE UNIQUE INDEX idx_bad_vol ON idx_t ((random()));
INSERT INTO idx_t(id, tenant_id, email, status, note)
VALUES (7, 10, 'q@example.com', 'active', 'q')
ON CONFLICT (email) DO NOTHING;
INSERT INTO idx_t(id, tenant_id, email, status, note)
VALUES (8, 10, 'r@example.com', 'active', 'r')
ON CONFLICT ((lower(email))) DO NOTHING;
SELECT pg_get_indexdef('missing_idx'::regclass);

DROP SCHEMA compat CASCADE;
