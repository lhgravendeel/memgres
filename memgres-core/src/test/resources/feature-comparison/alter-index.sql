-- ============================================================================
-- Feature Comparison: ALTER INDEX (A8)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS ai_test CASCADE;
CREATE SCHEMA ai_test;
SET search_path = ai_test, public;

CREATE TABLE ai_data (id integer PRIMARY KEY, val integer, label text);
INSERT INTO ai_data VALUES (1, 10, 'alpha'), (2, 20, 'beta'), (3, 30, 'gamma');

-- ============================================================================
-- 1. ALTER INDEX RENAME TO
-- ============================================================================

CREATE INDEX idx_ai_val ON ai_data (val);

ALTER INDEX idx_ai_val RENAME TO idx_ai_val_renamed;

-- begin-expected
-- columns: exists
-- row: true
-- end-expected
SELECT EXISTS(
  SELECT 1 FROM pg_class WHERE relname = 'idx_ai_val_renamed' AND relkind = 'i'
) AS exists;

-- Old name gone
-- begin-expected
-- columns: exists
-- row: false
-- end-expected
SELECT EXISTS(
  SELECT 1 FROM pg_class WHERE relname = 'idx_ai_val' AND relkind = 'i'
) AS exists;

-- ============================================================================
-- 2. ALTER INDEX IF EXISTS RENAME TO (exists)
-- ============================================================================

ALTER INDEX IF EXISTS idx_ai_val_renamed RENAME TO idx_ai_val2;

-- begin-expected
-- columns: exists
-- row: true
-- end-expected
SELECT EXISTS(
  SELECT 1 FROM pg_class WHERE relname = 'idx_ai_val2' AND relkind = 'i'
) AS exists;

-- ============================================================================
-- 3. ALTER INDEX IF EXISTS RENAME TO (does not exist — no error)
-- ============================================================================

-- command: ALTER INDEX
ALTER INDEX IF EXISTS idx_nonexistent RENAME TO idx_something;

-- ============================================================================
-- 4. ALTER INDEX RENAME on non-existent index (error)
-- ============================================================================

-- begin-expected-error
-- message-like: does not exist
-- end-expected-error
ALTER INDEX idx_nonexistent RENAME TO idx_something;

-- ============================================================================
-- 5. Rename PK index
-- ============================================================================

-- note: Primary key creates an index automatically
-- begin-expected
-- columns: exists
-- row: true
-- end-expected
SELECT EXISTS(
  SELECT 1 FROM pg_class WHERE relname = 'ai_data_pkey' AND relkind = 'i'
) AS exists;

ALTER INDEX ai_data_pkey RENAME TO ai_data_pk_renamed;

-- begin-expected
-- columns: exists
-- row: true
-- end-expected
SELECT EXISTS(
  SELECT 1 FROM pg_class WHERE relname = 'ai_data_pk_renamed' AND relkind = 'i'
) AS exists;

-- PK still functional
-- begin-expected-error
-- message-like: duplicate key value violates unique constraint
-- end-expected-error
INSERT INTO ai_data VALUES (1, 99, 'dup');

-- Rename back for cleanup
ALTER INDEX ai_data_pk_renamed RENAME TO ai_data_pkey;

-- ============================================================================
-- 6. Rename UNIQUE index
-- ============================================================================

CREATE UNIQUE INDEX idx_ai_label ON ai_data (label);

ALTER INDEX idx_ai_label RENAME TO idx_ai_label_renamed;

-- Uniqueness still enforced
-- begin-expected-error
-- message-like: duplicate key value violates unique constraint
-- end-expected-error
INSERT INTO ai_data VALUES (4, 40, 'alpha');

DROP INDEX idx_ai_label_renamed;

-- ============================================================================
-- 7. ALTER INDEX SET SCHEMA
-- ============================================================================

CREATE SCHEMA ai_other;
CREATE INDEX idx_ai_move ON ai_data (val);

ALTER INDEX idx_ai_move SET SCHEMA ai_other;

-- begin-expected
-- columns: nspname
-- row: ai_test
-- end-expected
SELECT n.nspname
FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE c.relname = 'idx_ai_move';

DROP SCHEMA ai_other CASCADE;

-- ============================================================================
-- 8. Rename expression index
-- ============================================================================

CREATE INDEX idx_ai_expr ON ai_data (lower(label));

ALTER INDEX idx_ai_expr RENAME TO idx_ai_expr_renamed;

-- begin-expected
-- columns: exists
-- row: true
-- end-expected
SELECT EXISTS(
  SELECT 1 FROM pg_class WHERE relname = 'idx_ai_expr_renamed' AND relkind = 'i'
) AS exists;

DROP INDEX idx_ai_expr_renamed;

-- ============================================================================
-- 9. Rename to duplicate name (error)
-- ============================================================================

CREATE INDEX idx_ai_dup1 ON ai_data (val);
CREATE INDEX idx_ai_dup2 ON ai_data (label);

-- begin-expected-error
-- message-like: already exists
-- end-expected-error
ALTER INDEX idx_ai_dup1 RENAME TO idx_ai_dup2;

DROP INDEX idx_ai_dup1;
DROP INDEX idx_ai_dup2;

-- ============================================================================
-- 10. Multiple indexes, rename one — others unaffected
-- ============================================================================

CREATE INDEX idx_ai_a ON ai_data (val);
CREATE INDEX idx_ai_b ON ai_data (label);

ALTER INDEX idx_ai_a RENAME TO idx_ai_a_new;

-- a renamed
-- begin-expected
-- columns: exists
-- row: true
-- end-expected
SELECT EXISTS(
  SELECT 1 FROM pg_class WHERE relname = 'idx_ai_a_new' AND relkind = 'i'
) AS exists;

-- b unchanged
-- begin-expected
-- columns: exists
-- row: true
-- end-expected
SELECT EXISTS(
  SELECT 1 FROM pg_class WHERE relname = 'idx_ai_b' AND relkind = 'i'
) AS exists;

DROP INDEX idx_ai_a_new;
DROP INDEX idx_ai_b;

-- ============================================================================
-- 11. COMMENT ON INDEX
-- ============================================================================

CREATE INDEX idx_ai_comment ON ai_data (val);

COMMENT ON INDEX idx_ai_comment IS 'This is a test index';

-- begin-expected
-- columns: description
-- row: This is a test index
-- end-expected
SELECT d.description
FROM pg_description d
JOIN pg_class c ON d.objoid = c.oid
WHERE c.relname = 'idx_ai_comment';

-- Remove comment
COMMENT ON INDEX idx_ai_comment IS NULL;

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::integer AS cnt
FROM pg_description d
JOIN pg_class c ON d.objoid = c.oid
WHERE c.relname = 'idx_ai_comment';

DROP INDEX idx_ai_comment;

-- ============================================================================
-- 12. pg_class reflects renamed index
-- ============================================================================

CREATE INDEX idx_ai_catalog ON ai_data (val);

-- begin-expected
-- columns: relname, relkind
-- row: idx_ai_catalog, i
-- end-expected
SELECT relname, relkind FROM pg_class WHERE relname = 'idx_ai_catalog';

ALTER INDEX idx_ai_catalog RENAME TO idx_ai_catalog_new;

-- begin-expected
-- columns: relname
-- row: idx_ai_catalog_new
-- end-expected
SELECT relname FROM pg_class WHERE relname = 'idx_ai_catalog_new';

-- Old name gone from catalog
-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::integer AS cnt FROM pg_class WHERE relname = 'idx_ai_catalog';

DROP INDEX idx_ai_catalog_new;

-- ============================================================================
-- 13. Index still functional after rename
-- ============================================================================

CREATE INDEX idx_ai_func ON ai_data (val);
ALTER INDEX idx_ai_func RENAME TO idx_ai_func_new;

-- Index should still work for queries
-- begin-expected
-- columns: id
-- row: 2
-- end-expected
SELECT id FROM ai_data WHERE val = 20;

DROP INDEX idx_ai_func_new;

-- ============================================================================
-- 14. ALTER INDEX on partial index
-- ============================================================================

CREATE INDEX idx_ai_partial ON ai_data (val) WHERE val > 50;
ALTER INDEX idx_ai_partial RENAME TO idx_ai_partial_new;

-- begin-expected
-- columns: relname
-- row: idx_ai_partial_new
-- end-expected
SELECT relname FROM pg_class WHERE relname = 'idx_ai_partial_new';

-- Partial index still functional
-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::integer AS cnt FROM ai_data WHERE val > 50;

DROP INDEX idx_ai_partial_new;

-- ============================================================================
-- 15. ALTER INDEX SET storage parameter
-- ============================================================================

CREATE INDEX idx_ai_storage ON ai_data (val);

ALTER INDEX idx_ai_storage SET (fillfactor = 70);

-- begin-expected
-- columns: has_option
-- row: true
-- end-expected
SELECT (reloptions @> ARRAY['fillfactor=70']) AS has_option
FROM pg_class WHERE relname = 'idx_ai_storage';

ALTER INDEX idx_ai_storage RESET (fillfactor);

-- begin-expected
-- columns: no_options
-- row: true
-- end-expected
SELECT (reloptions IS NULL OR NOT reloptions @> ARRAY['fillfactor=70']) AS no_options
FROM pg_class WHERE relname = 'idx_ai_storage';

DROP INDEX idx_ai_storage;

-- ============================================================================
-- 16. Schema-qualified ALTER INDEX
-- ============================================================================

CREATE SCHEMA ai_other;
CREATE TABLE ai_other.data2 (id integer PRIMARY KEY, val integer);
CREATE INDEX idx_ai_other ON ai_other.data2 (val);

ALTER INDEX ai_other.idx_ai_other RENAME TO idx_ai_other_new;

-- begin-expected
-- columns: relname
-- row: idx_ai_other_new
-- end-expected
SELECT relname FROM pg_class WHERE relname = 'idx_ai_other_new';

DROP SCHEMA ai_other CASCADE;

-- ============================================================================
-- 17. ALTER INDEX on multicolumn index
-- ============================================================================

CREATE INDEX idx_ai_multi ON ai_data (id, val);
ALTER INDEX idx_ai_multi RENAME TO idx_ai_multi_new;

-- begin-expected
-- columns: relname
-- row: idx_ai_multi_new
-- end-expected
SELECT relname FROM pg_class WHERE relname = 'idx_ai_multi_new';

-- Still functional
-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM ai_data WHERE id = 1 AND val = 10;

DROP INDEX idx_ai_multi_new;

-- ============================================================================
-- 18. COMMENT ON INDEX set, change, and remove
-- ============================================================================

CREATE INDEX idx_ai_comment ON ai_data (val);

COMMENT ON INDEX idx_ai_comment IS 'initial comment';

-- begin-expected
-- columns: description
-- row: initial comment
-- end-expected
SELECT description FROM pg_description
WHERE objoid = 'idx_ai_comment'::regclass;

COMMENT ON INDEX idx_ai_comment IS 'updated comment';

-- begin-expected
-- columns: description
-- row: updated comment
-- end-expected
SELECT description FROM pg_description
WHERE objoid = 'idx_ai_comment'::regclass;

COMMENT ON INDEX idx_ai_comment IS NULL;

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::integer AS cnt FROM pg_description
WHERE objoid = 'idx_ai_comment'::regclass;

DROP INDEX idx_ai_comment;

-- ============================================================================
-- 19. ALTER INDEX on GIN index
-- ============================================================================

CREATE TABLE ai_gin_data (id integer PRIMARY KEY, tags text[]);
INSERT INTO ai_gin_data VALUES (1, ARRAY['a','b']), (2, ARRAY['b','c']), (3, ARRAY['c','d']);

CREATE INDEX idx_ai_gin ON ai_gin_data USING gin (tags);
ALTER INDEX idx_ai_gin RENAME TO idx_ai_gin_new;

-- begin-expected
-- columns: relname
-- row: idx_ai_gin_new
-- end-expected
SELECT relname FROM pg_class WHERE relname = 'idx_ai_gin_new';

-- GIN index still functional after rename
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM ai_gin_data WHERE tags @> ARRAY['b'] ORDER BY id;

DROP TABLE ai_gin_data;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA ai_test CASCADE;
SET search_path = public;
