-- ============================================================================
-- Feature Comparison: Identifier Length Limits (NAMEDATALEN = 64, max 63 chars)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PG 18: Identifiers are silently truncated to NAMEDATALEN-1 = 63 bytes.
-- Two identifiers that differ only after the 63rd character are treated as
-- the same name, causing collisions.
--
-- Memgres: No identifier length limit is enforced. Identifiers can be
-- arbitrarily long and are stored/compared in full.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- 1. Table name with 80 characters should be truncated to 63
-- ============================================================================

DROP TABLE IF EXISTS "taaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
DROP TABLE IF EXISTS "taaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

CREATE TABLE "taaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" (id int);

-- note: PG truncates the name to 63 chars. The actual stored name is 63 chars.

-- begin-expected
-- columns: name_len
-- row: 63
-- end-expected
SELECT length(tablename) AS name_len
FROM pg_tables
WHERE schemaname = 'public'
  AND tablename LIKE 'taaa%';

DROP TABLE IF EXISTS "taaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

-- ============================================================================
-- 2. Two names differing only at position 64+ should collide
-- ============================================================================

DROP TABLE IF EXISTS "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxaaaa";
DROP TABLE IF EXISTS "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxbbbb";

CREATE TABLE "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxaaaa" (id int);

-- note: Both names truncate to the same 63-char prefix.

-- begin-expected-error
-- message-like: already exists
-- end-expected-error
CREATE TABLE "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxbbbb" (id int);

DROP TABLE IF EXISTS "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";

-- ============================================================================
-- 3. Column name should also be truncated
-- ============================================================================

DROP TABLE IF EXISTS col_len_test;
CREATE TABLE col_len_test ("coooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooool" int);

-- begin-expected
-- columns: col_len
-- row: 63
-- end-expected
SELECT length(column_name) AS col_len
FROM information_schema.columns
WHERE table_name = 'col_len_test' AND table_schema = 'public';

DROP TABLE col_len_test;

-- ============================================================================
-- 4. Exactly 63 characters should work without truncation
-- ============================================================================

DROP TABLE IF EXISTS "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
CREATE TABLE "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" (id int);

-- begin-expected
-- columns: name_len
-- row: 63
-- end-expected
SELECT length(tablename) AS name_len
FROM pg_tables
WHERE tablename = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';

DROP TABLE "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

-- ============================================================================
-- 5. Index name truncation
-- ============================================================================

DROP TABLE IF EXISTS idx_len_test;
CREATE TABLE idx_len_test (id int);
CREATE INDEX "idxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" ON idx_len_test (id);

-- begin-expected
-- columns: idx_len
-- row: 63
-- end-expected
SELECT length(indexname) AS idx_len
FROM pg_indexes
WHERE tablename = 'idx_len_test' AND schemaname = 'public';

DROP TABLE idx_len_test;
