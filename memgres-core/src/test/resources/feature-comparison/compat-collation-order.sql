-- ============================================================================
-- Feature Comparison: String Collation Ordering
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PG 18 uses locale-specific collation for text ordering. The en_US.utf8
-- collation is only available when the OS has en_US.UTF-8 locale installed;
-- many minimal/Docker PG environments lack it, causing 42704 errors.
--
-- Memgres accepts any collation name and uses Java locale-aware comparison
-- for non-C/POSIX collations, binary comparison for C/POSIX.
--
-- Key ordering difference (when locale is available):
--   en_US.UTF-8:  a < A < b < B  (locale-aware)
--   C/binary:     A < B < a < b  (byte-value order)
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

DROP TABLE IF EXISTS collation_data;
CREATE TABLE collation_data (word text);
INSERT INTO collation_data VALUES ('b'), ('A'), ('a'), ('B');

-- ============================================================================
-- 1. ORDER BY with explicit en_US.utf8 collation
-- ============================================================================

-- note: PG errors if en_US.utf8 collation is not installed on the server OS.
-- note: Memgres accepts any collation name and applies locale-aware ordering.

-- begin-expected-error
-- sqlstate: 42704
-- message-like: collation
-- end-expected-error
SELECT word FROM collation_data ORDER BY word COLLATE "en_US.utf8";

-- ============================================================================
-- 2. ORDER BY with C collation (should match binary/Memgres ordering)
-- ============================================================================

-- note: C collation sorts by raw byte value, same as Memgres/Java.

-- begin-expected
-- columns: word
-- row: A
-- row: B
-- row: a
-- row: b
-- end-expected
SELECT word FROM collation_data ORDER BY word COLLATE "C";

-- ============================================================================
-- 3. MIN/MAX with locale collation
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42704
-- message-like: collation
-- end-expected-error
SELECT min(word COLLATE "en_US.utf8") AS min_word,
       max(word COLLATE "en_US.utf8") AS max_word
FROM collation_data;

-- ============================================================================
-- 4. Comparison operators under collation
-- ============================================================================

-- note: Under en_US.utf8, 'a' < 'A' is true (lowercase sorts first).
-- note: Under C/binary, 'a' < 'A' is false (a=97 > A=65).
-- note: PG errors if en_US.utf8 collation is not installed.

-- begin-expected-error
-- sqlstate: 42704
-- message-like: collation
-- end-expected-error
SELECT 'a' < 'A' COLLATE "en_US.utf8" AS a_before_cap_a;

-- ============================================================================
-- 5. DISTINCT with default collation (should preserve case)
-- ============================================================================

-- begin-expected
-- columns: cnt
-- row: 4
-- end-expected
SELECT count(DISTINCT word) AS cnt FROM collation_data;

-- ============================================================================
-- 6. Index with collation specification
-- ============================================================================

-- note: CREATE INDEX with en_US.utf8 also fails when collation not installed.
-- note: Skipping index test as it depends on collation availability.

-- begin-expected-error
-- sqlstate: 42704
-- message-like: collation
-- end-expected-error
CREATE INDEX collation_data_idx ON collation_data (word COLLATE "en_US.utf8");

-- begin-expected-error
-- sqlstate: 42704
-- message-like: collation
-- end-expected-error
SELECT word FROM collation_data ORDER BY word COLLATE "en_US.utf8";

DROP INDEX IF EXISTS collation_data_idx;

-- ============================================================================
-- 7. Mixed-case sort with more data points
-- ============================================================================

DROP TABLE IF EXISTS collation_mixed;
CREATE TABLE collation_mixed (name text);
INSERT INTO collation_mixed VALUES
  ('Charlie'), ('alice'), ('Bob'), ('charlie'), ('Alice'), ('bob');

-- begin-expected-error
-- sqlstate: 42704
-- message-like: collation
-- end-expected-error
SELECT name FROM collation_mixed ORDER BY name COLLATE "en_US.utf8";

-- ============================================================================
-- Cleanup
-- ============================================================================
DROP TABLE collation_data;
DROP TABLE collation_mixed;
