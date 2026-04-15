-- ============================================================================
-- Feature Comparison: String Collation Ordering
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PG 18 uses locale-specific collation for text ordering. With the default
-- en_US.UTF-8 locale, strings are sorted case-insensitively first, then by case.
-- The COLLATE clause on ORDER BY, indexes, and column definitions affects sort order.
--
-- Memgres uses Java String.compareTo() which is binary UTF-8 ordering.
-- COLLATE is parsed but has no effect on sort order.
--
-- Key ordering difference:
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

-- note: PG en_US.UTF-8: sorts case-insensitively first, then by case.
-- note: Memgres binary: sorts by byte value (uppercase before lowercase).

-- begin-expected
-- columns: word
-- row: a
-- row: A
-- row: b
-- row: B
-- end-expected
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

-- begin-expected
-- columns: min_word, max_word
-- row: a, B
-- end-expected
SELECT min(word COLLATE "en_US.utf8") AS min_word,
       max(word COLLATE "en_US.utf8") AS max_word
FROM collation_data;

-- ============================================================================
-- 4. Comparison operators under collation
-- ============================================================================

-- note: Under en_US.utf8, 'a' < 'A' is true (lowercase sorts first).
-- note: Under C/binary, 'a' < 'A' is false (a=97 > A=65).

-- begin-expected
-- columns: a_before_cap_a
-- row: true
-- end-expected
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

CREATE INDEX collation_data_idx ON collation_data (word COLLATE "en_US.utf8");

-- begin-expected
-- columns: word
-- row: a
-- row: A
-- row: b
-- row: B
-- end-expected
SELECT word FROM collation_data ORDER BY word COLLATE "en_US.utf8";

DROP INDEX collation_data_idx;

-- ============================================================================
-- 7. Mixed-case sort with more data points
-- ============================================================================

DROP TABLE IF EXISTS collation_mixed;
CREATE TABLE collation_mixed (name text);
INSERT INTO collation_mixed VALUES
  ('Charlie'), ('alice'), ('Bob'), ('charlie'), ('Alice'), ('bob');

-- begin-expected
-- columns: name
-- row: alice
-- row: Alice
-- row: bob
-- row: Bob
-- row: charlie
-- row: Charlie
-- end-expected
SELECT name FROM collation_mixed ORDER BY name COLLATE "en_US.utf8";

-- ============================================================================
-- Cleanup
-- ============================================================================
DROP TABLE collation_data;
DROP TABLE collation_mixed;
