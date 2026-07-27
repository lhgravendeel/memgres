-- ============================================================================
-- Feature Comparison: NULL arguments to strict string functions
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- These functions are strict: a NULL in any argument makes the whole call NULL.
-- A fill, delimiter or pattern that is NULL must not be rendered as the text
-- "null" and used as data, nor reach an unguarded dereference.
-- ============================================================================

-- ============================================================================
-- 1. Padding fill characters
-- ============================================================================
SELECT coalesce(rpad('abc', 10, NULL), 'NULL') AS a;
SELECT coalesce(lpad('abc', 10, NULL), 'NULL') AS a;
SELECT coalesce(rpad(NULL, 10, 'x'), 'NULL') AS a;
SELECT coalesce(lpad(NULL, 10, 'x'), 'NULL') AS a;
SELECT coalesce(rpad('abc', NULL, 'x'), 'NULL') AS a;
SELECT coalesce(lpad('abc', NULL, 'x'), 'NULL') AS a;
-- the ordinary forms are unaffected
SELECT rpad('abc', 6, 'xy') AS a;
SELECT lpad('abc', 6, 'xy') AS a;
SELECT rpad('abc', 6) AS a;
SELECT lpad('abc', 6) AS a;
SELECT rpad('abcdef', 3, 'x') AS a;
SELECT lpad('abcdef', 3, 'x') AS a;

-- ============================================================================
-- 2. replace and translate
-- ============================================================================
SELECT coalesce(replace('abc', NULL, 'x'), 'NULL') AS a;
SELECT coalesce(replace('abc', 'b', NULL), 'NULL') AS a;
SELECT coalesce(replace(NULL, 'b', 'x'), 'NULL') AS a;
SELECT coalesce(translate('abc', NULL, 'x'), 'NULL') AS a;
SELECT coalesce(translate('abc', 'a', NULL), 'NULL') AS a;
SELECT coalesce(translate(NULL, 'a', 'x'), 'NULL') AS a;
SELECT replace('abcabc', 'b', 'X') AS a;
SELECT translate('abcdef', 'ace', 'xyz') AS a;
SELECT translate('abcdef', 'ace', 'x') AS a;

-- ============================================================================
-- 3. Delimiters
-- ============================================================================
SELECT coalesce(split_part('a,b,c', NULL, 1), 'NULL') AS a;
SELECT coalesce(split_part(NULL, ',', 1), 'NULL') AS a;
SELECT coalesce(split_part('a,b,c', ',', NULL), 'NULL') AS a;
SELECT split_part('a,b,c', ',', 2) AS a;
SELECT split_part('a,b,c', ',', -1) AS a;
SELECT coalesce(array_to_string(ARRAY[1,2,3], NULL), 'NULL') AS a;
SELECT coalesce(array_to_string(NULL::int[], ','), 'NULL') AS a;
SELECT array_to_string(ARRAY[1,2,3], ',') AS a;
SELECT array_to_string(ARRAY[1,NULL,3], ',') AS a;
SELECT array_to_string(ARRAY[1,NULL,3], ',', '?') AS a;
SELECT coalesce(concat_ws(NULL, 'a', 'b'), 'NULL') AS a;
SELECT concat_ws(',', 'a', NULL, 'b') AS a;

-- ============================================================================
-- 4. Overlay and other placement arguments
-- ============================================================================
SELECT coalesce(overlay('abc' placing NULL from 2), 'NULL') AS a;
SELECT coalesce(overlay(NULL placing 'X' from 2), 'NULL') AS a;
SELECT coalesce(overlay('abc' placing 'X' from NULL), 'NULL') AS a;
SELECT overlay('abcdef' placing 'XY' from 2) AS a;
SELECT overlay('abcdef' placing 'XY' from 2 for 3) AS a;
SELECT coalesce(overlay('abc' placing 'X' from 2 for NULL), 'NULL') AS a;

-- ============================================================================
-- 5. Neighbouring functions that were already strict, to keep them so
-- ============================================================================
SELECT coalesce(left(NULL, 2), 'NULL') AS a;
SELECT coalesce(right(NULL, 2), 'NULL') AS a;
SELECT coalesce(repeat('a', NULL), 'NULL') AS a;
SELECT coalesce(repeat(NULL, 2), 'NULL') AS a;
SELECT coalesce(strpos('abc', NULL)::text, 'NULL') AS a;
SELECT coalesce(strpos(NULL, 'b')::text, 'NULL') AS a;
SELECT coalesce(substr(NULL, 1, 2), 'NULL') AS a;
SELECT coalesce(btrim(NULL, 'x'), 'NULL') AS a;
SELECT coalesce(btrim('xax', NULL), 'NULL') AS a;
SELECT coalesce(ltrim('xax', NULL), 'NULL') AS a;
SELECT coalesce(rtrim('xax', NULL), 'NULL') AS a;
SELECT coalesce(upper(NULL), 'NULL') AS a;
SELECT coalesce(md5(NULL), 'NULL') AS a;
SELECT coalesce(length(NULL)::text, 'NULL') AS a;

-- ============================================================================
-- 6. Column-driven, so the NULLs arrive as data rather than literals
-- ============================================================================
DROP TABLE IF EXISTS sfn_t CASCADE;
CREATE TABLE sfn_t (s text, fill text, delim text);
INSERT INTO sfn_t VALUES ('abc', 'x', ','), ('abc', NULL, ','), ('abc', 'x', NULL), (NULL, 'x', ',');
SELECT string_agg(coalesce(rpad(s, 6, fill), 'NULL'), '|' ORDER BY o) AS a FROM (
  SELECT row_number() OVER () AS o, s, fill FROM sfn_t) t;
SELECT string_agg(coalesce(replace(s, 'b', fill), 'NULL'), '|' ORDER BY o) AS a FROM (
  SELECT row_number() OVER () AS o, s, fill FROM sfn_t) t;
SELECT string_agg(coalesce(split_part(s, delim, 1), 'NULL'), '|' ORDER BY o) AS a FROM (
  SELECT row_number() OVER () AS o, s, delim FROM sfn_t) t;
SELECT count(*)::text AS a FROM sfn_t WHERE rpad(s, 6, fill) IS NULL;
SELECT count(*)::text AS a FROM sfn_t WHERE translate(s, 'a', fill) IS NULL;

DROP TABLE IF EXISTS sfn_t CASCADE;
