-- ============================================================================
-- Feature Comparison: the width of a count argument
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- These routines are declared with an integer count and PostgreSQL has no
-- bigint overload, so a wider count means no such function rather than a
-- value to narrow. Narrowing it turned a count of four billion into an empty
-- string, which is indistinguishable from a legitimate result.
-- ============================================================================

DROP TABLE IF EXISTS scw_t CASCADE;
CREATE TABLE scw_t (t text, n int);
INSERT INTO scw_t VALUES ('abcde', 3);

-- ============================================================================
-- 1. A count wider than integer has no matching function
-- ============================================================================
SELECT left('abcde', 4294967296) AS a;
SELECT right('abcde', 4294967296) AS a;
SELECT repeat('ab', 4294967296) AS a;
SELECT lpad('abc', 4294967296, 'x') AS a;
SELECT rpad('abc', 4294967296, 'x') AS a;
SELECT substr('abcde', 4294967296) AS a;
SELECT substr('abcde', 1, 4294967296) AS a;
SELECT split_part('a,b', ',', 4294967296) AS a;
-- the same through a column rather than a literal
SELECT left(t, 4294967296) AS a FROM scw_t;
SELECT repeat(t, 4294967296) AS a FROM scw_t;
SELECT lpad(t, 4294967296, 'x') AS a FROM scw_t;
-- and with an explicit cast to bigint
SELECT left('abcde', 4294967296::bigint) AS a;
SELECT left('abcde', 2147483648) AS a;
-- a negative value beyond the integer range is equally unmatched
SELECT left('abcde', -4294967296) AS a;

-- ============================================================================
-- 2. Counts that fit are unaffected
-- ============================================================================
SELECT left('abcde', 2147483647) AS a;
SELECT left('abcde', 2) AS a;
SELECT right('abcde', 2) AS a;
SELECT left('abcde', -1) AS a;
SELECT right('abcde', -1) AS a;
SELECT left('abcde', 0) AS a;
SELECT repeat('ab', 3) AS a;
SELECT repeat('ab', 0) AS a;
SELECT lpad('abc', 6, 'xy') AS a;
SELECT rpad('abc', 6, 'xy') AS a;
SELECT substr('abcde', 2) AS a;
SELECT substr('abcde', 2, 2) AS a;
SELECT split_part('a,b,c', ',', 2) AS a;
SELECT split_part('a,b,c', ',', -1) AS a;
SELECT left('abcde', 3::smallint) AS a;
SELECT left('abcde', n) AS a FROM scw_t;
SELECT repeat('ab', n) AS a FROM scw_t;
SELECT left('abcde', 2147483647::int) AS a;

-- ============================================================================
-- 3. NULL counts stay strict rather than becoming a width complaint
-- ============================================================================
SELECT coalesce(left('abcde', NULL), 'NULL') AS a;
SELECT coalesce(right('abcde', NULL), 'NULL') AS a;
SELECT coalesce(repeat('ab', NULL), 'NULL') AS a;
SELECT coalesce(lpad('abc', NULL, 'x'), 'NULL') AS a;
SELECT coalesce(rpad('abc', NULL, 'x'), 'NULL') AS a;
SELECT coalesce(split_part('a,b', ',', NULL), 'NULL') AS a;
SELECT coalesce(substr(NULL, 1, 2), 'NULL') AS a;

DROP TABLE IF EXISTS scw_t CASCADE;
