-- ============================================================================
-- Feature Comparison: Round 14 — Advanced regex surfaces
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

DROP SCHEMA IF EXISTS r14_re CASCADE;
CREATE SCHEMA r14_re;
SET search_path = r14_re, public;

-- ============================================================================
-- SECTION A: Embedded regex flags
-- ============================================================================

-- 1. (?x) extended / whitespace-allowed mode
-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT ('hello' ~ '(?x) h e l l o ')::text AS v;

-- 2. (?n) newline-sensitive
-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT (E'line1\nline2' ~ '(?n)^line2$')::text AS v;

-- 3. (?i) case-insensitive
-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT ('HELLO' ~ '(?i)hello')::text AS v;

-- 4. (?s) . matches newline
-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT (E'a\nb' ~ '(?s)a.b')::text AS v;

-- ============================================================================
-- SECTION B: regexp_substr
-- ============================================================================

-- 5. Basic
-- begin-expected
-- columns: v
-- row: foo
-- end-expected
SELECT regexp_substr('foobar', 'foo') AS v;

-- 6. With start position, occurrence
-- begin-expected
-- columns: v
-- row: bar
-- end-expected
SELECT regexp_substr('foobar foobar', 'foo|bar', 1, 2) AS v;

-- 7. With capture group
-- begin-expected
-- columns: v
-- row: 42
-- end-expected
SELECT regexp_substr('abc=42', '([a-z]+)=([0-9]+)', 1, 1, 'c', 2) AS v;

-- ============================================================================
-- SECTION C: regexp_count
-- ============================================================================

-- 8. Basic
-- begin-expected
-- columns: v
-- row: 3
-- end-expected
SELECT regexp_count('aaa', 'a')::text AS v;

-- 9. With start position
-- begin-expected
-- columns: v
-- row: 2
-- end-expected
SELECT regexp_count('aaaa', 'a', 3)::text AS v;

-- 10. With flags
-- begin-expected
-- columns: v
-- row: 3
-- end-expected
SELECT regexp_count('AaA', 'a', 1, 'i')::text AS v;

-- ============================================================================
-- SECTION D: regexp_instr
-- ============================================================================

-- 11. Basic
-- begin-expected
-- columns: v
-- row: 1
-- end-expected
SELECT regexp_instr('foobar', 'foo')::text AS v;

-- 12. Second occurrence
-- begin-expected
-- columns: v
-- row: 5
-- end-expected
SELECT regexp_instr('foo foo', 'foo', 1, 2)::text AS v;

-- 13. Return position after match
-- begin-expected
-- columns: v
-- row: 4
-- end-expected
SELECT regexp_instr('foobar', 'foo', 1, 1, 1)::text AS v;

-- ============================================================================
-- SECTION E: regexp_replace occurrence param
-- ============================================================================

-- 14. Replace only Nth occurrence
-- begin-expected
-- columns: v
-- row: aXa
-- end-expected
SELECT regexp_replace('aaa', 'a', 'X', 1, 2) AS v;

-- ============================================================================
-- SECTION F: regexp_like
-- ============================================================================

-- 15. Basic (PG 17+)
-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT regexp_like('abc', 'b')::text AS v;

-- 16. With flags
-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT regexp_like('ABC', 'abc', 'i')::text AS v;

-- ============================================================================
-- SECTION G: SUBSTRING regex with group
-- ============================================================================

-- 17. Capture group extraction via SUBSTRING
-- begin-expected
-- columns: v
-- row: 123
-- end-expected
SELECT SUBSTRING('abc123' FROM '([0-9]+)') AS v;
