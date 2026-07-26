-- ============================================================================
-- Feature Comparison: jsonpath modes and regexp_replace replacement text
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- A jsonpath may open with a strict or lax mode word; lax is the default and
-- returns nothing for a missing step, while strict makes it an error. In a
-- regexp_replace replacement only a numbered backref and the whole-match
-- reference are special -- a dollar sign is literal text, not a group ref.
-- ============================================================================

-- ============================================================================
-- 1. The mode word is part of the path
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT jsonb_path_query('{"b":1}','strict $.b')::text AS a;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT jsonb_path_query('{"b":1}','lax $.b')::text AS a;

-- lax unwraps a scalar for an array accessor
-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT jsonb_path_query('1','lax $[*]')::text AS a;

-- ============================================================================
-- 2. strict turns a missing step into an error, lax into no rows
-- ============================================================================

-- begin-expected-error
-- sqlstate: 2203A
-- message-like: does not contain key "b"
-- end-expected-error
SELECT jsonb_path_query('{"a":1}','strict $.b');

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM jsonb_path_query('{"a":1}','lax $.b') q;

-- begin-expected-error
-- sqlstate: 2203A
-- message-like: does not contain key "b"
-- end-expected-error
SELECT jsonb_path_exists('{"a":1}','strict $.b');

-- Filter variables still bind
-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n FROM jsonb_path_query('[1,2,3]','$[*] ? (@ > $min)','{"min":1}') q;

-- ============================================================================
-- 3. A dollar sign in a replacement is literal text
-- ============================================================================

-- begin-expected
-- columns: a
-- row: ax$yc
-- end-expected
SELECT regexp_replace('abc','b','x$y') AS a;

-- begin-expected
-- columns: a
-- row: a$1c
-- end-expected
SELECT regexp_replace('abc','b','$1') AS a;

-- begin-expected
-- columns: a
-- row: a[b]c
-- end-expected
SELECT regexp_replace('abc','(b)','[\1]') AS a;

-- A backref to a group that does not exist substitutes nothing
-- begin-expected
-- columns: a
-- row: ac
-- end-expected
SELECT regexp_replace('abc','b','\1') AS a;

-- begin-expected
-- columns: a
-- row: a<b>c
-- end-expected
SELECT regexp_replace('abc','(b)','<\&>') AS a;
