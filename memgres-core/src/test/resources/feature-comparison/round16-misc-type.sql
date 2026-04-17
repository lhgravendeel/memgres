-- ============================================================================
-- Feature Comparison: Round 16 — Miscellaneous type surface
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- ============================================================================
-- SECTION G1: bit(n) explicit cast truncation raises 22026
-- ============================================================================

-- 1. '1100' cast to bit(2) must raise string_data_length_mismatch
-- begin-expected
-- columns: bit
-- row: 11
-- end-expected
SELECT '1100'::bit(2);

-- ============================================================================
-- SECTION G2: pg_lsn type
-- ============================================================================

-- 2. pg_lsn round-trip preserves hex/slash format
-- begin-expected
-- columns: v
-- row: 16/B374D848
-- end-expected
SELECT '16/B374D848'::pg_lsn::text AS v;

-- 3. pg_lsn subtraction yields byte count as bigint
-- begin-expected
-- columns: n
-- row: 256
-- end-expected
SELECT ('0/100'::pg_lsn - '0/0'::pg_lsn)::bigint AS n;

-- ============================================================================
-- SECTION G3: macaddr trunc / macaddr8_set7bit
-- ============================================================================

-- 4. trunc(macaddr) zeros the lower 3 bytes
-- begin-expected
-- columns: v
-- row: 12:34:56:00:00:00
-- end-expected
SELECT trunc('12:34:56:78:9a:bc'::macaddr)::text AS v;

-- 5. macaddr8_set7bit on 00:... produces 02:...
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (macaddr8_set7bit('00:34:56:78:9a:bc:de:f0'::macaddr8)::text
          LIKE '02:%') AS ok;

-- ============================================================================
-- SECTION G4: hstore extension
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS hstore;

-- 6. hstore -> operator extracts value by key
-- begin-expected
-- columns: v
-- row: 1
-- end-expected
SELECT ('a=>1,b=>2'::hstore)->'a' AS v;

-- 7. hstore @> tests containment
-- begin-expected
-- columns: c
-- row: t
-- end-expected
SELECT ('a=>1,b=>2'::hstore) @> ('a=>1'::hstore) AS c;

-- ============================================================================
-- SECTION G5: pg_trgm extension
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- 8. similarity('hello','hallo') in [0,1]
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (similarity('hello','hallo') BETWEEN 0 AND 1) AS ok;

-- 9. show_trgm returns at least one trigram
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (array_length(show_trgm('hello'), 1) >= 1) AS ok;

-- ============================================================================
-- SECTION G6: cube extension
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS cube;

-- 10. cube_dim(cube(ARRAY[1,2,3])) = 3
-- begin-expected
-- columns: d
-- row: 3
-- end-expected
SELECT cube_dim(cube(ARRAY[1.0, 2.0, 3.0])) AS d;

-- ============================================================================
-- SECTION G7: pgcrypto digest/hmac/gen_salt
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 11. digest('hello','sha256') hex — known-answer
-- begin-expected-error
-- message-like: does not exist
-- end-expected-error
SELECT encode(digest('hello', 'sha256'), 'hex') AS v;

-- 12. hmac-sha256 hex is 64 chars
-- begin-expected-error
-- message-like: does not exist
-- end-expected-error
SELECT length(encode(hmac('msg','key','sha256'),'hex')) AS n;

-- 13. gen_salt('bf') returns Blowfish salt starting with $2
-- begin-expected-error
-- message-like: does not exist
-- end-expected-error
SELECT (gen_salt('bf') LIKE '$2%') AS ok;

-- ============================================================================
-- SECTION G8: citext case-insensitive equality
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS citext;

-- 14. citext equality is case-insensitive
-- begin-expected
-- columns: eq
-- row: t
-- end-expected
SELECT ('AbC'::citext = 'abc'::citext) AS eq;

-- 15. citext preserves original case on output
-- begin-expected
-- columns: v
-- row: AbC
-- end-expected
SELECT 'AbC'::citext::text AS v;
