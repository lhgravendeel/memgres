-- ============================================================================
-- Feature Comparison: regclass resolution
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- An all-digit string is an OID written out, which PostgreSQL takes verbatim
-- without a lookup -- that is how a catalog dump round-trips a regclass. Every
-- relation resolves by name, including an index PG materialises from a
-- primary-key or unique constraint.
-- ============================================================================

DROP TABLE IF EXISTS rcr CASCADE;
CREATE TABLE rcr (id int PRIMARY KEY, v int);
CREATE INDEX rcr_v_idx ON rcr (v);

-- ============================================================================
-- 1. A numeric string is an OID, not a name
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 99999999
-- end-expected
SELECT '99999999'::regclass::text AS a;

-- ============================================================================
-- 2. Every relation resolves by name
-- ============================================================================

-- begin-expected
-- columns: a
-- row: rcr
-- end-expected
SELECT 'rcr'::regclass::text AS a;

-- An explicitly created index
-- begin-expected
-- columns: a
-- row: rcr_v_idx
-- end-expected
SELECT 'rcr_v_idx'::regclass::text AS a;

-- ...and one backed by a constraint
-- begin-expected
-- columns: a
-- row: rcr_pkey
-- end-expected
SELECT 'rcr_pkey'::regclass::text AS a;

-- ============================================================================
-- 3. A name that is nothing is still an error
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "nosuchtable" does not exist
-- end-expected-error
SELECT 'nosuchtable'::regclass::text;

DROP TABLE rcr;
