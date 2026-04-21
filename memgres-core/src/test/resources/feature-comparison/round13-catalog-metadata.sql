-- ============================================================================
-- Feature Comparison: Round 13 — Catalog Metadata Gaps
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Columns in pg_catalog / information_schema views, and system-info fns,
-- that Memgres currently reports incorrectly or omits.
-- ============================================================================

DROP SCHEMA IF EXISTS r13_cat CASCADE;
CREATE SCHEMA r13_cat;
SET search_path = r13_cat, public;

-- ============================================================================
-- SECTION A: pg_constraint — confmatchtype, conpfeqop
-- ============================================================================

CREATE TABLE r13_cat_parent (id int PRIMARY KEY);
CREATE TABLE r13_cat_child (pid int REFERENCES r13_cat_parent(id));

-- 1. confmatchtype is 's' for SIMPLE match
-- begin-expected
-- columns: m
-- row: s
-- end-expected
SELECT confmatchtype AS m FROM pg_constraint
  WHERE conrelid = 'r13_cat_child'::regclass AND contype = 'f';

-- 2. conpfeqop is non-empty oid[]
-- begin-expected
-- columns: nonempty
-- row: t
-- end-expected
SELECT (array_length(conpfeqop, 1) > 0)::text AS nonempty
  FROM pg_constraint WHERE conrelid = 'r13_cat_child'::regclass AND contype = 'f';

-- ============================================================================
-- SECTION B: pg_proc columns
-- ============================================================================

-- 3. proargmodes for IN/OUT/OUT
CREATE FUNCTION r13_p_inout(IN x int, OUT y int, OUT z text)
  AS $$ SELECT $1 * 2, 'hi' $$ LANGUAGE SQL;

-- begin-expected
-- columns: modes
-- row: {i,o,o}
-- end-expected
SELECT proargmodes::text AS modes FROM pg_proc WHERE proname = 'r13_p_inout';

-- 4. proargnames
CREATE FUNCTION r13_p_names(a int, b text) RETURNS int
  AS 'SELECT 1' LANGUAGE SQL;

-- begin-expected
-- columns: names
-- row: {a,b}
-- end-expected
SELECT proargnames::text AS names FROM pg_proc WHERE proname = 'r13_p_names';

-- 5. proparallel 's' for PARALLEL SAFE
CREATE FUNCTION r13_p_ps() RETURNS int
  LANGUAGE SQL PARALLEL SAFE AS 'SELECT 1';

-- begin-expected
-- columns: p
-- row: s
-- end-expected
SELECT proparallel AS p FROM pg_proc WHERE proname = 'r13_p_ps';

-- 6. proleakproof true for LEAKPROOF
CREATE FUNCTION r13_p_lp() RETURNS int
  LANGUAGE SQL LEAKPROOF AS 'SELECT 1';

-- begin-expected
-- columns: l
-- row: t
-- end-expected
SELECT proleakproof::text AS l FROM pg_proc WHERE proname = 'r13_p_lp';

-- ============================================================================
-- SECTION C: pg_attribute — attstattarget
-- ============================================================================

CREATE TABLE r13_attr (id int, v text);
ALTER TABLE r13_attr ALTER COLUMN id SET STATISTICS 100;

-- begin-expected
-- columns: t
-- row: 100
-- end-expected
SELECT attstattarget::text AS t FROM pg_attribute
  WHERE attrelid = 'r13_attr'::regclass AND attname = 'id';

-- ============================================================================
-- SECTION D: pg_type
-- ============================================================================

-- 7. int4 is typcategory 'N'
-- begin-expected
-- columns: c
-- row: N
-- end-expected
SELECT typcategory AS c FROM pg_type WHERE typname = 'int4';

-- 8. int4 is NOT typispreferred (int8 is)
-- begin-expected
-- columns: p
-- row: f
-- end-expected
SELECT typispreferred::text AS p FROM pg_type WHERE typname = 'int4';

-- ============================================================================
-- SECTION E: pg_stat_user_tables
-- ============================================================================

CREATE TABLE r13_stat_ins (id int);
INSERT INTO r13_stat_ins SELECT generate_series(1, 50);

-- begin-expected
-- columns: n_ins
-- row: 0
-- end-expected
SELECT n_tup_ins::int::text AS n_ins FROM pg_stat_user_tables
  WHERE relname = 'r13_stat_ins';

-- ============================================================================
-- SECTION F: Size functions
-- ============================================================================

CREATE TABLE r13_size (id int, v text);
INSERT INTO r13_size SELECT i, repeat('x',100) FROM generate_series(1,100) i;

-- 9. pg_relation_size > 0
-- begin-expected
-- columns: positive
-- row: t
-- end-expected
SELECT (pg_relation_size('r13_size') > 0)::text AS positive;

-- 10. pg_total_relation_size >= pg_relation_size
-- begin-expected
-- columns: tot_ge_rel
-- row: t
-- end-expected
SELECT (pg_total_relation_size('r13_size') >= pg_relation_size('r13_size'))::text AS tot_ge_rel;

-- ============================================================================
-- SECTION G: information_schema views
-- ============================================================================

-- 11. enabled_roles includes current_user
-- begin-expected
-- columns: has
-- row: t
-- end-expected
SELECT (count(*) >= 1)::text AS has FROM information_schema.enabled_roles
  WHERE role_name = current_user;

-- 12. collations view has rows
-- begin-expected
-- columns: has
-- row: t
-- end-expected
SELECT (count(*) > 0)::text AS has FROM information_schema.collations;

-- 13. check_constraints view exposes CHECK
CREATE TABLE r13_chk (x int CHECK (x > 0));

-- begin-expected
-- columns: has
-- row: t
-- end-expected
SELECT (count(*) >= 1)::text AS has FROM information_schema.check_constraints
  WHERE constraint_name LIKE '%r13_chk%';

-- 14. sequences view exposed
CREATE SEQUENCE r13_seq START 100;

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM information_schema.sequences
  WHERE sequence_name = 'r13_seq';

-- ============================================================================
-- SECTION H: pg_extension
-- ============================================================================

-- 15. plpgsql listed
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM pg_extension WHERE extname = 'plpgsql';

-- ============================================================================
-- SECTION I: logical replication catalogs queryable
-- ============================================================================

-- 16. pg_publication queryable
-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::text AS c FROM pg_publication;

-- 17. pg_subscription queryable
-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::text AS c FROM pg_subscription;
