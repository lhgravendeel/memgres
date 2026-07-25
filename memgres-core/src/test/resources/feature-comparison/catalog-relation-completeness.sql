-- ============================================================================
-- Feature Comparison: catalog relation and attribute completeness
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PostgreSQL registers a pg_class row for every relation-like object, not just
-- tables: composite types (relkind 'c'), sequences ('S') and indexes ('i') —
-- including the index that backs an EXCLUDE constraint. Each of those has
-- pg_attribute rows behind it, and every function argument appears in
-- information_schema.parameters.
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP TABLE IF EXISTS crc_t CASCADE;
DROP TABLE IF EXISTS crc_e CASCADE;
DROP TABLE IF EXISTS crc_ix CASCADE;
DROP TYPE IF EXISTS crc_comp CASCADE;
DROP SEQUENCE IF EXISTS crc_seq CASCADE;
DROP FUNCTION IF EXISTS crc_f1(int, text) CASCADE;
DROP FUNCTION IF EXISTS crc_f2(int) CASCADE;

CREATE TYPE crc_comp AS (a int, b text);
CREATE SEQUENCE crc_seq;
-- gist over tsrange needs no btree_gist, so this works on a bare PG install.
CREATE TABLE crc_t (id int PRIMARY KEY, during tsrange,
                    EXCLUDE USING gist (during WITH &&));
CREATE TABLE crc_e (a int, b int, EXCLUDE (a WITH =));
CREATE TABLE crc_ix (id int, name text, qty int);
CREATE INDEX crc_x1 ON crc_ix (lower(name));
CREATE INDEX crc_x2 ON crc_ix ((qty + 1));
CREATE INDEX crc_x3 ON crc_ix ((qty::text));
CREATE INDEX crc_x4 ON crc_ix (name, qty);

-- ============================================================================
-- 1. Composite types get a pg_class row of relkind 'c'
-- ============================================================================

-- begin-expected
-- columns: relname | relkind | relnatts
-- row: crc_comp, c, 2
-- end-expected
SELECT relname, relkind, relnatts FROM pg_class WHERE relname = 'crc_comp';

-- ============================================================================
-- 2. pg_type.typrelid resolves through pg_class to the composite's attributes
-- ============================================================================

-- begin-expected
-- columns: attname | typname
-- row: a, int4
-- row: b, text
-- end-expected
SELECT a.attname, ty.typname
FROM pg_type t
JOIN pg_class c ON c.oid = t.typrelid
JOIN pg_attribute a ON a.attrelid = c.oid
JOIN pg_type ty ON ty.oid = a.atttypid
WHERE t.typname = 'crc_comp' AND a.attnum > 0
ORDER BY a.attnum;

-- ============================================================================
-- 3. An EXCLUDE constraint is backed by a real index relation
-- ============================================================================

-- begin-expected
-- columns: relname | relkind | relnatts
-- row: crc_t_during_excl, i, 1
-- end-expected
SELECT relname, relkind, relnatts FROM pg_class WHERE relname = 'crc_t_during_excl';

-- begin-expected
-- columns: relname | indisunique | indisexclusion | indnkeyatts
-- row: crc_t_during_excl, f, t, 1
-- end-expected
SELECT c.relname, i.indisunique, i.indisexclusion, i.indnkeyatts
FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
WHERE c.relname = 'crc_t_during_excl';

-- begin-expected
-- columns: conname | contype | has_index | def
-- row: crc_t_during_excl, x, t, EXCLUDE USING gist (during WITH &&)
-- end-expected
SELECT conname, contype, conindid <> 0 AS has_index, pg_get_constraintdef(oid) AS def
FROM pg_constraint WHERE conname = 'crc_t_during_excl';

-- Without USING, PG uses the default access method
-- begin-expected
-- columns: conname | def
-- row: crc_e_a_excl, EXCLUDE USING btree (a WITH =)
-- end-expected
SELECT conname, pg_get_constraintdef(oid) AS def
FROM pg_constraint WHERE conname = 'crc_e_a_excl';

-- ============================================================================
-- 4. Sequences have their three fixed attributes
-- ============================================================================

-- begin-expected
-- columns: attname | attnum | typname | attnotnull
-- row: last_value, 1, int8, t
-- row: log_cnt, 2, int8, t
-- row: is_called, 3, bool, t
-- end-expected
SELECT a.attname, a.attnum, t.typname, a.attnotnull
FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_type t ON t.oid = a.atttypid
WHERE c.relname = 'crc_seq' AND a.attnum > 0
ORDER BY a.attnum;

-- relnatts must agree with the number of attribute rows
-- begin-expected
-- columns: relnatts | attcount
-- row: 3, 3
-- end-expected
SELECT c.relnatts,
       (SELECT count(*) FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0) AS attcount
FROM pg_class c WHERE c.relname = 'crc_seq';

-- ============================================================================
-- 5. Indexes have one attribute per key column
-- ============================================================================

-- begin-expected
-- columns: relname | attname | attnum | typname
-- row: crc_x1, lower, 1, text
-- row: crc_x2, expr, 1, int4
-- row: crc_x3, qty, 1, text
-- row: crc_x4, name, 1, text
-- row: crc_x4, qty, 2, int4
-- end-expected
SELECT c.relname, a.attname, a.attnum, t.typname
FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_type t ON t.oid = a.atttypid
WHERE c.relname LIKE 'crc\_x%' AND a.attnum > 0
ORDER BY c.relname, a.attnum;

-- begin-expected
-- columns: attname | attnum | typname
-- row: id, 1, int4
-- end-expected
SELECT a.attname, a.attnum, t.typname
FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_type t ON t.oid = a.atttypid
WHERE c.relname = 'crc_t_pkey' AND a.attnum > 0
ORDER BY a.attnum;

-- ============================================================================
-- 6. information_schema.parameters lists every function argument
-- ============================================================================

CREATE FUNCTION crc_f1(x int, y text) RETURNS int AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql;
CREATE FUNCTION crc_f2(q int) RETURNS int AS $$ BEGIN RETURN q; END; $$ LANGUAGE plpgsql;

-- specific_name embeds the function OID in PG, so join through routines rather
-- than comparing it directly.
-- begin-expected
-- columns: routine_name | ordinal_position | parameter_mode | parameter_name | data_type | udt_name
-- row: crc_f1, 1, IN, x, integer, int4
-- row: crc_f1, 2, IN, y, text, text
-- row: crc_f2, 1, IN, q, integer, int4
-- end-expected
SELECT r.routine_name, p.ordinal_position, p.parameter_mode, p.parameter_name,
       p.data_type, p.udt_name
FROM information_schema.parameters p
JOIN information_schema.routines r USING (specific_name)
WHERE r.routine_name IN ('crc_f1', 'crc_f2')
ORDER BY r.routine_name, p.ordinal_position;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP FUNCTION crc_f1(int, text);
DROP FUNCTION crc_f2(int);
DROP TABLE crc_ix CASCADE;
DROP TABLE crc_e CASCADE;
DROP TABLE crc_t CASCADE;
DROP SEQUENCE crc_seq CASCADE;
DROP TYPE crc_comp CASCADE;
