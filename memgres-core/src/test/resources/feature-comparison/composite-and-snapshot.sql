-- ============================================================================
-- Feature Comparison: a domain's constraints on a composite type's field
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- A composite value is built field by field however it is written, and a
-- domain's CHECK runs wherever a value of the domain is built -- so the domain
-- judges the field it types on every spelling of a write.
-- ============================================================================

DROP TABLE IF EXISTS zzw3f_ct CASCADE;
DROP TYPE IF EXISTS zzw3f_c CASCADE;
DROP DOMAIN IF EXISTS zzw3f_d CASCADE;

CREATE DOMAIN zzw3f_d AS int CHECK (VALUE > 0);
CREATE TYPE zzw3f_c AS (a zzw3f_d, b text);
CREATE TABLE zzw3f_ct (id int, c zzw3f_c);

-- 1. a composite written as a bare text literal
-- begin-expected-error
-- sqlstate: 23514
-- message-like: value for domain zzw3f_d violates check constraint
-- end-expected-error
INSERT INTO zzw3f_ct VALUES (1, '(-1,x)');

-- 2. nothing was written
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM zzw3f_ct;

-- 3. a composite the domain accepts
INSERT INTO zzw3f_ct VALUES (2, ROW(5,'y')::zzw3f_c);

-- 4. an assignment to one field builds a value of that field's type
-- begin-expected-error
-- sqlstate: 23514
-- message-like: value for domain zzw3f_d violates check constraint
-- end-expected-error
UPDATE zzw3f_ct SET c.a = -9 WHERE id = 2;

-- 5. the row is untouched
-- begin-expected
-- columns: c
-- row: (5,y)
-- end-expected
SELECT c::text AS c FROM zzw3f_ct WHERE id = 2;

-- 6. an assignment the domain accepts is written
UPDATE zzw3f_ct SET c.a = 6 WHERE id = 2;

-- begin-expected
-- columns: c
-- row: (6,y)
-- end-expected
SELECT c::text AS c FROM zzw3f_ct WHERE id = 2;

-- 7. a text literal the domain accepts is written and reads back by field
INSERT INTO zzw3f_ct VALUES (3, '(7,z)');

-- begin-expected
-- columns: id | a | b
-- row: 2 | 6 | y
-- row: 3 | 7 | z
-- end-expected
SELECT id::text AS id, ((c).a)::text AS a, (c).b AS b FROM zzw3f_ct ORDER BY id;

DROP TABLE zzw3f_ct;
DROP TYPE zzw3f_c;
DROP DOMAIN zzw3f_d;


-- ============================================================================
-- Feature Comparison: a field of an element of an array of a composite
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- A subscript of an array of a composite is a value of that composite, and
-- unnest of one returns the composite -- so a FROM item over it supplies one
-- column per field of the composite.
-- ============================================================================

DROP TABLE IF EXISTS zzw3f_ct10 CASCADE;
DROP TYPE IF EXISTS zzw3f_c9 CASCADE;

CREATE TYPE zzw3f_c9 AS (a int, b text);
CREATE TABLE zzw3f_ct10 (id int, cs zzw3f_c9[]);
INSERT INTO zzw3f_ct10 VALUES (1, ARRAY[ROW(1,'x')::zzw3f_c9, ROW(2,'y')::zzw3f_c9]);

-- 1. the whole column
-- begin-expected
-- columns: t
-- row: {"(1,x)","(2,y)"}
-- end-expected
SELECT cs::text AS t FROM zzw3f_ct10;

-- 2. a field of a subscripted element
-- begin-expected
-- columns: a1
-- row: 1
-- end-expected
SELECT ((cs[1]).a)::text AS a1 FROM zzw3f_ct10;

-- begin-expected
-- columns: b2
-- row: y
-- end-expected
SELECT (cs[2]).b AS b2 FROM zzw3f_ct10;

-- 3. a subscript past the end selects nothing, so the field is null
-- begin-expected
-- columns: a3
-- row: none
-- end-expected
SELECT coalesce(((cs[3]).a)::text, 'none') AS a3 FROM zzw3f_ct10;

-- 4. every field of a subscripted element
-- begin-expected
-- columns: a | b
-- row: 1 | x
-- end-expected
SELECT (cs[1]).* FROM zzw3f_ct10;

-- 5. unnest returns the composite, so the alias carries its fields
-- begin-expected
-- columns: a | b
-- row: 1 | x
-- row: 2 | y
-- end-expected
SELECT u.a::text AS a, u.b AS b FROM zzw3f_ct10, unnest(cs) AS u ORDER BY u.a;

-- begin-expected
-- columns: b
-- row: x
-- end-expected
SELECT u.b AS b FROM zzw3f_ct10, unnest(cs) AS u WHERE u.a = 1;

DROP TABLE zzw3f_ct10;
DROP TYPE zzw3f_c9;
