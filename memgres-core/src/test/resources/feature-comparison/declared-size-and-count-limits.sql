-- ============================================================================
-- Feature Comparison: the limits a declaration or a call may not exceed
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Some limits are structural rather than semantic: an index tuple holds 32
-- attributes, a pg_proc entry holds 100 argument types, a varchar typmod holds
-- a length no greater than 10485760 and a numeric one a precision no greater
-- than 1000. PostgreSQL checks each of these where the declaration is written
-- or the call is parsed, so an index, a key, a domain or a column it could
-- never store is never created. Accepting them instead records a definition
-- PostgreSQL would have refused, and the disagreement surfaces later against
-- some innocent statement that reads it back.
-- ============================================================================

-- ============================================================================
-- 1. An index names at most 32 columns
-- ============================================================================

DROP TABLE IF EXISTS dsl_wide CASCADE;

CREATE TABLE dsl_wide (c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int, c9 int, c10 int, c11 int, c12 int, c13 int, c14 int, c15 int, c16 int, c17 int, c18 int, c19 int, c20 int, c21 int, c22 int, c23 int, c24 int, c25 int, c26 int, c27 int, c28 int, c29 int, c30 int, c31 int, c32 int, c33 int);

-- begin-expected-error
-- sqlstate: 54011
-- message-like: cannot use more than 32 columns in an index
-- end-expected-error
CREATE INDEX dsl_wide_i33 ON dsl_wide (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33);

-- 32 is the limit, not one short of it
CREATE INDEX dsl_wide_i32 ON dsl_wide (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32);

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT count(*) AS a FROM pg_indexes WHERE tablename = 'dsl_wide';

-- A unique index is bounded the same way
-- begin-expected-error
-- sqlstate: 54011
-- message-like: cannot use more than 32 columns in an index
-- end-expected-error
CREATE UNIQUE INDEX dsl_wide_u33 ON dsl_wide (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33);

-- INCLUDE columns count towards the same 32
-- begin-expected-error
-- sqlstate: 54011
-- message-like: cannot use more than 32 columns in an index
-- end-expected-error
CREATE INDEX dsl_wide_inc33 ON dsl_wide (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30) INCLUDE (c31, c32, c33);

-- 30 key columns plus 2 included is exactly 32
CREATE INDEX dsl_wide_inc32 ON dsl_wide (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30) INCLUDE (c31, c32);

-- The relation is resolved before the columns are counted
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "dsl_absent" does not exist
-- end-expected-error
CREATE INDEX dsl_absent_i ON dsl_absent (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33);

-- ... and the count is settled before the access method is looked up
-- begin-expected-error
-- sqlstate: 54011
-- message-like: cannot use more than 32 columns in an index
-- end-expected-error
CREATE INDEX dsl_wide_am33 ON dsl_wide USING dsl_nosuchmethod (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33);

-- An ordinary index is unaffected
DROP TABLE IF EXISTS dsl_small CASCADE;

CREATE TABLE dsl_small (a int, b text);

CREATE INDEX dsl_small_ab ON dsl_small (a, b);

INSERT INTO dsl_small VALUES (1, 'x'), (2, 'y');

-- begin-expected
-- columns: a
-- row: x
-- end-expected
SELECT b AS a FROM dsl_small WHERE a = 1;

-- ============================================================================
-- 2. A PRIMARY KEY or UNIQUE constraint is stored as one of those indexes
-- ============================================================================

-- begin-expected-error
-- sqlstate: 54011
-- message-like: cannot use more than 32 columns in an index
-- end-expected-error
ALTER TABLE dsl_wide ADD PRIMARY KEY (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33);

-- begin-expected-error
-- sqlstate: 54011
-- message-like: cannot use more than 32 columns in an index
-- end-expected-error
ALTER TABLE dsl_wide ADD UNIQUE (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33);

-- begin-expected-error
-- sqlstate: 54011
-- message-like: cannot use more than 32 columns in an index
-- end-expected-error
CREATE TABLE dsl_wide_pk (c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int, c9 int, c10 int, c11 int, c12 int, c13 int, c14 int, c15 int, c16 int, c17 int, c18 int, c19 int, c20 int, c21 int, c22 int, c23 int, c24 int, c25 int, c26 int, c27 int, c28 int, c29 int, c30 int, c31 int, c32 int, c33 int, PRIMARY KEY (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33));

-- begin-expected-error
-- sqlstate: 54011
-- message-like: cannot use more than 32 columns in an index
-- end-expected-error
CREATE TABLE dsl_wide_uq (c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int, c9 int, c10 int, c11 int, c12 int, c13 int, c14 int, c15 int, c16 int, c17 int, c18 int, c19 int, c20 int, c21 int, c22 int, c23 int, c24 int, c25 int, c26 int, c27 int, c28 int, c29 int, c30 int, c31 int, c32 int, c33 int, UNIQUE (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33));

-- 32-column keys are accepted
DROP TABLE IF EXISTS dsl_wide32 CASCADE;

CREATE TABLE dsl_wide32 (c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int, c9 int, c10 int, c11 int, c12 int, c13 int, c14 int, c15 int, c16 int, c17 int, c18 int, c19 int, c20 int, c21 int, c22 int, c23 int, c24 int, c25 int, c26 int, c27 int, c28 int, c29 int, c30 int, c31 int, c32 int, c33 int, PRIMARY KEY (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32));

ALTER TABLE dsl_wide32 ADD UNIQUE (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32);

-- An ordinary key still enforces itself
DROP TABLE IF EXISTS dsl_key CASCADE;

CREATE TABLE dsl_key (a int, b int, PRIMARY KEY (a, b));

INSERT INTO dsl_key VALUES (1, 1);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint
-- end-expected-error
INSERT INTO dsl_key VALUES (1, 1);

-- ============================================================================
-- 3. A function call passes at most 100 arguments
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 192
-- end-expected
SELECT length(concat(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100)) AS a;

-- begin-expected-error
-- sqlstate: 54023
-- message-like: cannot pass more than 100 arguments to a function
-- end-expected-error
SELECT concat(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101);

-- The separator of concat_ws is an argument like any other
-- begin-expected
-- columns: a
-- row: 287
-- end-expected
SELECT length(concat_ws(',', 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99)) AS a;

-- begin-expected-error
-- sqlstate: 54023
-- message-like: cannot pass more than 100 arguments to a function
-- end-expected-error
SELECT concat_ws(',', 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101);

-- begin-expected
-- columns: a
-- row: 100
-- end-expected
SELECT num_nonnulls(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100) AS a;

-- begin-expected-error
-- sqlstate: 54023
-- message-like: cannot pass more than 100 arguments to a function
-- end-expected-error
SELECT num_nonnulls(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101);

-- begin-expected-error
-- sqlstate: 54023
-- message-like: cannot pass more than 100 arguments to a function
-- end-expected-error
SELECT num_nulls(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101);

-- begin-expected-error
-- sqlstate: 54023
-- message-like: cannot pass more than 100 arguments to a function
-- end-expected-error
SELECT format('%s', 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101);

-- COALESCE, GREATEST, LEAST and NULLIF are grammar productions rather than
-- function calls, so the limit never reaches them
-- begin-expected
-- columns: a
-- row: 101
-- end-expected
SELECT greatest(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101)::text AS a;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT least(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101)::text AS a;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT coalesce(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101)::text AS a;

-- Ordinary calls are untouched, on literals and on columns alike
DROP TABLE IF EXISTS dsl_args CASCADE;

CREATE TABLE dsl_args (a text, b int);

INSERT INTO dsl_args VALUES ('x', 1), ('y', 2), (NULL, 3);

-- begin-expected
-- columns: a
-- row: 3
-- row: x1
-- row: y2
-- end-expected
SELECT concat(a, b) AS a FROM dsl_args ORDER BY 1;

-- begin-expected
-- columns: a
-- row: -
-- row: x-
-- row: y-
-- end-expected
SELECT concat(a, '-') AS a FROM dsl_args WHERE concat(a, b) <> 'zz'
  GROUP BY concat(a, '-') ORDER BY concat(a, '-');

DROP VIEW IF EXISTS dsl_args_v CASCADE;

CREATE VIEW dsl_args_v AS SELECT concat_ws('/', a, b) AS c FROM dsl_args;

-- begin-expected
-- columns: a
-- row: 3
-- row: x/1
-- row: y/2
-- end-expected
SELECT sub.c AS a FROM (SELECT c FROM dsl_args_v) sub ORDER BY 1;

-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT num_nonnulls(a, b) AS a FROM dsl_args WHERE b = 1;

-- ============================================================================
-- 4. A type modifier out of range is refused where it is written
-- ============================================================================

DROP DOMAIN IF EXISTS dsl_dom CASCADE;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: length for type varchar cannot exceed 10485760
-- end-expected-error
CREATE DOMAIN dsl_dom AS varchar(10485761);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: length for type char must be at least 1
-- end-expected-error
CREATE DOMAIN dsl_dom AS char(0);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: NUMERIC precision 1001 must be between 1 and 1000
-- end-expected-error
CREATE DOMAIN dsl_dom AS numeric(1001,2);

-- The largest accepted modifiers still make a domain
CREATE DOMAIN dsl_dom AS varchar(10485760);

DROP DOMAIN IF EXISTS dsl_dom2 CASCADE;

CREATE DOMAIN dsl_dom2 AS numeric(1000,2);

-- The name collision is reported before the base type's modifier is resolved
-- begin-expected-error
-- sqlstate: 42710
-- message-like: type "dsl_dom" already exists
-- end-expected-error
CREATE DOMAIN dsl_dom AS varchar(10485761);

DROP TABLE IF EXISTS dsl_retype CASCADE;

CREATE TABLE dsl_retype (a text);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: length for type varchar cannot exceed 10485760
-- end-expected-error
ALTER TABLE dsl_retype ALTER COLUMN a TYPE varchar(10485761);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: length for type char must be at least 1
-- end-expected-error
ALTER TABLE dsl_retype ALTER COLUMN a TYPE char(0);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: NUMERIC precision 1001 must be between 1 and 1000
-- end-expected-error
ALTER TABLE dsl_retype ALTER COLUMN a TYPE numeric(1001,2);

-- The column is looked up before the target type's modifier is resolved
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" of relation "dsl_retype" does not exist
-- end-expected-error
ALTER TABLE dsl_retype ALTER COLUMN nosuchcol TYPE varchar(10485761);

-- ADD COLUMN checks the same widths
-- begin-expected-error
-- sqlstate: 22023
-- message-like: length for type varchar cannot exceed 10485760
-- end-expected-error
ALTER TABLE dsl_retype ADD COLUMN b varchar(10485761);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: NUMERIC precision 1001 must be between 1 and 1000
-- end-expected-error
ALTER TABLE dsl_retype ADD COLUMN b numeric(1001,2);

-- A refused ALTER leaves the table alterable
ALTER TABLE dsl_retype ADD COLUMN b int;

ALTER TABLE dsl_retype ALTER COLUMN a TYPE varchar(20);

INSERT INTO dsl_retype VALUES ('hello', 7);

-- begin-expected
-- columns: a
-- row: hello7
-- end-expected
SELECT a || b AS a FROM dsl_retype;

-- The largest accepted modifiers still make a column
DROP TABLE IF EXISTS dsl_widecol CASCADE;

CREATE TABLE dsl_widecol (a varchar(10485760), b numeric(1000,2));

ALTER TABLE dsl_widecol ALTER COLUMN a TYPE varchar(10485760);

-- ============================================================================
-- 5. array_ndims counts dimensions and answers with an integer
-- ============================================================================

-- begin-expected
-- columns: a
-- row: integer
-- end-expected
SELECT pg_typeof(array_ndims(ARRAY[1,2]))::text AS a;

-- begin-expected
-- columns: a
-- row: 6
-- end-expected
SELECT array_ndims(ARRAY[[[[[[1]]]]]]) AS a;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT array_ndims(ARRAY[1,2,3]) AS a;

-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT array_ndims('{{1,2},{3,4}}'::int[]) AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT array_ndims(NULL::int[]) AS a;

-- A seventh dimension does not exist to be counted
-- begin-expected-error
-- sqlstate: 54000
-- message-like: number of array dimensions (7) exceeds the maximum allowed (6)
-- end-expected-error
SELECT array_ndims(ARRAY[[[[[[[1]]]]]]]);

-- The neighbouring dimension functions answer with an integer too
-- begin-expected
-- columns: a | b | c | d
-- row: integer, integer, integer, integer
-- end-expected
SELECT pg_typeof(array_upper(ARRAY[1,2],1))::text AS a,
       pg_typeof(array_lower(ARRAY[1,2],1))::text AS b,
       pg_typeof(array_length(ARRAY[1,2],1))::text AS c,
       pg_typeof(cardinality(ARRAY[1,2]))::text AS d;

-- begin-expected
-- columns: a | b
-- row: integer, integer
-- end-expected
SELECT pg_typeof(num_nonnulls(1,NULL))::text AS a,
       pg_typeof(num_nulls(1,NULL))::text AS b;

DROP TABLE IF EXISTS dsl_arr CASCADE;

CREATE TABLE dsl_arr (a int[]);

INSERT INTO dsl_arr VALUES ('{1,2}'), ('{{1,2},{3,4}}'), (NULL);

-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- end-expected
SELECT array_ndims(a) AS a FROM dsl_arr WHERE array_ndims(a) >= 1 ORDER BY 1;

-- ============================================================================
-- cleanup
-- ============================================================================

DROP VIEW IF EXISTS dsl_args_v CASCADE;
DROP TABLE IF EXISTS dsl_wide CASCADE;
DROP TABLE IF EXISTS dsl_wide32 CASCADE;
DROP TABLE IF EXISTS dsl_small CASCADE;
DROP TABLE IF EXISTS dsl_key CASCADE;
DROP TABLE IF EXISTS dsl_args CASCADE;
DROP TABLE IF EXISTS dsl_retype CASCADE;
DROP TABLE IF EXISTS dsl_widecol CASCADE;
DROP TABLE IF EXISTS dsl_arr CASCADE;
DROP DOMAIN IF EXISTS dsl_dom CASCADE;
DROP DOMAIN IF EXISTS dsl_dom2 CASCADE;
