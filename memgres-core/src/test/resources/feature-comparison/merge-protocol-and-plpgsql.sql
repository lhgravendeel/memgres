-- ============================================================================
-- Feature Comparison: MERGE clause rules, rule row counts, PL/pgSQL cursors
-- Target: PostgreSQL 18 vs Memgres
--
-- Statements ran that say nothing, and statements reported work they did not do:
--   * a MERGE WHEN clause written after an unconditional clause of its own kind
--     can never fire, an INSERT arm could name a column twice, and WITH
--     RECURSIVE was accepted where MERGE does not take it;
--   * WHEN NOT MATCHED BY SOURCE fired for target rows a source row had paired
--     with, so rows the ON clause matched were updated as if unmatched, and a
--     DO NOTHING arm counted rows it left alone;
--   * a NOT MATCHED BY SOURCE arm naming the source, and a NOT MATCHED arm
--     naming the target, were reported as a missing relation rather than one
--     that is present but out of reach here;
--   * a statement replaced by a DO INSTEAD rule reported the row count of the
--     statement that never ran;
--   * a cursor query whose select list is a bare parameter lost the column
--     name, and aliasing a parameter to its own name did not parse;
--   * CLOSE on a cursor that was never opened did nothing quietly;
--   * a row constructor returned as a composite kept text in numeric fields,
--     and a column definition list of the wrong shape was refused under a code
--     of its own rather than PostgreSQL's;
--   * a PL/pgSQL body that does not parse was reported as an internal error.
-- ============================================================================

DROP TABLE IF EXISTS mpp_mt CASCADE;
DROP TABLE IF EXISTS mpp_ms CASCADE;
DROP TABLE IF EXISTS mpp_st CASCADE;
DROP TABLE IF EXISTS mpp_ss CASCADE;
DROP TABLE IF EXISTS mpp_t173 CASCADE;
DROP TABLE IF EXISTS mpp_log173 CASCADE;
DROP TABLE IF EXISTS mpp_v173 CASCADE;
DROP TABLE IF EXISTS mpp_vlog173 CASCADE;
DROP FUNCTION IF EXISTS mpp_retc3(int);
DROP FUNCTION IF EXISTS mpp_retc4(int);
DROP FUNCTION IF EXISTS mpp_rettn();
DROP FUNCTION IF EXISTS mpp_rettt();
DROP FUNCTION IF EXISTS mpp_retbig();
DROP FUNCTION IF EXISTS mpp_srf();
DROP TYPE IF EXISTS mpp_two_int8s CASCADE;
DROP TYPE IF EXISTS mpp_two_texts CASCADE;

CREATE TABLE mpp_mt (id int PRIMARY KEY, v int, w int);
CREATE TABLE mpp_ms (id int PRIMARY KEY, v int);
INSERT INTO mpp_mt VALUES (1,1,1),(2,2,2),(3,3,3);
INSERT INTO mpp_ms VALUES (2,20),(4,40);

-- ---------------------------------------------------------------------------
-- A WHEN clause after an unconditional clause of the same kind never fires
-- ---------------------------------------------------------------------------

-- begin-expected-error
-- sqlstate: 42601
-- message-like: unreachable WHEN clause specified after unconditional WHEN clause
-- end-expected-error
MERGE INTO mpp_mt t USING mpp_ms s ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET v = s.v
  WHEN MATCHED AND t.v > 0 THEN UPDATE SET v = 1;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: unreachable WHEN clause specified after unconditional WHEN clause
-- end-expected-error
MERGE INTO mpp_mt t USING mpp_ms s ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET v = 1
  WHEN MATCHED THEN UPDATE SET v = 2;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: unreachable WHEN clause specified after unconditional WHEN clause
-- end-expected-error
MERGE INTO mpp_mt t USING mpp_ms s ON t.id = s.id
  WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)
  WHEN NOT MATCHED AND s.v > 0 THEN INSERT (id, v) VALUES (s.id, 0);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: unreachable WHEN clause specified after unconditional WHEN clause
-- end-expected-error
MERGE INTO mpp_mt t USING mpp_ms s ON t.id = s.id
  WHEN NOT MATCHED BY TARGET THEN INSERT (id, v) VALUES (s.id, 1)
  WHEN NOT MATCHED AND s.v > 0 THEN INSERT (id, v) VALUES (s.id, 2);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: unreachable WHEN clause specified after unconditional WHEN clause
-- end-expected-error
MERGE INTO mpp_mt t USING mpp_ms s ON t.id = s.id
  WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = 0
  WHEN NOT MATCHED BY SOURCE AND t.v > 0 THEN DELETE;

-- The kinds are independent: an unconditional MATCHED clause says nothing about
-- a later clause of a different kind.
MERGE INTO mpp_mt t USING mpp_ms s ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET v = s.v
  WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)
  WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = 0;

-- begin-expected
-- columns: id | v | w
-- row: 1, 0, 1
-- row: 2, 20, 2
-- row: 3, 0, 3
-- row: 4, 40, NULL
-- end-expected
SELECT id, v, w FROM mpp_mt ORDER BY id;

-- A conditional clause may still be written before an unconditional one.
MERGE INTO mpp_mt t USING mpp_ms s ON t.id = s.id
  WHEN MATCHED AND t.v > 100 THEN UPDATE SET v = 1
  WHEN MATCHED THEN UPDATE SET v = 7;

-- begin-expected
-- columns: id | v
-- row: 1, 0
-- row: 2, 7
-- row: 3, 0
-- row: 4, 7
-- end-expected
SELECT id, v FROM mpp_mt ORDER BY id;

-- ---------------------------------------------------------------------------
-- An INSERT arm names each target column once
-- ---------------------------------------------------------------------------

-- begin-expected-error
-- sqlstate: 42701
-- message-like: column "id" specified more than once
-- end-expected-error
MERGE INTO mpp_mt t USING mpp_ms s ON t.id = s.id
  WHEN NOT MATCHED THEN INSERT (id, id) VALUES (s.id, s.v);

-- begin-expected-error
-- sqlstate: 42701
-- message-like: column "v" specified more than once
-- end-expected-error
MERGE INTO mpp_mt t USING mpp_ms s ON t.id = s.id
  WHEN NOT MATCHED THEN INSERT (id, v, v) VALUES (s.id, s.v, 1);

-- The same rule for an INSERT that stands on its own.
-- begin-expected-error
-- sqlstate: 42701
-- message-like: column "id" specified more than once
-- end-expected-error
INSERT INTO mpp_mt (id, id) VALUES (9, 9);

-- Distinct columns are of course fine.
INSERT INTO mpp_mt (id, v, w) VALUES (9, 9, 9);
DELETE FROM mpp_mt WHERE id = 9;

-- ---------------------------------------------------------------------------
-- MERGE does not take WITH RECURSIVE
-- ---------------------------------------------------------------------------

-- begin-expected-error
-- sqlstate: 42601
-- message-like: WITH RECURSIVE is not supported for MERGE statement
-- end-expected-error
WITH RECURSIVE c(n) AS (SELECT 1)
MERGE INTO mpp_mt t USING c ON t.id = c.n WHEN MATCHED THEN UPDATE SET v = 7;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: WITH RECURSIVE is not supported for MERGE statement
-- end-expected-error
WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 3)
MERGE INTO mpp_mt t USING c ON t.id = c.n WHEN MATCHED THEN UPDATE SET v = 7;

-- The same statement without RECURSIVE runs.
WITH c(n) AS (SELECT 1)
MERGE INTO mpp_mt t USING c ON t.id = c.n WHEN MATCHED THEN UPDATE SET v = 5;

-- begin-expected
-- columns: id | v
-- row: 1, 5
-- row: 2, 7
-- row: 3, 0
-- row: 4, 7
-- end-expected
SELECT id, v FROM mpp_mt ORDER BY id;

-- ---------------------------------------------------------------------------
-- Each arm sees only the relation it has a row from
-- ---------------------------------------------------------------------------

CREATE TABLE mpp_st (id int PRIMARY KEY, v int);
CREATE TABLE mpp_ss (id int PRIMARY KEY, v int);
INSERT INTO mpp_st VALUES (1,1),(2,2),(3,3);
INSERT INTO mpp_ss VALUES (2,20);

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "s"
-- end-expected-error
MERGE INTO mpp_st t USING mpp_ss s ON t.id = s.id
  WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = s.v;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "s"
-- end-expected-error
MERGE INTO mpp_st t USING mpp_ss s ON t.id = s.id
  WHEN NOT MATCHED BY SOURCE AND s.v > 0 THEN DELETE;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "t"
-- end-expected-error
MERGE INTO mpp_st t USING mpp_ss s ON t.id = s.id
  WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, t.v);

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "t"
-- end-expected-error
MERGE INTO mpp_st t USING mpp_ss s ON t.id = s.id
  WHEN NOT MATCHED AND t.v > 0 THEN INSERT (id, v) VALUES (s.id, s.v);

-- A subquery in the same arm brings its own FROM list, so a relation named
-- there is that subquery's own.
MERGE INTO mpp_st t USING mpp_ss s ON t.id = s.id
  WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = (SELECT max(s.v) FROM mpp_ss s);

-- begin-expected
-- columns: id | v
-- row: 1, 20
-- row: 2, 2
-- row: 3, 20
-- end-expected
SELECT id, v FROM mpp_st ORDER BY id;

-- ---------------------------------------------------------------------------
-- NOT MATCHED BY SOURCE is about the ON clause, not about which arm fired
-- ---------------------------------------------------------------------------

UPDATE mpp_st SET v = id;

-- Only ids 1 and 3 are unpaired, so only they are updated.
MERGE INTO mpp_st t USING mpp_ss s ON t.id = s.id
  WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = 99;

-- begin-expected
-- columns: id | v
-- row: 1, 99
-- row: 2, 2
-- row: 3, 99
-- end-expected
SELECT id, v FROM mpp_st ORDER BY id;

UPDATE mpp_st SET v = id;

MERGE INTO mpp_st t USING mpp_ss s ON t.id = s.id
  WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- begin-expected
-- columns: id | v
-- row: 2, 2
-- end-expected
SELECT id, v FROM mpp_st ORDER BY id;

INSERT INTO mpp_st VALUES (1,1),(3,3);

-- DO NOTHING touches nothing, so it counts nothing.
MERGE INTO mpp_st t USING mpp_ss s ON t.id = s.id
  WHEN NOT MATCHED BY SOURCE THEN DO NOTHING;

-- begin-expected
-- columns: id | v
-- row: 1, 1
-- row: 2, 2
-- row: 3, 3
-- end-expected
SELECT id, v FROM mpp_st ORDER BY id;

-- ---------------------------------------------------------------------------
-- A statement a DO INSTEAD rule replaces reports what the rule did
-- ---------------------------------------------------------------------------

CREATE TABLE mpp_t173 (i int PRIMARY KEY);
CREATE TABLE mpp_log173 (m text);
INSERT INTO mpp_t173 VALUES (1),(2);
CREATE RULE mpp_r173 AS ON DELETE TO mpp_t173 DO INSTEAD
  INSERT INTO mpp_log173 VALUES ('d1');

-- The DELETE never runs; the rule's action does.
DELETE FROM mpp_t173 WHERE i = 1;

-- begin-expected
-- columns: i
-- row: 1
-- row: 2
-- end-expected
SELECT i FROM mpp_t173 ORDER BY i;

-- begin-expected
-- columns: m
-- row: d1
-- end-expected
SELECT m FROM mpp_log173 ORDER BY m;

-- An INSERT replaced by an INSERT does report that INSERT's rows.
CREATE TABLE mpp_v173 (i int PRIMARY KEY);
CREATE TABLE mpp_vlog173 (m text);
CREATE RULE mpp_rv173 AS ON INSERT TO mpp_v173 DO INSTEAD
  INSERT INTO mpp_vlog173 VALUES ('i');
INSERT INTO mpp_v173 VALUES (1);

-- begin-expected
-- columns: i
-- end-expected
SELECT i FROM mpp_v173 ORDER BY i;

-- begin-expected
-- columns: m
-- row: i
-- end-expected
SELECT m FROM mpp_vlog173 ORDER BY m;

-- ---------------------------------------------------------------------------
-- A cursor query keeps the names it is written with
-- ---------------------------------------------------------------------------

DO $$
declare c1 cursor (param1 int, param2 int) for select param1, param2;
  r record;
begin
  open c1 (param1 := 20, param2 := 21);
  fetch c1 into r;
  raise notice 'p1=% p2=%', r.param1, r.param2;
  close c1;
end$$;

DO $$
declare c1 cursor (param1 int, param2 int) for select param1, param2;
  r record;
begin
  open c1 (20, 21);
  fetch c1 into r;
  raise notice 'p1=% p2=%', r.param1, r.param2;
  close c1;
end$$;

DO $$
declare c1 cursor (param1 int) for select param1 AS param1;
  r record;
begin
  open c1 (param1 := 20);
  fetch c1 into r;
  raise notice 'p1=%', r.param1;
  close c1;
end$$;

DO $$
declare c1 cursor (param1 int, param2 int) for select param1 + param2 AS s;
  r record;
begin
  open c1 (param1 := 20, param2 := 21);
  fetch c1 into r;
  raise notice 's=%', r.s;
  close c1;
end$$;

DO $$
declare c1 cursor (p text) for select p;
  r record;
begin
  open c1 ('hi');
  fetch c1 into r;
  raise notice 'p=%', r.p;
  close c1;
end$$;

-- ---------------------------------------------------------------------------
-- CLOSE needs a cursor that was opened
-- ---------------------------------------------------------------------------

-- begin-expected-error
-- sqlstate: 22004
-- message-like: cursor variable "c" is null
-- end-expected-error
DO $$ declare c cursor for select 1; begin close c; end $$;

-- begin-expected-error
-- sqlstate: 22004
-- message-like: cursor variable "c" is null
-- end-expected-error
DO $$ declare c refcursor; begin close c; end $$;

-- Opening first is what makes CLOSE mean something.
DO $$ declare c cursor for select 1; v int; begin open c; fetch c into v; close c; raise notice '%', v; end $$;
DO $$ declare c refcursor; v int; begin open c for select 7; fetch c into v; close c; raise notice '%', v; end $$;

-- ---------------------------------------------------------------------------
-- A row constructor returned as a composite has to hold that type's values
-- ---------------------------------------------------------------------------

CREATE TYPE mpp_two_int8s AS (q1 bigint, q2 bigint);
CREATE TYPE mpp_two_texts AS (t1 text, t2 text);

CREATE FUNCTION mpp_retc3(x int) RETURNS mpp_two_int8s AS
  $body$ begin return row(x::text, x::text); end $body$ LANGUAGE plpgsql;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: returned record type does not match expected record type
-- end-expected-error
SELECT (mpp_retc3(42)).q1;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: returned record type does not match expected record type
-- end-expected-error
SELECT mpp_retc3(42)::text;

CREATE FUNCTION mpp_retc4(x int) RETURNS mpp_two_int8s AS
  $body$ begin return row('abc', 'def'); end $body$ LANGUAGE plpgsql;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: returned record type does not match expected record type
-- end-expected-error
SELECT mpp_retc4(42)::text;

CREATE FUNCTION mpp_rettn() RETURNS mpp_two_texts AS
  $body$ begin return row(1, 2); end $body$ LANGUAGE plpgsql;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: returned record type does not match expected record type
-- end-expected-error
SELECT mpp_rettn()::text;

-- A row of the declared kind is returned as it always was.
CREATE FUNCTION mpp_rettt() RETURNS mpp_two_texts AS
  $body$ begin return row('a'::text, 'b'::text); end $body$ LANGUAGE plpgsql;

-- begin-expected
-- columns: t1
-- row: a
-- end-expected
SELECT (mpp_rettt()).t1;

CREATE FUNCTION mpp_retbig() RETURNS mpp_two_int8s AS
  $body$ begin return row(1::bigint, 2::bigint); end $body$ LANGUAGE plpgsql;

-- begin-expected
-- columns: q2
-- row: 2
-- end-expected
SELECT (mpp_retbig()).q2;

-- ---------------------------------------------------------------------------
-- A column definition list that does not describe the result
-- ---------------------------------------------------------------------------

CREATE FUNCTION mpp_srf() RETURNS SETOF record AS
  $body$ begin return query select 1, 2; end $body$ LANGUAGE plpgsql;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: structure of query does not match function result type
-- end-expected-error
SELECT * FROM mpp_srf() AS t(x int, y int, z int);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: structure of query does not match function result type
-- end-expected-error
SELECT * FROM mpp_srf() AS t(x int);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: structure of query does not match function result type
-- end-expected-error
SELECT * FROM mpp_srf() AS t(x text, y text);

-- begin-expected
-- columns: x | y
-- row: 1, 2
-- end-expected
SELECT * FROM mpp_srf() AS t(x int, y int);

-- ---------------------------------------------------------------------------
-- A body that does not parse is a syntax error, not an internal one
-- ---------------------------------------------------------------------------

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "42"
-- end-expected-error
CREATE FUNCTION mpp_bad1() RETURNS int AS
  $body$ declare 42 int; begin return 1; end $body$ LANGUAGE plpgsql;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "42"
-- end-expected-error
DO $$ declare 42 int; begin null; end $$;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "1"
-- end-expected-error
CREATE FUNCTION mpp_bad3() RETURNS int AS
  $body$ declare x int; return 1; end $body$ LANGUAGE plpgsql;

-- ---------------------------------------------------------------------------
-- Cleanup
-- ---------------------------------------------------------------------------

DROP FUNCTION IF EXISTS mpp_retc3(int);
DROP FUNCTION IF EXISTS mpp_retc4(int);
DROP FUNCTION IF EXISTS mpp_rettn();
DROP FUNCTION IF EXISTS mpp_rettt();
DROP FUNCTION IF EXISTS mpp_retbig();
DROP FUNCTION IF EXISTS mpp_srf();
DROP TYPE IF EXISTS mpp_two_int8s CASCADE;
DROP TYPE IF EXISTS mpp_two_texts CASCADE;
DROP TABLE IF EXISTS mpp_v173 CASCADE;
DROP TABLE IF EXISTS mpp_vlog173 CASCADE;
DROP TABLE IF EXISTS mpp_t173 CASCADE;
DROP TABLE IF EXISTS mpp_log173 CASCADE;
DROP TABLE IF EXISTS mpp_st CASCADE;
DROP TABLE IF EXISTS mpp_ss CASCADE;
DROP TABLE IF EXISTS mpp_mt CASCADE;
DROP TABLE IF EXISTS mpp_ms CASCADE;
