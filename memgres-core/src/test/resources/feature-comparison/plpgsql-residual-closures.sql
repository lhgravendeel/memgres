-- ============================================================================
-- Feature Comparison: PL/pgSQL residual closures
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- The rest of what a PL/pgSQL body still got wrong. Three of these were bodies
-- the server runs and memgres refused, which is the direction that matters:
--
--   * A FOR over rows bound its variable in a scope of its own, so reading it
--     after the loop did not resolve at all. In PostgreSQL the target is the
--     variable the block declared, and it holds the last row the loop saw.
--   * A whole row assigned to a %ROWTYPE variable -- r := (SELECT t FROM t ...)
--     or r := ROW(...) -- was stored as one opaque value, so every field of it
--     read NULL.
--   * SELECT ... INTO a record that matched no row left the record with no
--     shape, so the field read that followed did not resolve.
--
-- Alongside those: what a % writes for a whole row, a float and a date; the
-- blanks a char(n) loses when it is read as text; the length and precision an
-- array declaration holds every element to; the fractional-seconds precision a
-- declared timestamp rounds to; the 25P02 an aborted transaction owes SAVEPOINT
-- and RELEASE SAVEPOINT; the 0A000 a FETCH with a multi-row direction and an
-- INTO owes while the body is compiled; the 3F000 a DROP naming an absent
-- schema owes; and the value of type void, which a PL/pgSQL function returns
-- and which is not NULL.
-- ============================================================================

DROP FUNCTION IF EXISTS r12_ch();
DROP FUNCTION IF EXISTS r12_ploop(int);
DROP FUNCTION IF EXISTS r12_vf();
DROP FUNCTION IF EXISTS r12_vfsql();
DROP TABLE IF EXISTS r12_t CASCADE;
DROP TABLE IF EXISTS r12_char CASCADE;
DROP TYPE IF EXISTS r12_nested CASCADE;
DROP TYPE IF EXISTS r12_two CASCADE;
DROP TYPE IF EXISTS r12_ct CASCADE;

CREATE TYPE r12_ct AS (a int, b text);
CREATE TYPE r12_two AS (q1 bigint, q2 bigint);
CREATE TYPE r12_nested AS (c1 bigint, c2 r12_two);
CREATE TABLE r12_t (id int primary key, n int, s varchar(10));
INSERT INTO r12_t VALUES (1,10,'aa'),(2,20,'bb');
CREATE TABLE r12_char (charcol char(4));

-- ---------------------------------------------------------------------------
-- The FOR loop target is the variable the block declared, and it survives
-- ---------------------------------------------------------------------------
DO $$ declare r record; begin
  for r in select * from r12_t order by id loop null; end loop;
  raise notice '% %', r.id, r.n;
end $$;

DO $$ declare r r12_t%rowtype; begin
  for r in select * from r12_t order by id loop null; end loop;
  raise notice '% %', r.id, r.n;
end $$;

DO $$ declare r record; begin
  for r in select * from r12_t order by id loop exit; end loop;
  raise notice '%', r.id;
end $$;

DO $$ declare r record; begin
  for r in execute 'select * from r12_t where id = 1' loop null; end loop;
  raise notice '% %', r.id, r.n;
end $$;

-- a loop that saw no row leaves the query's shape with every field NULL
DO $$ declare r record; begin
  for r in select * from r12_t where false loop null; end loop;
  raise notice '%', r.id;
end $$;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: record "r" has no field "nosuch"
-- end-expected-error
DO $$ declare r record; begin
  for r in select * from r12_t where false loop null; end loop;
  raise notice '%', r.nosuch;
end $$;

-- a single declared scalar is a list of one scalar, not a record target
DO $$ declare i int; begin
  for i in select id from r12_t order by id loop null; end loop;
  raise notice '%', i;
end $$;

CREATE FUNCTION r12_ploop(p int) RETURNS int AS $x$ begin
  for p in select id from r12_t order by id loop null; end loop;
  return p;
end $x$ LANGUAGE plpgsql;

-- begin-expected
-- columns: v
-- row: 2
-- end-expected
SELECT r12_ploop(9) AS v;

-- an inner block's own declaration still shadows
DO $$ declare r text := 'outer'; begin
  declare r record; begin
    for r in select * from r12_t loop null; end loop;
  end;
  raise notice '%', r;
end $$;

-- a comma-separated list of scalars is unchanged
DO $$ declare a int; b int; begin
  for a, b in select id, n from r12_t order by id loop null; end loop;
  raise notice '% %', a, b;
end $$;

-- ---------------------------------------------------------------------------
-- A loop target nobody declared is refused where the body is compiled
-- ---------------------------------------------------------------------------
-- begin-expected-error
-- sqlstate: 42601
-- message-like: loop variable of loop over rows must be a record variable
-- end-expected-error
DO $$ begin for r in select id from r12_t loop null; end loop; end $$;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: "a" is not a known variable
-- end-expected-error
DO $$ begin for a, b in select id, n from r12_t loop null; end loop; end $$;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: loop variable of loop over rows must be a record variable
-- end-expected-error
DO $$ begin for r in execute 'select 1' loop null; end loop; end $$;

-- an integer FOR defines its own variable and never needed a declaration
DO $$ begin for i in 1..3 loop null; end loop; end $$;

-- ---------------------------------------------------------------------------
-- A whole row assigned to a row variable reaches its fields
-- ---------------------------------------------------------------------------
DO $$ declare r r12_t%rowtype; begin
  r := (select x from r12_t x where id = 1);
  raise notice '% % %', r.id, r.n, r.s;
end $$;

DO $$ declare r r12_t%rowtype; begin
  r := row(1,2,'zz'); raise notice '% % %', r.id, r.n, r.s;
end $$;

DO $$ declare r r12_ct; begin
  r := row(7,'q'); raise notice '% %', r.a, r.b;
end $$;

DO $$ declare r r12_t%rowtype; s r12_t%rowtype; begin
  select * into r from r12_t where id = 1; s := r; raise notice '%', s.s;
end $$;

DO $$ declare r r12_t%rowtype; begin r := null; raise notice '%', r.s; end $$;

-- SELECT INTO that matched no row keeps the query's shape
DO $$ declare r record; begin
  select * into r from r12_t where false; raise notice '%', r.id;
end $$;

DO $$ declare a int; begin
  select id into a from r12_t where false; raise notice '% %', a, found;
end $$;

-- ---------------------------------------------------------------------------
-- What a % writes for a row, a float and a date
-- ---------------------------------------------------------------------------
DO $$ declare r record; begin select 1,2 into r; raise notice '%', r; end $$;
DO $$ declare r record; begin select 1 as a, 'x' as b into r; raise notice '%', r; end $$;
DO $$ declare r record; begin
  select 1 as a, 'a,b' as b, 'q"z' as c into r; raise notice '%', r;
end $$;
DO $$ declare r record; begin
  select 1 as a, 'x y' as b, null::text as c into r; raise notice '%', r;
end $$;

-- two columns of one name are two fields, and the first is the one read back
DO $$ declare r record; begin select 1 as a, 2 as a into r; raise notice '%', r.a; end $$;
DO $$ declare r record; begin select 1 as a, 2 as a into r; raise notice '%', r; end $$;

DO $$ begin raise notice '% % %', 3.0::float8, 3.5::float8, 1e20::float8; end $$;
DO $$ begin raise notice '% %', 3.0::float4, 3.0::numeric; end $$;
DO $$ begin raise notice '% % %',
  '2020-01-01 01:02:03'::timestamp, '2020-01-01'::date, '01:02:03'::time; end $$;
DO $$ begin raise notice '%', '1 day'::interval; end $$;

-- ---------------------------------------------------------------------------
-- An array declaration holds every element to its element type
-- ---------------------------------------------------------------------------
-- begin-expected-error
-- sqlstate: 22001
-- message-like: value too long for type character varying(5)
-- end-expected-error
DO $$ declare v varchar(5)[]; begin v := ARRAY['abcdefgh']; end $$;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: numeric field overflow
-- end-expected-error
DO $$ declare v numeric(4,2)[]; begin v := ARRAY[12345.1]; end $$;

DO $$ declare v numeric(4,2)[]; begin v := ARRAY[1.239]; raise notice '%', v; end $$;
DO $$ declare v char(3)[]; begin v := ARRAY['ab','c']; raise notice '%', v; end $$;
DO $$ declare v varchar(5)[]; begin v := ARRAY['abcde','x']; raise notice '%', v; end $$;
DO $$ declare v varchar(5)[]; begin v := ARRAY[NULL, 'ab']; raise notice '%', v; end $$;
DO $$ declare v varchar(5)[]; begin v := NULL; raise notice '%', v; end $$;
DO $$ declare v text[]; begin v := ARRAY['abcdefgh']; raise notice '%', v; end $$;
DO $$ declare v int[][]; begin v := ARRAY[ARRAY[1,2],ARRAY[3,4]]; raise notice '%', v; end $$;

-- ---------------------------------------------------------------------------
-- A declared fractional-seconds precision rounds rather than truncates
-- ---------------------------------------------------------------------------
DO $$ declare v timestamp(0); begin
  v := '2020-01-01 01:02:03.987'; raise notice '%', v; end $$;
DO $$ declare v timestamp(2); begin
  v := '2020-01-01 01:02:03.987'; raise notice '%', v; end $$;
DO $$ declare v time(0); begin v := '01:02:03.987'; raise notice '%', v; end $$;
DO $$ declare v interval(0); begin v := '1 day 00:00:01.987'; raise notice '%', v; end $$;
DO $$ declare v timestamp; begin
  v := '2020-01-01 01:02:03.987'; raise notice '%', v; end $$;
DO $$ declare v timestamp(6); begin
  v := '2020-01-01 01:02:03.987654'; raise notice '%', v; end $$;

-- begin-expected
-- columns: a | b
-- row: 2020-01-01 01:02:04, 01:02:03.99
-- end-expected
SELECT '2020-01-01 01:02:03.987'::timestamp(0)::text AS a,
       '01:02:03.987'::time(2)::text AS b;

-- ---------------------------------------------------------------------------
-- A char(n) loses its padding when it is read as text
-- ---------------------------------------------------------------------------
CREATE FUNCTION r12_ch() RETURNS text AS $x$
declare v r12_char.charcol%TYPE := 'ab'; begin return v; end $x$ LANGUAGE plpgsql;

-- begin-expected
-- columns: v
-- row: [ab]
-- end-expected
SELECT '[' || r12_ch() || ']' AS v;

DO $$ declare v char(4) := 'ab'; w text; begin w := v; raise notice '[%]', w; end $$;
DO $$ declare v char(4) := 'ab'; w varchar; begin w := v; raise notice '[%]', w; end $$;
-- the variable itself is still padded, and a text value keeps blanks of its own
DO $$ declare v char(4) := 'ab'; begin raise notice '[%]', v; end $$;
DO $$ declare w text; begin w := 'ab  '; raise notice '[%]', w; end $$;

-- ---------------------------------------------------------------------------
-- An aborted transaction refuses SAVEPOINT and RELEASE SAVEPOINT while still
-- allowing ROLLBACK TO, which is asserted in PlpgsqlResidualClosuresTest rather
-- than here: this harness ends the block when a statement errors, so the aborted
-- state never reaches the next statement and all three answer 25P01 instead.
-- ---------------------------------------------------------------------------

-- the ordinary savepoint sequence is unaffected
BEGIN;
SAVEPOINT r12_a;
INSERT INTO r12_t VALUES (3,30,'cc');
SAVEPOINT r12_b;
INSERT INTO r12_t VALUES (4,40,'dd');
ROLLBACK TO r12_a;
RELEASE SAVEPOINT r12_a;
COMMIT;

-- begin-expected
-- columns: v
-- row: 1,2
-- end-expected
SELECT string_agg(id::text, ',' ORDER BY id) AS v FROM r12_t;

-- ---------------------------------------------------------------------------
-- FETCH ... INTO takes one row, and the direction decides
-- ---------------------------------------------------------------------------
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FETCH statement cannot return multiple rows
-- end-expected-error
DO $$ declare c refcursor; a int; b int; begin
  open c for select id, n from r12_t; fetch forward all from c into a, b; end $$;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FETCH statement cannot return multiple rows
-- end-expected-error
DO $$ declare c refcursor; a int; b int; begin
  open c for select id, n from r12_t; fetch forward 1 from c into a, b; end $$;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FETCH statement cannot return multiple rows
-- end-expected-error
DO $$ declare c refcursor; a int; b int; begin
  open c for select id, n from r12_t; fetch backward 1 from c into a, b; end $$;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FETCH statement cannot return multiple rows
-- end-expected-error
DO $$ declare c refcursor; a int; b int; begin
  open c for select id, n from r12_t; fetch all from c into a, b; end $$;

-- the single-row directions and MOVE are unaffected
DO $$ declare c refcursor; a int; b text; begin
  open c for select id, s from r12_t order by id;
  fetch from c into a, b; raise notice '% %', a, b; end $$;
DO $$ declare c refcursor; a int; b text; begin
  open c for select id, s from r12_t order by id;
  fetch last from c into a, b; raise notice '% %', a, b; end $$;
DO $$ declare c refcursor; a int; b text; begin
  open c for select id, s from r12_t order by id;
  fetch absolute 2 from c into a, b; raise notice '% %', a, b; end $$;
DO $$ declare c refcursor; a int; b text; begin
  open c for select id, s from r12_t order by id;
  fetch forward from c into a, b; raise notice '% %', a, b; end $$;
DO $$ declare c refcursor; begin
  open c for select id from r12_t; move forward all in c; raise notice 'ok'; end $$;

-- ---------------------------------------------------------------------------
-- A DROP naming a schema that is not there reports the schema
-- ---------------------------------------------------------------------------
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "r12_nosuch" does not exist
-- end-expected-error
DROP TABLE r12_nosuch.t;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "r12_nosuch" does not exist
-- end-expected-error
DROP VIEW r12_nosuch.v;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "r12_nosuch" does not exist
-- end-expected-error
DROP SEQUENCE r12_nosuch.s;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "r12_nosuch" does not exist
-- end-expected-error
DROP INDEX r12_nosuch.i;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "r12_nosuch" does not exist
-- end-expected-error
DROP TYPE r12_nosuch.ty;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "r12_nosuch" does not exist
-- end-expected-error
TRUNCATE r12_nosuch.t;

DROP TABLE IF EXISTS r12_nosuch.t;
DROP VIEW IF EXISTS r12_nosuch.v;
DROP SCHEMA IF EXISTS r12_nosuch;

-- a schema that is there still reports the object
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: table "r12_nosuchtable" does not exist
-- end-expected-error
DROP TABLE public.r12_nosuchtable;

DROP TABLE IF EXISTS public.r12_nosuchtable;

-- ---------------------------------------------------------------------------
-- A field path of three parts is not a field path at all
-- ---------------------------------------------------------------------------
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "c2"
-- end-expected-error
DO $$ declare c r12_nested; begin raise notice '%', c.c2.q1; end $$;

-- the write is a target rather than an expression, and it does resolve
DO $$ declare c r12_nested; begin c.c2.q1 := 5; raise notice '%', (c.c2).q1; end $$;
DO $$ declare c r12_nested; begin raise notice '%', c.c1; end $$;

-- ---------------------------------------------------------------------------
-- The value of type void
-- ---------------------------------------------------------------------------
CREATE FUNCTION r12_vf() RETURNS void LANGUAGE plpgsql AS $x$ begin null; end $x$;
CREATE FUNCTION r12_vfsql() RETURNS void LANGUAGE sql AS $x$ SELECT 1 $x$;

-- begin-expected
-- columns: a | b | c
-- row: f, [], t
-- end-expected
SELECT r12_vf() IS NULL AS a,
       '[' || r12_vf()::text || ']' AS b,
       r12_vfsql() IS NULL AS c;

-- ---------------------------------------------------------------------------
-- Teardown
-- ---------------------------------------------------------------------------
DROP FUNCTION IF EXISTS r12_ch();
DROP FUNCTION IF EXISTS r12_ploop(int);
DROP FUNCTION IF EXISTS r12_vf();
DROP FUNCTION IF EXISTS r12_vfsql();
DROP TABLE IF EXISTS r12_t CASCADE;
DROP TABLE IF EXISTS r12_char CASCADE;
DROP TYPE IF EXISTS r12_nested CASCADE;
DROP TYPE IF EXISTS r12_two CASCADE;
DROP TYPE IF EXISTS r12_ct CASCADE;
