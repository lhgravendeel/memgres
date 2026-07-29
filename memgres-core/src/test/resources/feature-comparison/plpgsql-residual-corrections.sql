-- ============================================================================
-- Feature Comparison: PL/pgSQL residual corrections
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- The checks a PL/pgSQL body carries have to stop where PostgreSQL's stop.
-- Four places where they did not:
--
--   * A RAISE argument was split on every comma, so ARRAY[1,2] counted as two
--     arguments and the body was refused with "too many parameters".
--   * A format string written with dollar quotes was read as a condition name
--     instead, so the RAISE would not parse at all.
--   * SELECT/FETCH/EXECUTE/FOR into a %ROWTYPE or composite variable keyed the
--     row by the query's column names, so anything the query computed —
--     n*2, upper(s) — landed under a name the variable has not got, and the
--     field read that followed was refused.
--   * ALIAS FOR $1 reached only an unnamed parameter, so the ordinary
--     "declare a alias for $1" over a named parameter failed when it ran.
--
-- Alongside those: what a % writes (an array in braces, a boolean as t or f,
-- a NULL as <NULL>), the message RAISE SQLSTATE reports, the length a number
-- stored in a varchar(n) is held to, and the case a trigger names its row in.
-- ============================================================================

DROP FUNCTION IF EXISTS plrc_ph();
DROP FUNCTION IF EXISTS plrc_render();
DROP FUNCTION IF EXISTS plrc_rows();
DROP FUNCTION IF EXISTS plrc_loop();
DROP FUNCTION IF EXISTS plrc_alias(int);
DROP FUNCTION IF EXISTS plrc_aliasw(int);
DROP FUNCTION IF EXISTS plrc_tf() CASCADE;
DROP TABLE IF EXISTS plrc_t CASCADE;
DROP TYPE IF EXISTS plrc_two CASCADE;

CREATE TYPE plrc_two AS (q1 bigint, q2 bigint);
CREATE TABLE plrc_t (id int primary key, n int, s varchar(10));
INSERT INTO plrc_t VALUES (1,10,'aa'),(2,20,'bb');

-- ---------------------------------------------------------------------------
-- An array literal is one RAISE argument, not one per element
-- ---------------------------------------------------------------------------
CREATE FUNCTION plrc_ph() RETURNS void AS $x$ begin raise notice '%', ARRAY[1,2]; end $x$ LANGUAGE plpgsql;
DO $$ begin raise notice '%', ARRAY[1,2,3]; end $$;
DO $$ begin raise notice '% %', ARRAY[1,2], ARRAY['a','b']; end $$;
DO $$ begin raise notice '%', array[ 1 , 2 ]; end $$;
DO $$ begin raise notice '%', ARRAY[1] || ARRAY[2,3]; end $$;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: {1,2}
-- end-expected-error
DO $$ begin raise exception '%', ARRAY[1,2] using errcode = '22012'; end $$;

-- the shapes either side of it are read the same way they always were
DO $$ begin raise notice '% %', coalesce(1,2), greatest(3,4); end $$;
DO $$ begin raise notice '%', (select count(*) from plrc_t); end $$;
DO $$ begin raise notice '%', row(1,2); end $$;
DO $$ begin raise notice '100%% done'; end $$;

-- and a comma that really does separate arguments still counts
-- begin-expected-error
-- sqlstate: 42601
-- message-like: too many parameters specified for RAISE
-- end-expected-error
DO $$ begin raise notice '%', 1, 2; end $$;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: too few parameters specified for RAISE
-- end-expected-error
DO $$ begin raise notice '% %', ARRAY[1,2]; end $$;

-- ---------------------------------------------------------------------------
-- A dollar-quoted format string is a format string
-- ---------------------------------------------------------------------------
DO $$ begin raise notice $q$dollar % quoted$q$, 7; end $$;

-- ---------------------------------------------------------------------------
-- What a % writes: the value as its type writes it, and <NULL> for nothing
-- ---------------------------------------------------------------------------
CREATE FUNCTION plrc_render() RETURNS text AS $x$
declare a int[] := ARRAY[1,2]; b boolean := false; c int;
begin
  begin raise exception '%/%/%', a, b, c; exception when others then return sqlerrm; end;
end $x$ LANGUAGE plpgsql;

-- begin-expected
-- columns: v
-- row: {1,2}/f/<NULL>
-- end-expected
SELECT plrc_render() AS v;

-- ---------------------------------------------------------------------------
-- A row variable takes the query's columns in order, under its own field names
-- ---------------------------------------------------------------------------
CREATE FUNCTION plrc_rows() RETURNS text AS $x$
declare r plrc_t%rowtype; c plrc_two; out_ text := '';
begin
  select id, n*2, upper(s) into r from plrc_t where id = 1;
  out_ := r.id || '/' || r.n || '/' || r.s;
  select 5, 6 into c;
  out_ := out_ || ' ' || c.q1 || '/' || c.q2;
  execute 'select id, n*3, s from plrc_t where id = 2' into r;
  out_ := out_ || ' ' || r.id || '/' || r.n || '/' || r.s;
  return out_;
end $x$ LANGUAGE plpgsql;

-- begin-expected
-- columns: v
-- row: 1/20/AA 5/6 2/60/bb
-- end-expected
SELECT plrc_rows() AS v;

-- a cursor fetches into one the same way
DO $$ declare c cursor for select id, n*2, s from plrc_t order by id; r plrc_t%rowtype; begin
  open c; fetch c into r;
  if r.id || '/' || r.n || '/' || r.s <> '1/20/aa' then raise exception 'bad %', r.n; end if;
end $$;

-- and a FOR loop binds each row into one the same way
CREATE FUNCTION plrc_loop() RETURNS text AS $x$
declare r plrc_t%rowtype; c plrc_two; out_ text := '';
begin
  for r in select id, n*2, s from plrc_t order by id loop out_ := out_ || r.id || ':' || r.n || ' '; end loop;
  for c in select 8, 9 loop out_ := out_ || c.q1 || '/' || c.q2; end loop;
  return out_;
end $x$ LANGUAGE plpgsql;

-- begin-expected
-- columns: v
-- row: 1:20 2:40 8/9
-- end-expected
SELECT plrc_loop() AS v;

-- a record loop variable still takes the query's own column names
DO $$ declare r record; t int := 0; begin
  for r in select id, n*2 as doubled from plrc_t order by id loop t := t + r.doubled; end loop;
  if t <> 60 then raise exception 'bad %', t; end if;
end $$;

-- and the whole row still arrives when the names do match
DO $$ declare r plrc_t%rowtype; begin
  select * into r from plrc_t where id = 2;
  if r.id || '/' || r.n || '/' || r.s <> '2/20/bb' then raise exception 'bad %', r.n; end if;
end $$;

-- a field neither kind of loop variable has is refused
-- begin-expected-error
-- sqlstate: 42703
-- message-like: record "r" has no field "nosuch"
-- end-expected-error
DO $$ declare r plrc_t%rowtype; begin for r in select * from plrc_t loop raise notice '%', r.nosuch; end loop; end $$;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: record "r" has no field "nosuch"
-- end-expected-error
DO $$ declare r record; begin for r in select * from plrc_t loop raise notice '%', r.nosuch; end loop; end $$;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: record "r" has no field "nosuch"
-- end-expected-error
DO $$ declare r record; begin for r in execute 'select * from plrc_t' loop raise notice '%', r.nosuch; end loop; end $$;

-- ---------------------------------------------------------------------------
-- ALIAS FOR reaches a named parameter through its position
-- ---------------------------------------------------------------------------
CREATE FUNCTION plrc_alias(p int) RETURNS int AS $x$ declare a alias for $1; begin return a + 1; end $x$ LANGUAGE plpgsql;

-- begin-expected
-- columns: v
-- row: 6
-- end-expected
SELECT plrc_alias(5) AS v;

-- and writing through it writes the parameter, which the name still reads
CREATE FUNCTION plrc_aliasw(p int) RETURNS int AS $x$ declare a alias for $1; begin a := a * 2; return p; end $x$ LANGUAGE plpgsql;

-- begin-expected
-- columns: v
-- row: 10
-- end-expected
SELECT plrc_aliasw(5) AS v;

-- ---------------------------------------------------------------------------
-- RAISE SQLSTATE reports the SQLSTATE as its message when nothing else does
-- ---------------------------------------------------------------------------
-- begin-expected-error
-- sqlstate: 22012
-- message-like: 22012
-- end-expected-error
DO $$ begin raise sqlstate '22012'; end $$;

-- begin-expected-error
-- sqlstate: ZZ999
-- message-like: ZZ999
-- end-expected-error
DO $$ begin raise exception sqlstate 'ZZ999'; end $$;

-- but a message it was given is the message
-- begin-expected-error
-- sqlstate: 22012
-- message-like: said so
-- end-expected-error
DO $$ begin raise exception sqlstate '22012' using message = 'said so'; end $$;

-- ---------------------------------------------------------------------------
-- A number stored in a varchar(n) is held to its written length
-- ---------------------------------------------------------------------------
-- begin-expected-error
-- sqlstate: 22001
-- message-like: value too long for type character varying(5)
-- end-expected-error
DO $$ declare v varchar(5); begin v := 123456; end $$;

DO $$ declare v varchar(5); begin v := 12345; if v::text <> '12345' then raise exception 'bad %', v; end if; end $$;

-- ---------------------------------------------------------------------------
-- A trigger names its row the way the body wrote it
-- ---------------------------------------------------------------------------
CREATE FUNCTION plrc_tf() RETURNS trigger AS $x$ begin new.nosuchcol := 1; return new; end $x$ LANGUAGE plpgsql;
CREATE TRIGGER plrc_tr BEFORE INSERT ON plrc_t FOR EACH ROW EXECUTE FUNCTION plrc_tf();

-- begin-expected-error
-- sqlstate: 42703
-- message-like: record "new" has no field "nosuchcol"
-- end-expected-error
INSERT INTO plrc_t VALUES (9,9,'z');

DROP TRIGGER plrc_tr ON plrc_t;

DROP FUNCTION IF EXISTS plrc_ph();
DROP FUNCTION IF EXISTS plrc_render();
DROP FUNCTION IF EXISTS plrc_rows();
DROP FUNCTION IF EXISTS plrc_loop();
DROP FUNCTION IF EXISTS plrc_alias(int);
DROP FUNCTION IF EXISTS plrc_aliasw(int);
DROP FUNCTION IF EXISTS plrc_tf() CASCADE;
DROP TABLE IF EXISTS plrc_t CASCADE;
DROP TYPE IF EXISTS plrc_two CASCADE;
