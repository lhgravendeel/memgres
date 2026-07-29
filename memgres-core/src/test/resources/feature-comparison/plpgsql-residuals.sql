-- ============================================================================
-- Feature Comparison: PL/pgSQL residuals
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- What a PL/pgSQL body still let through. A FETCH that landed on no row left
-- the INTO target holding the previous row's value, so a loop reading "until
-- the variable stops changing" saw the last row twice. A write to a field a
-- composite variable has not got was dropped into a map nobody reads back, so
-- a misspelt field name lost the value with no sign of it. A declaration's
-- length and precision were advisory: a varchar(3) local held anything.
--
-- Alongside those: ALIAS FOR did not parse at all; RAISE accepted a format
-- string with more placeholders than arguments and an unrecognised USING
-- option, and refused a condition name after a level; GET STACKED DIAGNOSTICS
-- judged its own arguments in the wrong order; and SAVEPOINT worked outside a
-- transaction block.
--
-- Which checks belong to the compile and which to the run is measured, not
-- assumed: everything below that PostgreSQL reports at CREATE FUNCTION is
-- shown firing on a body whose statement is unreachable, and everything it
-- reports at run time is shown *not* firing there.
-- ============================================================================

DROP FUNCTION IF EXISTS plr_fetchkeep();
DROP FUNCTION IF EXISTS plr_fetchloop();
DROP FUNCTION IF EXISTS plr_fetchrec();
DROP FUNCTION IF EXISTS plr_alias(int);
DROP FUNCTION IF EXISTS plr_aliasvar(int);
DROP FUNCTION IF EXISTS plr_ph();
DROP FUNCTION IF EXISTS plr_diagok();
DROP TYPE IF EXISTS plr_nested CASCADE;
DROP TYPE IF EXISTS plr_two CASCADE;
DROP TABLE IF EXISTS plr_dt CASCADE;

CREATE TYPE plr_two AS (q1 bigint, q2 bigint);
CREATE TYPE plr_nested AS (c1 bigint, c2 plr_two);
CREATE TABLE plr_dt (a varchar(3), b int);

-- ---------------------------------------------------------------------------
-- A FETCH that finds no row sets its target to NULL
-- ---------------------------------------------------------------------------
CREATE FUNCTION plr_fetchkeep() RETURNS text AS $$
declare cur cursor for select 42; n int;
begin open cur; fetch cur into n; fetch cur into n; return coalesce(n::text,'NULL'); end $$ LANGUAGE plpgsql;

-- begin-expected
-- columns: v
-- row: NULL
-- end-expected
SELECT plr_fetchkeep() AS v;

-- FOUND says the fetch failed, and the target no longer carries the last row
CREATE FUNCTION plr_fetchloop() RETURNS text AS $$
declare cur refcursor; n int; acc text := '';
begin
  open cur for select * from (values (1),(2),(3)) v(x);
  loop fetch cur into n; exit when not found; acc := acc || n::text; end loop;
  fetch cur into n;
  return acc || '/' || found::text || '/' || coalesce(n::text,'NULL');
end $$ LANGUAGE plpgsql;

-- begin-expected
-- columns: v
-- row: 123/false/NULL
-- end-expected
SELECT plr_fetchloop() AS v;

-- A record target is emptied the same way
CREATE FUNCTION plr_fetchrec() RETURNS text AS $$
declare cur cursor for select 7 as a; r record;
begin open cur; fetch cur into r; fetch cur into r; return coalesce(r.a::text,'NULL'); end $$ LANGUAGE plpgsql;

-- begin-expected
-- columns: v
-- row: NULL
-- end-expected
SELECT plr_fetchrec() AS v;

-- ---------------------------------------------------------------------------
-- A field a composite variable has not got cannot be written or read
-- ---------------------------------------------------------------------------
-- begin-expected-error
-- sqlstate: 42703
-- message-like: record "c" has no field "x"
-- end-expected-error
DO $$ declare c plr_two; begin c.x := 1; end $$;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: record "c" has no field "x"
-- end-expected-error
DO $$ declare c plr_two; v bigint; begin v := c.x; end $$;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: no such column in data type plr_two
-- end-expected-error
DO $$ declare c plr_nested; begin c.c2.x := 1; end $$;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: record "c" has no field "zz"
-- end-expected-error
DO $$ declare c plr_nested; begin c.zz.q1 := 1; end $$;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: record "r" has no field "nosuch"
-- end-expected-error
DO $$ declare r plr_dt%rowtype; begin r.nosuch := 1; end $$;

-- A record's fields are the ones the row assigned to it carried
-- begin-expected-error
-- sqlstate: 42703
-- message-like: record "r" has no field "zzz"
-- end-expected-error
DO $$ declare r record; begin select 1 as a into r; r.zzz := 2; end $$;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: record "r" has no field "zzz"
-- end-expected-error
DO $$ declare r record; v int; begin select 1 as a into r; v := r.zzz; end $$;

-- The check is reached, not compiled: an unreachable bad write is accepted
DO $$ declare c plr_two; begin if false then c.x := 1; end if; end $$;
DO $$ declare r plr_dt%rowtype; begin if false then r.nosuch := 1; end if; end $$;

-- The fields it does have work in both directions, nested ones included
DO $$ declare c plr_two; begin c.q1 := 1; if c.q1 <> 1 then raise exception 'bad'; end if; end $$;
DO $$ declare c plr_nested; begin c.c2.q1 := 5; end $$;
DO $$ declare c plr_two; v bigint; begin v := c.q1; if v is not null then raise exception 'bad'; end if; end $$;
DO $$ declare r record; begin select 1 as a into r; r.a := 2; if r.a <> 2 then raise exception 'bad'; end if; end $$;
DO $$ declare r record; v text; begin select 1 as a, 2 as b into r; v := r.a::text || '/' || r.b::text; if v <> '1/2' then raise exception 'bad %', v; end if; end $$;
DO $$ declare r plr_dt%rowtype; begin select 'ab' into r.a; if r.a <> 'ab' then raise exception 'bad %', r.a; end if; end $$;

-- ---------------------------------------------------------------------------
-- A declaration's length and precision hold on every write, not just the first
-- ---------------------------------------------------------------------------
-- begin-expected-error
-- sqlstate: 22001
-- message-like: value too long for type character varying(3)
-- end-expected-error
DO $$ declare v varchar(3); begin v := 'abcdef'; end $$;

-- begin-expected-error
-- sqlstate: 22001
-- message-like: value too long for type character varying(3)
-- end-expected-error
DO $$ declare v varchar(3) := 'abcdef'; begin null; end $$;

-- %TYPE copies the column's length along with its type
-- begin-expected-error
-- sqlstate: 22001
-- message-like: value too long for type character varying(3)
-- end-expected-error
DO $$ declare v plr_dt.a%type; begin v := 'abcdef'; end $$;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: numeric field overflow
-- end-expected-error
DO $$ declare v numeric(3,1); begin v := 12345.6; end $$;

-- Reached, not compiled
DO $$ declare v varchar(3); begin if false then v := 'abcdef'; end if; end $$;

-- What fits is stored, char pads and numeric rounds to its scale
DO $$ declare v varchar(3); begin v := 'abc'; if v <> 'abc' then raise exception 'bad'; end if; end $$;
DO $$ declare v char(5); begin v := 'ab'; if v <> 'ab   ' then raise exception 'nopad [%]', v; end if; end $$;
DO $$ declare v numeric(4,2); begin v := 1.239; if v <> 1.24 then raise exception 'bad %', v; end if; end $$;
DO $$ declare v varchar; begin v := 'abcdefghij'; if length(v) <> 10 then raise exception 'bad'; end if; end $$;
DO $$ declare v text; begin v := 'abcdefghij'; end $$;

-- ---------------------------------------------------------------------------
-- CONSTANT and NOT NULL, and which of the two checks the compile makes
-- ---------------------------------------------------------------------------
-- Writing to a CONSTANT is refused while the body is compiled, so even an
-- unreachable write refuses the function
-- begin-expected-error
-- sqlstate: 22005
-- message-like: is declared CONSTANT
-- end-expected-error
CREATE FUNCTION plr_ph() RETURNS void AS $x$ declare c constant int := 1; begin if false then c := 2; end if; end $x$ LANGUAGE plpgsql;

-- begin-expected-error
-- sqlstate: 22005
-- message-like: is declared CONSTANT
-- end-expected-error
DO $$ declare c constant int := 1; cur cursor for select 1; begin open cur; fetch cur into c; end $$;

-- begin-expected-error
-- sqlstate: 22005
-- message-like: is declared CONSTANT
-- end-expected-error
DO $$ declare c constant int := 1; begin get diagnostics c = row_count; end $$;

-- begin-expected-error
-- sqlstate: 22005
-- message-like: is declared CONSTANT
-- end-expected-error
DO $$ declare c constant int := 1; begin foreach c in array array[1,2] loop null; end loop; end $$;

-- A NOT NULL variable with no default is refused at the compile too
-- begin-expected-error
-- sqlstate: 22004
-- message-like: must have a default value
-- end-expected-error
CREATE FUNCTION plr_ph() RETURNS void AS $x$ declare y int not null; begin null; end $x$ LANGUAGE plpgsql;

-- but assigning NULL to it is only refused where the assignment is reached
-- begin-expected-error
-- sqlstate: 22004
-- message-like: declared NOT NULL
-- end-expected-error
DO $$ declare x int not null := 1; begin x := null; end $$;

DO $$ declare x int not null := 1; begin if false then x := null; end if; end $$;
DO $$ declare c constant int := 1; begin if c <> 1 then raise exception 'bad'; end if; end $$;
DO $$ declare x int not null default 3; begin if x <> 3 then raise exception 'bad'; end if; end $$;

-- ---------------------------------------------------------------------------
-- ALIAS FOR gives an existing variable a second name
-- ---------------------------------------------------------------------------
CREATE FUNCTION plr_alias(int) RETURNS int AS $$ declare n alias for $1; begin return n + 1; end $$ LANGUAGE plpgsql;

-- begin-expected
-- columns: v
-- row: 6
-- end-expected
SELECT plr_alias(5) AS v;

-- Writing through the alias writes the variable it names
CREATE FUNCTION plr_aliasvar(a int) RETURNS int AS $$ declare n alias for a; begin n := n + 1; return a; end $$ LANGUAGE plpgsql;

-- begin-expected
-- columns: v
-- row: 6
-- end-expected
SELECT plr_aliasvar(5) AS v;

-- The name it stands for has to exist, and the compile says so
-- begin-expected-error
-- sqlstate: 42704
-- message-like: variable "plr_nosuchvar" does not exist
-- end-expected-error
DO $$ declare n alias for plr_nosuchvar; begin null; end $$;

-- ---------------------------------------------------------------------------
-- RAISE: what the compile checks and what the run checks
-- ---------------------------------------------------------------------------
-- The format string and its arguments have to agree, and the compile says so
-- begin-expected-error
-- sqlstate: 42601
-- message-like: too few parameters specified for RAISE
-- end-expected-error
DO $$ begin raise notice 'too few: %, %, %', 1, 1; end $$;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: too few parameters specified for RAISE
-- end-expected-error
DO $$ begin raise notice 'trailing percent %'; end $$;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: too few parameters specified for RAISE
-- end-expected-error
CREATE FUNCTION plr_ph() RETURNS void AS $x$ begin if false then raise notice '% %', 1; end if; end $x$ LANGUAGE plpgsql;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: too many parameters specified for RAISE
-- end-expected-error
DO $$ begin raise notice 'one %', 1, 2; end $$;

-- An option name that is not one of RAISE's is a compile error
-- begin-expected-error
-- sqlstate: 42601
-- message-like: unrecognized RAISE statement option
-- end-expected-error
DO $$ begin raise notice 'x' using nosuchopt = 'd'; end $$;

-- A condition name has to be one that exists
-- begin-expected-error
-- sqlstate: 42704
-- message-like: unrecognized exception condition
-- end-expected-error
DO $$ begin raise notice plr_no_such_condition; end $$;

-- Giving the same option twice is judged when the RAISE runs, not before
-- begin-expected-error
-- sqlstate: 42601
-- message-like: RAISE option already specified: DETAIL
-- end-expected-error
DO $$ begin raise notice 'x' using detail = 'd', detail = 'e'; end $$;

-- A format string is already the message, so USING MESSAGE would be a second
-- begin-expected-error
-- sqlstate: 42601
-- message-like: RAISE option already specified: MESSAGE
-- end-expected-error
DO $$ begin raise exception 'lit' using message = 'other'; end $$;

DO $$ begin if false then raise notice 'x' using hint = 'a', hint = 'b'; end if; end $$;

-- A bare RAISE has an exception to re-raise only inside a handler
-- begin-expected-error
-- sqlstate: 0Z002
-- message-like: RAISE without parameters cannot be used outside an exception handler
-- end-expected-error
DO $$ begin raise; end $$;

DO $$ begin if false then raise; end if; end $$;

-- A condition name may follow a level, and it names the SQLSTATE to report
DO $$ begin raise notice division_by_zero; end $$;
DO $$ begin raise notice unique_violation; end $$;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: custom message
-- end-expected-error
DO $$ begin raise division_by_zero using message = 'custom' || ' message'; end $$;

-- The ordinary shapes still work
DO $$ begin raise notice 'plain %', 42; end $$;
DO $$ begin raise notice '100%% done'; end $$;
DO $$ begin raise debug 'd'; raise log 'l'; raise info 'i'; raise warning 'w'; end $$;

-- begin-expected-error
-- sqlstate: 22012
-- message-like: boom
-- end-expected-error
DO $$ begin raise exception 'boom' using hint = 'a' || 'b', errcode = '22012'; end $$;

-- ---------------------------------------------------------------------------
-- GET DIAGNOSTICS checks its arguments before it asks where it is
-- ---------------------------------------------------------------------------
-- An unknown target name outranks everything else about the statement
-- begin-expected-error
-- sqlstate: 42601
-- message-like: "plr_nv" is not a known variable
-- end-expected-error
DO $$ begin get stacked diagnostics plr_nv = returned_sqlstate; end $$;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: "plr_nv" is not a known variable
-- end-expected-error
DO $$ begin get stacked diagnostics plr_nv = plr_no_such_item; end $$;

-- then an unknown item name, ahead of the form's own list
-- begin-expected-error
-- sqlstate: 42601
-- message-like: unrecognized GET DIAGNOSTICS item
-- end-expected-error
DO $$ declare v text; begin get stacked diagnostics v = plr_no_such_item; end $$;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: unrecognized GET DIAGNOSTICS item
-- end-expected-error
DO $$ declare v int; w text; begin get stacked diagnostics v = row_count, w = plr_no_such_item; end $$;

-- and only then whether the item belongs to this form
-- begin-expected-error
-- sqlstate: 42601
-- message-like: ROW_COUNT is not allowed in GET STACKED DIAGNOSTICS
-- end-expected-error
DO $$ declare n int; begin get stacked diagnostics n = row_count; end $$;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: RETURNED_SQLSTATE is not allowed in GET CURRENT DIAGNOSTICS
-- end-expected-error
DO $$ declare v text; begin get diagnostics v = returned_sqlstate; end $$;

-- Whether a handler is running is the one question left to the run
-- begin-expected-error
-- sqlstate: 0Z002
-- message-like: GET STACKED DIAGNOSTICS cannot be used outside an exception handler
-- end-expected-error
DO $$ declare v text; begin get stacked diagnostics v = returned_sqlstate; end $$;

DO $$ declare v text; begin if false then get stacked diagnostics v = returned_sqlstate; end if; end $$;

-- GET CURRENT DIAGNOSTICS says what plain GET DIAGNOSTICS already means
DO $$ declare n int; s text; begin get current diagnostics n = row_count, s = pg_context; end $$;

-- Inside a handler it reports the raised condition by its own name
CREATE FUNCTION plr_diagok() RETURNS text AS $$
declare s text; m text;
begin
  begin raise division_by_zero; exception when others then
    get stacked diagnostics s = returned_sqlstate, m = message_text;
  end;
  return s || '/' || m;
end $$ LANGUAGE plpgsql;

-- begin-expected
-- columns: v
-- row: 22012/division_by_zero
-- end-expected
SELECT plr_diagok() AS v;

-- ---------------------------------------------------------------------------
-- Guards a statement gets before it runs
-- ---------------------------------------------------------------------------
-- begin-expected-error
-- sqlstate: 25P01
-- message-like: SAVEPOINT can only be used in transaction blocks
-- end-expected-error
SAVEPOINT plr_sp;

-- begin-expected-error
-- sqlstate: 25P01
-- message-like: RELEASE SAVEPOINT can only be used in transaction blocks
-- end-expected-error
RELEASE SAVEPOINT plr_nosuch;

-- begin-expected-error
-- sqlstate: 25P01
-- message-like: ROLLBACK TO SAVEPOINT can only be used in transaction blocks
-- end-expected-error
ROLLBACK TO SAVEPOINT plr_nosuch;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: unrecognized encoding: "nosuch"
-- end-expected-error
SELECT encode('a'::bytea, 'nosuch');

-- begin-expected
-- columns: a | b | c
-- row: YQ==, 61, a
-- end-expected
SELECT encode('a'::bytea,'base64') AS a, encode('a'::bytea,'hex') AS b, encode('a'::bytea,'escape') AS c;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type numeric
-- end-expected-error
SELECT to_number('abc', '9999');

-- A query names a relation, not a schema, so the whole qualified name is what
-- is reported missing
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "plr_nosuchschema.t" does not exist
-- end-expected-error
SELECT * FROM plr_nosuchschema.t;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "plr_nosuchschema.t" does not exist
-- end-expected-error
INSERT INTO plr_nosuchschema.t VALUES (1);

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "plr_nosuchschema.t" does not exist
-- end-expected-error
DELETE FROM plr_nosuchschema.t;

-- A year past what the type holds is a range problem, not a spelling one
-- begin-expected-error
-- sqlstate: 22008
-- message-like: timestamp out of range
-- end-expected-error
SELECT '294277-01-01'::timestamp;

-- begin-expected-error
-- sqlstate: 22008
-- message-like: date out of range
-- end-expected-error
SELECT '5874898-01-01'::date;

-- but a year the type does hold is read, however wide it is written
-- begin-expected
-- columns: d
-- row: 294277-01-01
-- end-expected
SELECT '294277-01-01'::date AS d;

-- and text that is not a date at all never had a field to overflow
-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type date
-- end-expected-error
SELECT 'nonsense'::date;

DROP FUNCTION IF EXISTS plr_fetchkeep();
DROP FUNCTION IF EXISTS plr_fetchloop();
DROP FUNCTION IF EXISTS plr_fetchrec();
DROP FUNCTION IF EXISTS plr_alias(int);
DROP FUNCTION IF EXISTS plr_aliasvar(int);
DROP FUNCTION IF EXISTS plr_ph();
DROP FUNCTION IF EXISTS plr_diagok();
DROP TYPE IF EXISTS plr_nested CASCADE;
DROP TYPE IF EXISTS plr_two CASCADE;
DROP TABLE IF EXISTS plr_dt CASCADE;
