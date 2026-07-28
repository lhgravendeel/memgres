-- ============================================================================
-- Feature Comparison: PL/pgSQL declaration enforcement
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Every modifier a DECLARE section offers was parsed and then ignored, so a
-- function carried none of the safety its declarations promised: CONSTANT
-- could be written through assignment, SELECT INTO, a field, a subscript, a
-- FOR target, EXECUTE INTO, FETCH INTO and GET DIAGNOSTICS; NOT NULL demanded
-- neither a default nor a non-null value; a domain's constraints stopped at
-- the variable; an initialiser never met the declared type's input function;
-- and %TYPE and %ROWTYPE resolved against relations, columns and schemas that
-- do not exist. The last is the one a schema change produces, and it has to
-- fail where the function is created rather than surface later as a wrong
-- value.
-- ============================================================================

DROP TABLE IF EXISTS ppv_misc_table CASCADE;
DROP TABLE IF EXISTS ppv_dom_table CASCADE;
DROP TYPE IF EXISTS ppv_record CASCADE;
DROP DOMAIN IF EXISTS ppv_int_nn CASCADE;
DROP DOMAIN IF EXISTS ppv_pos CASCADE;
DROP FUNCTION IF EXISTS ppv_const_fn();
DROP FUNCTION IF EXISTS ppv_dom_fn();
DROP FUNCTION IF EXISTS ppv_badinit_fn();
DROP FUNCTION IF EXISTS ppv_type_fn();
DROP FUNCTION IF EXISTS ppv_dup_fn();
DROP FUNCTION IF EXISTS ppv_ok_fn(int);

CREATE TABLE ppv_misc_table (f1 int, f2 text);
CREATE TYPE ppv_record AS (f1 int, f2 int);
CREATE DOMAIN ppv_int_nn AS int NOT NULL;
CREATE DOMAIN ppv_pos AS int CHECK (VALUE > 0);
CREATE TABLE ppv_dom_table (c ppv_pos);

-- ---------------------------------------------------------------------------
-- CONSTANT is enforced on every write path
-- ---------------------------------------------------------------------------
DO $$ declare x constant int := 1; begin x := 2; end$$;
DO $$ declare x constant int := 1; begin select 2 into x; end$$;
DO $$ declare x constant ppv_record; begin x.f1 := 42; end$$;
DO $$ declare x constant int[] := array[1,2]; begin x[1] := 9; end$$;
DO $$ declare x constant int; y int; begin for x, y in select 1,2 loop end loop; end$$;
DO $$ declare x constant int := 1; begin for x in select 1 loop null; end loop; end$$;
DO $$ declare x constant int := 1; begin execute 'select 2' into x; end$$;
DO $$ declare x constant int := 1; begin get diagnostics x = row_count; end$$;
DO $$ declare x constant int := 1; begin foreach x in array array[1] loop null; end loop; end$$;
DO $$ declare c cursor for select 1; x constant int := 1; begin open c; fetch c into x; end$$;
CREATE FUNCTION ppv_const_fn() RETURNS int AS $$ declare x constant int := 1; begin x := 2; return x; end $$ LANGUAGE plpgsql;
SELECT ppv_const_fn();

-- reading a constant, shadowing it, and an integer FOR loop's own variable stay legal
DO $$ declare x constant int := 1; begin raise notice '%', x; end$$;
DO $$ declare x constant int := 1; begin declare x int := 3; begin x := 2; end; end$$;
DO $$ declare x constant int := 1; begin for x in 1..2 loop null; end loop; end$$;
DO $$ declare c constant refcursor; begin null; end$$;

-- ---------------------------------------------------------------------------
-- NOT NULL: a default is required, and null may never be assigned
-- ---------------------------------------------------------------------------
DO $$ declare x int not null; begin null; end$$;
DO $$ declare x record not null; begin x := row(1); end$$;
DO $$ declare x ppv_misc_table.f1%type not null; begin null; end$$;
DO $$ declare x int not null := null; begin null; end$$;
DO $$ declare x int not null := 3; begin x := null; end$$;
DO $$ declare x int not null := 3; begin select null::int into x; end$$;
DO $$ declare x record not null := row(42); begin x := null; end$$;
DO $$ declare x int not null := 3; begin x := 4; end$$;
DO $$ declare x record not null := row(1); begin x := row(2); end$$;
DO $$ declare x constant int not null := 1; begin raise notice '%', x; end$$;

-- ---------------------------------------------------------------------------
-- A domain's constraints travel with the type into a variable
-- ---------------------------------------------------------------------------
DO $$ declare x ppv_int_nn; begin null; end$$;
DO $$ declare x ppv_int_nn := 42; begin x := null; end$$;
DO $$ declare x ppv_pos := -1; begin null; end$$;
DO $$ declare x ppv_pos := 5; begin x := -3; end$$;
DO $$ declare x ppv_pos := 5; begin select -7 into x; end$$;
DO $$ declare x ppv_pos := 5; begin x := x - 9; end$$;
DO $$ declare x ppv_pos[] := array[-1]; begin null; end$$;
DO $$ declare x ppv_dom_table.c%type := -5; begin null; end$$;
DO $$ declare x ppv_pos := 5; begin x := 7; raise notice '%', x; end$$;
DO $$ declare x ppv_pos := null; begin raise notice '%', x; end$$;
DO $$ declare x ppv_pos; begin raise notice '%', x; end$$;
DO $$ declare x ppv_int_nn := 42; begin x := 7; raise notice '%', x; end$$;
CREATE FUNCTION ppv_dom_fn() RETURNS int AS $$ declare x ppv_pos := 5; begin x := -1; return x; end $$ LANGUAGE plpgsql;
SELECT ppv_dom_fn();

-- ---------------------------------------------------------------------------
-- An initialiser goes through the declared type's input function
-- ---------------------------------------------------------------------------
DO $$ declare x int := 'abc'; begin null; end$$;
DO $$ declare x boolean := 'notabool'; begin null; end$$;
DO $$ declare x numeric := 'zz'; begin null; end$$;
DO $$ declare x ppv_misc_table.f1%type := 'abc'; begin null; end$$;
CREATE FUNCTION ppv_badinit_fn() RETURNS int AS $$ declare x int := 'abc'; begin return x; end $$ LANGUAGE plpgsql;
SELECT ppv_badinit_fn();

-- initialisers the type does accept are unchanged
DO $$ declare x int := '42'; begin raise notice '%', x; end$$;
DO $$ declare x int := 3.7; begin raise notice '%', x; end$$;
DO $$ declare x text := 42; begin raise notice '%', x; end$$;
DO $$ declare x int[] := '{1,2}'; begin raise notice '%', x[1]; end$$;
DO $$ declare x numeric(4,2) := 1.005; begin raise notice '%', x; end$$;
DO $$ declare x date := '2020-01-01'; begin raise notice '%', x; end$$;
DO $$ declare x jsonb := '{"a":1}'; begin raise notice '%', x; end$$;
DO $$ declare x text[] := array['a','b']; begin raise notice '%', x[2]; end$$;
DO $$ declare x refcursor := 'cx'; begin raise notice '%', x; end$$;
DO $$ declare x ppv_misc_table%rowtype := null; begin raise notice 'ok'; end$$;

-- ---------------------------------------------------------------------------
-- %TYPE and %ROWTYPE must resolve
-- ---------------------------------------------------------------------------
DO $$ declare x ppv_nosuch%type; begin null; end $$;
DO $$ declare x ppv_nosuch.bar%type; begin null; end $$;
DO $$ declare x public.ppv_misc_table.zed%type; begin null; end $$;
DO $$ declare x ppv_nosuch%rowtype; begin null; end $$;
DO $$ declare x ppv_nosuch.bar%rowtype; begin null; end $$;
DO $$ declare x public.ppv_nosuch%rowtype; begin null; end $$;
DO $$ declare x public.ppv_nosuch.col%type; begin null; end $$;
DO $$ declare x nosuchschema.tbl.col%type; begin null; end $$;
DO $$ declare x ppv_notatype; begin null; end $$;
CREATE FUNCTION ppv_type_fn() RETURNS int AS $$ declare x ppv_nosuch%type; begin return 1; end $$ LANGUAGE plpgsql;

-- references that do resolve keep working
DO $$ declare x ppv_misc_table%rowtype; begin null; end $$;
DO $$ declare x public.ppv_misc_table%rowtype; begin null; end $$;
DO $$ declare x public.ppv_misc_table.f1%type := 4; begin raise notice '%', x; end $$;
DO $$ declare x ppv_misc_table.f2%type := 'hi'; begin raise notice '%', x; end$$;
DO $$ declare x int; y x%type; begin null; end $$;
CREATE FUNCTION ppv_ok_fn(a int) RETURNS int AS $$ declare x ppv_misc_table.f1%type := a; begin return x; end $$ LANGUAGE plpgsql;
SELECT ppv_ok_fn(7);

-- ---------------------------------------------------------------------------
-- A name may be declared once per block
-- ---------------------------------------------------------------------------
DO $$ declare x int := 1; x int := 2; begin raise notice '%', x; end $$;
DO $$ declare c cursor for select 1; c int; begin null; end $$;
CREATE FUNCTION ppv_dup_fn() RETURNS int AS $$ declare x int := 1; x int := 2; begin return x; end $$ LANGUAGE plpgsql;
DO $$ declare x int := 1; begin declare x int := 2; begin raise notice '%', x; end; end $$;

DROP FUNCTION IF EXISTS ppv_dom_fn();
DROP FUNCTION IF EXISTS ppv_badinit_fn();
DROP FUNCTION IF EXISTS ppv_ok_fn(int);
DROP TABLE IF EXISTS ppv_dom_table CASCADE;
DROP TABLE IF EXISTS ppv_misc_table CASCADE;
DROP TYPE IF EXISTS ppv_record CASCADE;
DROP DOMAIN IF EXISTS ppv_int_nn CASCADE;
DROP DOMAIN IF EXISTS ppv_pos CASCADE;
