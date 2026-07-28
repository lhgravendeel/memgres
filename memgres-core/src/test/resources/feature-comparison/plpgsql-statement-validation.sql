-- ============================================================================
-- Feature Comparison: PL/pgSQL statement validation
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Statements were accepted without the checks that give them meaning. EXIT and
-- CONTINUE named labels enclosing nothing; FOREACH iterated whatever it was
-- handed and gave a scalar variable sub-arrays instead of walking a
-- two-dimensional array element by element; RETURN ignored the routine it sat
-- in, and a function that forgot to RETURN yielded NULL where PG raises 2F005;
-- exception handlers named conditions that do not exist and so never fired;
-- ASSERT took anything truthy; GET DIAGNOSTICS answered for items it has no
-- business with, in or out of a handler; VARIADIC placement and parameter
-- defaults went unchecked; EXECUTE ran with placeholders nothing supplied and
-- INTO STRICT was inert under it; a returned record of the wrong width was
-- read at the wrong field offsets; and a cursor read after CLOSE ended quietly
-- rather than erroring.
-- ============================================================================

DROP TABLE IF EXISTS ppv_sv_t CASCADE;
DROP FUNCTION IF EXISTS ppv_sv_lbl();
DROP FUNCTION IF EXISTS ppv_sv_r1();
DROP FUNCTION IF EXISTS ppv_sv_r2();
DROP FUNCTION IF EXISTS ppv_sv_r3(int);
DROP FUNCTION IF EXISTS ppv_sv_noret();
DROP FUNCTION IF EXISTS ppv_sv_maybe(int);
DROP FUNCTION IF EXISTS ppv_sv_shape();
DROP FUNCTION IF EXISTS ppv_sv_badnext();
DROP FUNCTION IF EXISTS ppv_sv_ok();
DROP FUNCTION IF EXISTS ppv_sv_sq1();
DROP FUNCTION IF EXISTS ppv_sv_sq2();
DROP FUNCTION IF EXISTS ppv_sv_v1(int);
DROP FUNCTION IF EXISTS ppv_sv_v2(int[], int);
DROP FUNCTION IF EXISTS ppv_sv_v3(int[]);
DROP FUNCTION IF EXISTS ppv_sv_d1(int, int);
DROP FUNCTION IF EXISTS ppv_sv_d2(int, int);
DROP FUNCTION IF EXISTS ppv_sv_rec(int);
DROP PROCEDURE IF EXISTS ppv_sv_p1();
DROP PROCEDURE IF EXISTS ppv_sv_p2();
DROP TYPE IF EXISTS ppv_sv_two CASCADE;

CREATE TABLE ppv_sv_t (a int, b int);
INSERT INTO ppv_sv_t VALUES (1,2),(3,4);
CREATE TYPE ppv_sv_two AS (q1 bigint, q2 bigint);

-- ---------------------------------------------------------------------------
-- EXIT and CONTINUE must name something that encloses them
-- ---------------------------------------------------------------------------
DO $$ begin begin continue; end; end; $$;
DO $$ begin begin exit; end; end; $$;
DO $$ begin exit; end; $$;
DO $$ begin loop continue ppv_nolabel; end loop; end; $$;
DO $$ begin loop exit ppv_nolabel; end loop; end; $$;
DO $$ <<ppv_blk>> begin loop continue ppv_blk; end loop; end; $$;
CREATE FUNCTION ppv_sv_lbl() RETURNS int AS $$ begin loop continue ppv_nolabel; end loop; return 1; end $$ LANGUAGE plpgsql;

DO $$ <<ppv_blk>> begin exit ppv_blk; end; $$;
DO $$ begin <<lp>> loop exit lp; end loop; end; $$;
DO $$ declare i int := 0; begin <<lp>> loop i := i + 1; continue lp when i < 2; exit lp; end loop; raise notice '%', i; end; $$;

-- ---------------------------------------------------------------------------
-- FOREACH checks its operand, and a scalar variable walks the flattened array
-- ---------------------------------------------------------------------------
DO $$ declare x int; begin foreach x in array null::int[] loop null; end loop; end $$;
DO $$ declare x int[]; begin foreach x slice 2 in array array[1,2,3] loop null; end loop; end $$;
DO $$ declare x int; begin foreach x slice 1 in array array[1,2] loop null; end loop; end $$;
DO $$ declare x int; begin foreach x in array 42 loop null; end loop; end $$;
DO $$ declare x int; s text := ''; begin foreach x in array array[[1,2],[3,4]] loop s := s || x || ','; end loop; raise notice '%', s; end $$;
DO $$ declare x int; s text := ''; begin foreach x in array array[1,2,3] loop s := s || x || ','; end loop; raise notice '%', s; end $$;
DO $$ declare x int[]; s text := ''; begin foreach x slice 1 in array array[[1,2],[3,4]] loop s := s || x[1] || ','; end loop; raise notice '%', s; end $$;

-- ---------------------------------------------------------------------------
-- RETURN must suit the routine, and falling off the end is an error
-- ---------------------------------------------------------------------------
CREATE FUNCTION ppv_sv_r1() RETURNS SETOF int AS $$ begin return 1; end $$ LANGUAGE plpgsql;
CREATE FUNCTION ppv_sv_r2() RETURNS int AS $$ begin return next 1; end $$ LANGUAGE plpgsql;
CREATE FUNCTION ppv_sv_r3(IN a int, OUT b int) AS $$ begin return 1; end $$ LANGUAGE plpgsql;
CREATE PROCEDURE ppv_sv_p1() AS $$ begin return 1; end $$ LANGUAGE plpgsql;
CREATE FUNCTION ppv_sv_noret() RETURNS int AS $$ begin null; end $$ LANGUAGE plpgsql;
SELECT ppv_sv_noret();
CREATE FUNCTION ppv_sv_maybe(a int) RETURNS int AS $$ begin if a > 0 then return 1; end if; end $$ LANGUAGE plpgsql;
SELECT ppv_sv_maybe(1);
SELECT ppv_sv_maybe(-1);
CREATE FUNCTION ppv_sv_shape() RETURNS SETOF ppv_sv_t AS $$ begin return query select a from ppv_sv_t; end $$ LANGUAGE plpgsql;
SELECT * FROM ppv_sv_shape();
CREATE FUNCTION ppv_sv_badnext() RETURNS SETOF int AS $$ begin return next 'notanint'; end $$ LANGUAGE plpgsql;
SELECT * FROM ppv_sv_badnext();
CREATE FUNCTION ppv_sv_ok() RETURNS SETOF int AS $$ begin return next 1; return next 2; end $$ LANGUAGE plpgsql;
SELECT * FROM ppv_sv_ok();
CREATE PROCEDURE ppv_sv_p2() AS $$ begin return; end $$ LANGUAGE plpgsql;
CALL ppv_sv_p2();

-- ---------------------------------------------------------------------------
-- Exception conditions must exist
-- ---------------------------------------------------------------------------
DO $$ begin null; exception when ppv_no_such_condition then null; end $$;
DO $$ begin null; exception when SQLSTATE 'notavalidstate' then null; end $$;
DO $$ begin null; exception when SQLSTATE '22012' then null; end $$;
DO $$ begin null; exception when division_by_zero then null; end $$;
DO $$ begin null; exception when no_data_found or too_many_rows then null; end $$;
DO $$ begin null; exception when others then null; end $$;

-- ---------------------------------------------------------------------------
-- ASSERT takes a boolean
-- ---------------------------------------------------------------------------
DO $$ begin assert 42; end $$;
DO $$ begin assert 'x'; end $$;
DO $$ begin assert true; end $$;
DO $$ begin assert false, 'boom'; end $$;
DO $$ begin assert null; end $$;

-- ---------------------------------------------------------------------------
-- GET DIAGNOSTICS: each form has its own items and its own scope
-- ---------------------------------------------------------------------------
DO $$ declare st text; begin get stacked diagnostics st = returned_sqlstate; end $$;
DO $$ declare n int; begin begin null; exception when others then get stacked diagnostics n = row_count; end; end $$;
DO $$ declare v text; begin get diagnostics v = ppv_no_such_item; end $$;
DO $$ declare v text; begin get diagnostics v = message_text; end $$;
DO $$ declare n int; begin insert into ppv_sv_t values (9,9); get diagnostics n = row_count; raise notice '%', n; end $$;
DO $$ declare st text; begin begin raise division_by_zero; exception when others then get stacked diagnostics st = returned_sqlstate; raise notice '%', st; end; end $$;

-- SQLSTATE and SQLERRM exist only inside a handler
CREATE FUNCTION ppv_sv_sq1() RETURNS text AS $$ begin return sqlstate; end $$ LANGUAGE plpgsql;
SELECT ppv_sv_sq1();
CREATE FUNCTION ppv_sv_sq2() RETURNS text AS $$ begin raise division_by_zero; exception when others then return sqlstate; end $$ LANGUAGE plpgsql;
SELECT ppv_sv_sq2();

-- ---------------------------------------------------------------------------
-- Signature rules
-- ---------------------------------------------------------------------------
CREATE FUNCTION ppv_sv_v1(VARIADIC a int) RETURNS int AS $$ begin return 1; end $$ LANGUAGE plpgsql;
SELECT ppv_sv_v1(1);
CREATE FUNCTION ppv_sv_v2(VARIADIC a int[], b int) RETURNS int AS $$ begin return 1; end $$ LANGUAGE plpgsql;
CREATE FUNCTION ppv_sv_d1(a int DEFAULT 1, b int) RETURNS int AS $$ begin return 1; end $$ LANGUAGE plpgsql;
CREATE FUNCTION ppv_sv_v3(VARIADIC a int[]) RETURNS int AS $$ begin return array_length(a,1); end $$ LANGUAGE plpgsql;
SELECT ppv_sv_v3(1,2,3);
CREATE FUNCTION ppv_sv_d2(a int, b int DEFAULT 2) RETURNS int AS $$ begin return a+b; end $$ LANGUAGE plpgsql;
SELECT ppv_sv_d2(1);

-- ---------------------------------------------------------------------------
-- EXECUTE placeholders, USING arguments and INTO STRICT
-- ---------------------------------------------------------------------------
DO $$ declare v int; begin execute 'select a from ppv_sv_t where a = $1 and b = $2' into v using 1; raise notice '%', v; end $$;
DO $$ declare v int; begin execute 'select a from ppv_sv_t where a = 99' into strict v; end $$;
DO $$ declare v int; begin execute 'select a from ppv_sv_t' into strict v; end $$;
DO $$ declare v int; begin execute 'select a from ppv_sv_t where a = 1' into strict v; raise notice '%', v; end $$;
DO $$ declare v int; begin execute 'select a from ppv_sv_t where a = $1' into v using 1, 2; raise notice '%', v; end $$;
DO $$ declare v int; begin execute 'select 1' into v using 5; raise notice '%', v; end $$;
DO $$ declare v int; begin select a into strict v from ppv_sv_t where a = 99; end $$;
DO $$ declare v int; begin select a into strict v from ppv_sv_t; end $$;

-- ---------------------------------------------------------------------------
-- A returned record must have the declared type's fields
-- ---------------------------------------------------------------------------
CREATE FUNCTION ppv_sv_rec(i int) RETURNS ppv_sv_two AS $$ declare r record; begin r := row(i, i, i); return r; end $$ LANGUAGE plpgsql;
SELECT (ppv_sv_rec(42)).q1;

-- ---------------------------------------------------------------------------
-- Cursor lifecycle
-- ---------------------------------------------------------------------------
DO $$ declare c refcursor; r record; begin for r in c loop null; end loop; end $$;
DO $$ declare c refcursor; v int; begin fetch c into v; end $$;
DO $$ declare c cursor for select 1; v int; begin open c; fetch c into v; close c; raise notice '%', v; end $$;
DO $$ declare c refcursor; v int; begin open c for select 7; fetch c into v; close c; raise notice '%', v; end $$;
DO $$ declare c cursor for select 5; v int; begin open c; fetch c into v; close c; open c; fetch c into v; close c; raise notice '%', v; end $$;

DROP FUNCTION IF EXISTS ppv_sv_noret();
DROP FUNCTION IF EXISTS ppv_sv_maybe(int);
DROP FUNCTION IF EXISTS ppv_sv_shape();
DROP FUNCTION IF EXISTS ppv_sv_badnext();
DROP FUNCTION IF EXISTS ppv_sv_ok();
DROP FUNCTION IF EXISTS ppv_sv_sq1();
DROP FUNCTION IF EXISTS ppv_sv_sq2();
DROP FUNCTION IF EXISTS ppv_sv_v3(int[]);
DROP FUNCTION IF EXISTS ppv_sv_d2(int, int);
DROP FUNCTION IF EXISTS ppv_sv_rec(int);
DROP PROCEDURE IF EXISTS ppv_sv_p2();
DROP TYPE IF EXISTS ppv_sv_two CASCADE;
DROP TABLE IF EXISTS ppv_sv_t CASCADE;
