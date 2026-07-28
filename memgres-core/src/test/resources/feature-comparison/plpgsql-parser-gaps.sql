-- ============================================================================
-- Feature Comparison: PL/pgSQL the parser would not accept
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Valid PL/pgSQL that Memgres rejected outright: "=" as a declaration
-- initialiser, array variables in every spelling (constructor and literal
-- initialisers, element, slice, two-dimensional and composite-field
-- assignment), SCROLL cursors with backward and absolute navigation, MOVE,
-- loop end labels, the #variable_conflict pragma, block-label qualification,
-- named cursor arguments, expression-valued RAISE options, and the composite
-- type a table implicitly defines. A rejected function cannot be worked
-- around, so each of these cost more than a missing check would.
-- ============================================================================

DROP TABLE IF EXISTS ppg_rr CASCADE;
DROP TABLE IF EXISTS ppg_nums CASCADE;
DROP TYPE IF EXISTS ppg_pc CASCADE;
DROP FUNCTION IF EXISTS ppg_eq1();
DROP FUNCTION IF EXISTS ppg_eq2();
DROP FUNCTION IF EXISTS ppg_ar1();
DROP FUNCTION IF EXISTS ppg_ar2();
DROP FUNCTION IF EXISTS ppg_ar3();
DROP FUNCTION IF EXISTS ppg_ar4();
DROP FUNCTION IF EXISTS ppg_ar5();
DROP FUNCTION IF EXISTS ppg_ar6();
DROP FUNCTION IF EXISTS ppg_ar7();
DROP FUNCTION IF EXISTS ppg_ar8();
DROP FUNCTION IF EXISTS ppg_ar9();
DROP FUNCTION IF EXISTS ppg_ar10();
DROP FUNCTION IF EXISTS ppg_ar11();
DROP FUNCTION IF EXISTS ppg_ar12();
DROP FUNCTION IF EXISTS ppg_sc1();
DROP FUNCTION IF EXISTS ppg_sc2();
DROP FUNCTION IF EXISTS ppg_sc3();
DROP FUNCTION IF EXISTS ppg_sc4();
DROP FUNCTION IF EXISTS ppg_sc5();
DROP FUNCTION IF EXISTS ppg_vc1();
DROP FUNCTION IF EXISTS ppg_vc2();
DROP FUNCTION IF EXISTS ppg_vc3();
DROP FUNCTION IF EXISTS ppg_vc5();
DROP FUNCTION IF EXISTS ppg_bl1();
DROP FUNCTION IF EXISTS ppg_cu1();
DROP FUNCTION IF EXISTS ppg_cu2();
DROP FUNCTION IF EXISTS ppg_rc1();
DROP FUNCTION IF EXISTS ppg_nb1();
DROP FUNCTION IF EXISTS ppg_nb2();
DROP FUNCTION IF EXISTS ppg_nb3();
DROP FUNCTION IF EXISTS ppg_nb4();
DROP FUNCTION IF EXISTS ppg_nb5();
CREATE TABLE ppg_rr (a int, b int);
CREATE TABLE ppg_nums (n int);
INSERT INTO ppg_nums VALUES (1),(2),(3),(4),(5);
CREATE TYPE ppg_pc AS (x int, y text);

-- ============================================================================
-- 1. "=" is a synonym for ":=" in DECLARE
-- ============================================================================
CREATE OR REPLACE FUNCTION ppg_eq1() RETURNS text LANGUAGE plpgsql AS $$
declare a int = 10; b int = 1;
begin return (a + b)::text; end $$;
SELECT ppg_eq1() AS a;

CREATE OR REPLACE FUNCTION ppg_eq2() RETURNS text LANGUAGE plpgsql AS $$
declare a int = 1; b int := 2; c int default 3;
begin return (a + b + c)::text; end $$;
SELECT ppg_eq2() AS a;

-- a declaration with no initialiser at all is still a syntax error, and the
-- body is compiled by CREATE FUNCTION rather than by the first call
CREATE OR REPLACE FUNCTION ppg_eq3() RETURNS int LANGUAGE plpgsql AS $$
declare a int 10; begin return a; end $$;
SELECT ppg_eq3() AS a;

-- ============================================================================
-- 2. Array variables: initialisers
-- ============================================================================
CREATE OR REPLACE FUNCTION ppg_ar1() RETURNS text LANGUAGE plpgsql AS $$
declare a int[] := array[1,2,3]; begin return a::text; end $$;
SELECT ppg_ar1() AS a;

CREATE OR REPLACE FUNCTION ppg_ar2() RETURNS text LANGUAGE plpgsql AS $$
declare a int[] := '{1,2,3}'; begin return a::text; end $$;
SELECT ppg_ar2() AS a;

-- ============================================================================
-- 3. Array variables: subscripted assignment
-- ============================================================================
CREATE OR REPLACE FUNCTION ppg_ar3() RETURNS text LANGUAGE plpgsql AS $$
declare a int[] := '{1,2,3}'; begin a[2] := 99; return a::text; end $$;
SELECT ppg_ar3() AS a;

CREATE OR REPLACE FUNCTION ppg_ar4() RETURNS text LANGUAGE plpgsql AS $$
declare a int[] := array[1,2,3]; begin a[5] := 7; return a::text; end $$;
SELECT ppg_ar4() AS a;

CREATE OR REPLACE FUNCTION ppg_ar5() RETURNS text LANGUAGE plpgsql AS $$
declare a int[]; begin a[1] := 5; return a::text; end $$;
SELECT ppg_ar5() AS a;

CREATE OR REPLACE FUNCTION ppg_ar6() RETURNS text LANGUAGE plpgsql AS $$
declare a int[]; begin a := array[1,2,3]; a[1] := 42; return a::text; end $$;
SELECT ppg_ar6() AS a;

CREATE OR REPLACE FUNCTION ppg_ar7() RETURNS text LANGUAGE plpgsql AS $$
declare a int[] := '{1,2,3,4}'; begin a[2:3] := '{8,9}'; return a::text; end $$;
SELECT ppg_ar7() AS a;

CREATE OR REPLACE FUNCTION ppg_ar8() RETURNS text LANGUAGE plpgsql AS $$
declare a int[] := '{{1,2},{3,4}}'; begin a[1][2] := 20; return a::text; end $$;
SELECT ppg_ar8() AS a;

CREATE OR REPLACE FUNCTION ppg_ar9() RETURNS text LANGUAGE plpgsql AS $$
declare a ppg_pc[] := '{"(1,a)"}'; begin a[1].x := 9; return a::text; end $$;
SELECT ppg_ar9() AS a;

CREATE OR REPLACE FUNCTION ppg_ar10() RETURNS text LANGUAGE plpgsql AS $$
declare a text[] := '{a,b}'; begin a[3] := 'c'; return a::text; end $$;
SELECT ppg_ar10() AS a;

-- whole-variable assignment, which already worked, is unchanged
CREATE OR REPLACE FUNCTION ppg_ar11() RETURNS text LANGUAGE plpgsql AS $$
declare a int[] := '{1,2}'; begin a := a || 3; return a::text; end $$;
SELECT ppg_ar11() AS a;

CREATE OR REPLACE FUNCTION ppg_ar12() RETURNS text LANGUAGE plpgsql AS $$
declare a int[] := array[1,2]; begin return array_append(a, 3)::text; end $$;
SELECT ppg_ar12() AS a;

-- ============================================================================
-- 4. SCROLL cursors, backward and absolute FETCH, and MOVE
-- ============================================================================
CREATE OR REPLACE FUNCTION ppg_sc1() RETURNS text LANGUAGE plpgsql AS $$
declare c scroll cursor for select n from ppg_nums order by n; r int; out text := '';
begin
  open c; fetch c into r; out := out || r;
  fetch next from c into r; out := out || ',' || r;
  fetch prior from c into r; out := out || ',' || r;
  fetch last from c into r; out := out || ',' || r;
  fetch absolute 1 from c into r; out := out || ',' || r;
  close c; return out;
end $$;
SELECT ppg_sc1() AS a;

CREATE OR REPLACE FUNCTION ppg_sc2() RETURNS text LANGUAGE plpgsql AS $$
declare c no scroll cursor for select n from ppg_nums order by n; r int;
begin open c; fetch c into r; close c; return r::text; end $$;
SELECT ppg_sc2() AS a;

CREATE OR REPLACE FUNCTION ppg_sc3() RETURNS text LANGUAGE plpgsql AS $$
declare c scroll cursor for select n from ppg_nums order by n; r int; out text := '';
begin
  open c; move 2 from c; fetch c into r; out := out || r;
  move absolute 1 from c; fetch c into r; out := out || ',' || r;
  move relative 2 from c; fetch c into r; out := out || ',' || r;
  close c; return out;
end $$;
SELECT ppg_sc3() AS a;

CREATE OR REPLACE FUNCTION ppg_sc4() RETURNS text LANGUAGE plpgsql AS $$
declare c scroll cursor for select n from ppg_nums order by n; r int; out text := '';
begin
  open c; fetch relative 3 from c into r; out := out || r;
  fetch relative -1 from c into r; out := out || ',' || r;
  close c; return out;
end $$;
SELECT ppg_sc4() AS a;

-- an ordinary bound cursor still works
CREATE OR REPLACE FUNCTION ppg_sc5() RETURNS text LANGUAGE plpgsql AS $$
declare c cursor for select n from ppg_nums order by n; r int;
begin open c; fetch c into r; close c; return r::text; end $$;
SELECT ppg_sc5() AS a;

-- fetching before OPEN reports the null cursor variable
DO $$ declare c refcursor; r int; begin fetch c into r; end $$;

-- ============================================================================
-- 5. Loop end labels: accepted when they match, rejected when they do not
-- ============================================================================
DO $$ begin <<ppg_flbl>> for i in 1 .. 10 loop exit ppg_flbl; end loop ppg_flbl; end $$;
DO $$ begin <<ppg_l2>> loop exit ppg_l2; end loop ppg_l2; end $$;
DO $$ begin <<ppg_l3>> while true loop exit ppg_l3; end loop ppg_l3; end $$;
DO $$ declare x int; begin <<ppg_l4>> foreach x in array array[1,2] loop exit ppg_l4; end loop ppg_l4; end $$;
DO $$ <<ppg_b2>> begin null; end ppg_b2 $$;
DO $$ begin for i in 1 .. 10 loop exit; end loop ppg_flbl; end $$;
DO $$ <<ppg_outer>> begin <<ppg_inner>> for i in 1..3 loop exit; end loop ppg_outer; end $$;
DO $$ <<ppg_b>> begin null; end ppg_c $$;
DO $$ begin null; end ppg_c $$;

-- ============================================================================
-- 6. The #variable_conflict pragma
-- ============================================================================
CREATE OR REPLACE FUNCTION ppg_vc1() RETURNS int LANGUAGE plpgsql AS $$
#variable_conflict use_column
declare n int := 99; begin return (select max(n) from ppg_nums); end $$;
SELECT ppg_vc1() AS a;

CREATE OR REPLACE FUNCTION ppg_vc2() RETURNS int LANGUAGE plpgsql AS $$
#variable_conflict use_variable
declare n int := 99; begin return (select max(n) from ppg_nums); end $$;
SELECT ppg_vc2() AS a;

CREATE OR REPLACE FUNCTION ppg_vc3() RETURNS int LANGUAGE plpgsql AS $$
#variable_conflict error
declare n int := 99; begin return n; end $$;
SELECT ppg_vc3() AS a;

-- an unknown value for the pragma is a syntax error
CREATE OR REPLACE FUNCTION ppg_vc4() RETURNS int LANGUAGE plpgsql AS $$
#variable_conflict nonsense_value
declare n int := 99; begin return n; end $$;

-- and without the pragma an ambiguous reference is still an error
CREATE OR REPLACE FUNCTION ppg_vc5() RETURNS int LANGUAGE plpgsql AS $$
declare n int := 99; begin return (select max(n) from ppg_nums); end $$;
SELECT ppg_vc5() AS a;

-- ============================================================================
-- 7. A block label qualifies a variable an inner block shadows
-- ============================================================================
CREATE OR REPLACE FUNCTION ppg_bl1() RETURNS text LANGUAGE plpgsql AS $$
<<ppg_ob>> declare x int := 1;
begin declare x int := 2; begin return ppg_ob.x::text || '/' || x::text; end; end $$;
SELECT ppg_bl1() AS a;

-- ============================================================================
-- 8. Cursor arguments, named and positional
-- ============================================================================
CREATE OR REPLACE FUNCTION ppg_cu1() RETURNS text LANGUAGE plpgsql AS $$
declare c1 cursor (param1 int, param2 int) for select param1 + param2; r int;
begin open c1(param1 := 20, param2 := 21); fetch c1 into r; close c1; return r::text; end $$;
SELECT ppg_cu1() AS a;

CREATE OR REPLACE FUNCTION ppg_cu2() RETURNS text LANGUAGE plpgsql AS $$
declare c1 cursor (param1 int, param2 int) for select param1 + param2; r int;
begin open c1(20, 21); fetch c1 into r; close c1; return r::text; end $$;
SELECT ppg_cu2() AS a;

-- a bad argument list keeps the function from being created at all
CREATE OR REPLACE FUNCTION ppg_cu3() RETURNS int LANGUAGE plpgsql AS $$
declare c1 cursor (param1 int, param2 int) for select param1 + param2; r int;
begin open c1(param2 := 20, 21); fetch c1 into r; close c1; return r; end $$;

CREATE OR REPLACE FUNCTION ppg_cu4() RETURNS int LANGUAGE plpgsql AS $$
declare c1 cursor (param1 int, param2 int) for select param1 + param2; r int;
begin open c1(param2 := 20, param2 := 21); fetch c1 into r; close c1; return r; end $$;

CREATE OR REPLACE FUNCTION ppg_cu5() RETURNS int LANGUAGE plpgsql AS $$
declare c1 cursor (param1 int, param2 int) for select param1 + param2; r int;
begin open c1(param2 := 20); fetch c1 into r; close c1; return r; end $$;

SELECT ppg_cu5() AS a;

-- ============================================================================
-- 9. RAISE option values are expressions
-- ============================================================================
DO $$ begin raise division_by_zero using message = 'custom' || ' message'; end $$;
DO $$ declare v text := 'abc'; begin raise exception using message = 'got: ' || v, errcode = '22012'; end $$;
DO $$ begin raise notice 'x' using detail = 'a' || 'b'; end $$;
DO $$ begin raise exception using message = 'plain'; end $$;
DO $$ begin raise exception 'boom %', 42 using hint = 'try', detail = 'dd'; end $$;
DO $$ begin raise notice 'x' using detail = 'd', detail = 'e'; end $$;

-- ============================================================================
-- 10. A table also names the composite type of its rows
-- ============================================================================
SELECT (row(1,null)::ppg_rr)::text AS a;
SELECT (row(1,2)::ppg_rr).a AS a;
CREATE OR REPLACE FUNCTION ppg_rc1() RETURNS text LANGUAGE plpgsql AS $$
declare r ppg_rr; begin r := row(1,2)::ppg_rr; return r.a::text; end $$;
SELECT ppg_rc1() AS a;
SELECT row(1,2)::ppg_nope AS a;

-- ============================================================================
-- 11. Neighbouring PL/pgSQL that must keep working
-- ============================================================================
CREATE OR REPLACE FUNCTION ppg_nb1() RETURNS text LANGUAGE plpgsql AS $$
declare v ppg_pc; begin v.x := 3; v.y := 'q'; return v::text; end $$;
SELECT ppg_nb1() AS a;

CREATE OR REPLACE FUNCTION ppg_nb2() RETURNS text LANGUAGE plpgsql AS $$
declare v ppg_pc; t text; begin v.x := 42; t := v.x::text; return t; end $$;
SELECT ppg_nb2() AS a;

CREATE OR REPLACE FUNCTION ppg_nb3() RETURNS text LANGUAGE plpgsql AS $$
declare a int = 10; b int = 1;
begin case when a > b then return 'gt'; else return 'le'; end case; end $$;
SELECT ppg_nb3() AS a;

CREATE OR REPLACE FUNCTION ppg_nb4() RETURNS text LANGUAGE plpgsql AS $$
declare a int = 3;
begin case a when 3,4,3+5 then return 'in'; else return 'out'; end case; end $$;
SELECT ppg_nb4() AS a;

DO $$ declare a int = 9; begin case a when 1 then raise notice 'one'; end case; end $$;

CREATE OR REPLACE FUNCTION ppg_nb5() RETURNS text LANGUAGE plpgsql AS $$
declare x int; out text := '';
begin
  foreach x in array array[1,2,3] loop
    out := out || case when out = '' then '' else ',' end || x;
  end loop;
  return out;
end $$;
SELECT ppg_nb5() AS a;

DROP TABLE IF EXISTS ppg_rr CASCADE;
DROP TABLE IF EXISTS ppg_nums CASCADE;
DROP TYPE IF EXISTS ppg_pc CASCADE;
