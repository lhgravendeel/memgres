-- source: investigation-2026-08.md
-- finding: 260
-- title: The FROM-clause shape of a function's result is derived from the value that came back rather than from the declaration. resolveUserFunction only expands a compo
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_vf_nc AS (a int, b text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_k14() RETURNS SETOF zz_vf_nc AS $$ begin return next row(1,'a')::zz_vf_nc; end $$ LANGUAGE plpgsql;
-- begin-expected
-- columns: a:int4 | b:text
-- row: 1 | a
-- rowcount: 1
-- end-expected
SELECT * FROM zz_vf_k14();
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_vf_rc AS (a int, b text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_w6() RETURNS zz_vf_rc AS $$ declare r record; begin select 9 as a, 'k'::text as b into r; return r; end $$ LANGUAGE plpgsql;
-- begin-expected
-- columns: a:int4 | b:text
-- row: 9 | k
-- rowcount: 1
-- end-expected
SELECT * FROM zz_vf_w6();
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_o6(OUT a int, OUT b int) RETURNS SETOF record AS $$ begin a:=1; b:=2; return next; a:=3; b:=4; return next; end $$ LANGUAGE plpgsql;
-- begin-expected
-- columns: a:int4 | b:int4
-- row: 1 | 2
-- row: 3 | 4
-- rowcount: 2
-- end-expected
SELECT * FROM zz_vf_o6() ORDER BY a;
