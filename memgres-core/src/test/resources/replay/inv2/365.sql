-- source: investigation-2026-08.md
-- finding: 365
-- title: There is no single output function. Four container writers each re-implement a partial one and fall back to Object.toString(): TypeCoercion.toString has no Bool
-- begin-expected
-- columns: a1:text
-- row: {t}
-- rowcount: 1
-- end-expected
SELECT ARRAY[true]::text AS a1;
-- begin-expected
-- columns: a2:text
-- row: {"\\x0102"}
-- rowcount: 1
-- end-expected
SELECT ARRAY['\x0102'::bytea]::text AS a2;
-- begin-expected
-- columns: a3:text
-- row: {0.0000000000000001}
-- rowcount: 1
-- end-expected
SELECT ARRAY['1e-16'::numeric]::text AS a3;
-- begin-expected
-- columns: a4:text
-- row: {t}
-- rowcount: 1
-- end-expected
SELECT array_agg(x)::text AS a4 FROM (SELECT true AS x) t;
-- begin-expected
-- columns: a5:text
-- row: t,f
-- rowcount: 1
-- end-expected
SELECT array_to_string(ARRAY[true,false], ',') AS a5;
-- begin-expected
-- columns: r1:text
-- row: ("4713-01-01 BC",24:00:00,1e-300)
-- rowcount: 1
-- end-expected
SELECT ROW('4713-01-01 BC'::date, '24:00:00'::time, 1e-300::float8)::text AS r1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_rt_ba (a bytea[]);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_rt_ba VALUES (ARRAY['\x0102'::bytea]);
-- begin-expected
-- columns: a:text
-- row: {"\\x0102"}
-- rowcount: 1
-- end-expected
SELECT a::text FROM zz_vf2_rt_ba;
-- begin-expected
-- columns: ok:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT a[1] = '\x0102'::bytea AS ok FROM zz_vf2_rt_ba;
-- begin-expected
-- columns: u:text
-- row: \x0102
-- rowcount: 1
-- end-expected
SELECT unnest(a)::text AS u FROM zz_vf2_rt_ba;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_vf2_rt_ca AS (arr text[]);
-- begin-expected
-- columns: lit:text
-- row: ("{a,b}")
-- rowcount: 1
-- end-expected
SELECT ROW(ARRAY['a','b'])::zz_vf2_rt_ca::text AS lit;
-- begin-expected
-- columns: e:text
-- row: b
-- rowcount: 1
-- end-expected
SELECT ((ROW(ARRAY['a','b'])::zz_vf2_rt_ca::text::zz_vf2_rt_ca).arr)[2] AS e;
