-- source: review-2026-08.md
-- finding: Root cause 7: numeric results carry a scale memgres invented
-- area: Types, casts and coercion
-- title: Root cause 7: numeric results carry a scale memgres invented
-- begin-expected
-- columns: text:text
-- row: 2.5000000000000000
-- rowcount: 1
-- end-expected
SELECT (10.00 / 4)::text;
-- begin-expected
-- columns: text:text
-- row: 3333333333.33333333
-- rowcount: 1
-- end-expected
SELECT (1e10::numeric / 3)::text;
-- begin-expected
-- columns: text:text
-- row: 0.0000000000333333333333333333
-- rowcount: 1
-- end-expected
SELECT (1e-10::numeric / 3)::text;
-- begin-expected
-- columns: variance:text
-- row: 100.0000000000000000
-- rowcount: 1
-- end-expected
SELECT variance(v)::text FROM (VALUES (10),(20),(30)) t(v);
-- begin-expected
-- columns: var_pop:text
-- row: 1.00000000000000000000
-- rowcount: 1
-- end-expected
SELECT var_pop(v)::text FROM (VALUES (1),(3)) t(v);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_s2 (s int2);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_s2 VALUES (1),(2),(4);
-- begin-expected
-- columns: sum:text
-- row: 7
-- rowcount: 1
-- end-expected
SELECT sum(s)::text FROM zz_s2;
-- begin-expected
-- columns: s:text
-- row: 1
-- row: 2
-- row: 4
-- rowcount: 3
-- end-expected
SELECT s::numeric::text FROM zz_s2 ORDER BY s;
