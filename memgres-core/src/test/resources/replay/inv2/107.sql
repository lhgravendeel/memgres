-- source: investigation-2026-08.md
-- finding: 107
-- title: Nine of the eleven regression aggregates end their branch in stripTrailingZeros().toPlainString(), returning a Java String instead of a double, and corr/regr_r2
-- begin-expected
-- columns: pg_typeof:text
-- row: double precision
-- rowcount: 1
-- end-expected
SELECT pg_typeof(regr_sxx(y,x))::text FROM (VALUES (1,2),(2,4)) t(x,y);
-- begin-expected
-- columns: regr_syy:float8
-- row: 12.666666666666666
-- rowcount: 1
-- end-expected
SELECT regr_syy(y,x) FROM (VALUES (1,2),(2,4),(3,7)) t(x,y);
-- begin-expected
-- columns: regr_avgy:float8
-- row: 4.333333333333333
-- rowcount: 1
-- end-expected
SELECT regr_avgy(y,x) FROM (VALUES (1,2),(2,4),(3,7)) t(x,y);
-- begin-expected
-- columns: regr_r2:float8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT regr_r2(y,x) FROM (VALUES (1,2),(2,2)) t(x,y);
-- begin-expected
-- columns: regr_r2:float8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT regr_r2(y,x) FROM (VALUES (1,2),(2,2),(3,2)) t(x,y);
-- begin-expected
-- columns: corr:float8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT corr(y,x) FROM (VALUES (1,2),(2,4),(3,6)) t(x,y);
-- begin-expected
-- columns: regr_r2:float8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT regr_r2(y,x) FROM (VALUES (1,2),(2,4),(3,6)) t(x,y);
