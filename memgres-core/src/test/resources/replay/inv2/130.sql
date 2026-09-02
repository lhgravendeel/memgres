-- source: investigation-2026-08.md
-- finding: 130
-- title: Column and relation references are resolved while a row is evaluated rather than in an analysis pass, so a query over an empty table never notices an out-of-sco
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_e0 (a int);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "zz_e0"
-- end-expected-error
SELECT count(*) FROM zz_e0 x WHERE public.zz_e0.a = 1;
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_e0 VALUES (1);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "zz_e0"
-- end-expected-error
SELECT count(*) FROM zz_e0 x WHERE public.zz_e0.a = 1;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "zz_e0"
-- end-expected-error
SELECT count(*) FROM (SELECT a FROM zz_e0) s WHERE zz_e0.a = 1;
