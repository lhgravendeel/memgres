-- source: investigation-2026-08.md
-- finding: 126
-- title: Routine bodies are validated by regex against a hand-written list of built-ins plus the user-function registry, so anything outside the list — including the fun
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_f1() RETURNS int LANGUAGE sql AS $$ SELECT ascii('a') $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_f2() RETURNS int LANGUAGE sql AS $$ SELECT cardinality(ARRAY[1,2,3]) $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_sf(n int) RETURNS int LANGUAGE sql AS $$ SELECT CASE WHEN n <= 0 THEN 0 ELSE 1 + zz_sf(n - 1) END $$;
-- begin-expected
-- columns: zz_sf:int4
-- row: 10
-- rowcount: 1
-- end-expected
SELECT zz_sf(10);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_f5(x int DEFAULT ascii('a')) RETURNS int LANGUAGE sql AS $$ SELECT x $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_f4() RETURNS int LANGUAGE sql AS $$ SELECT 1 -- one; two
$$;
