-- source: review-2026-08.md
-- finding: Root cause 8: routine bodies are validated by regex against a hand-written builtin list
-- area: Joins, CTEs, subqueries — and the DDL that came with them
-- title: Root cause 8: routine bodies are validated by regex against a hand-written builtin list
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
CREATE FUNCTION zz_sf(n int) RETURNS int LANGUAGE sql AS $$
  SELECT CASE WHEN n <= 0 THEN 0 ELSE 1 + zz_sf(n - 1) END $$;
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
