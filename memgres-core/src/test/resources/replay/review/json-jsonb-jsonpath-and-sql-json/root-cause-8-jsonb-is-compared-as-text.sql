-- source: review-2026-08.md
-- finding: Root cause 8: jsonb is compared as text
-- area: JSON, JSONB, jsonpath and SQL/JSON
-- title: Root cause 8: jsonb is compared as text
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '1'::jsonb = '1.0'::jsonb;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '{"a":[1.0]}'::jsonb @> '{"a":[1]}'::jsonb;
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(DISTINCT v) FROM (VALUES ('1'::jsonb),('1.0'::jsonb)) t(v);
-- begin-expected
-- ok: 0
-- end-expected
DROP TABLE IF EXISTS zz_pk;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_pk (j jsonb PRIMARY KEY);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_pk VALUES ('{"a":1}');
-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "zz_pk_pkey"
-- end-expected-error
INSERT INTO zz_pk VALUES ('{"a":1.0}');
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_pk;
-- begin-expected
-- columns: v:text
-- row: []
-- row: {}
-- row: 1
-- row: null
-- row: "s"
-- row: true
-- rowcount: 6
-- end-expected
SELECT v::text FROM (VALUES ('{}'::jsonb),('[]'::jsonb),('1'::jsonb),('null'::jsonb),('true'::jsonb),('"s"'::jsonb)) t(v) ORDER BY v;
