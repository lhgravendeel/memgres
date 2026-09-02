-- source: investigation.md
-- finding: 117
-- title: Finding #1 blocks PG's own catalog checks
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_type WHERE typinput = 0 OR typoutput = 0;
--   mg: 42883 operator does not exist: text = integer
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_proc WHERE 0 = ANY (proargtypes);
--   mg: 42883 operator does not exist: integer = text
-- begin-expected
-- columns: count:int8
-- row: 169
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_proc
 WHERE array_length(proargtypes, 1) IS DISTINCT FROM NULLIF(pronargs, 0);
--   mg: 42804 function array_length(text, integer) does not exist;
