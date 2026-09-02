-- source: review-2026-08.md
-- finding: Root cause 8: the declared type and label come from the first branch or the innermost operand
-- area: Types, casts and coercion
-- title: Root cause 8: the declared type and label come from the first branch or the innermost operand
-- begin-expected
-- columns: text:text
-- row: 0.1
-- rowcount: 1
-- end-expected
SELECT 0.1::float4::text;
-- begin-expected
-- columns: int4:int4
-- row: 0
-- rowcount: 1
-- end-expected
SELECT 0.5::float8::int;
-- begin-expected
-- columns: sql_identifier:name
-- row: x
-- rowcount: 1
-- end-expected
SELECT 'x'::information_schema.sql_identifier;
-- begin-expected
-- columns: pg_typeof:regtype
-- row: numeric
-- rowcount: 1
-- end-expected
SELECT pg_typeof(COALESCE(1, 1.5));
-- begin-expected
-- columns: pg_typeof:regtype
-- row: text
-- rowcount: 1
-- end-expected
SELECT pg_typeof(COALESCE(NULL, NULL));
-- begin-expected
-- columns: pg_typeof:regtype
-- row: bigint
-- rowcount: 1
-- end-expected
SELECT pg_typeof(CASE WHEN true THEN 1::int2 ELSE 1::int8 END);
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type numeric: "abc"
-- end-expected-error
SELECT CASE WHEN true THEN 1.5 ELSE 'abc' END;
