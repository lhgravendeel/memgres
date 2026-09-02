-- source: investigation-2026-08.md
-- finding: 38
-- title: Aggregate modifiers are consulted only in some branches of the aggregate evaluator: fn.distinct() is read in the count/sum/avg/min/max, string_agg and array_agg
-- begin-expected
-- columns: json_agg:text
-- row: [1]
-- rowcount: 1
-- end-expected
SELECT json_agg(DISTINCT v)::text FROM (VALUES (1),(1)) t(v);
-- begin-expected
-- columns: jsonb_agg:text
-- row: [1]
-- rowcount: 1
-- end-expected
SELECT jsonb_agg(DISTINCT v)::text FROM (VALUES (1),(1)) t(v);
-- begin-expected
-- columns: json_object_agg:text
-- row: { "a" : 1 }
-- rowcount: 1
-- end-expected
SELECT json_object_agg(DISTINCT k, v)::text FROM (VALUES ('a',1),('a',1)) t(k,v);
-- begin-expected
-- columns: bit_xor:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT bit_xor(DISTINCT v) FROM (VALUES (1),(1)) t(v);
-- begin-expected
-- columns: variance:numeric
-- row: 2.0000000000000000
-- rowcount: 1
-- end-expected
SELECT variance(DISTINCT v) FROM (VALUES (1),(1),(3)) t(v);
-- begin-expected
-- columns: stddev:numeric
-- row: 1.4142135623730950
-- rowcount: 1
-- end-expected
SELECT stddev(DISTINCT v) FROM (VALUES (1),(1),(3)) t(v);
-- begin-expected
-- columns: json_agg:text
-- row: [null, 1, 2, 3]
-- rowcount: 1
-- end-expected
SELECT json_agg(v ORDER BY v NULLS FIRST)::text FROM (VALUES (3),(1),(2),(NULL::int)) t(v);
-- begin-expected
-- columns: array_agg:text
-- row: {NULL,1,2,3}
-- rowcount: 1
-- end-expected
SELECT array_agg(v ORDER BY v NULLS FIRST)::text FROM (VALUES (3),(1),(2),(NULL::int)) t(v);
-- begin-expected
-- columns: array_agg:text
-- row: {3,2,1,NULL}
-- rowcount: 1
-- end-expected
SELECT array_agg(v ORDER BY v DESC NULLS LAST)::text FROM (VALUES (3),(1),(2),(NULL::int)) t(v);
