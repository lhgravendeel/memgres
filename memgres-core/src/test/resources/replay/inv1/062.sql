-- source: investigation.md
-- finding: 62
-- title: Miscellaneous from the same seeds
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT 'a'::tsquery @@ to_tsvector('simple','a b');
--   PG: true | mg: 42601 syntax error in tsquery: "'a':1 'b':2"   ← operands swapped
-- begin-expected
-- columns: jsonb_exists_any:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT jsonb_exists_any('{"a":1,"b":2}'::jsonb, '{a,z}');
--   PG: true | mg: 42883 function does not exist
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: unknown = point
-- end-expected-error
SELECT '(1,2)' = '(1,2)'::point;
-- PG: 42883 operator does not exist: unknown = point | mg: true
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: point = point
-- end-expected-error
SELECT '(1,2)'::point = ANY(ARRAY['(1,2)'::point]);
--   PG: 42883 operator does not exist: point = point | mg: true;
