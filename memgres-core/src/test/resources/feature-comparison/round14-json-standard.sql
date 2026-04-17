-- ============================================================================
-- Feature Comparison: Round 14 — SQL/JSON standard & jsonb extensions
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

DROP SCHEMA IF EXISTS r14_json CASCADE;
CREATE SCHEMA r14_json;
SET search_path = r14_json, public;

-- ============================================================================
-- SECTION A: JSON_TABLE (PG 17+)
-- ============================================================================

-- 1. Scalar columns
-- begin-expected
-- columns: a,b
-- row: 1 | x
-- row: 2 | y
-- end-expected
SELECT a, b FROM JSON_TABLE(
  '[{"a":1,"b":"x"},{"a":2,"b":"y"}]'::jsonb, '$[*]'
  COLUMNS (a int PATH '$.a', b text PATH '$.b')) ORDER BY a;

-- 2. NESTED PATH
-- begin-expected
-- columns: a,tag
-- row: 1 | x
-- row: 1 | y
-- end-expected
SELECT a, tag FROM JSON_TABLE(
  '{"rows":[{"a":1,"tags":["x","y"]}]}'::jsonb, '$.rows[*]'
  COLUMNS (a int PATH '$.a',
           NESTED PATH '$.tags[*]' COLUMNS (tag text PATH '$'))) ORDER BY tag;

-- 3. FOR ORDINALITY
-- begin-expected
-- columns: n,v
-- row: 1 | 10
-- row: 2 | 20
-- row: 3 | 30
-- end-expected
SELECT n, v FROM JSON_TABLE('[10,20,30]'::jsonb, '$[*]'
  COLUMNS (n FOR ORDINALITY, v int PATH '$')) ORDER BY n;

-- ============================================================================
-- SECTION B: JSON_VALUE / JSON_QUERY / JSON_EXISTS
-- ============================================================================

-- 4. JSON_VALUE scalar
-- begin-expected
-- columns: v
-- row: 1
-- end-expected
SELECT JSON_VALUE('{"a":1}'::jsonb, '$.a') AS v;

-- 5. JSON_VALUE DEFAULT ... ON EMPTY
-- begin-expected
-- columns: v
-- row: missing
-- end-expected
SELECT JSON_VALUE('{"a":1}'::jsonb, '$.b' DEFAULT 'missing' ON EMPTY) AS v;

-- 6. JSON_QUERY returns jsonb
-- begin-expected
-- columns: has
-- row: t
-- end-expected
SELECT (JSON_QUERY('{"a":[1,2]}'::jsonb, '$.a')::text LIKE '%1%2%')::text AS has;

-- 7. JSON_EXISTS true
-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT JSON_EXISTS('{"a":1}'::jsonb, '$.a')::text AS v;

-- 8. JSON_EXISTS false
-- begin-expected
-- columns: v
-- row: f
-- end-expected
SELECT JSON_EXISTS('{"a":1}'::jsonb, '$.b')::text AS v;

-- ============================================================================
-- SECTION C: jsonb subscript operator (PG 14+)
-- ============================================================================

-- 9. Key subscript
-- begin-expected
-- columns: v
-- row: 1
-- end-expected
SELECT ('{"a":1}'::jsonb)['a']::text AS v;

-- 10. Index subscript
-- begin-expected
-- columns: v
-- row: 2
-- end-expected
SELECT ('[1,2,3]'::jsonb)[1]::text AS v;

-- 11. Chained subscripts
-- begin-expected
-- columns: v
-- row: 42
-- end-expected
SELECT ('{"a":{"b":42}}'::jsonb)['a']['b']::text AS v;

-- 12. UPDATE via subscript
CREATE TABLE r14_js_sub (j jsonb);
INSERT INTO r14_js_sub VALUES ('{"a":1}');
UPDATE r14_js_sub SET j['a'] = '99'::jsonb;

-- begin-expected
-- columns: v
-- row: 99
-- end-expected
SELECT (j->'a')::text AS v FROM r14_js_sub;

-- ============================================================================
-- SECTION D: jsonb_path_*_tz (PG 17+)
-- ============================================================================

-- 13. jsonb_path_exists_tz
-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT jsonb_path_exists_tz('"2024-01-01T00:00:00Z"'::jsonb,
  '$.datetime() < "2025-01-01".datetime()')::text AS v;

-- 14. jsonb_path_match_tz
-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT jsonb_path_match_tz('"2024-01-01T00:00:00Z"'::jsonb,
  '$.datetime() <= "2030-01-01".datetime()')::text AS v;

-- ============================================================================
-- SECTION E: jsonb_set_lax
-- ============================================================================

-- 15. return_target
-- begin-expected
-- columns: v
-- row: {"a": 1}
-- end-expected
SELECT jsonb_set_lax('{"a":1}'::jsonb, '{b}', NULL, true, 'return_target')::text AS v;

-- 16. delete_key
-- begin-expected
-- columns: v
-- row: {"a": 1}
-- end-expected
SELECT jsonb_set_lax('{"a":1,"b":2}'::jsonb, '{b}', NULL, true, 'delete_key')::text AS v;

-- 17. use_json_null
-- begin-expected
-- columns: v
-- row: {"a": null}
-- end-expected
SELECT jsonb_set_lax('{"a":1}'::jsonb, '{a}', NULL, true, 'use_json_null')::text AS v;

-- ============================================================================
-- SECTION F: @? and @@ operators
-- ============================================================================

-- 18. @? existence
-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT ('{"a":1}'::jsonb @? '$.a')::text AS v;

-- 19. @@ predicate
-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT ('{"a":1}'::jsonb @@ '$.a == 1')::text AS v;

-- ============================================================================
-- SECTION G: JSON_ARRAYAGG / JSON_OBJECTAGG
-- ============================================================================

-- 20. JSON_ARRAYAGG
-- begin-expected
-- columns: v
-- row: [1, 2, 3]
-- end-expected
SELECT JSON_ARRAYAGG(x) AS v FROM (VALUES (1),(2),(3)) t(x);

-- 21. JSON_OBJECTAGG WITH UNIQUE KEYS (duplicate keys error)
-- begin-expected-error
-- message-like: duplicate
-- end-expected-error
SELECT JSON_OBJECTAGG(k VALUE v WITH UNIQUE KEYS) FROM (VALUES ('a',1),('a',2)) t(k,v);

-- ============================================================================
-- SECTION H: JSON_OBJECT KEY VALUE syntax
-- ============================================================================

-- 22. KEY ... VALUE ... grammar
-- begin-expected
-- columns: v
-- row: {"a" : 1, "b" : 2}
-- end-expected
SELECT JSON_OBJECT('a' VALUE 1, 'b' VALUE 2)::text AS v;

-- ============================================================================
-- SECTION I: jsonb_object constructors
-- ============================================================================

-- 23. Two arrays form
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (jsonb_object(ARRAY['a','b']::text[], ARRAY['1','2']::text[])::text LIKE '%"a"%')::text AS ok;

-- 24. Single alternating array
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (jsonb_object(ARRAY['a','1','b','2'])::text LIKE '%"a"%"b"%')::text AS ok;

-- ============================================================================
-- SECTION J: json_strip_nulls (json variant)
-- ============================================================================

-- 25. Strip null fields from json
-- begin-expected
-- columns: v
-- row: {"a":1}
-- end-expected
SELECT json_strip_nulls('{"a":1,"b":null}'::json)::text AS v;
