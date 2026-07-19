-- ============================================================================
-- jsonb operator semantics: containment (@> / <@), text extraction (->> / #>>),
-- concatenation (||), jsonb_set / jsonb_insert path handling, key existence
-- (? / ?| / ?&), element deletion (-), composite literal quoted empty strings,
-- and hstore key-sorted text output.
-- ============================================================================

-- setup
-- Note: an earlier suite file installs hstore into a schema it later drops with
-- DROP SCHEMA CASCADE, which removes the extension's objects but leaves the
-- extension entry registered. CREATE EXTENSION IF NOT EXISTS would then no-op
-- against that ghost entry, so recreate the extension outright.
DROP EXTENSION IF EXISTS hstore CASCADE;
CREATE EXTENSION hstore;
CREATE TYPE jsonb_ops_ct AS (a int, b text);

-- ============================================================================
-- SECTION A: containment
-- ============================================================================

-- A1. top-level array contains scalar
-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT '[1,2,3]'::jsonb @> '3'::jsonb AS v;

-- A2. scalar-in-array special case does NOT apply below top level
-- begin-expected
-- columns: v
-- row: f
-- end-expected
SELECT '{"a":[1]}'::jsonb @> '{"a":1}'::jsonb AS v;

-- A3. nested array does not contain scalar-style
-- begin-expected
-- columns: v
-- row: f
-- end-expected
SELECT '[[1]]'::jsonb @> '[1]'::jsonb AS v;

-- A4. array contained by superset array
-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT '[1,2]'::jsonb <@ '[1,2,3]'::jsonb AS v;

-- A5. scalar contained by top-level array
-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT '3'::jsonb <@ '[1,2,3]'::jsonb AS v;

-- A6. scalar contains equal scalar
-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT '"foo"'::jsonb @> '"foo"'::jsonb AS v;

-- A7. object containment inside array elements
-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT '[{"a":1,"b":2}]'::jsonb @> '[{"b":2}]'::jsonb AS v;

-- A8. scalar not present in array
-- begin-expected
-- columns: v
-- row: f
-- end-expected
SELECT '[1,2,3]'::jsonb @> '4'::jsonb AS v;

-- A9. scalar does not contain array
-- begin-expected
-- columns: v
-- row: f
-- end-expected
SELECT '3'::jsonb @> '[3]'::jsonb AS v;

-- ============================================================================
-- SECTION B: ->> and #>> text extraction
-- ============================================================================

-- B1. #>> maps JSON null to SQL NULL
-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT ('{"a":null}'::jsonb #>> '{a}') IS NULL AS v;

-- B2. ->> maps JSON null to SQL NULL
-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT ('{"a":null}'::jsonb ->> 'a') IS NULL AS v;

-- B3. ->> unescapes JSON strings
-- begin-expected
-- columns: v
-- row: x"y
-- end-expected
SELECT '{"a":"x\"y"}'::jsonb ->> 'a' AS v;

-- B4. #>> unescapes JSON strings at depth
-- begin-expected
-- columns: v
-- row: x"y
-- end-expected
SELECT '{"a":{"b":"x\"y"}}'::jsonb #>> '{a,b}' AS v;

-- B5. ->> decodes unicode escapes
-- begin-expected
-- columns: v
-- row: AB
-- end-expected
SELECT '{"a":"\u0041B"}'::jsonb ->> 'a' AS v;

-- B6. JSON string "null" is text 'null', not SQL NULL
-- begin-expected
-- columns: v
-- row: null
-- end-expected
SELECT '["null"]'::jsonb ->> 0 AS v;

-- B7. ->> on JSON null array element is SQL NULL
-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT ('[null]'::jsonb ->> 0) IS NULL AS v;

-- ============================================================================
-- SECTION C: || concatenation matrix
-- ============================================================================

-- C1. scalar || scalar wraps into array
-- begin-expected
-- columns: v
-- row: [1, 2]
-- end-expected
SELECT ('1'::jsonb || '2'::jsonb)::text AS v;

-- C2. array || scalar appends
-- begin-expected
-- columns: v
-- row: [1, 2, 3]
-- end-expected
SELECT ('[1,2]'::jsonb || '3'::jsonb)::text AS v;

-- C3. scalar || array prepends
-- begin-expected
-- columns: v
-- row: [3, 1, 2]
-- end-expected
SELECT ('3'::jsonb || '[1,2]'::jsonb)::text AS v;

-- C4. object || object merges, right wins
-- begin-expected
-- columns: v
-- row: {"a": 3, "b": 2}
-- end-expected
SELECT ('{"a":1}'::jsonb || '{"b":2,"a":3}'::jsonb)::text AS v;

-- C5. array || array concatenates
-- begin-expected
-- columns: v
-- row: [1, 2, 3]
-- end-expected
SELECT ('[1]'::jsonb || '[2,3]'::jsonb)::text AS v;

-- C6. object || array wraps the object
-- begin-expected
-- columns: v
-- row: [{"a": 1}, 1]
-- end-expected
SELECT ('{"a":1}'::jsonb || '[1]'::jsonb)::text AS v;

-- C7. array || object wraps the object
-- begin-expected
-- columns: v
-- row: [1, {"a": 1}]
-- end-expected
SELECT ('[1]'::jsonb || '{"a":1}'::jsonb)::text AS v;

-- C8. object || scalar wraps both
-- begin-expected
-- columns: v
-- row: [{"a": 1}, 2]
-- end-expected
SELECT ('{"a":1}'::jsonb || '2'::jsonb)::text AS v;

-- C9. string scalar || string scalar
-- begin-expected
-- columns: v
-- row: ["x", "y"]
-- end-expected
SELECT ('"x"'::jsonb || '"y"'::jsonb)::text AS v;

-- ============================================================================
-- SECTION D: jsonb_set path semantics
-- ============================================================================

-- D1. missing intermediate path step returns target unchanged
-- begin-expected
-- columns: v
-- row: {"a": 1}
-- end-expected
SELECT jsonb_set('{"a":1}'::jsonb, '{b,c}', '2'::jsonb)::text AS v;

-- D2. create_missing (default true) adds the final step
-- begin-expected
-- columns: v
-- row: {"a": 1, "b": 2}
-- end-expected
SELECT jsonb_set('{"a":1}'::jsonb, '{b}', '2'::jsonb)::text AS v;

-- D3. create_missing false does not add the final step
-- begin-expected
-- columns: v
-- row: {"a": 1}
-- end-expected
SELECT jsonb_set('{"a":1}'::jsonb, '{b}', '2'::jsonb, false)::text AS v;

-- D4. create_missing false still replaces existing values
-- begin-expected
-- columns: v
-- row: {"a": 2}
-- end-expected
SELECT jsonb_set('{"a":1}'::jsonb, '{a}', '2'::jsonb, false)::text AS v;

-- D5. final step created inside existing nested object
-- begin-expected
-- columns: v
-- row: {"a": {"b": 1, "c": 2}}
-- end-expected
SELECT jsonb_set('{"a":{"b":1}}'::jsonb, '{a,c}', '2'::jsonb)::text AS v;

-- D6. path navigates through arrays
-- begin-expected
-- columns: v
-- row: {"a": [1, 9]}
-- end-expected
SELECT jsonb_set('{"a":[1,2]}'::jsonb, '{a,1}', '9'::jsonb)::text AS v;

-- D7. negative array index counts from the end
-- begin-expected
-- columns: v
-- row: ["a", "c"]
-- end-expected
SELECT jsonb_set('["a","b"]'::jsonb, '{-1}', '"c"'::jsonb)::text AS v;

-- D8. out-of-range positive index appends
-- begin-expected
-- columns: v
-- row: [1, 2]
-- end-expected
SELECT jsonb_set('[1]'::jsonb, '{5}', '2'::jsonb)::text AS v;

-- D9. out-of-range negative index prepends
-- begin-expected
-- columns: v
-- row: [2, 1]
-- end-expected
SELECT jsonb_set('[1]'::jsonb, '{-5}', '2'::jsonb)::text AS v;

-- ============================================================================
-- SECTION E: jsonb_insert
-- ============================================================================

-- E1. missing intermediate path step returns target unchanged
-- begin-expected
-- columns: v
-- row: {"a": 1}
-- end-expected
SELECT jsonb_insert('{"a":1}'::jsonb, '{b,c}', '2'::jsonb)::text AS v;

-- E2. new object key is inserted
-- begin-expected
-- columns: v
-- row: {"a": 1, "b": 2}
-- end-expected
SELECT jsonb_insert('{"a":1}'::jsonb, '{b}', '2'::jsonb)::text AS v;

-- E3. inserting at an existing object key is an error
-- begin-expected-error
-- sqlstate: 22023
-- message-like: cannot replace existing key
-- end-expected-error
SELECT jsonb_insert('{"a":1}'::jsonb, '{a}', '2'::jsonb)::text;

-- E4. array insert before position
-- begin-expected
-- columns: v
-- row: {"a": [0, 9, 1, 2]}
-- end-expected
SELECT jsonb_insert('{"a":[0,1,2]}'::jsonb, '{a,1}', '9'::jsonb)::text AS v;

-- E5. negative index with insert_after
-- begin-expected
-- columns: v
-- row: [1, 2, 3, 9]
-- end-expected
SELECT jsonb_insert('[1,2,3]'::jsonb, '{-1}', '9'::jsonb, true)::text AS v;

-- ============================================================================
-- SECTION F: ? / ?| / ?& key existence
-- ============================================================================

-- F1. ? matches a top-level scalar string equal to the key
-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT '"foo"'::jsonb ? 'foo' AS v;

-- F2. ? matches string array elements
-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT '["a","b"]'::jsonb ? 'a' AS v;

-- F3. ? does not match non-string array elements
-- begin-expected
-- columns: v
-- row: f
-- end-expected
SELECT '[1,2]'::jsonb ? '1' AS v;

-- F4. ?| matches a top-level scalar string
-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT '"foo"'::jsonb ?| array['bar','foo'] AS v;

-- F5. ?& over string array elements
-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT '["a","b"]'::jsonb ?& array['a','b'] AS v;

-- ============================================================================
-- SECTION G: jsonb - integer deletion
-- ============================================================================

-- G1. negative index deletes from the end
-- begin-expected
-- columns: v
-- row: ["a", "b"]
-- end-expected
SELECT ('["a","b","c"]'::jsonb - -1)::text AS v;

-- G2. out-of-range positive index leaves array unchanged
-- begin-expected
-- columns: v
-- row: ["a", "b", "c"]
-- end-expected
SELECT ('["a","b","c"]'::jsonb - 5)::text AS v;

-- G3. out-of-range negative index leaves array unchanged
-- begin-expected
-- columns: v
-- row: ["a", "b", "c"]
-- end-expected
SELECT ('["a","b","c"]'::jsonb - -5)::text AS v;

-- ============================================================================
-- SECTION H: composite literal quoted empty string
-- ============================================================================

-- H1. quoted empty string field is the empty string
-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT ('(1,"")'::jsonb_ops_ct).b = '' AS v;

-- H2. quoted empty string field is not NULL
-- begin-expected
-- columns: v
-- row: f
-- end-expected
SELECT ('(1,"")'::jsonb_ops_ct).b IS NULL AS v;

-- H3. unquoted empty field is NULL
-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT ('(1,)'::jsonb_ops_ct).b IS NULL AS v;

-- ============================================================================
-- SECTION I: hstore key-sorted text output
-- ============================================================================

-- I1. hstore output is sorted by key
-- begin-expected
-- columns: v
-- row: "a"=>"1", "b"=>"2", "c"=>"3"
-- end-expected
SELECT ('c=>3, a=>1, b=>2'::hstore)::text AS v;

-- cleanup
DROP TYPE jsonb_ops_ct;
