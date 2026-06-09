-- ============================================================================
-- Feature Comparison: hstore Extension
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Tests comprehensive hstore usage:
--   A. Column creation (CREATE TABLE, ALTER TABLE ADD COLUMN)
--   B. INSERT / UPDATE / DELETE with hstore data
--   C. Operators: ->  @>  <@  ||  ?  -  (key delete)
--   D. Casting: text→hstore, hstore→text, hstore→json/jsonb, arrow→int/numeric/bool
--   E. Functions: akeys, avals, skeys, svals, each, hstore(), exist, defined, delete, slice
--   F. WHERE clause filtering on hstore columns
--   G. NULL handling, empty hstore, defaults
--   H. hstore from arrays, hstore from record
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS hs_test CASCADE;
CREATE SCHEMA hs_test;
SET search_path = hs_test, public;
CREATE EXTENSION IF NOT EXISTS hstore;

-- ============================================================================
-- A. Column creation
-- ============================================================================

CREATE TABLE hs_test.t_create (id int PRIMARY KEY, data hstore);
INSERT INTO hs_test.t_create VALUES (1, 'color=>blue, size=>large');

-- begin-expected
-- columns: val
-- row: blue
-- end-expected
SELECT data->'color' AS val FROM hs_test.t_create WHERE id = 1;

-- ALTER TABLE ADD COLUMN
CREATE TABLE hs_test.t_alter (id int PRIMARY KEY, name text);
ALTER TABLE hs_test.t_alter ADD COLUMN settings hstore;
INSERT INTO hs_test.t_alter VALUES (1, 'acme', 'theme=>dark, lang=>en');

-- begin-expected
-- columns: val
-- row: dark
-- end-expected
SELECT settings->'theme' AS val FROM hs_test.t_alter WHERE name = 'acme';

-- Default value
CREATE TABLE hs_test.t_default (id int PRIMARY KEY, opts hstore DEFAULT 'debug=>false');
INSERT INTO hs_test.t_default (id) VALUES (1);

-- begin-expected
-- columns: val
-- row: false
-- end-expected
SELECT opts->'debug' AS val FROM hs_test.t_default WHERE id = 1;

-- NOT NULL constraint
CREATE TABLE hs_test.t_notnull (id int PRIMARY KEY, tags hstore NOT NULL);
INSERT INTO hs_test.t_notnull VALUES (1, 'a=>1');

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value
-- end-expected-error
INSERT INTO hs_test.t_notnull VALUES (2, NULL);

-- ============================================================================
-- B. INSERT / UPDATE / DELETE
-- ============================================================================

CREATE TABLE hs_test.t_crud (id int PRIMARY KEY, data hstore);

-- Basic insert
INSERT INTO hs_test.t_crud VALUES (1, 'a=>1, b=>2, c=>3');

-- begin-expected
-- columns: a|c
-- row: 1|3
-- end-expected
SELECT data->'a' AS a, data->'c' AS c FROM hs_test.t_crud WHERE id = 1;

-- Insert with NULL value inside hstore
INSERT INTO hs_test.t_crud VALUES (2, 'a=>1, b=>NULL');

-- begin-expected
-- columns: b_is_null
-- row: true
-- end-expected
SELECT (data->'b') IS NULL AS b_is_null FROM hs_test.t_crud WHERE id = 2;

-- Insert NULL column
INSERT INTO hs_test.t_crud VALUES (3, NULL);

-- begin-expected
-- columns: data_is_null
-- row: true
-- end-expected
SELECT data IS NULL AS data_is_null FROM hs_test.t_crud WHERE id = 3;

-- Insert empty hstore
INSERT INTO hs_test.t_crud VALUES (4, '');

-- begin-expected
-- columns: val_is_null
-- row: true
-- end-expected
SELECT (data->'anything') IS NULL AS val_is_null FROM hs_test.t_crud WHERE id = 4;

-- Update
UPDATE hs_test.t_crud SET data = 'x=>new, y=>added' WHERE id = 1;

-- begin-expected
-- columns: x|y
-- row: new|added
-- end-expected
SELECT data->'x' AS x, data->'y' AS y FROM hs_test.t_crud WHERE id = 1;

-- Delete with hstore condition
CREATE TABLE hs_test.t_del (id int PRIMARY KEY, data hstore);
INSERT INTO hs_test.t_del VALUES (1, 'keep=>yes'), (2, 'keep=>no');
DELETE FROM hs_test.t_del WHERE data->'keep' = 'no';

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM hs_test.t_del;

-- ============================================================================
-- C. Operators
-- ============================================================================

CREATE TABLE hs_test.t_ops (id int PRIMARY KEY, data hstore);
INSERT INTO hs_test.t_ops VALUES
    (1, 'a=>1, b=>2, c=>3'),
    (2, 'x=>10'),
    (3, 'a=>1');

-- C1. -> (key extraction)
-- begin-expected
-- columns: val
-- row: 2
-- end-expected
SELECT data->'b' AS val FROM hs_test.t_ops WHERE id = 1;

-- C2. @> (contains)
-- begin-expected
-- columns: id
-- row: 1
-- row: 3
-- end-expected
SELECT id FROM hs_test.t_ops WHERE data @> 'a=>1' ORDER BY id;

-- C3. <@ (contained by)
-- begin-expected
-- columns: id
-- row: 1
-- row: 3
-- end-expected
SELECT id FROM hs_test.t_ops WHERE data <@ 'a=>1, b=>2, c=>3, d=>4' ORDER BY id;

-- C4. || (merge/concat)
-- begin-expected
-- columns: merged
-- row: 99
-- end-expected
SELECT (data || 'b=>99'::hstore)->'b' AS merged FROM hs_test.t_ops WHERE id = 1;

-- C5. exist() function (key exists) — using function instead of ? operator for JDBC compat
-- begin-expected
-- columns: has_a|has_z
-- row: true|false
-- end-expected
SELECT exist(data, 'a') AS has_a, exist(data, 'z') AS has_z FROM hs_test.t_ops WHERE id = 1;

-- C6. - text (delete key) — untyped literal 'b' resolved as hstore by PG, fails to parse
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error in hstore
-- end-expected-error
SELECT exist(data - 'b', 'b') AS has_b FROM hs_test.t_ops WHERE id = 1;

-- C7. - text[] (delete keys)
-- begin-expected
-- columns: has_a|has_c
-- row: false|false
-- end-expected
SELECT exist(data - ARRAY['a','c'], 'a') AS has_a, exist(data - ARRAY['a','c'], 'c') AS has_c FROM hs_test.t_ops WHERE id = 1;

-- ============================================================================
-- D. Casting
-- ============================================================================

CREATE TABLE hs_test.t_cast (id int PRIMARY KEY, data hstore);
INSERT INTO hs_test.t_cast VALUES (1, 'count=>42, price=>19.99, active=>true, name=>alice');

-- D1. text::hstore explicit cast in insert
CREATE TABLE hs_test.t_cast2 (id int PRIMARY KEY, data hstore);
INSERT INTO hs_test.t_cast2 VALUES (1, 'k=>v'::hstore);

-- begin-expected
-- columns: val
-- row: v
-- end-expected
SELECT data->'k' AS val FROM hs_test.t_cast2 WHERE id = 1;

-- D2. hstore column to text
-- begin-expected
-- columns: is_text
-- row: true
-- end-expected
SELECT pg_typeof(data::text) = 'text'::regtype AS is_text FROM hs_test.t_cast WHERE id = 1;

-- D3. Arrow result cast to int
-- begin-expected
-- columns: val
-- row: 42
-- end-expected
SELECT (data->'count')::int AS val FROM hs_test.t_cast WHERE id = 1;

-- D4. Arrow result cast to numeric
-- begin-expected
-- columns: val
-- row: 19.99
-- end-expected
SELECT (data->'price')::numeric AS val FROM hs_test.t_cast WHERE id = 1;

-- D5. Arrow result cast to boolean
-- begin-expected
-- columns: val
-- row: true
-- end-expected
SELECT (data->'active')::boolean AS val FROM hs_test.t_cast WHERE id = 1;

-- D6. hstore_to_json
-- begin-expected
-- columns: has_key
-- row: true
-- end-expected
SELECT hstore_to_json(data)::jsonb ? 'count' AS has_key FROM hs_test.t_cast WHERE id = 1;

-- D7. hstore_to_jsonb
-- begin-expected
-- columns: val
-- row: 42
-- end-expected
SELECT (hstore_to_jsonb(data)->>'count')::int AS val FROM hs_test.t_cast WHERE id = 1;

-- ============================================================================
-- E. Functions
-- ============================================================================

CREATE TABLE hs_test.t_fn (id int PRIMARY KEY, data hstore);
INSERT INTO hs_test.t_fn VALUES (1, 'b=>2, a=>1, c=>3');

-- E1. akeys — array of keys
-- begin-expected
-- columns: key_count
-- row: 3
-- end-expected
SELECT array_length(akeys(data), 1) AS key_count FROM hs_test.t_fn WHERE id = 1;

-- E2. avals — array of values
-- begin-expected
-- columns: val_count
-- row: 3
-- end-expected
SELECT array_length(avals(data), 1) AS val_count FROM hs_test.t_fn WHERE id = 1;

-- E3. skeys — set of keys
-- begin-expected
-- columns: key_count
-- row: 3
-- end-expected
SELECT count(*) AS key_count FROM skeys((SELECT data FROM hs_test.t_fn WHERE id = 1));

-- E4. svals — set of values
-- begin-expected
-- columns: val_count
-- row: 3
-- end-expected
SELECT count(*) AS val_count FROM svals((SELECT data FROM hs_test.t_fn WHERE id = 1));

-- E5. each — key/value pairs
-- begin-expected
-- columns: pair_count
-- row: 3
-- end-expected
SELECT count(*) AS pair_count FROM each((SELECT data FROM hs_test.t_fn WHERE id = 1));

-- E6. exist — key existence
-- begin-expected
-- columns: has_a|has_z
-- row: true|false
-- end-expected
SELECT exist(data, 'a') AS has_a, exist(data, 'z') AS has_z FROM hs_test.t_fn WHERE id = 1;

-- E7. defined — key has non-NULL value
INSERT INTO hs_test.t_fn VALUES (2, 'a=>1, b=>NULL');

-- begin-expected
-- columns: a_defined|b_defined
-- row: true|false
-- end-expected
SELECT defined(data, 'a') AS a_defined, defined(data, 'b') AS b_defined FROM hs_test.t_fn WHERE id = 2;

-- E8. delete function — remove key
-- begin-expected
-- columns: has_b
-- row: false
-- end-expected
SELECT exist(delete(data, 'b'), 'b') AS has_b FROM hs_test.t_fn WHERE id = 1;

-- E9. slice — extract subset by key array
-- begin-expected
-- columns: val
-- row: 1
-- end-expected
SELECT (slice(data, ARRAY['a']))->'a' AS val FROM hs_test.t_fn WHERE id = 1;

-- E10. hstore from two arrays
-- begin-expected
-- columns: val
-- row: 2
-- end-expected
SELECT (hstore(ARRAY['a','b'], ARRAY['1','2']))->'b' AS val;

-- E11. hstore from key-value pair
-- begin-expected
-- columns: val
-- row: alice
-- end-expected
SELECT (hstore('name', 'alice'))->'name' AS val;

-- ============================================================================
-- F. WHERE clause filtering
-- ============================================================================

CREATE TABLE hs_test.t_filter (id int PRIMARY KEY, props hstore);
INSERT INTO hs_test.t_filter VALUES
    (1, 'status=>active, region=>us'),
    (2, 'status=>inactive, region=>eu'),
    (3, 'status=>active, region=>eu');

-- F1. Arrow equality
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM hs_test.t_filter WHERE props->'status' = 'active';

-- F2. Contains
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM hs_test.t_filter WHERE props @> 'status=>active, region=>eu';

-- F3. Key exists (via exist function)
-- begin-expected
-- columns: count
-- row: 3
-- end-expected
SELECT count(*) FROM hs_test.t_filter WHERE exist(props, 'status');

-- ============================================================================
-- G. Multiple hstore columns and mixed types
-- ============================================================================

CREATE TABLE hs_test.t_multi (id int PRIMARY KEY, name text, config hstore, overrides hstore, extra jsonb);
INSERT INTO hs_test.t_multi VALUES (1, 'test', 'a=>1', 'a=>2', '{"j":1}');

-- begin-expected
-- columns: config_a|override_a
-- row: 1|2
-- end-expected
SELECT config->'a' AS config_a, overrides->'a' AS override_a FROM hs_test.t_multi WHERE id = 1;

-- ============================================================================
-- H. Edge cases
-- ============================================================================

-- H1. Quoted keys and values with special characters
CREATE TABLE hs_test.t_special (id int PRIMARY KEY, data hstore);
INSERT INTO hs_test.t_special VALUES (1, '"with space"=>"has value"');

-- begin-expected
-- columns: val
-- row: has value
-- end-expected
SELECT data->'with space' AS val FROM hs_test.t_special WHERE id = 1;

-- H2. hstore in subquery
CREATE TABLE hs_test.t_sub (id int PRIMARY KEY, data hstore);
INSERT INTO hs_test.t_sub VALUES (1, 'x=>10'), (2, 'x=>20');

-- begin-expected
-- columns: val
-- row: 20
-- end-expected
SELECT data->'x' AS val FROM hs_test.t_sub WHERE id = (SELECT id FROM hs_test.t_sub WHERE data->'x' = '20');

-- H3. Arrow on missing key returns NULL
-- begin-expected
-- columns: is_null
-- row: true
-- end-expected
SELECT (data->'nonexistent') IS NULL AS is_null FROM hs_test.t_sub WHERE id = 1;

-- H4. hstore in ORDER BY (via arrow extraction)
-- begin-expected
-- columns: id|val
-- row: 1|10
-- row: 2|20
-- end-expected
SELECT id, data->'x' AS val FROM hs_test.t_sub ORDER BY (data->'x')::int;

-- ============================================================================
-- Cleanup
-- ============================================================================

SET search_path = public;
DROP SCHEMA IF EXISTS hs_test CASCADE;
