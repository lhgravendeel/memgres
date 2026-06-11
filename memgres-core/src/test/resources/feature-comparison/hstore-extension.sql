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
-- I. Equality and comparison semantics
-- ============================================================================

-- I1. hstore equality is content-based, key-order-independent
-- begin-expected
-- columns: eq
-- row: true
-- end-expected
SELECT 'a=>1, b=>2'::hstore = 'b=>2, a=>1'::hstore AS eq;

-- I2. hstore inequality
-- begin-expected
-- columns: neq
-- row: true
-- end-expected
SELECT 'a=>1'::hstore <> 'a=>2'::hstore AS neq;

-- I3. Equal hstore with different whitespace
-- begin-expected
-- columns: eq
-- row: true
-- end-expected
SELECT 'a => 1 , b => 2'::hstore = 'a=>1,b=>2'::hstore AS eq;

-- ============================================================================
-- J. Duplicate keys in input
-- ============================================================================

-- J1. Duplicate keys — first value wins in PG
-- begin-expected
-- columns: val
-- row: 1
-- end-expected
SELECT ('a=>1, a=>2'::hstore)->'a' AS val;

-- J2. Duplicate keys — key count is 1 (deduplicated)
-- begin-expected
-- columns: key_count
-- row: 1
-- end-expected
SELECT array_length(akeys('a=>1, a=>2'::hstore), 1) AS key_count;

-- ============================================================================
-- K. hstore text output format
-- ============================================================================

-- K1. Simple hstore text representation contains key and value with =>
CREATE TABLE hs_test.t_textout (id int PRIMARY KEY, data hstore);
INSERT INTO hs_test.t_textout VALUES (1, 'a=>1');

-- begin-expected
-- columns: txt
-- row: "a"=>"1"
-- end-expected
SELECT data::text AS txt FROM hs_test.t_textout WHERE id = 1;

-- K2. Multi-key hstore text representation (keys are sorted by PG in output)
INSERT INTO hs_test.t_textout VALUES (2, 'b=>2, a=>1');

-- begin-expected-regex
-- columns: txt
-- row-regex: "a"=>"1".*"b"=>"2"|"b"=>"2".*"a"=>"1"
-- end-expected-regex
SELECT data::text AS txt FROM hs_test.t_textout WHERE id = 2;

-- ============================================================================
-- L. Concat operator with untyped literal
-- ============================================================================

CREATE TABLE hs_test.t_concat2 (id int PRIMARY KEY, data hstore);
INSERT INTO hs_test.t_concat2 VALUES (1, 'a=>1, b=>2');

-- L1. || with ::hstore cast (known working)
-- begin-expected
-- columns: val
-- row: 99
-- end-expected
SELECT (data || 'b=>99'::hstore)->'b' AS val FROM hs_test.t_concat2 WHERE id = 1;

-- L2. || with untyped literal — may have operator resolution issue like - operator
-- begin-expected
-- columns: val
-- row: 99
-- end-expected
SELECT (data || 'b=>99')->'b' AS val FROM hs_test.t_concat2 WHERE id = 1;

-- ============================================================================
-- M. hstore_to_json/jsonb with NULL values
-- ============================================================================

-- M1. hstore_to_json preserves NULL values as JSON null
-- begin-expected
-- columns: b_is_null
-- row: true
-- end-expected
SELECT (hstore_to_json('a=>1, b=>NULL'::hstore)->>'b') IS NULL AS b_is_null;

-- M2. hstore_to_jsonb preserves NULL values as JSON null
-- begin-expected
-- columns: b_is_null
-- row: true
-- end-expected
SELECT (hstore_to_jsonb('a=>1, b=>NULL'::hstore)->>'b') IS NULL AS b_is_null;

-- M3. hstore_to_json non-NULL value is present
-- begin-expected
-- columns: val
-- row: 1
-- end-expected
SELECT hstore_to_json('a=>1, b=>NULL'::hstore)->>'a' AS val;

-- ============================================================================
-- N. RETURNING clause with hstore
-- ============================================================================

CREATE TABLE hs_test.t_ret (id int PRIMARY KEY, data hstore);

-- N1. INSERT RETURNING with arrow extraction
-- begin-expected
-- columns: val
-- row: hello
-- end-expected
INSERT INTO hs_test.t_ret VALUES (1, 'key=>hello') RETURNING data->'key' AS val;

-- N2. UPDATE RETURNING with arrow extraction
-- begin-expected
-- columns: val
-- row: world
-- end-expected
UPDATE hs_test.t_ret SET data = 'key=>world' WHERE id = 1 RETURNING data->'key' AS val;

-- ============================================================================
-- O. DISTINCT and GROUP BY on hstore
-- ============================================================================

CREATE TABLE hs_test.t_distinct (id int PRIMARY KEY, data hstore);
INSERT INTO hs_test.t_distinct VALUES (1, 'a=>1'), (2, 'a=>1'), (3, 'b=>2');

-- O1. DISTINCT on hstore column
-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*) AS cnt FROM (SELECT DISTINCT data FROM hs_test.t_distinct) sub;

-- O2. GROUP BY on hstore column
-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*) AS cnt FROM (SELECT data, count(*) FROM hs_test.t_distinct GROUP BY data) sub;

-- ============================================================================
-- P. COALESCE and CASE with hstore
-- ============================================================================

CREATE TABLE hs_test.t_coalesce (id int PRIMARY KEY, data hstore);
INSERT INTO hs_test.t_coalesce VALUES (1, NULL), (2, 'x=>1');

-- P1. COALESCE with hstore — fallback to default
-- begin-expected
-- columns: val
-- row: fallback
-- end-expected
SELECT COALESCE(data, 'default=>fallback'::hstore)->'default' AS val FROM hs_test.t_coalesce WHERE id = 1;

-- P2. COALESCE with hstore — non-null passes through
-- begin-expected
-- columns: val
-- row: 1
-- end-expected
SELECT COALESCE(data, 'default=>fallback'::hstore)->'x' AS val FROM hs_test.t_coalesce WHERE id = 2;

-- P3. CASE expression returning hstore
-- begin-expected
-- columns: val
-- row: yes
-- end-expected
SELECT (CASE WHEN id = 2 THEN 'found=>yes'::hstore ELSE 'found=>no'::hstore END)->'found' AS val FROM hs_test.t_coalesce WHERE id = 2;

-- ============================================================================
-- Q. hstore constructor edge cases
-- ============================================================================

-- Q1. hstore('key', NULL) — creates hstore with NULL value
-- begin-expected
-- columns: is_null
-- row: true
-- end-expected
SELECT (hstore('key', NULL))->'key' IS NULL AS is_null;

-- Q2. hstore('key', NULL) — key exists but is not defined
-- begin-expected
-- columns: key_exists|key_defined
-- row: true|false
-- end-expected
SELECT exist(hstore('key', NULL), 'key') AS key_exists, defined(hstore('key', NULL), 'key') AS key_defined;

-- ============================================================================
-- R. delete function with array argument
-- ============================================================================

-- R1. delete(hstore, text[]) — remove multiple keys
-- begin-expected
-- columns: has_a|has_b|has_c
-- row: false|false|true
-- end-expected
SELECT exist(delete('a=>1, b=>2, c=>3'::hstore, ARRAY['a','b']), 'a') AS has_a,
       exist(delete('a=>1, b=>2, c=>3'::hstore, ARRAY['a','b']), 'b') AS has_b,
       exist(delete('a=>1, b=>2, c=>3'::hstore, ARRAY['a','b']), 'c') AS has_c;

-- ============================================================================
-- S. slice with non-existent keys
-- ============================================================================

-- S1. slice with all missing keys — returns empty hstore; array_length of empty array is NULL
-- begin-expected
-- columns: key_count
-- row: NULL
-- end-expected
SELECT array_length(akeys(slice('a=>1, b=>2'::hstore, ARRAY['x','y'])), 1) AS key_count;

-- S2. slice with mix of existing and missing keys
-- begin-expected
-- columns: val_a|val_x_null
-- row: 1|true
-- end-expected
SELECT (slice('a=>1, b=>2'::hstore, ARRAY['a','x']))->'a' AS val_a,
       (slice('a=>1, b=>2'::hstore, ARRAY['a','x']))->'x' IS NULL AS val_x_null;

-- ============================================================================
-- T. pg_typeof on hstore
-- ============================================================================

CREATE TABLE hs_test.t_typeof (id int PRIMARY KEY, data hstore);
INSERT INTO hs_test.t_typeof VALUES (1, 'a=>1');

-- T1. pg_typeof returns 'hstore' for hstore column
-- begin-expected
-- columns: tp
-- row: hstore
-- end-expected
SELECT pg_typeof(data)::text AS tp FROM hs_test.t_typeof WHERE id = 1;

-- ============================================================================
-- U. hstore - hstore (delete matching pairs)
-- ============================================================================

-- U1. Delete matching key+value pair
-- begin-expected
-- columns: has_a|has_b
-- row: false|true
-- end-expected
SELECT exist('a=>1, b=>2'::hstore - 'a=>1'::hstore, 'a') AS has_a,
       exist('a=>1, b=>2'::hstore - 'a=>1'::hstore, 'b') AS has_b;

-- U2. Non-matching value — key is NOT removed
-- begin-expected
-- columns: has_a
-- row: true
-- end-expected
SELECT exist('a=>1, b=>2'::hstore - 'a=>999'::hstore, 'a') AS has_a;

-- ============================================================================
-- V. Empty string keys and values
-- ============================================================================

-- V1. Empty string as value — PG rejects 'a=>' as invalid hstore syntax
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error in hstore
-- end-expected-error
SELECT ('a=>'::hstore)->'a' AS val;

-- V2. Empty string as value using quoted syntax (valid PG way)
-- begin-expected
-- columns: val
-- row:
-- end-expected
SELECT ('a=>""'::hstore)->'a' AS val;

-- V3. Empty string as key
-- begin-expected
-- columns: val
-- row: 1
-- end-expected
SELECT ('""=>1'::hstore)->'' AS val;

-- ============================================================================
-- W. Concat merge order — right side wins for duplicate keys
-- ============================================================================

-- W1. Right side value wins on key conflict
-- begin-expected
-- columns: val
-- row: new
-- end-expected
SELECT ('a=>old'::hstore || 'a=>new'::hstore)->'a' AS val;

-- W2. Left-only and right-only keys both preserved
-- begin-expected
-- columns: val_a|val_b
-- row: 1|2
-- end-expected
SELECT ('a=>1'::hstore || 'b=>2'::hstore)->'a' AS val_a,
       ('a=>1'::hstore || 'b=>2'::hstore)->'b' AS val_b;

-- ============================================================================
-- X. Multi-key arrow extraction (hstore -> text[])
-- ============================================================================

-- X1. Extract multiple keys as text array
-- begin-expected
-- columns: vals
-- row: {1,3}
-- end-expected
SELECT ('a=>1, b=>2, c=>3'::hstore -> ARRAY['a','c'])::text AS vals;

-- X2. Missing key produces NULL element
-- begin-expected
-- columns: is_null
-- row: true
-- end-expected
SELECT ('a=>1'::hstore -> ARRAY['a','z'])[2] IS NULL AS is_null;

-- ============================================================================
-- Y. hstore_to_array and hstore_to_matrix
-- ============================================================================

-- Y1. hstore_to_array returns flat alternating key/value array
-- begin-expected
-- columns: len
-- row: 4
-- end-expected
SELECT array_length(hstore_to_array('a=>1, b=>2'::hstore), 1) AS len;

-- Y2. hstore_to_matrix returns 2D array
-- begin-expected
-- columns: len
-- row: 1
-- end-expected
SELECT array_length(hstore_to_matrix('a=>1'::hstore), 1) AS len;

-- ============================================================================
-- Z. delete(hstore, hstore) function
-- ============================================================================

-- Z1. delete matching key+value pair
-- begin-expected
-- columns: has_a|has_b
-- row: false|true
-- end-expected
SELECT exist(delete('a=>1, b=>2'::hstore, 'a=>1'::hstore), 'a') AS has_a,
       exist(delete('a=>1, b=>2'::hstore, 'a=>1'::hstore), 'b') AS has_b;

-- Z2. Non-matching value — key NOT removed
-- begin-expected
-- columns: has_a
-- row: true
-- end-expected
SELECT exist(delete('a=>1, b=>2'::hstore, 'a=>999'::hstore), 'a') AS has_a;

-- ============================================================================
-- AA. isexists / isdefined aliases
-- ============================================================================

-- AA1. isexists — alias for exist
-- begin-expected
-- columns: has_a|has_z
-- row: true|false
-- end-expected
SELECT isexists('a=>1, b=>2'::hstore, 'a') AS has_a, isexists('a=>1, b=>2'::hstore, 'z') AS has_z;

-- AA2. isdefined — alias for defined
-- begin-expected
-- columns: a_def|b_def
-- row: true|false
-- end-expected
SELECT isdefined('a=>1, b=>NULL'::hstore, 'a') AS a_def, isdefined('a=>1, b=>NULL'::hstore, 'b') AS b_def;

-- ============================================================================
-- AB. hstore_to_json_loose — type inference
-- ============================================================================

-- AB1. Numbers unquoted in loose mode — use single key to avoid ordering issues
-- begin-expected
-- columns: val
-- row: {"count": 42}
-- end-expected
SELECT hstore_to_json_loose('count=>42'::hstore)::text AS val;

-- AB2. Booleans stay quoted in PG's hstore_to_json_loose
-- begin-expected
-- columns: val
-- row: {"active": "true"}
-- end-expected
SELECT hstore_to_json_loose('active=>true'::hstore)::text AS val;

-- ============================================================================
-- AC. hstore_to_jsonb_loose — type inference (jsonb variant)
-- ============================================================================

-- AC1. Numbers unquoted in jsonb_loose mode — single key to avoid ordering
-- begin-expected
-- columns: val
-- row: {"count": 42}
-- end-expected
SELECT hstore_to_jsonb_loose('count=>42'::hstore)::text AS val;

-- AC2. Booleans stay quoted in hstore_to_jsonb_loose (same as json variant)
-- begin-expected
-- columns: val
-- row: {"active": "true"}
-- end-expected
SELECT hstore_to_jsonb_loose('active=>true'::hstore)::text AS val;

-- ============================================================================
-- AD. hstore(text[]) — single array constructor
-- ============================================================================

-- AD1. hstore from flat alternating key/value array
-- begin-expected
-- columns: val
-- row: 2
-- end-expected
SELECT (hstore(ARRAY['a','1','b','2']))->'b' AS val;

-- AD2. hstore from 2D key/value array
-- begin-expected
-- columns: val
-- row: 4
-- end-expected
SELECT (hstore(ARRAY[['c','3'],['d','4']]))->'d' AS val;

-- ============================================================================
-- AE. ?& operator (all keys exist) and ?| operator (any key exists)
-- ============================================================================

-- AE1. ?& — all keys exist — true case
-- begin-expected
-- columns: val
-- row: true
-- end-expected
SELECT 'a=>1, b=>2, c=>3'::hstore ?& ARRAY['a','b'] AS val;

-- AE2. ?& — all keys exist — false case (missing key)
-- begin-expected
-- columns: val
-- row: false
-- end-expected
SELECT 'a=>1, b=>2'::hstore ?& ARRAY['a','z'] AS val;

-- AE3. ?| — any key exists — true case
-- begin-expected
-- columns: val
-- row: true
-- end-expected
SELECT 'a=>1, b=>2'::hstore ?| ARRAY['z','b'] AS val;

-- AE4. ?| — any key exists — false case (no matching keys)
-- begin-expected
-- columns: val
-- row: false
-- end-expected
SELECT 'a=>1, b=>2'::hstore ?| ARRAY['x','y'] AS val;

-- ============================================================================
-- AF. Implicit cast hstore → json and hstore → jsonb
-- ============================================================================

-- AF1. hstore::json implicit cast
-- begin-expected
-- columns: val
-- row: 1
-- end-expected
SELECT ('a=>1'::hstore::json)->>'a' AS val;

-- AF2. hstore::jsonb implicit cast
-- begin-expected
-- columns: val
-- row: 1
-- end-expected
SELECT ('a=>1'::hstore::jsonb)->>'a' AS val;

-- ============================================================================
-- AG. Subscript access h['key']
-- ============================================================================

CREATE TABLE hs_test.t_subscript (id int PRIMARY KEY, data hstore);
INSERT INTO hs_test.t_subscript VALUES (1, 'x=>10, y=>20');

-- AG1. Subscript fetch — read value by key
-- begin-expected
-- columns: val
-- row: 10
-- end-expected
SELECT data['x'] AS val FROM hs_test.t_subscript WHERE id = 1;

-- AG2. Subscript fetch — missing key returns NULL
-- begin-expected
-- columns: is_null
-- row: true
-- end-expected
SELECT data['z'] IS NULL AS is_null FROM hs_test.t_subscript WHERE id = 1;

-- AG3. Subscript update — set existing key
UPDATE hs_test.t_subscript SET data['x'] = '99' WHERE id = 1;

-- begin-expected
-- columns: val
-- row: 99
-- end-expected
SELECT data['x'] AS val FROM hs_test.t_subscript WHERE id = 1;

-- AG4. Subscript update — add new key
UPDATE hs_test.t_subscript SET data['z'] = 'new' WHERE id = 1;

-- begin-expected
-- columns: val
-- row: new
-- end-expected
SELECT data['z'] AS val FROM hs_test.t_subscript WHERE id = 1;

-- ============================================================================
-- AH. Prefix operators %% and %#
-- ============================================================================

-- AH1. %% — convert hstore to alternating key/value array
-- begin-expected
-- columns: len
-- row: 2
-- end-expected
SELECT array_length(%% 'a=>1'::hstore, 1) AS len;

-- AH2. %# — convert hstore to 2D key/value matrix
-- begin-expected
-- columns: len
-- row: 1
-- end-expected
SELECT array_length(%# 'a=>1'::hstore, 1) AS len;

-- ============================================================================
-- AI. hstore - text with explicit ::text cast (key deletion)
-- ============================================================================

-- AI1. Delete single key using explicit ::text cast
-- begin-expected
-- columns: has_b|has_a
-- row: false|true
-- end-expected
SELECT exist('a=>1, b=>2'::hstore - 'b'::text, 'b') AS has_b,
       exist('a=>1, b=>2'::hstore - 'b'::text, 'a') AS has_a;

-- ============================================================================
-- AJ. populate_record(anyelement, hstore)
-- ============================================================================

CREATE TYPE hs_test.pop_type AS (a int, b text, c boolean);

-- AJ1. Basic populate_record with NULL base
-- begin-expected
-- columns: a|b|c
-- row: 42|hello|t
-- end-expected
SELECT (populate_record(NULL::hs_test.pop_type, 'a=>42, b=>hello, c=>true'::hstore)).*;

-- AJ2. Preserves base values for unmentioned fields
-- begin-expected
-- columns: a|b
-- row: 99|new
-- end-expected
SELECT (populate_record(ROW(99, 'old', true)::hs_test.pop_type, 'b=>new'::hstore)).a,
       (populate_record(ROW(99, 'old', true)::hs_test.pop_type, 'b=>new'::hstore)).b;

-- AJ3. Extra hstore keys are ignored
-- begin-expected
-- columns: a
-- row: 42
-- end-expected
SELECT (populate_record(NULL::hs_test.pop_type, 'a=>42, z=>extra'::hstore)).a;

-- AJ4. populate_record in FROM clause
-- begin-expected
-- columns: a|b
-- row: 10|world
-- end-expected
SELECT a, b FROM populate_record(NULL::hs_test.pop_type, 'a=>10, b=>world'::hstore);

-- ============================================================================
-- AK. hstore(record) — convert composite type to hstore
-- ============================================================================

-- AK1. Convert composite type to hstore and extract a key
-- begin-expected
-- columns: val
-- row: hello
-- end-expected
SELECT hstore(ROW(1, 'hello', true)::hs_test.pop_type)->'b' AS val;

-- AK2. NULL field in record becomes NULL in hstore
-- begin-expected
-- columns: val
-- row: NULL
-- end-expected
SELECT hstore(ROW(1, NULL, false)::hs_test.pop_type)->'b' AS val;

-- ============================================================================
-- AL. #= operator (populate_record shorthand)
-- ============================================================================

-- AL1. Basic #= usage
-- begin-expected
-- columns: a|b
-- row: 5|hi
-- end-expected
SELECT (NULL::hs_test.pop_type #= 'a=>5, b=>hi'::hstore).a,
       (NULL::hs_test.pop_type #= 'a=>5, b=>hi'::hstore).b;

-- AL2. #= preserves base record values
-- begin-expected
-- columns: a|b
-- row: 99|new
-- end-expected
SELECT (ROW(99, 'old', true)::hs_test.pop_type #= 'b=>new'::hstore).a,
       (ROW(99, 'old', true)::hs_test.pop_type #= 'b=>new'::hstore).b;

-- ============================================================================
-- AM. populate_record with array of hstores (no PG equivalent — expected error)
-- ============================================================================

-- AM1. There is no populate_recordset for hstore in PG — this should error
-- begin-expected-error
-- end-expected
SELECT * FROM populate_recordset(NULL::hs_test.pop_type, ARRAY['a=>1, b=>x'::hstore, 'a=>2, b=>y'::hstore]);

DROP TYPE hs_test.pop_type CASCADE;

-- ============================================================================
-- Cleanup
-- ============================================================================

SET search_path = public;
DROP SCHEMA IF EXISTS hs_test CASCADE;
