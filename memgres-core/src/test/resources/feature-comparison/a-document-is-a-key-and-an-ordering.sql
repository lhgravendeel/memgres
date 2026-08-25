-- ============================================================================
-- -- A jsonb key and a jsonb ordering are both the document, not its text.
-- --
-- -- jsonb is held as the text it prints as, and every question about two of them was answered by
-- -- comparing those texts. So a primary key over jsonb admitted {"a":1} beside {"a":1.0} and
-- -- {"b":2,"a":1} beside {"a":1,"b":2}, and ORDER BY put them in alphabetical order rather than
-- -- in jsonb's own, which weighs a value's kind first and a container's size before its contents.
-- -- A scalar document is held as an array of that one scalar, which is why the empty array sorts
-- -- below every scalar and an array of one sorts above them. json is the other way round: it has
-- -- no equality at all, so PostgreSQL refuses to key anything by one.
--
-- ============================================================================

-- setup
DROP TABLE IF EXISTS jkey_pk CASCADE;
CREATE TABLE jkey_pk (j jsonb PRIMARY KEY);
-- A key is the document, not the text that spells it
INSERT INTO jkey_pk VALUES ('{"a":1}');
-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "jkey_pk_pkey"
-- end-expected-error
INSERT INTO jkey_pk VALUES ('{"a":1.0}');
INSERT INTO jkey_pk VALUES ('{"b":2,"a":1}');
-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "jkey_pk_pkey"
-- end-expected-error
INSERT INTO jkey_pk VALUES ('{"a":1,"b":2}');
-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT count(*)::text AS a FROM jkey_pk;
-- begin-expected
-- columns: a
-- row: {"a": 1} {"a": 1, "b": 2}
-- end-expected
SELECT string_agg(j::text, ' ' ORDER BY j) AS a FROM jkey_pk;
DROP TABLE IF EXISTS jkey_u CASCADE;
CREATE TABLE jkey_u (id int, j jsonb UNIQUE);
INSERT INTO jkey_u VALUES (1, '[1,2]');
-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "jkey_u_j_key"
-- end-expected-error
INSERT INTO jkey_u VALUES (2, '[1.0,2.00]');
INSERT INTO jkey_u VALUES (3, '[1,2,3]');
-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT count(*)::text AS a FROM jkey_u;
DROP TABLE IF EXISTS jkey_i CASCADE;
CREATE TABLE jkey_i (id int, j jsonb);
CREATE UNIQUE INDEX jkey_i_j ON jkey_i (j);
INSERT INTO jkey_i VALUES (1, '{"x": 1e3}');
-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "jkey_i_j"
-- end-expected-error
INSERT INTO jkey_i VALUES (2, '{"x": 1000}');
INSERT INTO jkey_i VALUES (3, '{"x": 1e3}') ON CONFLICT (j) DO NOTHING;
-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT count(*)::text AS a FROM jkey_i;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('{"a":1,"b":2}'::jsonb = '{"b":2,"a":1}'::jsonb)::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('[1,2]'::jsonb = '[1.0,2.00]'::jsonb)::text AS a;
-- begin-expected
-- columns: a
-- row: false
-- end-expected
SELECT ('[1,2]'::jsonb = '[2,1]'::jsonb)::text AS a;
-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT count(DISTINCT v)::text AS a FROM (VALUES ('1'::jsonb),('1.0'::jsonb)) t(v);
-- json has no equality at all, so nothing can be keyed by one
DROP TABLE IF EXISTS jkey_json CASCADE;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: data type json has no default operator class for access method "btree"
-- end-expected-error
CREATE TABLE jkey_json (j json PRIMARY KEY);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: data type json has no default operator class for access method "btree"
-- end-expected-error
CREATE TABLE jkey_json (j json UNIQUE);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: data type json has no default operator class for access method "btree"
-- end-expected-error
CREATE TABLE jkey_json (id int, j json, PRIMARY KEY (j));
-- begin-expected-error
-- sqlstate: 42704
-- message-like: data type json has no default operator class for access method "btree"
-- end-expected-error
CREATE TABLE jkey_json (id int, j json, UNIQUE (j));
DROP TABLE IF EXISTS jkey_jok CASCADE;
CREATE TABLE jkey_jok (id int, j json);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: data type json has no default operator class for access method "btree"
-- end-expected-error
ALTER TABLE jkey_jok ADD PRIMARY KEY (j);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: data type json has no default operator class for access method "btree"
-- end-expected-error
ALTER TABLE jkey_jok ADD UNIQUE (j);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: data type json has no default operator class for access method "btree"
-- end-expected-error
CREATE INDEX jkey_jok_j ON jkey_jok (j);
-- a json column that is not a key is untouched
DROP TABLE IF EXISTS jkey_plain CASCADE;
CREATE TABLE jkey_plain (id int PRIMARY KEY, j json);
INSERT INTO jkey_plain VALUES (1, '{"b":2,"a":1}');
-- begin-expected
-- columns: a
-- row: {"b":2,"a":1}
-- end-expected
SELECT j::text AS a FROM jkey_plain;
-- The order of two documents is the order of two values
-- begin-expected
-- columns: a
-- row: [] null "s" 1 true {}
-- end-expected
SELECT string_agg(v::text, ' ' ORDER BY v) AS a FROM (VALUES ('{}'::jsonb),('[]'::jsonb),('1'::jsonb),('null'::jsonb),('true'::jsonb),('"s"'::jsonb)) t(v);
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('null'::jsonb < '"a"'::jsonb)::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('"a"'::jsonb < '1'::jsonb)::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('1'::jsonb < 'false'::jsonb)::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('false'::jsonb < 'true'::jsonb)::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('true'::jsonb < '[1]'::jsonb)::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('[1,2]'::jsonb < '{}'::jsonb)::text AS a;
-- a container is ordered by how many members it holds before either is looked into
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('[3]'::jsonb < '[1,2]'::jsonb)::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('{"z":1}'::jsonb < '{"a":1,"b":2}'::jsonb)::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('{"a":1}'::jsonb < '{"a":2}'::jsonb)::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('{"a":2}'::jsonb < '{"b":1}'::jsonb)::text AS a;
-- a document that is nothing but a scalar is held as an array of that one scalar,
-- so the empty array sorts below every scalar and an array of one sorts above them
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('[]'::jsonb < 'null'::jsonb)::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('[]'::jsonb < '"a"'::jsonb)::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('[]'::jsonb < 'false'::jsonb)::text AS a;
-- begin-expected
-- columns: a
-- row: false
-- end-expected
SELECT ('"a"'::jsonb < '[]'::jsonb)::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('"a"'::jsonb < '[1]'::jsonb)::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('[]'::jsonb < '[1]'::jsonb)::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('[]'::jsonb < '{}'::jsonb)::text AS a;
-- inside a document there is no such wrapper, and a scalar is ordered by its kind
-- begin-expected
-- columns: a
-- row: [null] ["a"] [1] [true] [[]] [[1]] [[1, 2]] [{}]
-- end-expected
SELECT string_agg(v::text, ' ' ORDER BY v) AS a FROM (VALUES ('["a"]'::jsonb),('[[1,2]]'),('[[1]]'),('[[]]'),('[null]'),('[1]'),('[true]'),('[{}]')) t(v);
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('["a"]'::jsonb < '[[]]'::jsonb)::text AS a;
-- begin-expected
-- columns: a
-- row: false
-- end-expected
SELECT ('{"a":[]}'::jsonb < '{"a":null}'::jsonb)::text AS a;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function max(jsonb) does not exist
-- end-expected-error
SELECT max(v)::text AS a FROM (VALUES ('{}'::jsonb),('[]'::jsonb),('1'::jsonb)) t(v);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function min(jsonb) does not exist
-- end-expected-error
SELECT min(v)::text AS a FROM (VALUES ('{}'::jsonb),('[]'::jsonb),('1'::jsonb)) t(v);
-- A row of a jsonb column holds the document, however that row was written
-- begin-expected
-- columns: a
-- row: [1, 2] {"a": 2, "b": 1}
-- end-expected
SELECT string_agg(v::text, ' ') AS a FROM (VALUES ('[1,2]'::jsonb),('{"b":1,"a":2}')) t(v);
-- begin-expected
-- columns: a
-- row: [1, 2] {"a": 2, "b": 1}
-- end-expected
SELECT string_agg(v::text, ' ') AS a FROM (SELECT '[1,2]'::jsonb AS v UNION ALL SELECT '{"b":1,"a":2}') t;
-- begin-expected
-- columns: a
-- row: 1.0 1
-- end-expected
SELECT string_agg(v::text, ' ' ORDER BY v) AS a FROM (VALUES ('1.0'::jsonb),('1')) t(v);
-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT count(DISTINCT v)::text AS a FROM (VALUES ('1.0'::jsonb),('1')) t(v);
DROP TABLE IF EXISTS jkey_plain CASCADE;
DROP TABLE IF EXISTS jkey_jok CASCADE;
DROP TABLE IF EXISTS jkey_i CASCADE;
DROP TABLE IF EXISTS jkey_u CASCADE;
DROP TABLE IF EXISTS jkey_pk CASCADE;
