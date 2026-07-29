-- ============================================================================
-- Feature Comparison: set-returning functions and join columns, round two
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Six subjects, all reached through a FROM item that produces rows.
--
-- WITH ORDINALITY is one column, added once. It numbers the rows a FROM item
-- produced, whatever produced them, so it is the same bigint counting from 1 in
-- every case. Each resolver used to add it for itself, which meant the ones
-- nobody had asked about it -- string_to_table, regexp_split_to_table,
-- json_object_keys, generate_subscripts, regexp_matches, a function returning
-- TABLE -- dropped it, and the alias list that named it then had one name too
-- many for the columns that were left. It is added in one place now, it is a
-- bigint even where no row shows it, and it is refused beside a column
-- definition list, which describes the same alias list a second way.
--
-- A set expands where rows are still being produced and nowhere else. The
-- clauses that read rows already produced took one: a DELETE's WHERE, the WHERE
-- of an ON CONFLICT DO UPDATE, a FILTER, a VALUES list, every clause of a MERGE,
-- and a FROM function's own arguments. Each refusal names the clause PostgreSQL
-- names -- except a MERGE's INSERT, which writes one row for the source row that
-- reached it, where PostgreSQL names the value rather than the clause. The
-- one-row VALUES of an INSERT is the shape that does expand, and it still does.
--
-- A declared set-returning function is one too. RETURNS SETOF and RETURNS TABLE
-- answer a set for the same reason generate_series does. Recognising only the
-- built-in names left SELECT setof_fn() answering one row holding the whole set
-- rendered as an array, and described it as text.
--
-- DISTINCT reads what the expansion produced. Both DISTINCT and DISTINCT ON ran
-- before the sets were expanded, so a duplicate the expansion created survived
-- and a DISTINCT ON key that was a set grouped rows that did not exist yet.
--
-- A join's merged column is neither side's. PostgreSQL resolves one type both
-- sides can be read as -- for the numeric types the wider of the two -- and the
-- merged column is that type, which is also the type the comparison is made in.
-- Reading the left side's described int JOIN bigint USING (k) as int4 and would
-- not match 1 against 1.0. A pair of types with no = between them is refused
-- with the hint PostgreSQL attaches, and so is a type with no equality operator
-- of its own.
--
-- A name that is written but out of reach is not a missing one. The relations
-- under (a JOIN b) AS j, and a sibling FROM item read from a subquery that is
-- not LATERAL, are both there and unreachable; PostgreSQL says so, and says
-- which word would bring the second into reach. A near-miss column name is
-- suggested qualified by the relation that has it, one per relation.
--
-- The neighbouring shapes that must keep working are here too: a plain alias
-- list, a set in a nested query, a one-row INSERT VALUES, a LATERAL function
-- reading the column beside it, a bare alias list on a record-returning
-- function, a same-type USING join, and an array containment test in a join
-- condition.
-- ============================================================================

DROP TABLE IF EXISTS srn_t CASCADE;
-- srn_t is deleted from and updated below, and an earlier file in this suite leaves a
-- publication FOR ALL TABLES behind, under which a table with no replica identity may not be
-- updated or deleted from (55000). A replica identity of its own keeps those statements
-- measuring what they are written to measure.
CREATE TABLE srn_t (a int, b text);
ALTER TABLE srn_t REPLICA IDENTITY FULL;
INSERT INTO srn_t VALUES (1,'a'), (2,'b');

DROP TABLE IF EXISTS srn_tgt CASCADE;
CREATE TABLE srn_tgt (id int primary key, name text, dept_id int);
INSERT INTO srn_tgt VALUES (1,'x',9);

DROP TABLE IF EXISTS srn_i CASCADE;
CREATE TABLE srn_i (k int, iv text);
INSERT INTO srn_i VALUES (1,'i'), (2,'i2');

DROP TABLE IF EXISTS srn_l CASCADE;
CREATE TABLE srn_l (k bigint, lv text);
INSERT INTO srn_l VALUES (1,'l'), (3,'l3');

DROP TABLE IF EXISTS srn_n CASCADE;
CREATE TABLE srn_n (k numeric, nv text);
INSERT INTO srn_n VALUES (1,'n');

DROP TABLE IF EXISTS srn_r CASCADE;
CREATE TABLE srn_r (k real, rv text);
INSERT INTO srn_r VALUES (1,'r');

DROP TABLE IF EXISTS srn_tx CASCADE;
CREATE TABLE srn_tx (k text, xv text);

DROP TABLE IF EXISTS srn_d CASCADE;
CREATE TABLE srn_d (k date, dv text);

DROP TABLE IF EXISTS srn_j1 CASCADE;
CREATE TABLE srn_j1 (k int, js json, ar int[]);
INSERT INTO srn_j1 VALUES (1,'{"a":1}',ARRAY[1,2]);

DROP TABLE IF EXISTS srn_j2 CASCADE;
CREATE TABLE srn_j2 (k int, js json, ar int[]);
INSERT INTO srn_j2 VALUES (1,'{"a":1}',ARRAY[1,2]);

DROP TABLE IF EXISTS srn_a CASCADE;
CREATE TABLE srn_a (id int primary key, x int, t text);

DROP TABLE IF EXISTS srn_b CASCADE;
CREATE TABLE srn_b (id int primary key, y int, t text);

DROP FUNCTION IF EXISTS srn_setofint();
CREATE FUNCTION srn_setofint() RETURNS SETOF int AS $$
  SELECT 1 UNION ALL SELECT 2
$$ LANGUAGE sql;

DROP FUNCTION IF EXISTS srn_tbl();
CREATE FUNCTION srn_tbl() RETURNS TABLE(x int, y text) AS $$
  SELECT 1, 'p' UNION ALL SELECT 2, 'q'
$$ LANGUAGE sql;

-- ============================================================================
-- 1. WITH ORDINALITY is one column, added once
-- ============================================================================

-- Every set-returning item is numbered, not just the ones that had been asked
SELECT * FROM string_to_table('x,y', ',') WITH ORDINALITY AS t(v, n);
SELECT * FROM regexp_split_to_table('x,y', ',') WITH ORDINALITY AS t(v, n);
SELECT * FROM json_object_keys('{"a":1,"b":2}'::json) WITH ORDINALITY AS t(v, n);
SELECT * FROM generate_subscripts(ARRAY[5,6], 1) WITH ORDINALITY AS t(v, n);
SELECT * FROM unnest(ARRAY[7,8]) WITH ORDINALITY AS t(v, n);

-- Unnamed, the column keeps PostgreSQL's name
SELECT * FROM string_to_table('x,y', ',') WITH ORDINALITY;
SELECT * FROM generate_subscripts(ARRAY[5,6], 1) WITH ORDINALITY ORDER BY 1;

-- A function returning TABLE is numbered after its own columns
SELECT * FROM srn_tbl() WITH ORDINALITY ORDER BY 1;
SELECT * FROM srn_tbl() WITH ORDINALITY AS t(a, b, n) ORDER BY 3;

-- And so is a declared SETOF function
SELECT * FROM srn_setofint() WITH ORDINALITY AS t(v, n) ORDER BY 1;

-- Several functions side by side are numbered once for the whole item
SELECT * FROM ROWS FROM (generate_series(1,2), string_to_table('x,y',',')) WITH ORDINALITY AS t(a,b,n) ORDER BY 3;
SELECT * FROM unnest(ARRAY[1,2], ARRAY['a','b']) WITH ORDINALITY AS t(a,b,n) ORDER BY 3;

-- The column is a bigint even where the item produced no row to read it off
SELECT * FROM generate_series(1,0) WITH ORDINALITY AS t(v, n);
SELECT * FROM string_to_table('', ',') WITH ORDINALITY AS t(v, n);
SELECT * FROM srn_t LEFT JOIN LATERAL generate_series(1,0) WITH ORDINALITY AS t(v,n) ON true ORDER BY 1;

-- WITH ORDINALITY and a column definition list describe the same list twice
-- begin-expected-error
-- sqlstate: 42601
-- message-like: WITH ORDINALITY cannot be used with a column definition list
-- end-expected-error
SELECT * FROM json_to_recordset('[{"a":1}]'::json) WITH ORDINALITY AS t(a int, n bigint);

-- Ordinary: a plain alias list, with and without the clause
SELECT * FROM string_to_table('x,y', ',') AS t(v);
SELECT * FROM generate_series(1,2) AS t(v);
SELECT n FROM generate_series(5,6) WITH ORDINALITY AS t(v, n) ORDER BY 1;

-- An alias list longer than the item's columns is still refused
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: table "t" has 1 columns available but 2 columns specified
-- end-expected-error
SELECT * FROM generate_series(1,2) AS t(v, n);

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: table "t" has 1 columns available but 2 columns specified
-- end-expected-error
SELECT * FROM string_to_table('x,y', ',') AS t(v, n);

-- ============================================================================
-- 2. Where a set may not stand
-- ============================================================================

-- A DELETE's WHERE picks rows one at a time
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in WHERE
-- end-expected-error
DELETE FROM srn_t WHERE generate_series(1,2) > 5;

-- The WHERE of an ON CONFLICT DO UPDATE is an UPDATE's WHERE
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in WHERE
-- end-expected-error
INSERT INTO srn_tgt(id, name) VALUES (1,'a') ON CONFLICT (id) DO UPDATE SET name = 'b' WHERE generate_series(1,2) > 1;

-- FILTER decides per input row whether the aggregate accumulates it
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in FILTER
-- end-expected-error
SELECT count(*) FILTER (WHERE generate_series(1,2) > 1) FROM srn_t;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in FILTER
-- end-expected-error
SELECT sum(a) FILTER (WHERE generate_series(1,2) > 0) FROM srn_t;

-- A VALUES list is a constant table
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in VALUES
-- end-expected-error
VALUES (generate_series(1,3));

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in VALUES
-- end-expected-error
SELECT * FROM (VALUES (generate_series(1,2))) v(x);

-- Every clause of a MERGE
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in JOIN conditions
-- end-expected-error
MERGE INTO srn_tgt t USING (SELECT 1 AS id) s ON t.id = s.id AND generate_series(1,2) > 0
  WHEN MATCHED THEN UPDATE SET name = 'q';

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in MERGE WHEN conditions
-- end-expected-error
MERGE INTO srn_tgt t USING (SELECT 1 AS id) s ON t.id = s.id
  WHEN MATCHED AND generate_series(1,2) > 0 THEN UPDATE SET name = 'q';

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in UPDATE
-- end-expected-error
MERGE INTO srn_tgt t USING (SELECT 1 AS id) s ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET name = generate_series(1,2)::text;

-- A MERGE's INSERT writes one row for the source row that reached it, and
-- PostgreSQL names what the value is rather than which clause holds it
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-valued function called in context that cannot accept a set
-- end-expected-error
MERGE INTO srn_tgt t USING (SELECT 5 AS id) s ON t.id = s.id
  WHEN NOT MATCHED THEN INSERT (id, dept_id) VALUES (s.id, generate_series(1,2));

-- A FROM item's own call produces the rows; one nested in its arguments cannot
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions must appear at top level of FROM
-- end-expected-error
SELECT * FROM generate_series(1, generate_series(1,2));

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions must appear at top level of FROM
-- end-expected-error
SELECT * FROM unnest(ARRAY[generate_series(1,2)]);

-- Ordinary: the one-row VALUES of an INSERT does expand
INSERT INTO srn_tgt(id, name) VALUES (generate_series(50,51), 'q');
SELECT id FROM srn_tgt WHERE id >= 50 ORDER BY 1;

-- Ordinary: a set inside a nested query belongs to that query
DELETE FROM srn_t WHERE a IN (SELECT generate_series(9,9));
UPDATE srn_t SET b = b WHERE a IN (SELECT generate_series(1,1));
SELECT count(*) FILTER (WHERE a IN (SELECT generate_series(1,2))) FROM srn_t;
SELECT a FROM srn_t WHERE a IN (SELECT unnest(ARRAY[1,2])) ORDER BY 1;

-- Ordinary: a plain VALUES, a plain FILTER, a plain MERGE INSERT
SELECT * FROM (VALUES (1),(2)) v(x) ORDER BY 1;
SELECT count(*) FILTER (WHERE a > 1) FROM srn_t;
INSERT INTO srn_tgt(id, name) VALUES (1,'a') ON CONFLICT (id) DO UPDATE SET name = 'b' WHERE srn_tgt.id > 0;
MERGE INTO srn_tgt t USING (SELECT 7 AS id) s ON t.id = s.id
  WHEN NOT MATCHED THEN INSERT (id, dept_id) VALUES (s.id, 7);

-- Ordinary: a LATERAL function reading the column beside it, and a sub-query in
-- a FROM function's arguments
SELECT * FROM srn_t, LATERAL generate_series(1, a) g ORDER BY 1, 3;
SELECT * FROM generate_series(1, (SELECT 2)) ORDER BY 1;
SELECT * FROM unnest(ARRAY(SELECT generate_series(1,2))) ORDER BY 1;
SELECT * FROM ROWS FROM (generate_series(1,2), generate_series(1,2)) ORDER BY 1;

-- ============================================================================
-- 3. A declared set-returning function is one too
-- ============================================================================

SELECT srn_setofint() ORDER BY 1;
SELECT srn_setofint() AS a, generate_series(1,3) AS g ORDER BY g;
SELECT a, srn_setofint() FROM srn_t ORDER BY 1, 2;
SELECT srn_setofint() + 10 AS v ORDER BY 1;
SELECT pg_typeof(srn_setofint())::text;

-- Ordinary: the same function as a FROM item
SELECT * FROM srn_setofint() ORDER BY 1;
SELECT * FROM srn_setofint() s, LATERAL generate_series(1, s) g ORDER BY 1, 2;

-- ============================================================================
-- 4. DISTINCT reads what the expansion produced
-- ============================================================================

SELECT DISTINCT unnest(ARRAY[1,1,2]) ORDER BY 1;
SELECT DISTINCT unnest(ARRAY[1,1,2]) AS u ORDER BY u;
-- Written without an ORDER BY, which row of each group survives is unspecified,
-- so only how many groups there are is compared.
SELECT count(*) FROM (SELECT DISTINCT ON (generate_series(1,2)) a FROM srn_t) q;

-- Ordinary: DISTINCT and DISTINCT ON with no set in sight
SELECT DISTINCT a FROM srn_t ORDER BY 1;
SELECT DISTINCT ON (a) a, b FROM srn_t ORDER BY a;
SELECT DISTINCT generate_series(1,2) ORDER BY 1;

-- ============================================================================
-- 5. What a FROM item says its column is
-- ============================================================================

-- generate_series is declared over int4 and int8; a bigint bound picks int8
SELECT * FROM generate_series(1::bigint, 2::bigint);
SELECT pg_typeof(g)::text FROM generate_series(1::bigint, 2::bigint) g LIMIT 1;
SELECT pg_typeof(g)::text FROM generate_series(1, 2::bigint) g LIMIT 1;
SELECT pg_typeof(g)::text FROM generate_series(1, 2) g LIMIT 1;
SELECT pg_typeof(g)::text FROM generate_series(1, 4, 2) g LIMIT 1;
SELECT pg_typeof(g)::text FROM generate_series(1::numeric, 2) g LIMIT 1;

-- json_array_elements answers json; the jsonb spelling answers jsonb
SELECT * FROM json_array_elements('[1,2]'::json) WITH ORDINALITY AS t(v, n);
SELECT * FROM jsonb_array_elements('[1,2]'::jsonb) WITH ORDINALITY AS t(v, n);

-- ============================================================================
-- 6. Who may be told what its columns are
-- ============================================================================

-- A built-in whose signature already names its columns
-- begin-expected-error
-- sqlstate: 42601
-- message-like: a column definition list is redundant for a function with OUT parameters
-- end-expected-error
SELECT * FROM ROWS FROM (json_each('{"a":1}'::json) AS (k text, v json));

-- begin-expected-error
-- sqlstate: 42601
-- message-like: a column definition list is redundant for a function with OUT parameters
-- end-expected-error
SELECT * FROM json_each('{"a":1}'::json) AS t(k text, v json);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: a column definition list is redundant for a function with OUT parameters
-- end-expected-error
SELECT * FROM ROWS FROM (jsonb_each('{"a":1}'::jsonb) AS (k text, v jsonb));

-- A built-in returning bare record has to be told
-- begin-expected-error
-- sqlstate: 42601
-- message-like: a column definition list is required for functions returning "record"
-- end-expected-error
SELECT * FROM ROWS FROM (json_to_recordset('[{"a":1}]'::json));

-- begin-expected-error
-- sqlstate: 42601
-- message-like: a column definition list is required for functions returning "record"
-- end-expected-error
SELECT * FROM ROWS FROM (json_to_record('{"a":1}'::json));

-- Ordinary: a bare alias list only renames what is there
SELECT * FROM json_each('{"a":1}'::json) AS t(k, v);
SELECT * FROM json_to_record('{"a":1}'::json) AS t(a int, b text);
SELECT * FROM json_to_recordset('[{"a":1}]'::json) AS t(a int) ORDER BY 1;
SELECT * FROM jsonb_each('{"a":1}'::jsonb) ORDER BY 1;
SELECT * FROM ROWS FROM (generate_series(1,2), json_each('{"a":1}'::json)) ORDER BY 1;

-- ============================================================================
-- 7. A merged join column is neither side's
-- ============================================================================

SELECT pg_typeof(k)::text FROM srn_i JOIN srn_l USING (k);
SELECT pg_typeof(k)::text FROM srn_i JOIN srn_n USING (k);
SELECT pg_typeof(k)::text FROM srn_i JOIN srn_r USING (k);
SELECT k FROM srn_i JOIN srn_l USING (k) ORDER BY 1;
SELECT pg_typeof(k)::text FROM srn_i LEFT JOIN srn_l USING (k) ORDER BY 1 LIMIT 1;
SELECT k FROM srn_i FULL JOIN srn_l USING (k) ORDER BY 1;
SELECT k FROM (srn_i JOIN srn_l USING (k)) AS j ORDER BY 1;
SELECT j.k FROM (srn_i JOIN srn_l USING (k)) AS j ORDER BY 1;

-- The comparison is made in that type too, so 1 matches 1.0
SELECT count(*) FROM srn_i JOIN srn_r USING (k);
SELECT count(*) FROM srn_i JOIN srn_n USING (k);

-- Ordinary: two relations of the same type
SELECT * FROM srn_i JOIN srn_i i2 USING (k) ORDER BY 1;
SELECT k FROM srn_i NATURAL JOIN srn_i i2 ORDER BY 1;
SELECT * FROM srn_i a JOIN srn_i b USING (k, iv) ORDER BY 1;
SELECT srn_i.k FROM srn_i JOIN srn_l USING (k) ORDER BY 1;

-- ============================================================================
-- 8. A key PostgreSQL has no equality for
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer = text
-- end-expected-error
SELECT * FROM srn_i JOIN srn_tx USING (k);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: text = integer
-- end-expected-error
SELECT * FROM srn_tx JOIN srn_i USING (k);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer = date
-- end-expected-error
SELECT * FROM srn_i JOIN srn_d USING (k);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: date = integer
-- end-expected-error
SELECT * FROM srn_d JOIN srn_i USING (k);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: text = date
-- end-expected-error
SELECT * FROM srn_tx JOIN srn_d USING (k);

-- A type with no equality operator cannot be equated with itself either
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: json = json
-- end-expected-error
SELECT * FROM srn_j1 NATURAL JOIN srn_j2;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: json = json
-- end-expected-error
SELECT * FROM srn_j1 a JOIN srn_j2 b USING (js);

-- Ordinary: a key both sides share, and an array containment test in a join
-- condition, which is an array test rather than a geometric one
SELECT * FROM srn_j1 a JOIN srn_j2 b USING (k) ORDER BY 1;
SELECT * FROM srn_j1 a JOIN srn_j2 b USING (k, ar) ORDER BY 1;
SELECT count(*) FROM srn_j1 a JOIN srn_j2 b ON a.ar @> b.ar;
SELECT '{1,2}'::int[] @> '{1}'::int[];
SELECT '{1,2,3}'::int[] @> '{1}'::int[];
SELECT pg_typeof('{1,2,3}'::line)::text;

-- ============================================================================
-- 9. A name that is written but out of reach
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "srn_i"
-- end-expected-error
SELECT srn_i.k FROM (srn_i JOIN srn_l USING (k)) AS j;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "srn_l"
-- end-expected-error
SELECT srn_l.k FROM (srn_i JOIN srn_l USING (k)) AS j;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "srn_i"
-- end-expected-error
SELECT srn_i.k FROM (srn_i JOIN srn_l ON srn_i.k = srn_l.k) AS j;

-- A sibling FROM item that is not LATERAL
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "s"
-- end-expected-error
SELECT count(*) FROM (SELECT 1 AS a) s, (SELECT s.a) t;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "x"
-- end-expected-error
SELECT count(*) FROM srn_i x, (SELECT x.k) y;

-- A name nothing in the query has is missing, and PostgreSQL says so
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "nosuch"
-- end-expected-error
SELECT nosuch.k FROM srn_i;

-- Ordinary: the alias the join answers to, and a LATERAL sibling
SELECT j.k FROM (srn_i JOIN srn_l USING (k)) AS j ORDER BY 1;
SELECT j.iv FROM (srn_i JOIN srn_l USING (k)) AS j ORDER BY 1;
SELECT count(*) FROM (SELECT 1 AS a) s, LATERAL (SELECT s.a) t;
SELECT count(*) FROM (SELECT 1 AS a) s, (SELECT 2 AS b) t WHERE s.a < t.b;

-- ============================================================================
-- 10. A suggested column names the relation that has it
-- ============================================================================

-- note: the suggestion is a HINT beside the message, not part of the message itself
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "tt" does not exist
-- hint-like: Perhaps you meant to reference the column "a.t".
-- end-expected-error
SELECT tt FROM srn_a a;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column a.tt does not exist
-- hint-like: Perhaps you meant to reference the column "a.t".
-- end-expected-error
SELECT a.tt FROM srn_a a;

-- note: with no alias the relation is named by its own name
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "tt" does not exist
-- hint-like: Perhaps you meant to reference the column "srn_a.t".
-- end-expected-error
SELECT tt FROM srn_a;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column srn_a.tt does not exist
-- hint-like: Perhaps you meant to reference the column "srn_a.t".
-- end-expected-error
SELECT srn_a.tt FROM srn_a;

-- A qualified reference is answered for that relation only
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column b.yy does not exist
-- hint-like: Perhaps you meant to reference the column "b.y".
-- end-expected-error
SELECT b.yy FROM srn_a a JOIN srn_b b ON a.x = b.y;

-- Every relation with a near miss is offered
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "tt" does not exist
-- hint-like: Perhaps you meant to reference the column "a.t" or the column "b.t".
-- end-expected-error
SELECT tt FROM srn_a a JOIN srn_b b ON a.id = b.id;

-- Ordinary: a name every relation has, and one nothing is near enough to suggest
SELECT a.x FROM srn_a a JOIN srn_b b ON a.x = b.y;
SELECT b.y FROM srn_a a JOIN srn_b b ON a.x = b.y;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column a.zz does not exist
-- end-expected-error
SELECT a.zz FROM srn_a a;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP FUNCTION IF EXISTS srn_tbl();
DROP FUNCTION IF EXISTS srn_setofint();
DROP TABLE IF EXISTS srn_b CASCADE;
DROP TABLE IF EXISTS srn_a CASCADE;
DROP TABLE IF EXISTS srn_j2 CASCADE;
DROP TABLE IF EXISTS srn_j1 CASCADE;
DROP TABLE IF EXISTS srn_d CASCADE;
DROP TABLE IF EXISTS srn_tx CASCADE;
DROP TABLE IF EXISTS srn_r CASCADE;
DROP TABLE IF EXISTS srn_n CASCADE;
DROP TABLE IF EXISTS srn_l CASCADE;
DROP TABLE IF EXISTS srn_i CASCADE;
DROP TABLE IF EXISTS srn_tgt CASCADE;
DROP TABLE IF EXISTS srn_t CASCADE;
