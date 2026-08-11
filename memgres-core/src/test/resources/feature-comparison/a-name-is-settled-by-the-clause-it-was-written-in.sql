-- A qualified name reaches one entry of the FROM clause and no other, and which columns that entry
-- has is settled by the text: a view's are the ones it was defined with, a sub-select's and a WITH
-- item's are the ones their own target list writes, and a call's are the ones its signature gives
-- it. A subquery is a query of its own with the columns of the query around it in scope, and the
-- order the faults come out in is the order PostgreSQL settles the clauses.
-- Every expectation was measured on PostgreSQL 18.

CREATE TABLE nsr_t (i int, j text);
INSERT INTO nsr_t VALUES (1, 'a');
CREATE TABLE nsr_u (i int, k text);
INSERT INTO nsr_u VALUES (1, 'b');
CREATE VIEW nsr_v AS SELECT i, j FROM nsr_t;
CREATE VIEW nsr_v2 (a, b) AS SELECT i, j FROM nsr_t;
CREATE MATERIALIZED VIEW nsr_m AS SELECT i, j FROM nsr_t;
CREATE TYPE nsr_c AS (a int, b text);
CREATE TABLE nsr_ca (cs nsr_c[]);
INSERT INTO nsr_ca VALUES (ARRAY[(1,'x')::nsr_c]);

-- ============================================================================
-- A qualified name against a relation the query lists
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column t.nosuchcol does not exist
-- end-expected-error
SELECT t.nosuchcol FROM nsr_t t;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column nsr_t.nosuchcol does not exist
-- end-expected-error
SELECT nsr_t.nosuchcol FROM nsr_t;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column a.nosuchcol does not exist
-- end-expected-error
SELECT a.nosuchcol FROM nsr_t a JOIN nsr_t b ON a.i = b.i;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column t.nosuchcol does not exist
-- end-expected-error
SELECT i FROM nsr_t t WHERE t.nosuchcol = 1;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column t.nosuchcol does not exist
-- end-expected-error
SELECT i FROM nsr_t t ORDER BY t.nosuchcol;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column t.nosuchcol does not exist
-- end-expected-error
SELECT count(*) FROM nsr_t t GROUP BY t.nosuchcol;

-- A qualifier no entry answers to, and the relation's own name after a clause renamed it.
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "x"
-- end-expected-error
SELECT x.i FROM nsr_t t;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "nsr_t"
-- end-expected-error
SELECT nsr_t.i FROM nsr_t t;

-- A qualifier that resolves is left alone.
-- begin-expected
-- columns: i
-- row: 1
-- end-expected
SELECT t.i FROM nsr_t t;

-- begin-expected
-- columns: i | j
-- row: 1 | a
-- end-expected
SELECT t.* FROM nsr_t t;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(t.ctid) FROM nsr_t t;

-- The relation's own name reaches the entry that was not renamed, and only that one.
-- begin-expected
-- columns: i
-- row: 1
-- end-expected
SELECT nsr_t.i FROM nsr_t, nsr_t x;

-- ============================================================================
-- A relation that is not a table answers for its columns the same way
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column v.nosuchcol does not exist
-- end-expected-error
SELECT v.nosuchcol FROM nsr_v v;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column nsr_v.nosuchcol does not exist
-- end-expected-error
SELECT nsr_v.nosuchcol FROM nsr_v;

-- A view's columns are the ones it was defined with, whatever the query underneath called them.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column v.i does not exist
-- end-expected-error
SELECT v.i FROM nsr_v2 v;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT v.a FROM nsr_v2 v;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column m.nosuchcol does not exist
-- end-expected-error
SELECT m.nosuchcol FROM nsr_m m;

-- A materialized view is written out like a table, so it answers for the system columns.
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(m.ctid) FROM nsr_m m;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column s.nosuchcol does not exist
-- end-expected-error
SELECT s.nosuchcol FROM (SELECT i FROM nsr_t) s;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column s.j does not exist
-- end-expected-error
SELECT s.j FROM (SELECT i FROM nsr_t) s;

-- An alias list renames a sub-select's columns, so the old names are gone.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column s.i does not exist
-- end-expected-error
SELECT s.i FROM (SELECT i, j FROM nsr_t) s(y, z);

-- begin-expected
-- columns: z
-- row: a
-- end-expected
SELECT s.z FROM (SELECT i, j FROM nsr_t) s(y, z);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column w.nosuchcol does not exist
-- end-expected-error
WITH w AS (SELECT i FROM nsr_t) SELECT w.nosuchcol FROM w;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column w.i does not exist
-- end-expected-error
WITH w(x) AS (SELECT i FROM nsr_t) SELECT w.i FROM w;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column q.nosuchcol does not exist
-- end-expected-error
WITH w AS (SELECT i FROM nsr_t) SELECT q.nosuchcol FROM w q;

-- A call in FROM is one column wide, named after the alias the clause gave it — except where the
-- function declares a name for what it returns.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column g.nosuchcol does not exist
-- end-expected-error
SELECT g.nosuchcol FROM generate_series(1,3) g;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column g.generate_series does not exist
-- end-expected-error
SELECT g.generate_series FROM generate_series(1,3) g;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column g.g does not exist
-- end-expected-error
SELECT g.g FROM generate_series(1,3) g(x);

-- begin-expected
-- columns: g
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT g.g FROM generate_series(1,3) g ORDER BY 1;

-- begin-expected
-- columns: ordinality
-- row: 1
-- row: 2
-- end-expected
SELECT g.ordinality FROM generate_series(1,2) WITH ORDINALITY g ORDER BY 1;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column s.nosuchcol does not exist
-- end-expected-error
SELECT s.nosuchcol FROM string_to_table('a,b', ',') s;

-- json_each and its kin hand back a pair per row under the names their own signature gives them.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column e.nosuchcol does not exist
-- end-expected-error
SELECT e.nosuchcol FROM json_each('{"a":1}'::json) e;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column e.e does not exist
-- end-expected-error
SELECT e.e FROM json_each('{"a":1}'::json) e;

-- begin-expected
-- columns: key
-- row: a
-- end-expected
SELECT e.key FROM json_each('{"a":1}'::json) e;

-- begin-expected
-- columns: k
-- row: a
-- end-expected
SELECT e.k FROM json_each('{"a":1}'::json) e(k, v);

-- begin-expected
-- columns: value
-- row: 1
-- end-expected
SELECT e.value FROM jsonb_each('{"a":1}'::jsonb) e;

-- An array of literals is unnested into one column, named after the alias the clause gave it.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column u.unnest does not exist
-- end-expected-error
SELECT u.unnest FROM unnest(ARRAY[1,2]) u;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column u.nosuchcol does not exist
-- end-expected-error
SELECT u.nosuchcol FROM unnest('{1,2}'::int[]) u;

-- begin-expected
-- columns: u
-- row: 1
-- row: 2
-- end-expected
SELECT u.u FROM unnest(ARRAY[1,2]) u ORDER BY 1;

-- An array of a composite type is unnested into one column per field.
-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT u.a FROM nsr_ca, unnest(cs) AS u;

-- begin-expected
-- columns: k
-- row: a
-- end-expected
SELECT k.k FROM json_object_keys('{"a":1}'::json) k;

-- begin-expected
-- columns: p
-- row: 1
-- end-expected
SELECT p.p FROM jsonb_path_query('{"a":1}'::jsonb, '$.a') p;

-- ============================================================================
-- A schema written in front of a qualifier picks the entry that lives in it
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column nsr_t.nosuchcol does not exist
-- end-expected-error
SELECT public.nsr_t.nosuchcol FROM nsr_t;

-- begin-expected
-- columns: i
-- row: 1
-- end-expected
SELECT public.nsr_t.i FROM nsr_t;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "nsr_t"
-- end-expected-error
SELECT nosuchschema.nsr_t.i FROM nsr_t;

-- A WITH item and a sub-select live in no schema at all, so no schema reaches them.
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "w"
-- end-expected-error
WITH w AS (SELECT i FROM nsr_t) SELECT public.w.i FROM w;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "s"
-- end-expected-error
SELECT public.s.i FROM (SELECT i FROM nsr_t) s;

-- A relation the query does not read at all is missing rather than out of reach.
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "nsr_u"
-- end-expected-error
SELECT public.nsr_u.i FROM nsr_t;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "nosuchrel"
-- end-expected-error
SELECT nosuchrel.i FROM (nsr_t JOIN nsr_u USING (i)) AS j;

-- begin-expected
-- columns: i
-- row: 1
-- end-expected
SELECT j.i FROM (nsr_t JOIN nsr_u USING (i)) AS j;

-- ============================================================================
-- A column inside a subquery, and the query around it
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT (SELECT nosuchcol FROM nsr_t);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT (SELECT nosuchcol FROM nsr_t) FROM nsr_t o;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT 1 WHERE EXISTS (SELECT nosuchcol FROM nsr_t);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT i FROM nsr_t WHERE i IN (SELECT nosuchcol FROM nsr_t);

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "nsr_nosuchrel" does not exist
-- end-expected-error
SELECT (SELECT i FROM nsr_nosuchrel);

-- The columns of the query a subquery stands in are in scope inside it.
-- begin-expected
-- columns: max
-- row: 1
-- end-expected
SELECT (SELECT max(x.i) FROM nsr_t x WHERE x.i = o.i) AS max FROM nsr_t o;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT (SELECT count(*) FROM nsr_t x WHERE x.i = o.i) AS count FROM nsr_t o;

-- A column the query around it has not got is still a column that does not exist, named the way it
-- was written; a qualifier nothing answers to is a missing FROM-clause entry.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column o.nosuchcol does not exist
-- end-expected-error
SELECT (SELECT o.nosuchcol FROM nsr_t x LIMIT 1) FROM nsr_t o;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "q"
-- end-expected-error
SELECT (SELECT q.i FROM nsr_t x LIMIT 1) FROM nsr_t o;

-- ============================================================================
-- The order the clauses are settled in
-- ============================================================================

-- count(*) is the one call PostgreSQL settles without consulting anything, so a fault written
-- after it is still the fault it reports.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT count(*) FROM nsr_t GROUP BY nosuchcol;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT count(*), nosuchcol FROM nsr_t;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT count(*) FROM nsr_t HAVING nosuchcol > 1;

-- WHERE is settled before the grouping items.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchw" does not exist
-- end-expected-error
SELECT count(*) FROM nsr_t WHERE nosuchw = 1 GROUP BY nosuchg;

-- ... and so is the sort clause.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosucho" does not exist
-- end-expected-error
SELECT count(*) FROM nsr_t GROUP BY nosuchg ORDER BY nosucho;

-- A FILTER belongs to the call it is written on, and is settled there.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchfilt" does not exist
-- end-expected-error
SELECT count(*) FILTER (WHERE nosuchfilt), nosuchcol FROM nsr_t;

-- A call that does not resolve is the fault reported, not a column written after it.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function lower(integer, integer) does not exist
-- end-expected-error
SELECT lower(1, 2), nosuchcol FROM nsr_t;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function abs(text) does not exist
-- end-expected-error
SELECT abs(j), nosuchcol FROM nsr_t;

-- A column that is there but not grouped is a fault of its own.
-- begin-expected-error
-- sqlstate: 42803
-- message-like: must appear in the GROUP BY clause
-- end-expected-error
SELECT count(*), i FROM nsr_t;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM nsr_t GROUP BY i;

-- An output name the select list defines is a name the grouping items may use.
-- begin-expected
-- columns: k
-- row: 1
-- end-expected
SELECT i AS k FROM nsr_t GROUP BY k;

-- ============================================================================
-- A derived relation is named the way its own target list is
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column s.nosuchcol does not exist
-- end-expected-error
SELECT s.nosuchcol FROM (SELECT i + 1 FROM nsr_t) s;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column s.nosuchcol does not exist
-- end-expected-error
SELECT s.nosuchcol FROM (SELECT lower(j) FROM nsr_t) s;

-- A cast's column is named after the type it ends at.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column s.text does not exist
-- end-expected-error
SELECT s.text FROM (SELECT i::text FROM nsr_t) s;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column s.nosuchcol does not exist
-- end-expected-error
SELECT s.nosuchcol FROM (SELECT count(*) FROM nsr_t) s;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column w.nosuchcol does not exist
-- end-expected-error
WITH w AS (SELECT i + 1 FROM nsr_t) SELECT w.nosuchcol FROM w;

-- What nothing names at all is ?column?.
-- begin-expected
-- columns: ?column?
-- row: 2
-- end-expected
SELECT s."?column?" FROM (SELECT i + 1 FROM nsr_t) s;

-- begin-expected
-- columns: lower
-- row: a
-- end-expected
SELECT s.lower FROM (SELECT lower(j) FROM nsr_t) s;

-- begin-expected
-- columns: case
-- row: 1
-- end-expected
SELECT s.case FROM (SELECT CASE WHEN i > 0 THEN 1 END FROM nsr_t) s;

-- begin-expected
-- columns: lower
-- row: a
-- end-expected
WITH w AS (SELECT lower(j) FROM nsr_t) SELECT w.lower FROM w;

-- ============================================================================
-- An alias list renames a relation's columns one for one
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: table "z" has 2 columns available but 3 columns specified
-- end-expected-error
SELECT * FROM nsr_t AS z (a, b, c);

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: table "s" has 1 columns available but 2 columns specified
-- end-expected-error
SELECT * FROM (SELECT i FROM nsr_t) s (a, b);

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: table "x" has 1 columns available but 2 columns specified
-- end-expected-error
WITH w AS (SELECT i FROM nsr_t) SELECT * FROM w x (a, b);

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: table "g" has 2 columns available but 3 columns specified
-- end-expected-error
SELECT * FROM generate_series(1,2) WITH ORDINALITY g (a, b, c);

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: table "e" has 2 columns available but 3 columns specified
-- end-expected-error
SELECT * FROM json_each('{"a":1}'::json) e (a, b, c);

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT z.a FROM nsr_t AS z (a, b);

-- ============================================================================
-- What EXPLAIN is given is read where it stands, and oid is not a system column
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column s.nosuchcol does not exist
-- end-expected-error
EXPLAIN (COSTS OFF) SELECT s.nosuchcol FROM (SELECT i + 1 FROM nsr_t) s;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
EXPLAIN SELECT nosuchcol FROM nsr_t;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" of relation "nsr_t" does not exist
-- end-expected-error
EXPLAIN INSERT INTO nsr_t (nosuchcol) VALUES (1);

-- Which prepared statement an EXECUTE names is settled when it runs.
-- begin-expected-error
-- sqlstate: 26000
-- message-like: prepared statement "nsr_nosuchprep" does not exist
-- end-expected-error
EXECUTE nsr_nosuchprep(1);

-- A tuple has a ctid and a tableoid; it has had no oid since PostgreSQL stopped giving one to
-- ordinary rows. A relation that answers to the name declares it like any other column.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "oid" does not exist
-- end-expected-error
SELECT oid FROM nsr_t;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column nsr_t.oid does not exist
-- end-expected-error
SELECT nsr_t.oid FROM nsr_t;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "oid" of relation "nsr_t" does not exist
-- end-expected-error
INSERT INTO nsr_t (oid) VALUES (1);

-- begin-expected
-- columns: ctid
-- row: (0,1)
-- end-expected
SELECT ctid FROM nsr_t;

-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT count(oid) > 0 AS ok FROM pg_class;

-- SHOW names a setting the server has. A name with a schema in front of it is a custom parameter,
-- and there is no such parameter until something sets one.
-- begin-expected-error
-- sqlstate: 42704
-- message-like: unrecognized configuration parameter "nsr_nosuchguc"
-- end-expected-error
SHOW nsr_nosuchguc;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: unrecognized configuration parameter "nsr.nosuchcustom"
-- end-expected-error
SHOW nsr.nosuchcustom;

SET nsr.own = '10';

-- begin-expected
-- columns: nsr.own
-- row: 10
-- end-expected
SHOW nsr.own;

-- ============================================================================
-- What SEARCH and CYCLE add to a WITH item is in scope
-- ============================================================================

-- begin-expected
-- columns: n | ord
-- row: 1 | {(1)}
-- row: 2 | {(1),(2)}
-- row: 3 | {(1),(2),(3)}
-- end-expected
WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM r WHERE n < 3)
  SEARCH DEPTH FIRST BY n SET ord SELECT n, ord FROM r ORDER BY n;

-- begin-expected
-- columns: n | c
-- row: 1 | f
-- row: 2 | f
-- row: 3 | f
-- end-expected
WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM r WHERE n < 3)
  CYCLE n SET c USING p SELECT n, c FROM r ORDER BY n;

-- A WITH item that writes rows answers for its RETURNING list, and nothing it refuses runs.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column w.nosuch does not exist
-- end-expected-error
WITH w AS (INSERT INTO nsr_u VALUES (9,'z') RETURNING i) SELECT w.nosuch FROM w;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM nsr_u;

-- cleanup
DROP MATERIALIZED VIEW nsr_m;
DROP VIEW nsr_v2;
DROP VIEW nsr_v;
DROP TABLE nsr_ca;
DROP TYPE nsr_c;
DROP TABLE nsr_u;
DROP TABLE nsr_t;
