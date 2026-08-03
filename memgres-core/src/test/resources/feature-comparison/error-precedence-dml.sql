-- ============================================================================
-- Feature Comparison: which error a statement with several faults reports,
-- outside the SELECT path
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- PostgreSQL builds the range table before it reads any clause, and for a
-- statement that writes it puts the written relation into that table first.
-- So a data-modifying statement that both names a relation that is not there
-- and misuses a clause reports the relation, and one whose column list names a
-- column the target does not have reports the column — before either the VALUES
-- or the clauses standing in them are looked at.
--
-- Its sibling error-precedence.sql pins the same idea for SELECT. What is
-- pinned here is the four statement kinds that resolve their target inside
-- their own executor rather than through the FROM resolver: INSERT, UPDATE,
-- DELETE and MERGE.
--
-- The second half is about resolving a call. A function is resolved by its
-- name and its argument list together, so a call with more arguments than any
-- signature of that name resolves to nothing (42883) rather than to the
-- function with the extra ones dropped; the same holds for an aggregate. And a
-- qualifier is resolved to a schema before anything is looked for inside it, so
-- a qualifier naming no schema is 3F000 rather than a missing function.
--
-- Every table here has a primary key: the corpus leaves a FOR ALL TABLES
-- publication behind, and PostgreSQL refuses UPDATE and DELETE on a table with
-- no replica identity under one.
-- ============================================================================

DROP TABLE IF EXISTS bp_t CASCADE;
DROP TABLE IF EXISTS bp_u CASCADE;
CREATE TABLE bp_t (v int PRIMARY KEY, s text, b boolean);
CREATE TABLE bp_u (v int PRIMARY KEY, s text);
INSERT INTO bp_t VALUES (1,'a',true),(2,'b',false);
INSERT INTO bp_u VALUES (1,'a'),(2,'b');

-- ============================================================================
-- 1. The relation a statement writes is resolved before its clauses are judged
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "bp_nosuch" does not exist
-- end-expected-error
INSERT INTO bp_nosuch VALUES (9, abs(1) FILTER (WHERE true));

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "bp_nosuch" does not exist
-- end-expected-error
INSERT INTO bp_nosuch VALUES (9, abs(DISTINCT 1));

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "bp_nosuch" does not exist
-- end-expected-error
INSERT INTO bp_nosuch VALUES (9) RETURNING abs(1) FILTER (WHERE true);

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "bp_nosuch" does not exist
-- end-expected-error
INSERT INTO bp_nosuch VALUES (9) ON CONFLICT (v) DO UPDATE SET a = abs(1) FILTER (WHERE true);

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "bp_nosuch" does not exist
-- end-expected-error
UPDATE bp_nosuch SET a = abs(1) FILTER (WHERE true);

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "bp_nosuch" does not exist
-- end-expected-error
UPDATE bp_nosuch SET a = 1 WHERE abs(1) FILTER (WHERE true) = 1;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "bp_nosuch" does not exist
-- end-expected-error
DELETE FROM bp_nosuch WHERE abs(1) FILTER (WHERE true) = 1;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "bp_nosuch" does not exist
-- end-expected-error
DELETE FROM bp_nosuch RETURNING abs(1) FILTER (WHERE true);

-- the written relation goes into the range table before the ones read, so it
-- is the one reported when the statement names two that are missing
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "bp_nosuch" does not exist
-- end-expected-error
MERGE INTO bp_nosuch t USING bp_u s ON t.v = s.v
WHEN MATCHED THEN UPDATE SET a = abs(1) FILTER (WHERE true);

-- and a relation only read is resolved just the same
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "bp_nosuch" does not exist
-- end-expected-error
MERGE INTO bp_t t USING bp_nosuch s ON t.v = s.v
WHEN MATCHED THEN UPDATE SET s = (abs(1) FILTER (WHERE true))::text;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "bp_nosuch" does not exist
-- end-expected-error
INSERT INTO bp_t SELECT abs(1) FILTER (WHERE true), 'z', true FROM bp_nosuch;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "bp_nosuch" does not exist
-- end-expected-error
UPDATE bp_t SET v = 1 FROM bp_nosuch WHERE abs(1) FILTER (WHERE true) = 1;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "bp_nosuch" does not exist
-- end-expected-error
DELETE FROM bp_t USING bp_nosuch WHERE abs(1) FILTER (WHERE true) = 1;

-- ============================================================================
-- 2. The column list is validated against the target before the values are read
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" of relation "bp_t" does not exist
-- end-expected-error
INSERT INTO bp_t (nosuchcol) VALUES (abs(1) FILTER (WHERE true));

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" of relation "bp_t" does not exist
-- end-expected-error
INSERT INTO bp_t (nosuchcol) VALUES (abs(DISTINCT 1));

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" of relation "bp_t" does not exist
-- end-expected-error
INSERT INTO bp_t (v, nosuchcol) VALUES (9, 1);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" of relation "bp_t" does not exist
-- end-expected-error
INSERT INTO bp_t (nosuchcol) SELECT abs(1) FILTER (WHERE true) FROM bp_t;

-- and the WHERE of an UPDATE or a DELETE is resolved against the target
-- before the assignments or anything standing in them
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
UPDATE bp_t SET v = 1 WHERE nosuchcol = abs(1) FILTER (WHERE true);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
DELETE FROM bp_t WHERE nosuchcol = abs(1) FILTER (WHERE true);

-- ============================================================================
-- 3. A statement refused this way writes nothing
-- ============================================================================
DROP TABLE IF EXISTS bp_sink CASCADE;
CREATE TABLE bp_sink (id int PRIMARY KEY);

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "bp_nosuch" does not exist
-- end-expected-error
WITH ins AS (INSERT INTO bp_sink VALUES (1) RETURNING id)
INSERT INTO bp_nosuch SELECT id FROM ins;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM bp_sink;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" of relation "bp_sink" does not exist
-- end-expected-error
WITH ins AS (INSERT INTO bp_sink VALUES (2) RETURNING id)
INSERT INTO bp_sink (nosuchcol) SELECT id FROM ins;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM bp_sink;

DROP TABLE IF EXISTS bp_sink CASCADE;

-- ============================================================================
-- 4. A call is resolved by name and argument list together
-- ============================================================================
-- The two engines word the hint identically; the message names the argument
-- types, which memgres reads from the values rather than from a catalog, so
-- only the ones it types the same way are pinned by message.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function abs(integer, integer) does not exist
-- hint-like: No function matches the given name and argument types.
-- end-expected-error
SELECT abs(1, 2);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function upper(unknown, unknown) does not exist
-- end-expected-error
SELECT upper('a', 'b');

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function now(integer) does not exist
-- end-expected-error
SELECT now(1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function btrim(unknown, unknown, unknown) does not exist
-- end-expected-error
SELECT btrim('a', 'b', 'c');

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function substring(unknown, integer, integer, integer) does not exist
-- end-expected-error
SELECT substring('abc', 1, 2, 3);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function md5(unknown, unknown) does not exist
-- end-expected-error
SELECT md5('a', 'b');

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function chr(integer, integer) does not exist
-- end-expected-error
SELECT chr(65, 66);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function round(integer, integer, integer) does not exist
-- end-expected-error
SELECT round(1, 2, 3);

-- an aggregate is resolved the same way
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function sum(integer, integer) does not exist
-- end-expected-error
SELECT sum(v, v) FROM bp_t;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function count(integer, integer) does not exist
-- end-expected-error
SELECT count(v, v) FROM bp_t;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function max(integer, integer) does not exist
-- end-expected-error
SELECT max(v, v) FROM bp_t;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function string_agg(text, unknown, text) does not exist
-- end-expected-error
SELECT string_agg(s, ',', s) FROM bp_t;

-- with no argument at all it is the same fault
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function sum() does not exist
-- end-expected-error
SELECT sum() FROM bp_t;

-- ============================================================================
-- 5. A qualifier is resolved to a schema before anything is looked for in it
-- ============================================================================
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "bp_nosuchschema" does not exist
-- end-expected-error
SELECT bp_nosuchschema.f(1);

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "bp_nosuchschema" does not exist
-- end-expected-error
SELECT bp_nosuchschema.abs(1);

-- even where a clause would be complained about too
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "bp_nosuchschema" does not exist
-- end-expected-error
SELECT bp_nosuchschema.f(1) FILTER (WHERE true);

-- ============================================================================
-- 6. Within one query level, the leftmost fault of the earliest clause
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch2" does not exist
-- end-expected-error
SELECT abs(nosuch2), abs(1) FILTER (WHERE true) FROM bp_t;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch2" does not exist
-- end-expected-error
SELECT abs(nosuch2) FROM bp_t WHERE abs(1) FILTER (WHERE true) = 1;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
SELECT nosuchcol FROM bp_t WHERE abs(1) FILTER (WHERE true) = 1;

-- a join condition belongs to the FROM clause, which is built first of all
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column bp_t.nosuch2 does not exist
-- end-expected-error
SELECT abs(1) FILTER (WHERE true) FROM bp_t JOIN bp_u ON bp_t.nosuch2 = bp_u.v;

-- the call is resolved before the clause it carries is judged: by arity
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function abs(integer, integer) does not exist
-- end-expected-error
SELECT abs(v, v) FILTER (WHERE true) FROM bp_t;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function abs(integer, integer) does not exist
-- end-expected-error
SELECT abs(v, v) OVER () FROM bp_t;

-- and by the types of its arguments, a cast being as good a statement of one
-- as a column is
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function abs(text) does not exist
-- end-expected-error
SELECT abs('x'::text) FILTER (WHERE true) FROM bp_t;

-- ============================================================================
-- 7. The ordinary shapes none of this may touch
-- ============================================================================
-- begin-expected
-- columns: n
-- row: 30
-- end-expected
INSERT INTO bp_t (v, s) VALUES (30, 'x') RETURNING v::text AS n;

-- begin-expected
-- columns: n
-- row: 30
-- end-expected
UPDATE bp_t SET s = 'y' WHERE v = 30 RETURNING v::text AS n;

-- begin-expected
-- columns: n
-- row: 30
-- end-expected
DELETE FROM bp_t WHERE v = 30 RETURNING v::text AS n;

-- begin-expected
-- columns: n
-- row: 31
-- end-expected
INSERT INTO bp_t (v, s) SELECT 31, 'z' RETURNING v::text AS n;

-- begin-expected
-- columns: n
-- row: 31
-- end-expected
DELETE FROM bp_t WHERE v = 31 RETURNING v::text AS n;

-- begin-expected
-- columns: n
-- row: 40
-- end-expected
WITH c AS (SELECT 40 AS k) INSERT INTO bp_t (v) SELECT k FROM c RETURNING v::text AS n;

-- begin-expected
-- columns: n
-- row: 40
-- end-expected
DELETE FROM bp_t WHERE v = 40 RETURNING v::text AS n;

-- begin-expected
-- columns: n
-- row: 50
-- end-expected
INSERT INTO bp_t (v) VALUES (50) ON CONFLICT (v) DO NOTHING RETURNING v::text AS n;

-- begin-expected
-- columns: n
-- row: 50
-- end-expected
DELETE FROM bp_t WHERE v = 50 RETURNING v::text AS n;

-- begin-expected
-- columns: n
-- row: 51
-- end-expected
INSERT INTO bp_t (v, s) VALUES (51, 'a')
ON CONFLICT (v) DO UPDATE SET s = excluded.s RETURNING v::text AS n;

-- begin-expected
-- columns: n
-- row: 51
-- end-expected
DELETE FROM bp_t WHERE v = 51 RETURNING v::text AS n;

MERGE INTO bp_t t USING bp_u s ON t.v = s.v WHEN MATCHED THEN UPDATE SET s = s.s;

UPDATE bp_t SET s = 'q' FROM bp_u u WHERE bp_t.v = u.v;

DELETE FROM bp_t USING bp_u u WHERE bp_t.v = u.v AND false;

-- the arities every one of these functions really has still resolve
-- begin-expected
-- columns: a, b, c, d, e
-- row: 1, 2, 1.55, ab, abc
-- end-expected
SELECT abs(-1)::text AS a, round(1.5)::text AS b, round(1.55, 2)::text AS c,
       substring('abcdef', 1, 2) AS d, substring('abcdef' FROM 1 FOR 3) AS e;

-- begin-expected
-- columns: a, b, c, d
-- row: __a, aax, a,b, A
-- end-expected
SELECT lpad('a', 3, '_') AS a, btrim('xaxx', 'x') || 'ax' AS b,
       concat('a', ',', 'b') AS c, upper('a') AS d;

-- a variadic function takes as many as it is given
-- begin-expected
-- columns: a, b, c
-- row: abcd, 3, 1
-- end-expected
SELECT concat('a','b','c','d') AS a, greatest(1,2,3)::text AS b,
       coalesce(NULL, NULL, 1)::text AS c;

-- both rows carry the s the UPDATE ... FROM above wrote
-- begin-expected
-- columns: a, b, c
-- row: 3, 2, q,q
-- end-expected
SELECT sum(v)::text AS a, count(*)::text AS b, string_agg(s, ',' ORDER BY v) AS c FROM bp_t;

-- a qualifier that does name a schema resolves through it
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT pg_catalog.abs(-1)::text AS n;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::text AS n FROM pg_class WHERE relname = 'bp_t';

DROP TABLE IF EXISTS bp_t CASCADE;
DROP TABLE IF EXISTS bp_u CASCADE;
