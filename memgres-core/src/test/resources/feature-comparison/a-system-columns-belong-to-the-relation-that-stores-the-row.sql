-- ============================================================================
-- A system column is a column of the relation that stores the row
--
-- A relation that stores its rows carries six columns nobody declared: where the tuple sits,
-- which relation holds it, and which transaction and command wrote and deleted it. A relation
-- that composes its rows on the way past -- a sub-SELECT, a CTE, a VALUES list, an ordinary view,
-- a function scan -- carries none of them, because its rows are not anywhere.
--
-- They are resolved like any other column and are subject to the same rules: an unqualified one
-- is searched across the FROM items the query listed and is ambiguous when two of them offer it;
-- a join is one FROM item exposing its sides' ordinary columns, so an unqualified system column
-- written over a join names nothing the join has; an unmatched outer-join side is a relation with
-- no row, so its system columns are null with the rest of it; and a grouping masks one out
-- exactly as it masks out a declared column.
--
-- They are also nobody's to redefine. No ALTER TABLE drops, renames or alters one, no index is
-- built over one, and no generated column is computed from one.
--
-- Every value below was measured against PostgreSQL 18.
-- ============================================================================

-- setup
CREATE TABLE sysc_a (id int, v text);
CREATE TABLE sysc_b (id int, w text);
INSERT INTO sysc_a VALUES (1, 'one'), (2, 'two'), (3, 'three');
INSERT INTO sysc_b VALUES (1, 'uno'), (2, 'dos');
CREATE VIEW sysc_v AS SELECT id, v FROM sysc_a;
CREATE MATERIALIZED VIEW sysc_mv AS SELECT id, v FROM sysc_a;

-- ============================================================================
-- A relation that stores its rows has them
-- ============================================================================
-- begin-expected
-- columns: ctid
-- row: (0,1)
-- row: (0,2)
-- row: (0,3)
-- end-expected
SELECT ctid::text FROM sysc_a ORDER BY id;
-- begin-expected
-- columns: id | tableoid
-- row: 1 | sysc_a
-- row: 2 | sysc_a
-- row: 3 | sysc_a
-- end-expected
SELECT id, tableoid::regclass::text FROM sysc_a ORDER BY id;
-- begin-expected
-- columns: id | ?column? | ?column?
-- row: 1 | t | t
-- row: 2 | t | t
-- row: 3 | t | t
-- end-expected
SELECT id, xmin IS NOT NULL, xmax IS NOT NULL FROM sysc_a ORDER BY id;
-- begin-expected
-- columns: id | ?column? | ?column?
-- row: 1 | t | t
-- row: 2 | t | t
-- row: 3 | t | t
-- end-expected
SELECT id, cmin IS NOT NULL, cmax IS NOT NULL FROM sysc_a ORDER BY id;
-- begin-expected
-- columns: ctid
-- row: (0,1)
-- row: (0,2)
-- row: (0,3)
-- end-expected
SELECT sysc_a.ctid::text FROM sysc_a ORDER BY id;
-- begin-expected
-- columns: ctid
-- row: (0,1)
-- row: (0,2)
-- row: (0,3)
-- end-expected
SELECT a.ctid::text FROM sysc_a a ORDER BY a.id;

-- A materialized view keeps its rows where an ordinary view composes them each time.
-- begin-expected
-- columns: ctid
-- row: (0,1)
-- row: (0,2)
-- row: (0,3)
-- end-expected
SELECT ctid::text FROM sysc_mv ORDER BY id;

-- ============================================================================
-- A relation that composes its rows has none
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
SELECT ctid FROM sysc_v;
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
SELECT ctid FROM (SELECT id FROM sysc_a) s;
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
WITH c AS (SELECT id FROM sysc_a) SELECT ctid FROM c;
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
SELECT ctid FROM (VALUES (1), (2)) t(x);
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
SELECT tableoid FROM generate_series(1, 3) g;
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
SELECT xmin FROM (SELECT id FROM sysc_a) s;
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
SELECT s.ctid FROM (SELECT id FROM sysc_a) s;

-- ============================================================================
-- Unqualified, across the FROM items the query listed
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42702
-- end-expected-error
SELECT ctid FROM sysc_a, sysc_b;
-- begin-expected-error
-- sqlstate: 42702
-- end-expected-error
SELECT xmin FROM sysc_a, sysc_b;
-- begin-expected-error
-- sqlstate: 42702
-- end-expected-error
SELECT tableoid FROM sysc_a, sysc_b;
-- begin-expected
-- columns: ctid
-- row: (0,1)
-- row: (0,1)
-- row: (0,2)
-- row: (0,2)
-- row: (0,3)
-- row: (0,3)
-- end-expected
SELECT ctid::text FROM sysc_a, (SELECT id FROM sysc_b) s ORDER BY 1;
-- begin-expected
-- columns: ctid
-- row: (0,1)
-- row: (0,2)
-- row: (0,3)
-- end-expected
SELECT ctid::text FROM sysc_a, (VALUES (1)) t(x) ORDER BY 1;

-- ============================================================================
-- A join is one FROM item, and exposes its sides' ordinary columns
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
SELECT ctid FROM sysc_a a JOIN sysc_b b ON a.id = b.id;
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
SELECT ctid FROM sysc_a a LEFT JOIN sysc_b b ON a.id = b.id;
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
SELECT ctid FROM sysc_a CROSS JOIN sysc_b;
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
SELECT tableoid FROM sysc_a a JOIN sysc_b b ON a.id = b.id;
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
SELECT ctid FROM sysc_a a JOIN (SELECT id FROM sysc_b) b ON a.id = b.id;
-- begin-expected
-- columns: ctid
-- row: (0,1)
-- row: (0,2)
-- end-expected
SELECT a.ctid::text FROM sysc_a a JOIN sysc_b b ON a.id = b.id ORDER BY a.id;
-- begin-expected
-- columns: ctid | ctid
-- row: (0,1) | (0,1)
-- row: (0,2) | (0,2)
-- end-expected
SELECT a.ctid::text, b.ctid::text FROM sysc_a a JOIN sysc_b b ON a.id = b.id ORDER BY a.id;

-- ============================================================================
-- An unmatched outer-join side is a relation with no row
-- ============================================================================
-- begin-expected
-- columns: id | ?column?
-- row: 1 | f
-- row: 2 | f
-- row: 3 | t
-- end-expected
SELECT a.id, b.ctid IS NULL FROM sysc_a a LEFT JOIN sysc_b b ON a.id = b.id ORDER BY a.id;
-- begin-expected
-- columns: id | ?column?
-- row: 1 | f
-- row: 2 | f
-- row: 3 | t
-- end-expected
SELECT a.id, b.tableoid IS NULL FROM sysc_a a LEFT JOIN sysc_b b ON a.id = b.id ORDER BY a.id;
-- begin-expected
-- columns: id | ?column? | ?column?
-- row: 1 | f | f
-- row: 2 | f | f
-- row: 3 | t | t
-- end-expected
SELECT a.id, b.xmin IS NULL, b.cmin IS NULL FROM sysc_a a LEFT JOIN sysc_b b ON a.id = b.id ORDER BY a.id;
-- begin-expected
-- columns: id
-- row: 3
-- end-expected
SELECT a.id FROM sysc_a a LEFT JOIN sysc_b b ON a.id = b.id WHERE b.ctid IS NULL ORDER BY a.id;
-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM sysc_a a LEFT JOIN sysc_b b ON a.id = b.id WHERE b.ctid IS NOT NULL;

-- ============================================================================
-- A grouping masks one out as it masks out a declared column
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
SELECT ctid FROM sysc_a GROUP BY id;
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
SELECT id FROM sysc_a GROUP BY id ORDER BY ctid;
-- begin-expected-error
-- sqlstate: 42803
-- end-expected-error
SELECT count(*) FROM sysc_a GROUP BY id HAVING ctid IS NOT NULL;
-- begin-expected
-- columns: ctid
-- row: (0,1)
-- row: (0,2)
-- row: (0,3)
-- end-expected
SELECT ctid::text FROM sysc_a GROUP BY ctid ORDER BY 1;
-- begin-expected
-- columns: count
-- row: 1
-- row: 1
-- row: 1
-- end-expected
SELECT count(*) FROM sysc_a GROUP BY ctid ORDER BY 1;
-- begin-expected
-- columns: max
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT max(id) FROM sysc_a GROUP BY ctid ORDER BY 1;

-- ============================================================================
-- Where one may be written, and where it may not
-- ============================================================================
-- begin-expected
-- columns: ctid
-- row: (0,2)
-- end-expected
SELECT ctid::text FROM sysc_a WHERE id = 2;
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- row: 3
-- end-expected
SELECT id FROM sysc_a WHERE ctid IS NOT NULL ORDER BY id;
-- begin-expected
-- columns: ctid
-- row: (0,1)
-- row: (0,2)
-- row: (0,3)
-- end-expected
SELECT ctid::text FROM sysc_a ORDER BY ctid;
-- begin-expected
-- columns: ctid
-- row: (0,1)
-- row: (0,1)
-- row: (0,1)
-- end-expected
SELECT (SELECT ctid::text FROM sysc_b WHERE id = 1) FROM sysc_a ORDER BY id;
-- begin-expected
-- columns: ctid
-- row: (0,1)
-- row: (0,2)
-- row: (0,3)
-- row: (0,1)
-- row: (0,2)
-- end-expected
SELECT ctid::text FROM sysc_a UNION ALL SELECT ctid::text FROM sysc_b;
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
INSERT INTO sysc_a (ctid) VALUES ('(0,1)');
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
UPDATE sysc_a SET ctid = '(0,1)';
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- row: 3
-- end-expected
UPDATE sysc_a SET v = v WHERE ctid IS NOT NULL RETURNING id;

-- ============================================================================
-- No index is built over one
-- ============================================================================
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
CREATE INDEX sysc_i ON sysc_a (ctid);
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
CREATE UNIQUE INDEX sysc_i ON sysc_a (tableoid);
-- begin-expected-error
-- sqlstate: 42704
-- end-expected-error
CREATE INDEX sysc_i ON sysc_a (xmin);
-- begin-expected-error
-- sqlstate: 42704
-- end-expected-error
CREATE INDEX sysc_i ON sysc_a (cmin);
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
CREATE INDEX sysc_i ON sysc_a (id) INCLUDE (ctid);
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
CREATE INDEX sysc_i ON sysc_a ((ctid::text));
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
CREATE INDEX sysc_i ON sysc_a (id) WHERE ctid IS NOT NULL;
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
CREATE TABLE sysc_k (id int, UNIQUE (ctid));
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
CREATE TABLE sysc_k (id int, PRIMARY KEY (tableoid));
-- begin-expected-error
-- sqlstate: 42704
-- end-expected-error
CREATE TABLE sysc_k (id int, UNIQUE (xmax));
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
ALTER TABLE sysc_a ADD PRIMARY KEY (ctid);
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
ALTER TABLE sysc_a ADD UNIQUE (ctid);
-- begin-expected-error
-- sqlstate: 42704
-- end-expected-error
ALTER TABLE sysc_a ADD UNIQUE (xmin);
-- begin-expected-error
-- sqlstate: 42P10
-- end-expected-error
INSERT INTO sysc_a VALUES (9, 'nine') ON CONFLICT (ctid) DO NOTHING;

-- ============================================================================
-- No ALTER TABLE drops, renames or alters one
-- ============================================================================
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
ALTER TABLE sysc_a DROP COLUMN ctid;
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
ALTER TABLE sysc_a DROP COLUMN IF EXISTS xmin;
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
ALTER TABLE sysc_a RENAME COLUMN ctid TO z;
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
ALTER TABLE sysc_a ALTER COLUMN ctid SET NOT NULL;
-- begin-expected-error
-- sqlstate: 0A000
-- end-expected-error
ALTER TABLE sysc_a ALTER COLUMN xmin TYPE int;

-- ============================================================================
-- No generated column is computed from one, and no CHECK reads one
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42P10
-- end-expected-error
CREATE TABLE sysc_g (id int, c int GENERATED ALWAYS AS (xmin::text::int) STORED);
-- begin-expected-error
-- sqlstate: 42P10
-- end-expected-error
CREATE TABLE sysc_g (id int, c text GENERATED ALWAYS AS (ctid::text) STORED);
-- begin-expected-error
-- sqlstate: 42P10
-- end-expected-error
ALTER TABLE sysc_a ADD COLUMN c text GENERATED ALWAYS AS (ctid::text) STORED;
-- begin-expected-error
-- sqlstate: 42P10
-- end-expected-error
CREATE TABLE sysc_g (id int, CHECK (ctid IS NOT NULL));

-- teardown
DROP MATERIALIZED VIEW sysc_mv;
DROP VIEW sysc_v;
DROP TABLE sysc_a;
DROP TABLE sysc_b;
