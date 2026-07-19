-- View definition fidelity:
--  1. SELECT * is expanded (frozen) at CREATE VIEW time — a column added to the
--     base table later must not appear through a pre-existing view.
--  2. CREATE [MATERIALIZED] VIEW v(a, b) column alias lists are applied to the
--     view's output columns; more names than columns is an error.
--  3. Materialized views WITH NO DATA raise 55000 on any scan until REFRESH;
--     REFRESH ... WITH NO DATA depopulates again. pg_matviews.ispopulated tracks it.

-- setup
CREATE TABLE vdf_t (x int, y text);
INSERT INTO vdf_t VALUES (1, 'a'), (2, 'b');

-- stmt 1: star frozen at CREATE VIEW time — column added later is invisible
CREATE VIEW vdf_v AS SELECT * FROM vdf_t;
ALTER TABLE vdf_t ADD COLUMN z int;

-- begin-expected
-- columns: x | y
-- row: 1 | a
-- end-expected
SELECT * FROM vdf_v WHERE x = 1;

-- stmt 2: a view created after the ALTER sees the new column
CREATE VIEW vdf_v_new AS SELECT * FROM vdf_t;

-- begin-expected
-- columns: x | y | z
-- row: 1 | a | NULL
-- end-expected
SELECT * FROM vdf_v_new WHERE x = 1;

-- stmt 3: data inserted after view creation still flows through the frozen view
INSERT INTO vdf_t VALUES (3, 'c', 30);

-- begin-expected
-- columns: x | y
-- row: 3 | c
-- end-expected
SELECT * FROM vdf_v WHERE x = 3;

-- stmt 4: qualified alias.* is frozen too
CREATE TABLE vdf_q (id int, nm text);
INSERT INTO vdf_q VALUES (7, 'seven');
CREATE VIEW vdf_qv AS SELECT t.* FROM vdf_q t;
ALTER TABLE vdf_q ADD COLUMN extra int;

-- begin-expected
-- columns: id | nm
-- row: 7 | seven
-- end-expected
SELECT * FROM vdf_qv;

-- stmt 5: star over a USING join — merged column once, frozen against later ALTERs
CREATE TABLE vdf_a (id int, an text);
CREATE TABLE vdf_b (id int, bn text);
INSERT INTO vdf_a VALUES (1, 'left');
INSERT INTO vdf_b VALUES (1, 'right');
CREATE VIEW vdf_j AS SELECT * FROM vdf_a JOIN vdf_b USING (id);
ALTER TABLE vdf_b ADD COLUMN extra int;

-- begin-expected
-- columns: id | an | bn
-- row: 1 | left | right
-- end-expected
SELECT * FROM vdf_j;

-- stmt 6: view over view — outer star frozen against the inner view's columns
CREATE TABLE vdf_vv_t (x int);
INSERT INTO vdf_vv_t VALUES (5);
CREATE VIEW vdf_vv1 AS SELECT * FROM vdf_vv_t;
CREATE VIEW vdf_vv2 AS SELECT * FROM vdf_vv1;
ALTER TABLE vdf_vv_t ADD COLUMN y int;

-- begin-expected
-- columns: x
-- row: 5
-- end-expected
SELECT * FROM vdf_vv2;

-- stmt 7: column alias list applied to the view output
CREATE TABLE vdf_al_t (x int, y text);
INSERT INTO vdf_al_t VALUES (2, 'b'), (1, 'a');
CREATE VIEW vdf_al (p, q) AS SELECT x, y FROM vdf_al_t;

-- begin-expected
-- columns: p | q
-- row: 1 | a
-- row: 2 | b
-- end-expected
SELECT p, q FROM vdf_al ORDER BY p;

-- stmt 8: ORDER BY through the view by aliased name
-- begin-expected
-- columns: q
-- row: b
-- row: a
-- end-expected
SELECT q FROM vdf_al ORDER BY p DESC;

-- stmt 9: the original column name is not visible through the aliased view
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "x" does not exist
-- end-expected-error
SELECT x FROM vdf_al;

-- stmt 10: view over aliased view
CREATE VIEW vdf_al2 (r) AS SELECT p FROM vdf_al;

-- begin-expected
-- columns: r
-- row: 1
-- row: 2
-- end-expected
SELECT r FROM vdf_al2 ORDER BY r;

-- stmt 11: alias list applies to a star select too
CREATE VIEW vdf_al_star (a, b) AS SELECT * FROM vdf_al_t;

-- begin-expected
-- columns: a | b
-- row: 1 | a
-- end-expected
SELECT a, b FROM vdf_al_star ORDER BY a LIMIT 1;

-- stmt 12: aliases visible in information_schema.columns
-- begin-expected
-- columns: column_name
-- row: p
-- row: q
-- end-expected
SELECT column_name FROM information_schema.columns WHERE table_name = 'vdf_al' ORDER BY ordinal_position;

-- stmt 13: aliases visible in pg_attribute
-- begin-expected
-- columns: attname
-- row: p
-- row: q
-- end-expected
SELECT attname::text FROM pg_attribute WHERE attrelid = 'vdf_al'::regclass AND attnum > 0 ORDER BY attnum;

-- stmt 14: more column names than columns is an error
-- begin-expected-error
-- sqlstate: 42601
-- message-like: more column names than columns
-- end-expected-error
CREATE VIEW vdf_bad (a, b, c) AS SELECT x, y FROM vdf_al_t;

-- stmt 15: fewer names is allowed; remaining columns keep their own names
CREATE VIEW vdf_few (a) AS SELECT x, y FROM vdf_al_t;

-- begin-expected
-- columns: a | y
-- row: 1 | a
-- end-expected
SELECT * FROM vdf_few ORDER BY a LIMIT 1;

-- stmt 16: materialized view takes the same alias list
CREATE TABLE vdf_mv_t (x int, y text);
INSERT INTO vdf_mv_t VALUES (1, 'm');
CREATE MATERIALIZED VIEW vdf_mv (a, b) AS SELECT x, y FROM vdf_mv_t;

-- begin-expected
-- columns: a | b
-- row: 1 | m
-- end-expected
SELECT a, b FROM vdf_mv;

-- stmt 17: materialized view with too many column names
-- begin-expected-error
-- sqlstate: 42601
-- message-like: too many column names
-- end-expected-error
CREATE MATERIALIZED VIEW vdf_mv_bad (a, b) AS SELECT x FROM vdf_mv_t;

-- stmt 18: WITH NO DATA — any scan fails until REFRESH
CREATE MATERIALIZED VIEW vdf_mv_nd AS SELECT x FROM vdf_mv_t WITH NO DATA;

-- begin-expected-error
-- sqlstate: 55000
-- message-like: has not been populated
-- end-expected-error
SELECT * FROM vdf_mv_nd;

-- stmt 19: aggregates over the unpopulated matview also fail
-- begin-expected-error
-- sqlstate: 55000
-- message-like: has not been populated
-- end-expected-error
SELECT count(*) FROM vdf_mv_nd;

-- stmt 20: ispopulated is false before REFRESH
-- begin-expected
-- columns: ispopulated
-- row: f
-- end-expected
SELECT ispopulated FROM pg_matviews WHERE matviewname = 'vdf_mv_nd';

-- stmt 21: REFRESH populates the view
REFRESH MATERIALIZED VIEW vdf_mv_nd;

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM vdf_mv_nd;

-- stmt 22: ispopulated is true after REFRESH
-- begin-expected
-- columns: ispopulated
-- row: t
-- end-expected
SELECT ispopulated FROM pg_matviews WHERE matviewname = 'vdf_mv_nd';

-- stmt 23: REFRESH ... WITH NO DATA depopulates again
REFRESH MATERIALIZED VIEW vdf_mv_nd WITH NO DATA;

-- begin-expected-error
-- sqlstate: 55000
-- message-like: has not been populated
-- end-expected-error
SELECT x FROM vdf_mv_nd;

-- stmt 24: matviews are excluded from information_schema.columns (PG covers
-- only SQL-standard objects there; columns are describable via pg_attribute)
-- begin-expected
-- columns: column_name
-- end-expected
SELECT column_name FROM information_schema.columns WHERE table_name = 'vdf_mv_nd' ORDER BY ordinal_position;

-- cleanup
DROP MATERIALIZED VIEW vdf_mv_nd;
DROP MATERIALIZED VIEW vdf_mv;
DROP TABLE vdf_mv_t;
DROP VIEW vdf_few;
DROP VIEW vdf_al_star;
DROP VIEW vdf_al2;
DROP VIEW vdf_al;
DROP TABLE vdf_al_t;
DROP VIEW vdf_vv2;
DROP VIEW vdf_vv1;
DROP TABLE vdf_vv_t;
DROP VIEW vdf_j;
DROP TABLE vdf_a;
DROP TABLE vdf_b;
DROP VIEW vdf_qv;
DROP TABLE vdf_q;
DROP VIEW vdf_v_new;
DROP VIEW vdf_v;
DROP TABLE vdf_t;
