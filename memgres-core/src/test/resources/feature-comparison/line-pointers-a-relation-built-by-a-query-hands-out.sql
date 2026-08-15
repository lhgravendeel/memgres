-- ============================================================================
-- The line pointers a relation built by a query hands out
--
-- CREATE TABLE ... AS, SELECT ... INTO and CREATE MATERIALIZED VIEW fill the
-- new relation through the routine an INSERT writes through, so the rows the
-- query wrote take line pointers one after another and the relation goes on
-- from where they left off. The row written afterwards lands beyond them
-- rather than on top of the first, a tuple id still names one row once some of
-- them have been deleted, and the numbering survives a delete, an update and a
-- later INSERT into the same relation. A definition that asks for no data
-- leaves the relation empty, and the first row written afterwards takes the
-- first place. A refresh fills the relation again from the first place.
--
-- The rows go in under a command identifier of their own: the counter moves on
-- once the relation's catalogue rows are written, so cmin reports one more than
-- the statement began with, and the next statement of the transaction reports
-- one more again.
--
-- Every value below was measured against PostgreSQL 18.
-- ============================================================================

-- ============================================================================
-- The first row written afterwards does not take a place a row already holds
-- ============================================================================
CREATE TABLE qlp_a AS SELECT 8 AS i;
INSERT INTO qlp_a VALUES (9);

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 8
-- row: (0,2) | 9
-- end-expected
SELECT ctid::text AS ctid, i FROM qlp_a ORDER BY i;

DROP TABLE qlp_a;

-- ============================================================================
-- Several rows, and the place a delete gave up that is not handed out again
-- ============================================================================
CREATE TABLE qlp_b AS SELECT g FROM generate_series(1,3) g;
INSERT INTO qlp_b VALUES (9);

-- begin-expected
-- columns: ctid | g
-- row: (0,1) | 1
-- row: (0,2) | 2
-- row: (0,3) | 3
-- row: (0,4) | 9
-- end-expected
SELECT ctid::text AS ctid, g FROM qlp_b ORDER BY g;

DELETE FROM qlp_b WHERE g = 2;
INSERT INTO qlp_b VALUES (10);

-- begin-expected
-- columns: ctid | g
-- row: (0,1) | 1
-- row: (0,3) | 3
-- row: (0,4) | 9
-- row: (0,5) | 10
-- end-expected
SELECT ctid::text AS ctid, g FROM qlp_b ORDER BY g;

-- every row has a place of its own, and a place names one row
-- begin-expected
-- columns: places
-- row: 4
-- end-expected
SELECT count(DISTINCT ctid) AS places FROM qlp_b;

-- begin-expected
-- columns: g
-- row: 9
-- end-expected
SELECT g FROM qlp_b WHERE ctid = '(0,4)';

DROP TABLE qlp_b;

-- ============================================================================
-- Every later write goes on from where the query left off
-- ============================================================================
CREATE TABLE qlp_c AS SELECT g FROM generate_series(1,3) g;
INSERT INTO qlp_c SELECT g FROM generate_series(4,6) g;
DELETE FROM qlp_c WHERE g IN (2,5);
INSERT INTO qlp_c VALUES (7),(8);

-- begin-expected
-- columns: ctid | g
-- row: (0,1) | 1
-- row: (0,3) | 3
-- row: (0,4) | 4
-- row: (0,6) | 6
-- row: (0,7) | 7
-- row: (0,8) | 8
-- end-expected
SELECT ctid::text AS ctid, g FROM qlp_c ORDER BY g;

-- begin-expected
-- columns: rows | places
-- row: 6 | 6
-- end-expected
SELECT count(*) AS rows, count(DISTINCT ctid) AS places FROM qlp_c;

DROP TABLE qlp_c;

-- ============================================================================
-- A query that wrote nothing, and a definition that asked for no data
-- ============================================================================
CREATE TABLE qlp_d AS SELECT 1 AS a WHERE false;
INSERT INTO qlp_d VALUES (5);

-- begin-expected
-- columns: ctid | a
-- row: (0,1) | 5
-- end-expected
SELECT ctid::text AS ctid, a FROM qlp_d ORDER BY a;

DROP TABLE qlp_d;

CREATE TABLE qlp_e AS SELECT g FROM generate_series(1,3) g WITH NO DATA;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM qlp_e;

INSERT INTO qlp_e VALUES (7);
INSERT INTO qlp_e VALUES (8);

-- begin-expected
-- columns: ctid | g
-- row: (0,1) | 7
-- row: (0,2) | 8
-- end-expected
SELECT ctid::text AS ctid, g FROM qlp_e ORDER BY g;

DROP TABLE qlp_e;

-- ============================================================================
-- SELECT ... INTO numbers its rows the same way
-- ============================================================================
SELECT g INTO qlp_f FROM generate_series(1,2) g;
INSERT INTO qlp_f VALUES (9);

-- begin-expected
-- columns: ctid | g
-- row: (0,1) | 1
-- row: (0,2) | 2
-- row: (0,3) | 9
-- end-expected
SELECT ctid::text AS ctid, g FROM qlp_f ORDER BY g;

DELETE FROM qlp_f WHERE g = 1;
INSERT INTO qlp_f VALUES (11);

-- begin-expected
-- columns: ctid | g
-- row: (0,2) | 2
-- row: (0,3) | 9
-- row: (0,4) | 11
-- end-expected
SELECT ctid::text AS ctid, g FROM qlp_f ORDER BY g;

DROP TABLE qlp_f;

SELECT g INTO qlp_g FROM generate_series(1,2) g WHERE false;
INSERT INTO qlp_g VALUES (5);

-- begin-expected
-- columns: ctid | g
-- row: (0,1) | 5
-- end-expected
SELECT ctid::text AS ctid, g FROM qlp_g ORDER BY g;

DROP TABLE qlp_g;

-- ============================================================================
-- A materialized view numbers its rows from one, and a refresh begins again
-- ============================================================================
CREATE TABLE qlp_src (i int);
INSERT INTO qlp_src VALUES (1),(2),(3);
CREATE MATERIALIZED VIEW qlp_mv AS SELECT i FROM qlp_src;

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 1
-- row: (0,2) | 2
-- row: (0,3) | 3
-- end-expected
SELECT ctid::text AS ctid, i FROM qlp_mv ORDER BY i;

-- begin-expected
-- columns: rows | places
-- row: 3 | 3
-- end-expected
SELECT count(*) AS rows, count(DISTINCT ctid) AS places FROM qlp_mv;

-- begin-expected
-- columns: i
-- row: 2
-- end-expected
SELECT i FROM qlp_mv WHERE ctid = '(0,2)';

-- a row the source lost leaves no gap behind it: the refresh fills the
-- relation again from the first place
DELETE FROM qlp_src WHERE i = 2;
REFRESH MATERIALIZED VIEW qlp_mv;

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 1
-- row: (0,2) | 3
-- end-expected
SELECT ctid::text AS ctid, i FROM qlp_mv ORDER BY i;

INSERT INTO qlp_src VALUES (4),(5);
REFRESH MATERIALIZED VIEW qlp_mv;

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 1
-- row: (0,2) | 3
-- row: (0,3) | 4
-- row: (0,4) | 5
-- end-expected
SELECT ctid::text AS ctid, i FROM qlp_mv ORDER BY i;

-- begin-expected
-- columns: places
-- row: 4
-- end-expected
SELECT count(DISTINCT ctid) AS places FROM qlp_mv;

-- a materialized view takes no write of its own
-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot change materialized view "qlp_mv"
-- end-expected-error
INSERT INTO qlp_mv VALUES (9);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot change materialized view "qlp_mv"
-- end-expected-error
DELETE FROM qlp_mv WHERE i = 1;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot change materialized view "qlp_mv"
-- end-expected-error
UPDATE qlp_mv SET i = 8 WHERE i = 1;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "qlp_mv" is not a table
-- end-expected-error
TRUNCATE qlp_mv;

-- a relation built from the materialized view numbers its own rows from one
CREATE TABLE qlp_h AS SELECT i FROM qlp_mv;
INSERT INTO qlp_h VALUES (9);

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 1
-- row: (0,2) | 3
-- row: (0,3) | 4
-- row: (0,4) | 5
-- row: (0,5) | 9
-- end-expected
SELECT ctid::text AS ctid, i FROM qlp_h ORDER BY i;

DROP TABLE qlp_h;
DROP MATERIALIZED VIEW qlp_mv;
DROP TABLE qlp_src;

-- ============================================================================
-- A materialized view defined with no data is unpopulated until it is refreshed
-- ============================================================================
CREATE TABLE qlp_sr2 (i int);
INSERT INTO qlp_sr2 VALUES (7),(8);
CREATE MATERIALIZED VIEW qlp_mw AS SELECT i FROM qlp_sr2 WITH NO DATA;

-- begin-expected-error
-- sqlstate: 55000
-- message-like: materialized view "qlp_mw" has not been populated
-- end-expected-error
SELECT i FROM qlp_mw;

REFRESH MATERIALIZED VIEW qlp_mw WITH NO DATA;

-- begin-expected-error
-- sqlstate: 55000
-- message-like: materialized view "qlp_mw" has not been populated
-- end-expected-error
SELECT count(*) FROM qlp_mw;

REFRESH MATERIALIZED VIEW qlp_mw;

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 7
-- row: (0,2) | 8
-- end-expected
SELECT ctid::text AS ctid, i FROM qlp_mw ORDER BY i;

-- begin-expected
-- columns: rows | places
-- row: 2 | 2
-- end-expected
SELECT count(*) AS rows, count(DISTINCT ctid) AS places FROM qlp_mw;

DROP MATERIALIZED VIEW qlp_mw;
DROP TABLE qlp_sr2;

-- ============================================================================
-- A delete by tuple id, an update that moves a row, and the next write
-- ============================================================================
CREATE TABLE qlp_i AS SELECT g AS i, 'r' || g AS s FROM generate_series(1,4) g;
DELETE FROM qlp_i WHERE ctid = '(0,3)';
UPDATE qlp_i SET s = 'z' WHERE i = 1;
INSERT INTO qlp_i VALUES (9,'n');

-- begin-expected
-- columns: ctid | i | s
-- row: (0,5) | 1 | z
-- row: (0,2) | 2 | r2
-- row: (0,4) | 4 | r4
-- row: (0,6) | 9 | n
-- end-expected
SELECT ctid::text AS ctid, i, s FROM qlp_i ORDER BY i;

DROP TABLE qlp_i;

-- a delete of every row leaves the file alone, so the numbering goes on
CREATE TABLE qlp_j AS SELECT g FROM generate_series(1,5) g;
DELETE FROM qlp_j;
INSERT INTO qlp_j VALUES (9);

-- begin-expected
-- columns: ctid | g
-- row: (0,6) | 9
-- end-expected
SELECT ctid::text AS ctid, g FROM qlp_j ORDER BY g;

DROP TABLE qlp_j;

-- ============================================================================
-- TRUNCATE hands the relation a new file, and the numbering starts again
-- ============================================================================
CREATE TABLE qlp_k AS SELECT g FROM generate_series(1,2) g;
TRUNCATE qlp_k;
INSERT INTO qlp_k VALUES (5);

-- begin-expected
-- columns: ctid | g
-- row: (0,1) | 5
-- end-expected
SELECT ctid::text AS ctid, g FROM qlp_k ORDER BY g;

DROP TABLE qlp_k;

-- ============================================================================
-- The same in another schema, and for a temporary relation
-- ============================================================================
CREATE SCHEMA qlp_s;
CREATE TABLE qlp_s.qlp_m AS SELECT g FROM generate_series(1,2) g;
INSERT INTO qlp_s.qlp_m VALUES (9);

-- begin-expected
-- columns: ctid | g
-- row: (0,1) | 1
-- row: (0,2) | 2
-- row: (0,3) | 9
-- end-expected
SELECT ctid::text AS ctid, g FROM qlp_s.qlp_m ORDER BY g;

DROP SCHEMA qlp_s CASCADE;

CREATE TEMP TABLE qlp_n AS SELECT g FROM generate_series(1,2) g;
INSERT INTO qlp_n VALUES (9);

-- begin-expected
-- columns: ctid | g
-- row: (0,1) | 1
-- row: (0,2) | 2
-- row: (0,3) | 9
-- end-expected
SELECT ctid::text AS ctid, g FROM qlp_n ORDER BY g;

DROP TABLE qlp_n;

-- ============================================================================
-- A relation the transaction never left behind begins at one
-- ============================================================================
BEGIN;
CREATE TABLE qlp_o AS SELECT 1 AS a;
INSERT INTO qlp_o VALUES (2);

-- begin-expected
-- columns: ctid | a
-- row: (0,1) | 1
-- row: (0,2) | 2
-- end-expected
SELECT ctid::text AS ctid, a FROM qlp_o ORDER BY a;

ROLLBACK;
CREATE TABLE qlp_o AS SELECT 5 AS a;
INSERT INTO qlp_o VALUES (6);

-- begin-expected
-- columns: ctid | a
-- row: (0,1) | 5
-- row: (0,2) | 6
-- end-expected
SELECT ctid::text AS ctid, a FROM qlp_o ORDER BY a;

DROP TABLE qlp_o;

-- a write that was rolled back had already spent its place
CREATE TABLE qlp_p AS SELECT g FROM generate_series(1,2) g;
BEGIN;
INSERT INTO qlp_p VALUES (8);
ROLLBACK;
INSERT INTO qlp_p VALUES (9);

-- begin-expected
-- columns: ctid | g
-- row: (0,1) | 1
-- row: (0,2) | 2
-- row: (0,4) | 9
-- end-expected
SELECT ctid::text AS ctid, g FROM qlp_p ORDER BY g;

DROP TABLE qlp_p;

-- ============================================================================
-- Rows the query wrote that carry the same values still have places of their own
-- ============================================================================
CREATE TABLE qlp_q AS SELECT 1 AS a UNION ALL SELECT 1;
INSERT INTO qlp_q VALUES (1);

-- begin-expected
-- columns: ctid | a
-- row: (0,1) | 1
-- row: (0,2) | 1
-- row: (0,3) | 1
-- end-expected
SELECT ctid::text AS ctid, a FROM qlp_q ORDER BY ctid;

-- begin-expected
-- columns: places
-- row: 3
-- end-expected
SELECT count(DISTINCT ctid) AS places FROM qlp_q;

DROP TABLE qlp_q;

-- ============================================================================
-- The places go out in the order the query produced the rows
-- ============================================================================
CREATE TABLE qlp_r AS
  WITH c AS (SELECT g FROM generate_series(1,3) g) SELECT g FROM c ORDER BY g DESC;
INSERT INTO qlp_r VALUES (9);

-- begin-expected
-- columns: ctid | g
-- row: (0,1) | 3
-- row: (0,2) | 2
-- row: (0,3) | 1
-- row: (0,4) | 9
-- end-expected
SELECT ctid::text AS ctid, g FROM qlp_r ORDER BY ctid;

DROP TABLE qlp_r;

CREATE TABLE qlp_t AS SELECT g FROM generate_series(1,10) g ORDER BY g LIMIT 3;
INSERT INTO qlp_t VALUES (9);

-- begin-expected
-- columns: ctid | g
-- row: (0,1) | 1
-- row: (0,2) | 2
-- row: (0,3) | 3
-- row: (0,4) | 9
-- end-expected
SELECT ctid::text AS ctid, g FROM qlp_t ORDER BY ctid;

DROP TABLE qlp_t;

-- ============================================================================
-- The tuple ids the query read are values, not the places the new rows take
-- ============================================================================
CREATE TABLE qlp_u (i int);
INSERT INTO qlp_u VALUES (1),(2),(3);
DELETE FROM qlp_u WHERE i = 1;
CREATE TABLE qlp_v AS SELECT ctid::text AS oldplace, i FROM qlp_u;
INSERT INTO qlp_v VALUES ('x', 9);

-- begin-expected
-- columns: ctid | oldplace | i
-- row: (0,1) | (0,2) | 2
-- row: (0,2) | (0,3) | 3
-- row: (0,3) | x | 9
-- end-expected
SELECT ctid::text AS ctid, oldplace, i FROM qlp_v ORDER BY i;

DROP TABLE qlp_v;
DROP TABLE qlp_u;

-- ============================================================================
-- Two relations built by queries count their places apart
-- ============================================================================
CREATE TABLE qlp_w AS SELECT g FROM generate_series(1,2) g;
CREATE TABLE qlp_x AS SELECT g FROM generate_series(1,3) g;
INSERT INTO qlp_w VALUES (9);
INSERT INTO qlp_x VALUES (9);

-- begin-expected
-- columns: ctid | g
-- row: (0,1) | 1
-- row: (0,2) | 2
-- row: (0,3) | 9
-- end-expected
SELECT ctid::text AS ctid, g FROM qlp_w ORDER BY g;

-- begin-expected
-- columns: ctid | g
-- row: (0,1) | 1
-- row: (0,2) | 2
-- row: (0,3) | 3
-- row: (0,4) | 9
-- end-expected
SELECT ctid::text AS ctid, g FROM qlp_x ORDER BY g;

DROP TABLE qlp_w;
DROP TABLE qlp_x;

-- ============================================================================
-- A relation a prepared statement built counts the same
-- ============================================================================
PREPARE qlp_ps AS SELECT 3 AS a;
CREATE TABLE qlp_y AS EXECUTE qlp_ps;
INSERT INTO qlp_y VALUES (4);

-- begin-expected
-- columns: ctid | a
-- row: (0,1) | 3
-- row: (0,2) | 4
-- end-expected
SELECT ctid::text AS ctid, a FROM qlp_y ORDER BY a;

DROP TABLE qlp_y;
DEALLOCATE qlp_ps;

-- ============================================================================
-- The command identifier the rows were written under
-- ============================================================================
CREATE TABLE qlp_z AS SELECT g FROM generate_series(1,2) g;

-- begin-expected
-- columns: g | cmin | cmax | xmax
-- row: 1 | 1 | 1 | 0
-- row: 2 | 1 | 1 | 0
-- end-expected
SELECT g, cmin::text AS cmin, cmax::text AS cmax, xmax::text AS xmax
  FROM qlp_z ORDER BY g;

INSERT INTO qlp_z VALUES (7);

-- the INSERT is a transaction of its own and starts the counter again
-- begin-expected
-- columns: g | cmin
-- row: 1 | 1
-- row: 2 | 1
-- row: 7 | 0
-- end-expected
SELECT g, cmin::text AS cmin FROM qlp_z ORDER BY g;

DROP TABLE qlp_z;

-- inside one transaction the statement spends two: the relation's catalogue
-- rows take the first and the rows the query wrote take the second
CREATE TABLE qlp_aa (a int);
BEGIN;
INSERT INTO qlp_aa VALUES (0);
CREATE TABLE qlp_ab AS SELECT 7 AS b;
INSERT INTO qlp_aa VALUES (1);

-- begin-expected
-- columns: a | cmin
-- row: 0 | 0
-- row: 1 | 3
-- end-expected
SELECT a, cmin::text AS cmin FROM qlp_aa ORDER BY a;

-- begin-expected
-- columns: b | cmin | ctid
-- row: 7 | 2 | (0,1)
-- end-expected
SELECT b, cmin::text AS cmin, ctid::text AS ctid FROM qlp_ab;

COMMIT;
DROP TABLE qlp_ab;
DROP TABLE qlp_aa;
