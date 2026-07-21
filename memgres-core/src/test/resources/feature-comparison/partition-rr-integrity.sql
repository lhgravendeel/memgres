-- partition-rr-integrity.sql
-- Verification file for C4, C9, C10, C11, M7

-- === C4: ATTACH PARTITION column validation ===

-- C4a: Column mismatch detection
CREATE TABLE parent_c4(id int, name text) PARTITION BY RANGE(id);
CREATE TABLE bad_cols(id int, extra text, name text);
-- PG 18: ERROR 42804 "table ... contains column ... not found in parent"
ALTER TABLE parent_c4 ATTACH PARTITION bad_cols FOR VALUES FROM (1) TO (10);
-- expect: error 42804

-- C4b: Existing row bounds validation
CREATE TABLE parent_c4b(id int) PARTITION BY RANGE(id);
CREATE TABLE part_c4b(id int);
INSERT INTO part_c4b VALUES (50);
-- PG: ERROR 23514 "partition constraint of relation is violated by some row"
ALTER TABLE parent_c4b ATTACH PARTITION part_c4b FOR VALUES FROM (1) TO (10);
-- expect: error 23514

DROP TABLE parent_c4, bad_cols, parent_c4b, part_c4b;

-- === C9: RR snapshot reflects own TRUNCATE ===

CREATE TABLE t_c9(id int);
INSERT INTO t_c9 VALUES (1),(2),(3);

BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT count(*) FROM t_c9;
-- expect: 3
TRUNCATE t_c9;
SELECT count(*) FROM t_c9;
-- expect: 0
COMMIT;
DROP TABLE t_c9;

-- === C10: Partitioned parent reflects own INSERT under RR ===

CREATE TABLE parent_c10(id int) PARTITION BY RANGE(id);
CREATE TABLE child_c10 PARTITION OF parent_c10 FOR VALUES FROM (1) TO (100);
INSERT INTO parent_c10 VALUES (1);

BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT count(*) FROM parent_c10;
-- expect: 1
INSERT INTO parent_c10 VALUES (2);
SELECT count(*) FROM parent_c10;
-- expect: 2
COMMIT;
DROP TABLE parent_c10;

-- === C11: ROLLBACK restores sequences after TRUNCATE RESTART IDENTITY ===

CREATE TABLE parent_c11(id serial, v text) PARTITION BY RANGE(id);
CREATE TABLE child_c11 PARTITION OF parent_c11 FOR VALUES FROM (1) TO (1000);
INSERT INTO parent_c11(v) VALUES ('a'),('b');
-- ids: 1,2

BEGIN;
TRUNCATE parent_c11 RESTART IDENTITY;
ROLLBACK;

INSERT INTO parent_c11(v) VALUES ('c');
SELECT id FROM parent_c11 ORDER BY id;
-- expect: 1, 2, 3 (not 1, 1, 2)

DROP TABLE parent_c11;

-- === M7: RR write-write conflict raises 40001 ===

-- This requires concurrent sessions; described conceptually:
-- Session 1: BEGIN ISOLATION LEVEL REPEATABLE READ; UPDATE t SET v=1 WHERE id=1;
-- Session 2: BEGIN ISOLATION LEVEL REPEATABLE READ; UPDATE t SET v=2 WHERE id=1; COMMIT;
-- Session 1: COMMIT; → PG: ERROR 40001 "could not serialize access due to concurrent update"
