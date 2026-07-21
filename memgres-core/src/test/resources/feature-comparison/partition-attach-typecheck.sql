-- ATTACH PARTITION column validation (bug C4 residual).
-- PG raises 42804 (ERRCODE_DATATYPE_MISMATCH) when a candidate partition's columns
-- do not match the parent: a same-named column of a different type, an extra column,
-- or a missing column. These are single-connection, so the SQL verify harness can
-- express them (the M7 RR DELETE conflict is concurrency-only → unit test only).

-- stmt 1: same-named column of a DIFFERENT type is rejected (name int vs text)
CREATE TABLE pat_parent (id int, name text) PARTITION BY LIST (id);
CREATE TABLE pat_badtype (id int, name int);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: has different type for column
-- end-expected-error
ALTER TABLE pat_parent ATTACH PARTITION pat_badtype FOR VALUES IN (1);

-- stmt 2: after rejection the parent has no partition, so id=1 has nowhere to route
-- begin-expected-error
-- sqlstate: 23514
-- message-like: no partition of relation
-- end-expected-error
INSERT INTO pat_parent VALUES (1, 'x');

-- stmt 3: extra child column not present in the parent is rejected
CREATE TABLE pat_extra (id int, name text, junk int);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: contains column "junk" not found in parent
-- end-expected-error
ALTER TABLE pat_parent ATTACH PARTITION pat_extra FOR VALUES IN (2);

-- stmt 4: missing child column is rejected
CREATE TABLE pat_missing (id int);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: child table is missing column "name"
-- end-expected-error
ALTER TABLE pat_parent ATTACH PARTITION pat_missing FOR VALUES IN (3);

-- stmt 5: a partition with matching columns attaches and routes correctly
CREATE TABLE pat_good (id int, name text);
ALTER TABLE pat_parent ATTACH PARTITION pat_good FOR VALUES IN (1);
INSERT INTO pat_parent VALUES (1, 'ok');

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM pat_good;

DROP TABLE pat_parent;
DROP TABLE pat_badtype;
DROP TABLE pat_extra;
DROP TABLE pat_missing;
