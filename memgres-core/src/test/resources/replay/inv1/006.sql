-- source: investigation.md
-- finding: 6
-- title: DDL validation gaps ⚠️
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
ALTER TABLE t ADD CONSTRAINT ck CHECK (b > 0), ADD COLUMN b int NOT NULL DEFAULT 5;
--   PG: succeeds | mg: 42703 column "b" does not exist
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
ALTER TABLE t ADD COLUMN b text DEFAULT 'z', DROP COLUMN b;
--   PG: succeeds | mg: 42701 column "b" already exists
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
ALTER TABLE t ALTER COLUMN a TYPE int;
-- PG: 23505 could not create unique index | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
ALTER TABLE t ALTER COLUMN a TYPE int;
-- PG: 23514 check constraint violated by some row | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
ALTER TABLE t ALTER COLUMN a TYPE int USING 0;
-- PG: 42804 default cannot be cast automatically | mg: OK,
                                         --   and the stale default then fails at INSERT time
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
ALTER TABLE t ALTER COLUMN a TYPE bigint USING NULL;
-- PG: 23502 contains null values | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
ALTER TABLE t ADD COLUMN b int NOT NULL DEFAULT NULL;
-- PG: 23502 | mg: OK, column is nullable
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
ALTER TABLE t ADD CONSTRAINT nn NOT NULL a;
-- PG: 23502 | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "p1" does not exist
-- end-expected-error
CREATE TABLE c () INHERITS (p1, p2);
-- columns of conflicting type
--   PG: 42804 inherited column "shared" has a type conflict | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "child" does not exist
-- end-expected-error
INSERT INTO child (a) VALUES (NULL);
-- parent declares a NOT NULL
--   PG: 23502 | mg: accepted
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "r" does not exist
-- end-expected-error
CREATE TABLE x PARTITION OF r FOR VALUES FROM (10) TO (5);
--   PG: 42P17 empty range bound | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "r" does not exist
-- end-expected-error
CREATE TABLE x PARTITION OF r FOR VALUES IN (1,2,3);
-- r is RANGE-partitioned
--   PG: 42P16 invalid bound specification | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
CREATE TABLE p PARTITION OF t FOR VALUES FROM (1) TO (2);
-- t is not partitioned
--   PG: 42P17 | mg: OK
-- Hash partitioning is broken outright: a valid set of partitions is rejected,
-- and rows then have nowhere to go.
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "h" does not exist
-- end-expected-error
CREATE TABLE h1 PARTITION OF h FOR VALUES WITH (MODULUS 4, REMAINDER 1);
--   PG: OK | mg: 42P16 every hash partition modulus must be a factor of the largest modulus
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "h" does not exist
-- end-expected-error
INSERT INTO h SELECT generate_series(1,20);
--   PG: 20 rows | mg: 23514 no partition of relation "h" found for row;
