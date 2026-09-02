-- source: investigation.md
-- finding: 118
-- title: No cycle detection in inheritance or partitioning ⚠️ high
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE a (x int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE b (x int);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE b INHERIT a;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: circular inheritance not allowed
-- end-expected-error
ALTER TABLE a INHERIT b;
-- closes the loop
--   PG: 42P07 circular inheritance not allowed
--   mg: XX000 Internal error: StackOverflowError

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
ALTER TABLE t INHERIT t;
-- self-inheritance
--   PG: 42P07 | mg: XX000 StackOverflowError

-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE p (i int) PARTITION BY RANGE (i);
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: circular inheritance not allowed
-- end-expected-error
ALTER TABLE p ATTACH PARTITION p FOR VALUES FROM (1) TO (9);
-- attaches to itself
--   PG: 42P07 circular inheritance not allowed | mg: XX000 StackOverflowError;
