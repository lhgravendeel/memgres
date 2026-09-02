-- source: investigation.md
-- finding: 42
-- title: PG 18 features that are absent
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: partitioned tables cannot be unlogged
-- end-expected-error
CREATE UNLOGGED TABLE t (i int) PARTITION BY RANGE (i);
--   PG 18: 0A000 partitioned tables cannot be unlogged | mg: accepted
-- begin-expected
-- columns: int4:int4
-- row: 256
-- rowcount: 1
-- end-expected
SELECT '\x00000100'::bytea::int;
--   PG: 256 | mg: 22P02 invalid input syntax for type integer: "[B@54c26376";
