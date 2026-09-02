-- source: investigation.md
-- finding: 121
-- title: Volatile partition key expressions accepted
-- begin-expected-error
-- sqlstate: 42P17
-- message-like: functions in partition key expression must be marked IMMUTABLE
-- end-expected-error
CREATE TABLE t (i int) PARTITION BY RANGE ((random()));
--   PG: functions in partition key expression must be marked IMMUTABLE | mg: accepted;
