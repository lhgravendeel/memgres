-- source: review-2026-08.md
-- finding: Root cause 10: SET and SHOW have no value grammar — the parser joins every remaining token with spaces
-- area: COMMENT, VACUUM, ANALYZE, REINDEX, CLUSTER and the SET family
-- title: Root cause 10: SET and SHOW have no value grammar — the parser joins every remaining token with spaces
-- begin-expected
-- ok: 0
-- end-expected
SET TIME ZONE 7;
-- begin-expected
-- columns: TimeZone:text
-- row: <+07>-07
-- rowcount: 1
-- end-expected
SHOW TimeZone;
-- begin-expected
-- ok: 0
-- end-expected
SET TIME ZONE -7;
-- begin-expected
-- columns: TimeZone:text
-- row: <-07>+07
-- rowcount: 1
-- end-expected
SHOW TimeZone;
-- begin-expected
-- ok: 0
-- end-expected
SET TIME ZONE INTERVAL '1' HOUR;
-- begin-expected
-- columns: TimeZone:text
-- row: <+01>-01
-- rowcount: 1
-- end-expected
SHOW TimeZone;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter "TimeZone": "bogus/zone"
-- end-expected-error
SET TIME ZONE 'bogus/zone';
-- begin-expected
-- ok: 0
-- end-expected
SET work_mem = '7MB';
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter "work_mem": "DEFAULT"
-- end-expected-error
SET work_mem = 'DEFAULT';
-- begin-expected
-- columns: work_mem:text
-- row: 7MB
-- rowcount: 1
-- end-expected
SHOW work_mem;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "read"
-- end-expected-error
SET default_transaction_isolation = repeatable read;
-- begin-expected
-- columns: default_transaction_isolation:text
-- row: read committed
-- rowcount: 1
-- end-expected
SHOW default_transaction_isolation;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: SET zz_x.two takes only one argument
-- end-expected-error
SET zz_x.two = 'a', 'b';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ","
-- end-expected-error
SET work_mem, statement_timeout = '1MB';
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ","
-- end-expected-error
SHOW work_mem, statement_timeout;
