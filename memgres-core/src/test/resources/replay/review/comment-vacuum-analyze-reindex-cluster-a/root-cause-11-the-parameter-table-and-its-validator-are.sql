-- source: review-2026-08.md
-- finding: Root cause 11: the parameter table and its validator are incomplete, and their messages are built from the folded key
-- area: COMMENT, VACUUM, ANALYZE, REINDEX, CLUSTER and the SET family
-- title: Root cause 11: the parameter table and its validator are incomplete, and their messages are built from the folded key
-- begin-expected
-- ok: 0
-- end-expected
SET SEED TO 0.5;
-- begin-expected
-- ok: 0
-- end-expected
SET seed = 0.5;
-- begin-expected
-- columns: seed:text
-- row: unavailable
-- rowcount: 1
-- end-expected
SHOW seed;
-- begin-expected
-- ok: 0
-- end-expected
SET NAMES 'UTF8';
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter "client_encoding": "BOGUS"
-- end-expected-error
SET NAMES 'BOGUS';
-- begin-expected
-- ok: 0
-- end-expected
SET work_mem = 1e3;
-- begin-expected
-- columns: work_mem:text
-- row: 1000kB
-- rowcount: 1
-- end-expected
SHOW work_mem;
-- begin-expected
-- ok: 0
-- end-expected
SET statement_timeout = 1e3;
-- begin-expected
-- columns: statement_timeout:text
-- row: 1s
-- rowcount: 1
-- end-expected
SHOW statement_timeout;
-- begin-expected
-- ok: 0
-- end-expected
SET client_min_messages = 'info';
-- begin-expected
-- ok: 0
-- end-expected
SET client_min_messages = 'debug';
-- begin-expected
-- columns: client_min_messages:text
-- row: debug2
-- rowcount: 1
-- end-expected
SHOW client_min_messages;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter "client_encoding": "BOGUS"
-- end-expected-error
SET client_encoding = 'BOGUS';
-- begin-expected
-- columns: client_encoding:text
-- row: UTF8
-- rowcount: 1
-- end-expected
SHOW client_encoding;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: unrecognized configuration parameter "zz_x.neverset"
-- end-expected-error
SHOW zz_x.neverset;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter "work_mem": "2147483648"
-- end-expected-error
SET work_mem = 2147483648;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter "statement_timeout": "2147483648"
-- end-expected-error
SET statement_timeout = 2147483648;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: 0 kB is outside the valid range for parameter "work_mem" (64 kB .. 2147483647 kB)
-- end-expected-error
SET work_mem = '1B';
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter "IntervalStyle": "bogus"
-- end-expected-error
SET IntervalStyle = 'bogus';
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter "IntervalStyle": "bogus"
-- end-expected-error
SET intervalstyle = 'bogus';
