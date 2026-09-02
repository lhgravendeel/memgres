-- source: investigation-2026-08.md
-- finding: 205
-- title: GUC validation lives in one branch of executeSet. The assignability gate and the per-parameter value checks are reached only by a plain SET; RESET, SET ... TO D
-- begin-expected
-- ok: 0
-- end-expected
SET statement_timeout = 1.5;
-- begin-expected-error
-- sqlstate: 57014
-- message-like: canceling statement due to statement timeout
-- end-expected-error
SELECT count(*) FROM generate_series(1, 3000000) g;
-- begin-expected
-- ok: 0
-- end-expected
SET statement_timeout = '1500us';
-- begin-expected-error
-- sqlstate: 57014
-- message-like: canceling statement due to statement timeout
-- end-expected-error
SELECT count(*) FROM generate_series(1, 3000000) g;
-- begin-expected
-- ok: 0
-- end-expected
SET default_statistics_target = 100.7;
-- begin-expected
-- columns: current_setting:text
-- row: 101
-- rowcount: 1
-- end-expected
SELECT current_setting('default_statistics_target');
-- begin-expected
-- ok: 0
-- end-expected
SET lock_timeout = '2500us';
-- begin-expected
-- columns: lock_timeout:text
-- row: 2ms
-- rowcount: 1
-- end-expected
SHOW lock_timeout;
-- begin-expected
-- ok: 0
-- end-expected
SET datestyle = 'ISO, YMD';
-- begin-expected
-- ok: 0
-- end-expected
SET datestyle = 'ISO';
-- begin-expected
-- columns: current_setting:text
-- row: ISO, YMD
-- rowcount: 1
-- end-expected
SELECT current_setting('datestyle');
-- begin-expected-error
-- sqlstate: 55P02
-- message-like: parameter "block_size" cannot be changed
-- end-expected-error
RESET block_size;
-- begin-expected-error
-- sqlstate: 55P02
-- message-like: parameter "block_size" cannot be changed
-- end-expected-error
SET block_size TO DEFAULT;
-- begin-expected-error
-- sqlstate: 55P02
-- message-like: parameter "max_connections" cannot be changed without restarting the server
-- end-expected-error
RESET max_connections;
-- begin-expected-error
-- sqlstate: 55P02
-- message-like: parameter "wal_level" cannot be changed without restarting the server
-- end-expected-error
RESET wal_level;
-- begin-expected-error
-- sqlstate: 55P02
-- message-like: parameter "fsync" cannot be changed now
-- end-expected-error
RESET fsync;
-- begin-expected-error
-- sqlstate: 55P02
-- message-like: parameter "max_prepared_transactions" cannot be changed without restarting the server
-- end-expected-error
SET max_prepared_transactions = 10;
-- begin-expected
-- columns: current_setting:text
-- row: 0
-- rowcount: 1
-- end-expected
SELECT current_setting('max_prepared_transactions');
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter "TimeZone": "bogus/zone"
-- end-expected-error
SELECT set_config('TimeZone', 'bogus/zone', false);
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter "client_encoding": "BOGUS"
-- end-expected-error
SELECT set_config('client_encoding', 'BOGUS', false);
-- begin-expected-error
-- sqlstate: 22023
-- message-like: role "zz_nosuchrole" does not exist
-- end-expected-error
SELECT set_config('role', 'zz_nosuchrole', false);
