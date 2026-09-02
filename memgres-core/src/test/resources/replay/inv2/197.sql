-- source: investigation-2026-08.md
-- finding: 197
-- title: Unrelated singletons in this area
-- begin-expected
-- columns: count:int8
-- row: 18264
-- rowcount: 1
-- end-expected
SELECT count(*) FROM (SELECT generate_series('2000-01-01'::timestamp, '2050-01-01'::timestamp, '1 day'::interval) AS g) t;
-- begin-expected
-- columns: count:int8
-- row: 20000
-- rowcount: 1
-- end-expected
SELECT count(*) FROM (SELECT generate_series(1::numeric, 20000::numeric, 1::numeric) AS g) t;
-- begin-expected
-- ok: 0
-- end-expected
SET work_mem = '13MB';
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
SET LOCAL work_mem TO DEFAULT;
-- begin-expected
-- ok: 0
-- end-expected
COMMIT;
-- begin-expected
-- columns: work_mem:text
-- row: 13MB
-- rowcount: 1
-- end-expected
SHOW work_mem;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE;
-- begin-expected
-- columns: transaction_isolation:text
-- row: read committed
-- rowcount: 1
-- end-expected
SHOW transaction_isolation;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_c (i int);
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
DECLARE zz_vf_x4 ASENSITIVE CURSOR FOR SELECT i FROM zz_vf_c ORDER BY i;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
LOCK TABLE pg_class IN ACCESS SHARE MODE;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "ALL"
-- end-expected-error
SAVEPOINT ALL;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "select"
-- end-expected-error
SAVEPOINT select;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "ALL"
-- end-expected-error
RELEASE SAVEPOINT ALL;
-- begin-expected
-- ok: 0
-- end-expected
PREPARE zz_vf_xp AS SELECT $1::int;
-- begin-expected
-- columns: statement:text | parameter_types:text
-- row: PREPARE zz_vf_xp AS SELECT $1::int | {integer}
-- rowcount: 1
-- end-expected
SELECT statement, parameter_types::text FROM pg_prepared_statements WHERE name='zz_vf_xp';
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- columns: ?column?:int4
-- row: 1
-- rowcount: 1
-- end-expected
SELECT 1;
-- begin-expected
-- columns: n:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT pg_current_xact_id_if_assigned() IS NULL AS n;
-- begin-expected
-- columns: session_authorization:text
-- row: memgres
-- rowcount: 1
-- end-expected
SHOW SESSION AUTHORIZATION;
-- begin-expected-error
-- sqlstate: 25001
-- message-like: SET TRANSACTION ISOLATION LEVEL must be called before any query
-- end-expected-error
BEGIN ISOLATION LEVEL REPEATABLE READ;
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: a snapshot-importing transaction must have isolation level SERIALIZABLE or REPEATABLE READ
-- end-expected-error
SET TRANSACTION SNAPSHOT '00000003-0000001B-1';
-- begin-expected
-- ok: 0
-- end-expected
ROLLBACK;
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: a snapshot-importing transaction must have isolation level SERIALIZABLE or REPEATABLE READ
-- end-expected-error
SET TRANSACTION SNAPSHOT '00000003-0000001B-1';
