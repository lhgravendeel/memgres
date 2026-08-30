CREATE TABLE zlk_t (a int);
BEGIN;
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_locks WHERE locktype='transactionid';
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_locks WHERE locktype='virtualxid';
-- begin-expected
-- columns: t | r | m
-- row: relation | pg_locks | AccessShareLock
-- end-expected
SELECT locktype::text AS t, relation::regclass::text AS r, mode::text AS m FROM pg_locks WHERE locktype='relation' ORDER BY 1,2,3;
COMMIT;
BEGIN;
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM zlk_t;
-- begin-expected
-- columns: r | m
-- row: zlk_t | AccessShareLock
-- end-expected
SELECT relation::regclass::text AS r, mode::text AS m FROM pg_locks WHERE locktype='relation' AND relation='zlk_t'::regclass;
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_locks WHERE locktype='transactionid';
COMMIT;
BEGIN;
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_locks WHERE locktype='transactionid';
INSERT INTO zlk_t VALUES (1);
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_locks WHERE locktype='transactionid';
-- begin-expected
-- columns: r | m
-- row: zlk_t | RowExclusiveLock
-- end-expected
SELECT relation::regclass::text AS r, mode::text AS m FROM pg_locks WHERE locktype='relation' AND relation='zlk_t'::regclass;
COMMIT;
BEGIN;
SET work_mem = '4MB';
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_locks WHERE locktype='transactionid';
LOCK TABLE zlk_t IN ACCESS SHARE MODE;
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_locks WHERE locktype='transactionid';
ANALYZE zlk_t;
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_locks WHERE locktype='transactionid';
COMMENT ON TABLE zlk_t IS 'a comment writes a catalogue row';
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_locks WHERE locktype='transactionid';
COMMIT;
BEGIN;
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_locks WHERE locktype='transactionid';
GRANT SELECT ON zlk_t TO PUBLIC;
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_locks WHERE locktype='transactionid';
COMMIT;
BEGIN;
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_locks WHERE locktype='transactionid';
-- begin-expected
-- columns: assigned
-- row: t
-- end-expected
SELECT txid_current() > 0 AS assigned;
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_locks WHERE locktype='transactionid';
COMMIT;
BEGIN;
-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT a FROM zlk_t FOR UPDATE;
-- begin-expected
-- columns: m
-- row: RowShareLock
-- end-expected
SELECT mode::text AS m FROM pg_locks WHERE locktype='relation' AND relation='zlk_t'::regclass;
COMMIT;
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_locks WHERE locktype='relation' AND relation='zlk_t'::regclass;
DROP TABLE zlk_t;
