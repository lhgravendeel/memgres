-- ============================================================================
-- Feature Comparison: what a call answers with is what it was declared to
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- The function-call sub-protocol is how a JDBC client reaches the large-object
-- API, and the answer it puts on the wire is the type pg_proc declares for the
-- function, not the one the value happened to be carried in. memgres carried
-- an oid as a bigint and wrote eight bytes where four were due, so a driver
-- that asked it to create a large object could not open the one it got back.
--
-- The same reading is what pg_typeof owes a caller. An oid is a bare number
-- and a registry type a bare name, so neither value says which type produced
-- it: only the declaration does. Read off the value instead, lo_creat answered
-- bigint and to_regclass answered text.
--
-- The two 64-bit spellings of the seek and tell calls were missing outright,
-- so a client working on an object over two gigabytes was told the function
-- did not exist.
-- ============================================================================

SET search_path = public;


DROP TABLE IF EXISTS lo_ids;

CREATE TABLE lo_ids (id oid);


-- ============================================================================
-- What the large-object calls are declared to answer
-- ============================================================================

-- begin-expected
-- columns: r
-- row: lo_close -> integer
-- row: lo_creat -> oid
-- row: lo_create -> oid
-- row: lo_lseek -> integer
-- row: lo_lseek64 -> bigint
-- row: lo_open -> integer
-- row: lo_tell -> integer
-- row: lo_tell64 -> bigint
-- row: lo_truncate -> integer
-- row: lo_truncate64 -> integer
-- row: lo_unlink -> integer
-- row: loread -> bytea
-- row: lowrite -> integer
-- end-expected
SELECT proname || ' -> ' || pg_get_function_result(oid) AS r
FROM pg_catalog.pg_proc
WHERE proname IN ('lo_creat', 'lo_create', 'lo_open', 'lo_close', 'lo_unlink',
                  'lo_lseek', 'lo_lseek64', 'lo_tell', 'lo_tell64',
                  'lo_truncate', 'lo_truncate64', 'loread', 'lowrite')
ORDER BY 1;


-- begin-expected
-- columns: r
-- row: 2
-- end-expected
SELECT count(*)::int AS r FROM pg_catalog.pg_proc
WHERE proname IN ('lo_lseek64', 'lo_tell64');


-- begin-expected
-- columns: r
-- row: integer, bigint, integer
-- end-expected
SELECT pg_get_function_arguments(oid) AS r FROM pg_catalog.pg_proc
WHERE proname = 'lo_lseek64';


-- begin-expected
-- columns: r
-- row: integer
-- end-expected
SELECT pg_get_function_arguments(oid) AS r FROM pg_catalog.pg_proc
WHERE proname = 'lo_tell64';


-- ============================================================================
-- And what they answer with
-- ============================================================================

-- The object is made in a statement of its own: a large object created inside
-- one is not yet visible to the rest of it.
INSERT INTO lo_ids SELECT lo_from_bytea(0, '\x010203'::bytea);


-- begin-expected
-- columns: r
-- row: \x010203
-- end-expected
SELECT lo_get((SELECT id FROM lo_ids))::text AS r;


-- The 64-bit spellings seek and tell over the same object as the plain ones.
-- begin-expected
-- columns: r
-- row: 0
-- end-expected
SELECT lo_tell64(lo_open((SELECT id FROM lo_ids), 262144)) AS r;

-- begin-expected
-- columns: r
-- row: 3
-- end-expected
SELECT lo_lseek64(lo_open((SELECT id FROM lo_ids), 262144), 0, 2) AS r;

-- begin-expected
-- columns: r
-- row: 2
-- end-expected
SELECT lo_lseek64(lo_open((SELECT id FROM lo_ids), 262144), 2, 0) AS r;

-- begin-expected
-- columns: r
-- row: 0
-- end-expected
SELECT lo_tell(lo_open((SELECT id FROM lo_ids), 262144)) AS r;

-- begin-expected
-- columns: r
-- row: 3
-- end-expected
SELECT lo_lseek(lo_open((SELECT id FROM lo_ids), 262144), 0, 2) AS r;


-- begin-expected
-- columns: r
-- row: bigint
-- end-expected
SELECT pg_typeof(lo_tell64(lo_open((SELECT id FROM lo_ids), 262144)))::text AS r;

-- begin-expected
-- columns: r
-- row: bigint
-- end-expected
SELECT pg_typeof(lo_lseek64(lo_open((SELECT id FROM lo_ids), 262144), 0, 2))::text AS r;

-- begin-expected
-- columns: r
-- row: integer
-- end-expected
SELECT pg_typeof(lo_tell(lo_open((SELECT id FROM lo_ids), 262144)))::text AS r;

-- begin-expected
-- columns: r
-- row: integer
-- end-expected
SELECT pg_typeof(lo_open((SELECT id FROM lo_ids), 262144))::text AS r;

-- begin-expected
-- columns: r
-- row: bytea
-- end-expected
SELECT pg_typeof(lo_get((SELECT id FROM lo_ids)))::text AS r;


-- ============================================================================
-- Writing through the calls a driver uses
-- ============================================================================

-- begin-expected
-- columns: r
-- row: 5
-- end-expected
SELECT lowrite(lo_open((SELECT id FROM lo_ids), 393216), 'hello'::bytea) AS r;

-- begin-expected
-- columns: r
-- row: \x68656c6c6f
-- end-expected
SELECT lo_get((SELECT id FROM lo_ids))::text AS r;

-- begin-expected
-- columns: r
-- row: f
-- end-expected
SELECT lo_put((SELECT id FROM lo_ids), 0, 'HE'::bytea) IS NULL AS r;

-- begin-expected
-- columns: r
-- row: \x48456c6c6f
-- end-expected
SELECT lo_get((SELECT id FROM lo_ids))::text AS r;

-- begin-expected
-- columns: r
-- row: \x4845
-- end-expected
SELECT loread(lo_open((SELECT id FROM lo_ids), 262144), 2)::text AS r;


-- begin-expected
-- columns: r
-- row: integer
-- end-expected
SELECT pg_typeof(lo_unlink((SELECT id FROM lo_ids)))::text AS r;


DROP TABLE IF EXISTS lo_ids;


-- ============================================================================
-- An oid is not the number it is carried as
-- ============================================================================

-- begin-expected
-- columns: r
-- row: regclass
-- end-expected
SELECT pg_typeof(to_regclass('pg_class'))::text AS r;

-- begin-expected
-- columns: r
-- row: regtype
-- end-expected
SELECT pg_typeof(to_regtype('integer'))::text AS r;

-- begin-expected
-- columns: r
-- row: regproc
-- end-expected
SELECT pg_typeof(to_regproc('abs'))::text AS r;

-- begin-expected
-- columns: r
-- row: oid
-- end-expected
SELECT pg_typeof(pg_my_temp_schema())::text AS r;

-- begin-expected
-- columns: r
-- row: regtype
-- end-expected
SELECT pg_typeof(pg_typeof(1))::text AS r;


-- What the value really can witness is left to the value.
-- begin-expected
-- columns: r
-- row: integer
-- end-expected
SELECT pg_typeof(pg_backend_pid())::text AS r;

-- begin-expected
-- columns: r
-- row: integer
-- end-expected
SELECT pg_typeof(abs(-1))::text AS r;

-- begin-expected
-- columns: r
-- row: integer
-- end-expected
SELECT pg_typeof(length('ab'))::text AS r;

-- begin-expected
-- columns: r
-- row: timestamp with time zone
-- end-expected
SELECT pg_typeof(now())::text AS r;

-- begin-expected
-- columns: r
-- row: name
-- end-expected
SELECT pg_typeof(current_database())::text AS r;

-- begin-expected
-- columns: r
-- row: text
-- end-expected
SELECT pg_typeof(version())::text AS r;

