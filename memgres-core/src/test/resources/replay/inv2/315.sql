-- source: investigation-2026-08.md
-- finding: 315
-- title: Unrelated singletons in this area
-- begin-expected
-- ok: 0
-- end-expected
COMMENT ON CAST (int4 AS int8) IS 'c';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "zz_nosuchtype" does not exist
-- end-expected-error
COMMENT ON CAST (int AS zz_nosuchtype) IS 'x';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (id int);
-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zz_t" is not a view
-- end-expected-error
COMMENT ON VIEW zz_t IS 'x';
-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zz_t" is not an index
-- end-expected-error
COMMENT ON INDEX zz_t IS 'x';
-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zz_t" is not a sequence
-- end-expected-error
COMMENT ON SEQUENCE zz_t IS 'x';
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_nosuchrel" does not exist
-- end-expected-error
COMMENT ON VIEW zz_nosuchrel IS 'x';
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_t" already exists
-- end-expected-error
CREATE TABLE zz_t (a int, b int);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: INSERT has more expressions than target columns
-- end-expected-error
INSERT INTO zz_t SELECT g, g%3 FROM generate_series(1,100) g;
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "a" of relation "zz_t" does not exist
-- end-expected-error
ANALYZE zz_t (a);
-- begin-expected
-- columns: attname:text
-- rowcount: 0
-- end-expected
SELECT attname::text FROM pg_stats WHERE tablename='zz_t' ORDER BY 1;
-- begin-expected
-- ok: 0
-- end-expected
VACUUM (ONLY_DATABASE_STATS);
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ONLY_DATABASE_STATS cannot be specified with a list of tables
-- end-expected-error
VACUUM (ONLY_DATABASE_STATS) zz_t;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_t" already exists
-- end-expected-error
CREATE TABLE zz_t (id int);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: index_cleanup requires a Boolean value
-- end-expected-error
VACUUM (INDEX_CLEANUP BOGUS) zz_t;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: parallel option requires a value between 0 and 1024
-- end-expected-error
VACUUM (PARALLEL) zz_t;
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: VACUUM FULL cannot be performed in parallel
-- end-expected-error
VACUUM (PARALLEL 2, FULL) zz_t;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: BUFFER_USAGE_LIMIT option must be 0 or between 128 kB and 16777216 kB
-- end-expected-error
VACUUM (BUFFER_USAGE_LIMIT '1 kB') zz_t;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: BUFFER_USAGE_LIMIT option must be 0 or between 128 kB and 16777216 kB
-- end-expected-error
VACUUM (BUFFER_USAGE_LIMIT '99 TB') zz_t;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ")"
-- end-expected-error
VACUUM () zz_t;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_t" already exists
-- end-expected-error
CREATE TABLE zz_t (a int, b int);
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" of relation "zz_t" does not exist
-- end-expected-error
ANALYZE zz_t (nosuchcol);
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "a" of relation "zz_t" does not exist
-- end-expected-error
ANALYZE zz_t (a, nosuchcol);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ")"
-- end-expected-error
ANALYZE zz_t ();
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_t" already exists
-- end-expected-error
CREATE TABLE zz_t (id int PRIMARY KEY);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: unrecognized REINDEX option "bogus"
-- end-expected-error
REINDEX (BOGUS) TABLE zz_t;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ")"
-- end-expected-error
REINDEX () TABLE zz_t;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: tablespace "zz_nots" does not exist
-- end-expected-error
REINDEX (TABLESPACE zz_nots) TABLE zz_t;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_t" already exists
-- end-expected-error
CREATE TABLE zz_t (id int PRIMARY KEY);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "zz_t"
-- end-expected-error
REINDEX zz_t;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
REINDEX TABLE;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ","
-- end-expected-error
REINDEX TABLE zz_t, zz_t;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "ONLY"
-- end-expected-error
REINDEX TABLE ONLY zz_t;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ","
-- end-expected-error
REINDEX SCHEMA public, public;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "FAST"
-- end-expected-error
CHECKPOINT FAST;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "("
-- end-expected-error
CHECKPOINT (MODE FAST);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "("
-- end-expected-error
CHECKPOINT (BOGUS);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "zz_t"
-- end-expected-error
CHECKPOINT zz_t;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_t" already exists
-- end-expected-error
CREATE TABLE zz_t (a int);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "FULL"
-- end-expected-error
VACUUM FREEZE FULL zz_t;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "VERBOSE"
-- end-expected-error
VACUUM ANALYZE VERBOSE zz_t;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "FREEZE"
-- end-expected-error
VACUUM FULL VERBOSE FREEZE ANALYZE zz_t;
