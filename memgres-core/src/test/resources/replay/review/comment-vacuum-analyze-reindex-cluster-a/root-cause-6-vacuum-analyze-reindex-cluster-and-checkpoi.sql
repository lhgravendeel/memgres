-- source: review-2026-08.md
-- finding: Root cause 6: VACUUM, ANALYZE, REINDEX, CLUSTER and CHECKPOINT are parsed into a name and an encoded string
-- area: COMMENT, VACUUM, ANALYZE, REINDEX, CLUSTER and the SET family
-- title: Root cause 6: VACUUM, ANALYZE, REINDEX, CLUSTER and CHECKPOINT are parsed into a name and an encoded string
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (a int, b int);
-- begin-expected
-- ok: 100
-- end-expected
INSERT INTO zz_t SELECT g, g%3 FROM generate_series(1,100) g;
-- begin-expected
-- ok: 0
-- end-expected
ANALYZE zz_t (a);
-- begin-expected
-- columns: attname:text
-- row: a
-- rowcount: 1
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
-- begin-expected
-- ok: 0
-- end-expected
ANALYZE (VERBOSE) zz_t;
-- begin-expected
-- ok: 0
-- end-expected
ANALYZE (SKIP_LOCKED) zz_t;
-- begin-expected
-- ok: 0
-- end-expected
ANALYZE (VERBOSE FALSE) zz_t;
-- begin-expected
-- ok: 0
-- end-expected
ANALYZE (BUFFER_USAGE_LIMIT '256 kB') zz_t;
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "id" of relation "zz_t" does not exist
-- end-expected-error
ANALYZE (VERBOSE, SKIP_LOCKED) zz_t (id);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: index "zz_ix" for table "zz_t" does not exist
-- end-expected-error
CLUSTER (VERBOSE) zz_t USING zz_ix;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: index "zz_ix" for table "zz_t" does not exist
-- end-expected-error
CLUSTER zz_ix ON zz_t;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ","
-- end-expected-error
CLUSTER zz_t, zz_t;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "USING"
-- end-expected-error
CLUSTER USING zz_ix ON zz_t;
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
-- sqlstate: 42703
-- message-like: column "nosuchcol" of relation "zz_t" does not exist
-- end-expected-error
ANALYZE zz_t (nosuchcol);
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "id" of relation "zz_t" does not exist
-- end-expected-error
ANALYZE zz_t (id, nosuchcol);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ")"
-- end-expected-error
ANALYZE zz_t ();
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
