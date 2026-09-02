-- source: investigation-2026-08.md
-- finding: 308
-- title: VACUUM, ANALYZE, REINDEX, CLUSTER and CHECKPOINT have no AST: each is parsed into a two-string SetStmt(name, "encoded value") and taken apart again with split/s
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (id int, v text);
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
-- begin-expected
-- ok: 0
-- end-expected
ANALYZE (VERBOSE, SKIP_LOCKED) zz_t (id);
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_t" already exists
-- end-expected-error
CREATE TABLE zz_t (id int PRIMARY KEY, v text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE INDEX zz_ix ON zz_t (v);
-- begin-expected
-- ok: 0
-- end-expected
CLUSTER (VERBOSE) zz_t USING zz_ix;
-- begin-expected
-- ok: 0
-- end-expected
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
