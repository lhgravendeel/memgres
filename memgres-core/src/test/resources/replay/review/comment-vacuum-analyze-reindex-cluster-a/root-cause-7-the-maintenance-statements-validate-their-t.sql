-- source: review-2026-08.md
-- finding: Root cause 7: the maintenance statements validate their target with resolveTable and nothing else
-- area: COMMENT, VACUUM, ANALYZE, REINDEX, CLUSTER and the SET family
-- title: Root cause 7: the maintenance statements validate their target with resolveTable and nothing else
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (id int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE MATERIALIZED VIEW zz_m AS SELECT id FROM zz_t;
-- begin-expected
-- ok: 0
-- end-expected
VACUUM zz_m;
-- begin-expected
-- ok: 0
-- end-expected
ANALYZE zz_m;
-- begin-expected
-- ok: 0
-- end-expected
REINDEX TABLE zz_m;
-- begin-expected
-- ok: 0
-- end-expected
VACUUM pg_class;
-- begin-expected
-- ok: 0
-- end-expected
ANALYZE pg_class;
-- begin-expected
-- ok: 0
-- end-expected
ANALYZE pg_catalog.pg_class;
-- begin-expected
-- ok: 0
-- end-expected
REINDEX TABLE pg_am;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_t" already exists
-- end-expected-error
CREATE TABLE zz_t (id int PRIMARY KEY, v text);
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "v" does not exist
-- end-expected-error
CREATE INDEX zz_ix ON zz_t (v);
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_v AS SELECT id FROM zz_t;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_nosuch" does not exist
-- end-expected-error
REINDEX INDEX zz_nosuch;
-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zz_t" is not an index
-- end-expected-error
REINDEX INDEX zz_t;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_ix" does not exist
-- end-expected-error
REINDEX TABLE zz_ix;
-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zz_v" is not a table or materialized view
-- end-expected-error
REINDEX TABLE zz_v;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_v" already exists
-- end-expected-error
CREATE VIEW zz_v AS SELECT id FROM zz_t;
-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zz_v" is not a table or materialized view
-- end-expected-error
CLUSTER zz_v;
-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zz_v" is not a table or materialized view
-- end-expected-error
CLUSTER zz_v USING zz_ix;
