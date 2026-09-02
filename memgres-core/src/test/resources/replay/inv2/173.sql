-- source: investigation-2026-08.md
-- finding: 173
-- title: an operator class named on an index key is read as a single token and resolved against a static map of built-in names that CREATE OPERATOR CLASS never adds to
-- begin-expected
-- ok: 0
-- end-expected
CREATE OPERATOR CLASS zz_oc FOR TYPE int4 USING btree AS
  OPERATOR 1 <, OPERATOR 3 =, FUNCTION 1 btint4cmp(int4, int4);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_it" does not exist
-- end-expected-error
CREATE INDEX zz_i ON zz_it (i zz_oc);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_ix (a int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE INDEX zz_i2 ON zz_ix (a pg_catalog.int4_ops);
