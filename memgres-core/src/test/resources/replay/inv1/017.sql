-- source: investigation.md
-- finding: 17
-- title: Index definition options are unvalidated
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
CREATE INDEX i ON t USING hash (a, b);
-- PG: 0A000 no multicolumn hash | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
CREATE UNIQUE INDEX i ON t USING hash (a);
-- PG: 0A000 no unique hash      | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
CREATE INDEX i ON t USING hash (a) INCLUDE (b);
-- PG: 0A000 no included columns | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
CREATE INDEX i ON t USING hash (a DESC);
-- PG: 0A000 no ASC/DESC         | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
CREATE INDEX i ON t USING nosuchmethod (a);
-- PG: 42704 no such AM          | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
CREATE INDEX i ON t (txt bogus_ops);
-- PG: 42704 no such opclass     | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
CREATE INDEX i ON t (i text_pattern_ops);
-- PG: 42804 wrong type for opclass | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
CREATE INDEX i ON t (j);
-- j is json; PG: 42704 no btree opclass | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
CREATE INDEX i ON t (i COLLATE "C");
-- PG: 42804 int has no collation | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
INSERT INTO t VALUES (1),(1);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
CREATE UNIQUE INDEX u ON t (v);
-- PG: 23505 could not create unique index | mg: OK;
