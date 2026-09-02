-- source: investigation.md
-- finding: 14
-- title: Statement-level guards missing
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
SELECT DISTINCT ON (n) n FROM t ORDER BY id;
-- PG: 42P10 ON expressions must match ORDER BY | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
SELECT n FROM t GROUP BY n FOR UPDATE;
-- PG: 0A000 FOR UPDATE with GROUP BY | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
SELECT count(*) FROM t FOR UPDATE;
-- PG: 0A000 | mg: OK
-- begin-expected-error
-- sqlstate: 25P01
-- message-like: SAVEPOINT can only be used in transaction blocks
-- end-expected-error
SAVEPOINT sp1;
-- outside a transaction; PG: 25P01 | mg: OK
-- begin-expected-error
-- sqlstate: 25P01
-- message-like: RELEASE SAVEPOINT can only be used in transaction blocks
-- end-expected-error
RELEASE SAVEPOINT nosuch;
-- PG: 25P01 (no transaction) | mg: 3B001
-- begin-expected-error
-- sqlstate: 22023
-- message-like: unrecognized encoding: "nosuch"
-- end-expected-error
SELECT encode('a'::bytea, 'nosuch');
-- PG: 22023 | mg: OK
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type numeric: " "
-- end-expected-error
SELECT to_number('abc', '9999');
-- PG: 22P02 | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "nosuchschema.t" does not exist
-- end-expected-error
SELECT * FROM nosuchschema.t;
-- PG: 42P01 | mg: 3F000;
