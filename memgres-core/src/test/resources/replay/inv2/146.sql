-- source: investigation-2026-08.md
-- finding: 146
-- title: Sequence session state and catalog rows are held outside the Sequence object: currval/lastval live in a map keyed by name that DROP and DISCARD never clear, pg_
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_s;
-- begin-expected
-- columns: nextval:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT nextval('zz_s');
-- begin-expected
-- ok: 0
-- end-expected
DROP SEQUENCE zz_s;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_s;
-- begin-expected-error
-- sqlstate: 55000
-- message-like: currval of sequence "zz_s" is not yet defined in this session
-- end-expected-error
SELECT currval('zz_s');
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_s" already exists
-- end-expected-error
CREATE SEQUENCE zz_s CACHE 7;
-- begin-expected
-- columns: seqcache:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT seqcache FROM pg_sequence WHERE seqrelid='zz_s'::regclass;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_s" already exists
-- end-expected-error
CREATE UNLOGGED SEQUENCE zz_s;
-- begin-expected
-- columns: relpersistence:char
-- row: p
-- rowcount: 1
-- end-expected
SELECT relpersistence FROM pg_class WHERE relname='zz_s';
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_s" already exists
-- end-expected-error
CREATE SEQUENCE zz_s CACHE 5 CYCLE;
-- begin-expected
-- columns: nextval:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT nextval('zz_s');
-- begin-expected
-- columns: last_value:int8 | is_called:bool
-- row: 1 | t
-- rowcount: 1
-- end-expected
SELECT last_value, is_called FROM zz_s;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: conflicting or redundant options
-- end-expected-error
CREATE SEQUENCE zz_s START 1 START 2;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_s" already exists
-- end-expected-error
CREATE SEQUENCE zz_s;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
ALTER SEQUENCE zz_s;
