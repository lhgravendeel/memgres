-- source: investigation.md
-- finding: 70
-- title: `ALTER` on a nonexistent object succeeds for most object kinds ⚠️ (13 cases)
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "dz_nosuch" does not exist
-- end-expected-error
ALTER SCHEMA dz_nosuch RENAME TO other;
-- PG: 3F000 | mg: OK
-- begin-expected-error
-- sqlstate: 42883
-- message-like: aggregate dz_nosuch(integer) does not exist
-- end-expected-error
ALTER AGGREGATE dz_nosuch(integer) RENAME TO o;
-- PG: 42883 | mg: OK
-- begin-expected-error
-- sqlstate: 42704
-- message-like: collation "dz_nosuch" for encoding "UTF8" does not exist
-- end-expected-error
ALTER COLLATION dz_nosuch RENAME TO o;
-- PG: 42704 | mg: OK
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "t" does not exist
-- end-expected-error
ALTER RULE dz_nosuch ON t RENAME TO o;
-- PG: 42704 | mg: OK
-- begin-expected-error
-- sqlstate: 42704
-- message-like: conversion "dz_nosuch" does not exist
-- end-expected-error
ALTER CONVERSION dz_nosuch RENAME TO o;
-- PG: 42704 | mg: OK
-- begin-expected-error
-- sqlstate: 42704
-- message-like: language "dz_nosuch" does not exist
-- end-expected-error
ALTER LANGUAGE dz_nosuch OWNER TO CURRENT_USER;
-- PG: 42704 | mg: OK
-- begin-expected-error
-- sqlstate: 42704
-- message-like: tablespace "dz_nosuch" does not exist
-- end-expected-error
ALTER TABLESPACE dz_nosuch RENAME TO o;
-- PG: 42704 | mg: OK
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "a" does not exist
-- end-expected-error
ALTER SCHEMA a RENAME TO b;
-- b exists; PG: 42P06 | mg: OK
-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "a" does not exist
-- end-expected-error
ALTER ROLE a RENAME TO b;
-- b exists; PG: 42710 | mg: OK
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "a" does not exist
-- end-expected-error
ALTER TYPE a RENAME TO b;
-- b exists; PG: 42710 | mg: OK;
