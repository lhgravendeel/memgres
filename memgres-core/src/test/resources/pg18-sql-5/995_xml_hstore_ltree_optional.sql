DROP SCHEMA IF EXISTS test_995 CASCADE;
CREATE SCHEMA test_995;
SET search_path TO test_995;

CREATE EXTENSION IF NOT EXISTS hstore;
CREATE EXTENSION IF NOT EXISTS ltree;

-- hstore and ltree types are not available in memgres.
-- The CREATE TABLE using these types will fail.

-- begin-expected-error
-- message-like: does not exist
-- end-expected-error
CREATE TABLE docs (
    doc_id integer PRIMARY KEY,
    attrs hstore NOT NULL,
    path ltree NOT NULL,
    payload xml NOT NULL
);
