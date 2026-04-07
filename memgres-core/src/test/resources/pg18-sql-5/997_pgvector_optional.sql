-- Optional: requires pgvector
DROP SCHEMA IF EXISTS test_997 CASCADE;
CREATE SCHEMA test_997;
SET search_path TO test_997;

CREATE EXTENSION IF NOT EXISTS vector;

-- pgvector type is not available in memgres.

-- begin-expected-error
-- message-like: does not exist
-- end-expected-error
CREATE TABLE embeddings (
    item_id integer PRIMARY KEY,
    label text NOT NULL,
    emb vector(3) NOT NULL
);
