\echo '=== 12_pg_specific.sql ==='
\set VERBOSITY verbose
\set SHOW_CONTEXT never
\set ON_ERROR_STOP off
SET search_path = pg_catalog, public;
DROP SCHEMA IF EXISTS compat CASCADE;
CREATE SCHEMA compat;
SET search_path = compat, pg_catalog;

-- PostgreSQL-specific or commonly nonportable behavior
CREATE TYPE status_enum AS ENUM ('new', 'open', 'closed');
CREATE TABLE pg_only(
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tags text[],
    payload jsonb,
    status status_enum,
    note text
);
INSERT INTO pg_only(tags, payload, status, note)
VALUES
    (ARRAY['a','b'], '{"k":1}', 'new', 'n1'),
    (ARRAY['x'], '{"k":2}', 'open', 'n2');

SELECT id, tags, payload, status, note FROM pg_only ORDER BY id;
SELECT note FROM pg_only WHERE payload @> '{"k":1}'::jsonb;
SELECT note FROM pg_only WHERE tags @> ARRAY['a'];
SELECT note FROM pg_only WHERE status = 'open';
SELECT * FROM pg_only WHERE note ILIKE 'N%';
SELECT DISTINCT ON (status) status, id, note FROM pg_only ORDER BY status, id;
SELECT unnest(tags) FROM pg_only WHERE id = 1 ORDER BY 1;
SELECT generate_series(1, 3) AS gs;
SELECT uuidv7();
SELECT crc32('abc');
SELECT array_sort(ARRAY[3,1,2]);
SELECT array_reverse(ARRAY[1,2,3]);

-- nonportable / PG-specific errors
SELECT DISTINCT ON () * FROM pg_only;
SELECT unnest(1);
SELECT generate_series('a', 'z');
SELECT uuidv7(1);
SELECT crc32(123);
SELECT array_sort(1);
SELECT payload ->> 1 FROM pg_only;
SELECT tags @> 'a' FROM pg_only;

DROP SCHEMA compat CASCADE;
