-- Optional: requires PostGIS
DROP SCHEMA IF EXISTS test_996 CASCADE;
CREATE SCHEMA test_996;
SET search_path TO test_996;

CREATE EXTENSION IF NOT EXISTS postgis;

-- PostGIS geometry type is not available in memgres.

-- begin-expected-error
-- message-like: does not exist
-- end-expected-error
CREATE TABLE places (
    place_id integer PRIMARY KEY,
    name text NOT NULL,
    geom geometry(Point, 4326) NOT NULL
);
