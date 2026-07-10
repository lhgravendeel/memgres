-- Arrays of custom enum types: select value round-trip, unnest, and membership checks.
--
-- Regression coverage for mtask-8 Wave 4 (Group 8): reading an array-of-custom-enum column via
-- real pgjdbc previously failed with `PSQLException: No results were returned by the query` from
-- TypeInfoCache.getArrayDelimiter, because memgres advertised the enum ELEMENT type's own OID for
-- the ARRAY column too (indistinguishable Column metadata), instead of a distinct array-type OID
-- with a matching pg_type row. That specific client-side wire/OID failure is a JDBC-level defect
-- (rs.getArray()/Array.getResultSet()/ResultSetMetaData.getColumnTypeName(), not a SQL-visible
-- value difference), so it is exercised directly by EnumArrayColumnOidResolutionTest via real
-- pgjdbc in both text and binary transfer modes; this file covers the SQL-visible value semantics
-- (array values round-trip, unnest, membership) that a PG-vs-Memgres diff run would also want to
-- confirm.

-- setup
CREATE TYPE eacs_region AS ENUM ('north', 'south', 'east', 'west');
CREATE TABLE eacs_sellers (id serial PRIMARY KEY, name text, regions eacs_region[]);
INSERT INTO eacs_sellers (name, regions) VALUES ('acme', ARRAY['north','south']::eacs_region[]);
INSERT INTO eacs_sellers (name, regions) VALUES ('globex', ARRAY['east']::eacs_region[]);

-- stmt 1: enum array value round-trips as its PG array-literal text
-- begin-expected
-- columns: name | regions
-- row: acme, {north,south}
-- end-expected
SELECT name, regions FROM eacs_sellers WHERE id = 1;

-- stmt 2: unnest an enum array column
-- begin-expected
-- columns: unnest
-- row: north
-- row: south
-- end-expected
SELECT unnest(regions) FROM eacs_sellers WHERE id = 1;

-- stmt 3: membership check against an enum array column
-- begin-expected
-- columns: name
-- row: globex
-- end-expected
SELECT name FROM eacs_sellers WHERE 'east' = ANY(regions);

-- cleanup
DROP TABLE eacs_sellers;
DROP TYPE eacs_region;
