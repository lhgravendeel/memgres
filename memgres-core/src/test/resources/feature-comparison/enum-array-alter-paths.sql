-- Enum-array columns created through ALTER TABLE ADD COLUMN and ALTER COLUMN TYPE must carry
-- their enum identity and array-ness, exactly like CREATE TABLE columns (see
-- enum-array-column-select.sql for the CREATE path).

-- setup
CREATE TYPE eaap_region AS ENUM ('north', 'south', 'east');
CREATE TABLE eaap_t (id int PRIMARY KEY);
ALTER TABLE eaap_t ADD COLUMN regions eaap_region[];
INSERT INTO eaap_t VALUES (1, '{north,south}');

-- stmt 1: ALTER-added enum[] column round-trips its literal text
-- begin-expected
-- columns: regions
-- row: {north,south}
-- end-expected
SELECT regions FROM eaap_t;

-- stmt 2: unnest over the ALTER-added column, ordered by declared enum order
-- begin-expected
-- columns: r
-- row: north
-- row: south
-- end-expected
SELECT unnest(regions) AS r FROM eaap_t ORDER BY 1;

-- stmt 3: ANY() membership against the ALTER-added column
-- begin-expected
-- columns: m
-- row: t
-- end-expected
SELECT 'south' = ANY(regions) AS m FROM eaap_t;

-- setup: retype an existing text column to the enum array
CREATE TABLE eaap_t2 (id int, regions text);
INSERT INTO eaap_t2 VALUES (1, '{east,north}');
ALTER TABLE eaap_t2 ALTER COLUMN regions TYPE eaap_region[] USING regions::eaap_region[];

-- stmt 4: the retyped column round-trips its literal text
-- begin-expected
-- columns: regions
-- row: {east,north}
-- end-expected
SELECT regions FROM eaap_t2;

-- stmt 5: unnest over the retyped column, ordered by declared enum order (north before east)
-- begin-expected
-- columns: r
-- row: north
-- row: east
-- end-expected
SELECT unnest(regions) AS r FROM eaap_t2 ORDER BY 1;

-- cleanup
DROP TABLE eaap_t;
DROP TABLE eaap_t2;
DROP TYPE eaap_region;
