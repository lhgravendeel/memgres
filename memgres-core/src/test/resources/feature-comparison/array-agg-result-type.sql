-- array_agg element typing: text, int, and custom-enum elements round-trip with the correct
-- array values (and, at the wire level, the correct array type OIDs).
--
-- Regression coverage for mtask-8 Wave 5 (Group 9): ExprEvaluator.inferTypeFromContext hardcoded
-- DataType.INT4_ARRAY for every array_agg result, so a text-element array_agg (the app's
-- `array_agg(DISTINCT provider_id)` feeding battery_sets_aggregated.supported_providers)
-- advertised _int4: pgjdbc's text mode failed `Bad value for type int : tennet`, and binary mode
-- (int4[] is in pgjdbc's binary-transfer set) decoded the text payload as a binary int4 array,
-- with a garbage dimension count triggering OutOfMemoryError at PgArray.readBinaryResultSet.
-- The advertised-OID side (RowDescription _text/_int4/enum-array OID, both transfer modes, the
-- scalar-subquery shape) is exercised by ArrayAggResultTypeTest via real pgjdbc; this file covers
-- the SQL-visible value semantics a PG-vs-Memgres diff run would also want to confirm.
-- (pg_typeof(array_agg(...)) still reports text[] for int/enum elements in memgres — a separate,
-- pre-existing pg_typeof-over-aggregates gap, deliberately not covered here.)

-- setup
CREATE TABLE aacs_prov (battery_set_id int, provider_id text);
INSERT INTO aacs_prov VALUES (101, 'tennet'), (101, 'frank');
CREATE TABLE aacs_nums (id int, n int);
INSERT INTO aacs_nums VALUES (1, 7), (1, 42);
CREATE TYPE aacs_kind AS ENUM ('alpha', 'beta', 'gamma');
CREATE TABLE aacs_kinds (id int, kind aacs_kind);
INSERT INTO aacs_kinds VALUES (1, 'alpha'), (1, 'beta');

-- stmt 1: array_agg over text elements
-- begin-expected
-- columns: array_agg
-- row: {frank,tennet}
-- end-expected
SELECT array_agg(provider_id ORDER BY provider_id) FROM aacs_prov;

-- stmt 2: array_agg over int elements
-- begin-expected
-- columns: array_agg
-- row: {7,42}
-- end-expected
SELECT array_agg(n ORDER BY n) FROM aacs_nums;

-- stmt 3: array_agg over custom enum elements
-- begin-expected
-- columns: array_agg
-- row: {alpha,beta}
-- end-expected
SELECT array_agg(kind ORDER BY kind) FROM aacs_kinds;

-- cleanup
DROP TABLE aacs_kinds;
DROP TYPE aacs_kind;
DROP TABLE aacs_nums;
DROP TABLE aacs_prov;
