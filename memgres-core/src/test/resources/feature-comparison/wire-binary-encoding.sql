-- wire-binary-encoding.sql
-- Verification file for C8, H8, H9, H10, H11, M16, M17, L4

-- === C8: infinity sentinel dates/timestamps ===
SELECT 'infinity'::date AS d; -- expect: infinity
SELECT '-infinity'::date AS d; -- expect: -infinity
SELECT 'infinity'::timestamp AS ts; -- expect: infinity
SELECT '-infinity'::timestamp AS ts; -- expect: -infinity

-- === H9: numeric NaN and negative dscale ===
SELECT 'NaN'::numeric AS n; -- expect: NaN
SELECT 1234500::numeric(10,-2) AS n; -- expect: 1234500

-- === H10: array type OIDs ===
SELECT ARRAY[1,2,3]::int[] AS a; -- expect: {1,2,3}
SELECT ARRAY['a','b']::text[] AS a; -- expect: {a,b}
SELECT ARRAY[true,false]::bool[] AS a; -- expect: {t,f}

-- === L4: OID type ===
SELECT 42::oid AS o; -- expect: 42 (as oid type, not int4)
SELECT pg_typeof(42::oid) AS t; -- expect: oid

-- === M17: SELECT * metadata ===
CREATE TABLE m17_t(id serial PRIMARY KEY, name text NOT NULL);
INSERT INTO m17_t(name) VALUES ('test');
SELECT * FROM m17_t; -- expect: table OID and attnum in RowDescription
SELECT id, name FROM m17_t; -- expect: same metadata as above
DROP TABLE m17_t;
