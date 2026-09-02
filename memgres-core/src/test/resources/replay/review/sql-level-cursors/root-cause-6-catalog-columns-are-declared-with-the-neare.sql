-- source: review-2026-08.md
-- finding: Root cause 6: catalog columns are declared with the nearest ordinary SQL type instead of the catalog's real type
-- area: SQL-level cursors
-- title: Root cause 6: catalog columns are declared with the nearest ordinary SQL type instead of the catalog's real type
-- begin-expected
-- columns: pg_typeof:regtype | pg_typeof:regtype | pg_typeof:regtype
-- row: oid | oid | name
-- rowcount: 1
-- end-expected
SELECT pg_typeof(oid), pg_typeof(datdba), pg_typeof(datname) FROM pg_database WHERE datname='template1';
-- begin-expected
-- columns: pg_typeof:regtype | pg_typeof:regtype | pg_typeof:regtype | pg_typeof:regtype | pg_typeof:regtype
-- row: oid | name | oid | oid[] | text[]
-- rowcount: 1
-- end-expected
SELECT pg_typeof(oid), pg_typeof(extname), pg_typeof(extowner), pg_typeof(extconfig), pg_typeof(extcondition)
  FROM pg_extension WHERE extname='plpgsql';
-- begin-expected
-- columns: pg_typeof:regtype | pg_typeof:regtype | pg_typeof:regtype
-- row: oid | name | oid
-- rowcount: 1
-- end-expected
SELECT pg_typeof(oid), pg_typeof(lanname), pg_typeof(lanplcallfoid) FROM pg_language WHERE lanname='plpgsql';
-- begin-expected
-- columns: pg_typeof:regtype | pg_typeof:regtype | pg_typeof:regtype
-- row: oid | oid | "char"
-- rowcount: 1
-- end-expected
SELECT pg_typeof(oid), pg_typeof(amopfamily), pg_typeof(amoppurpose) FROM pg_amop LIMIT 1;
-- begin-expected
-- columns: pg_typeof:regtype
-- row: text[]
-- rowcount: 1
-- end-expected
SELECT pg_typeof(rolconfig) FROM pg_roles WHERE rolname='pg_monitor';
