-- source: review-2026-08.md
-- finding: Root cause 6: the catalog rows that describe a column's type, collation and partition opclass are written as constants
-- area: User-defined casts, operators, aggregates, collations and extensions
-- title: Root cause 6: the catalog rows that describe a column's type, collation and partition opclass are written as constants
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_cc (c_coll text COLLATE "C");
-- begin-expected
-- columns: ?column?:bool | collname:name
-- row: t | C
-- rowcount: 1
-- end-expected
SELECT a.attcollation <> 0, (SELECT collname FROM pg_collation WHERE oid=a.attcollation)
FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid WHERE c.relname='zz_cc' AND a.attname='c_coll';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_ct20 AS (street text, city text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t20 (c_comp zz_ct20);
-- begin-expected
-- columns: typname:name | format_type:text
-- row: zz_ct20 | zz_ct20
-- rowcount: 1
-- end-expected
SELECT t.typname, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a
 JOIN pg_class c ON c.oid=a.attrelid JOIN pg_type t ON t.oid=a.atttypid
 WHERE c.relname='zz_t20' AND a.attname='c_comp';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_p25 (id integer NOT NULL, ts date NOT NULL) PARTITION BY RANGE (ts);
-- begin-expected
-- columns: partclass:text
-- row: 3122
-- rowcount: 1
-- end-expected
SELECT partclass::text FROM pg_partitioned_table WHERE partrelid='zz_p25'::regclass;
