-- source: review-2026-08.md
-- finding: Root cause 6: pg_attribute is a projection of the live column list
-- area: System catalogs, information_schema and security
-- title: Root cause 6: pg_attribute is a projection of the live column list
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (a int);
-- begin-expected
-- columns: attname:name | attnum:int2
-- row: tableoid | -6
-- row: cmax | -5
-- row: xmax | -4
-- row: cmin | -3
-- row: xmin | -2
-- row: ctid | -1
-- rowcount: 6
-- end-expected
SELECT attname, attnum FROM pg_attribute WHERE attrelid='zz_t'::regclass AND attnum < 0 ORDER BY attnum;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_m (a int, b text, c int);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_m DROP COLUMN b;
-- begin-expected
-- columns: attname:name | attnum:int2 | attisdropped:text
-- row: a | 1 | false
-- row: ........pg.dropped.2........ | 2 | true
-- row: c | 3 | false
-- rowcount: 3
-- end-expected
SELECT attname, attnum, attisdropped::text FROM pg_attribute
 WHERE attrelid='zz_m'::regclass AND attnum>0 ORDER BY attnum;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_c (i int, s text, v varchar(10), n name);
-- begin-expected
-- columns: attname:name | attcollation:oid
-- row: i | 0
-- row: s | 100
-- row: v | 100
-- row: n | 950
-- rowcount: 4
-- end-expected
SELECT attname, attcollation FROM pg_attribute WHERE attrelid='zz_c'::regclass AND attnum>0 ORDER BY attnum;
