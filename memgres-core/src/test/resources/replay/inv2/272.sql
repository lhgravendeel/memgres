-- source: investigation-2026-08.md
-- finding: 272
-- title: pg_attribute is projected from the table's live column list, one row per current user column. It therefore has no negative attnums for the system columns, no to
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
-- columns: count:int8
-- row: 6
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_attribute WHERE attrelid='pg_class'::regclass AND attnum < 0;
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
SELECT attname, attnum, attisdropped::text FROM pg_attribute WHERE attrelid='zz_m'::regclass AND attnum>0 ORDER BY attnum;
-- begin-expected
-- columns: relnatts:int2
-- row: 3
-- rowcount: 1
-- end-expected
SELECT relnatts FROM pg_class WHERE relname='zz_m';
-- begin-expected
-- columns: column_name:name | ordinal_position:int4
-- row: a | 1
-- row: c | 3
-- rowcount: 2
-- end-expected
SELECT column_name, ordinal_position FROM information_schema.columns WHERE table_name='zz_m' ORDER BY ordinal_position;
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_m ADD COLUMN d int;
-- begin-expected
-- columns: attnum:int2
-- row: 4
-- rowcount: 1
-- end-expected
SELECT attnum FROM pg_attribute WHERE attrelid='zz_m'::regclass AND attname='d';
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
