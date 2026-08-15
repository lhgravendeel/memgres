-- ============================================================================
-- A dropped column keeps its attribute number
--
-- PostgreSQL does not renumber the columns of a relation when one is dropped.
-- It leaves the attribute in pg_attribute under a placeholder name with
-- attisdropped set, counts it in relnatts, and goes on numbering new columns
-- past it -- so every attribute number a constraint, an index, a default, a
-- comment or a trigger recorded still means the column it meant. Every value
-- here was read off PostgreSQL 18.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- The row the drop leaves behind, and the number it goes on holding
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_t (a int, b varchar(5), c int, d int DEFAULT 7, CHECK (c > 0));
CREATE UNIQUE INDEX zw5x_i ON zw5x_t (c, a);
COMMENT ON COLUMN zw5x_t.d IS 'note on d';
ALTER TABLE zw5x_t DROP COLUMN b;
ALTER TABLE zw5x_t ADD COLUMN e text;

-- begin-expected
-- columns: atts
-- row: a/1/false,........pg.dropped.2......../2/true,c/3/false,d/4/false,e/5/false
-- end-expected
SELECT string_agg(attname || '/' || attnum::text || '/' || attisdropped::text, ',' ORDER BY attnum) AS atts FROM pg_attribute WHERE attrelid = 'zw5x_t'::regclass AND attnum > 0;

-- The tombstone carries no type and the storage the declared type gave it.
-- begin-expected
-- columns: atttypid | atttypmod | attlen | attstorage | attalign
-- row: 0 | 9 | -1 | x | i
-- end-expected
SELECT atttypid::text, atttypmod, attlen, attstorage::text, attalign::text FROM pg_attribute WHERE attrelid = 'zw5x_t'::regclass AND attnum = 2;

-- And is required of nothing.
-- begin-expected
-- columns: attnotnull | atthasdef | attidentity | attgenerated | atthasmissing | attndims
-- row: false | false |  |  | false | 0
-- end-expected
SELECT attnotnull, atthasdef, attidentity::text, attgenerated::text, atthasmissing, attndims FROM pg_attribute WHERE attrelid = 'zw5x_t'::regclass AND attnum = 2;

-- begin-expected
-- columns: relnatts
-- row: 5
-- end-expected
SELECT relnatts FROM pg_class WHERE relname = 'zw5x_t';

-- The catalogue of live columns leaves the hole where the drop was.
-- begin-expected
-- columns: cols
-- row: a:1:1,c:3:3,d:4:4,e:5:5
-- end-expected
SELECT string_agg(column_name || ':' || ordinal_position || ':' || dtd_identifier, ',' ORDER BY ordinal_position) AS cols FROM information_schema.columns WHERE table_name = 'zw5x_t';

-- Everything that recorded an attribute number still names the same column.
-- begin-expected
-- columns: conname | conkey
-- row: zw5x_t_c_check | 3
-- end-expected
SELECT conname, array_to_string(conkey, ' ') AS conkey FROM pg_constraint WHERE conrelid = 'zw5x_t'::regclass ORDER BY conname;

-- begin-expected
-- columns: relname | indkey | indnatts
-- row: zw5x_i | 3 1 | 2
-- end-expected
SELECT c.relname, i.indkey::text AS indkey, i.indnatts FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE i.indrelid = 'zw5x_t'::regclass ORDER BY 1;

-- begin-expected
-- columns: adnum | expr
-- row: 4 | 7
-- end-expected
SELECT adnum, pg_get_expr(adbin, adrelid) AS expr FROM pg_attrdef WHERE adrelid = 'zw5x_t'::regclass ORDER BY adnum;

-- begin-expected
-- columns: objsubid | description
-- row: 4 | note on d
-- end-expected
SELECT objsubid, description FROM pg_description WHERE objoid = 'zw5x_t'::regclass ORDER BY objsubid;

-- begin-expected
-- columns: col_description
-- row: note on d
-- end-expected
SELECT col_description('zw5x_t'::regclass, 4) AS col_description;

-- The relation itself is unchanged: an INSERT with no column list takes four
-- values, not five, because the tombstone is not a column of the relation.
INSERT INTO zw5x_t VALUES (1, 2, 3, 'x');

-- begin-expected
-- columns: a | c | d | e
-- row: 1 | 2 | 3 | x
-- end-expected
SELECT * FROM zw5x_t;

DROP TABLE zw5x_t;

-- ----------------------------------------------------------------------------
-- A foreign key holds numbers on both sides of itself
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_pk (p int, q int, r int UNIQUE);
ALTER TABLE zw5x_pk DROP COLUMN q;
CREATE TABLE zw5x_fk (x int, y int, z int REFERENCES zw5x_pk(r));
ALTER TABLE zw5x_fk DROP COLUMN y;

-- begin-expected
-- columns: conname | conkey | confkey
-- row: zw5x_fk_z_fkey | 3 | 3
-- end-expected
SELECT conname, array_to_string(conkey,' ') AS conkey, array_to_string(confkey,' ') AS confkey FROM pg_constraint WHERE conrelid = 'zw5x_fk'::regclass ORDER BY conname;

DROP TABLE zw5x_fk;
DROP TABLE zw5x_pk;

-- ----------------------------------------------------------------------------
-- A new column takes the next number, not the freed one
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_n (a int, b int, c int, d int);
ALTER TABLE zw5x_n DROP COLUMN b, DROP COLUMN c;
ALTER TABLE zw5x_n ADD COLUMN e int;
ALTER TABLE zw5x_n ADD COLUMN b int;

-- The name b is free again but the number 2 is not.
-- begin-expected
-- columns: atts
-- row: a:1,d:4,e:5,b:6
-- end-expected
SELECT string_agg(attname || ':' || attnum, ',' ORDER BY attnum) AS atts FROM pg_attribute WHERE attrelid = 'zw5x_n'::regclass AND attnum > 0 AND NOT attisdropped;

DROP TABLE zw5x_n;

-- The numbers taken are held in ascending order, not in the order the drops
-- happened.
CREATE TABLE zw5x_m (a int, b int, c int, d int, e int);
ALTER TABLE zw5x_m DROP COLUMN d;
ALTER TABLE zw5x_m DROP COLUMN b;
ALTER TABLE zw5x_m ADD COLUMN f int;

-- begin-expected
-- columns: atts
-- row: a:1,........pg.dropped.2........:2,c:3,........pg.dropped.4........:4,e:5,f:6
-- end-expected
SELECT string_agg(attname || ':' || attnum, ',' ORDER BY attnum) AS atts FROM pg_attribute WHERE attrelid = 'zw5x_m'::regclass AND attnum > 0;

-- begin-expected
-- columns: relnatts
-- row: 6
-- end-expected
SELECT relnatts FROM pg_class WHERE relname = 'zw5x_m';

-- begin-expected
-- columns: cols
-- row: a:1,c:3,e:5,f:6
-- end-expected
SELECT string_agg(column_name || ':' || ordinal_position, ',' ORDER BY ordinal_position) AS cols FROM information_schema.columns WHERE table_name = 'zw5x_m';

DROP TABLE zw5x_m;

-- ----------------------------------------------------------------------------
-- A column added and rolled back gives its number back
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_rb (a int, b int, c int);
BEGIN;
ALTER TABLE zw5x_rb ADD COLUMN d int;
ROLLBACK;
ALTER TABLE zw5x_rb ADD COLUMN e int;

-- begin-expected
-- columns: atts
-- row: a:1,b:2,c:3,e:4
-- end-expected
SELECT string_agg(attname || ':' || attnum, ',' ORDER BY attnum) AS atts FROM pg_attribute WHERE attrelid = 'zw5x_rb'::regclass AND attnum > 0;

-- And a drop that rolled back leaves nothing behind.
BEGIN;
ALTER TABLE zw5x_rb DROP COLUMN b;
ROLLBACK;

-- begin-expected
-- columns: atts
-- row: a:1,b:2,c:3,e:4
-- end-expected
SELECT string_agg(attname || ':' || attnum, ',' ORDER BY attnum) AS atts FROM pg_attribute WHERE attrelid = 'zw5x_rb'::regclass AND attnum > 0;

-- begin-expected
-- columns: relnatts
-- row: 4
-- end-expected
SELECT relnatts FROM pg_class WHERE relname = 'zw5x_rb';

-- The same through a savepoint.
BEGIN;
SAVEPOINT zw5x_sp;
ALTER TABLE zw5x_rb DROP COLUMN b;
ROLLBACK TO SAVEPOINT zw5x_sp;
COMMIT;

-- begin-expected
-- columns: atts
-- row: a:1,b:2,c:3,e:4
-- end-expected
SELECT string_agg(attname || ':' || attnum, ',' ORDER BY attnum) AS atts FROM pg_attribute WHERE attrelid = 'zw5x_rb'::regclass AND attnum > 0;

DROP TABLE zw5x_rb;

-- ----------------------------------------------------------------------------
-- A child and a partition keep the hole their parent's drop made
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_pa (p int, q int, r int);
CREATE TABLE zw5x_ch (s int) INHERITS (zw5x_pa);
ALTER TABLE zw5x_pa DROP COLUMN q;

-- The tombstone keeps the attislocal and attinhcount the column had.
-- begin-expected
-- columns: atts
-- row: p/1/false/false/1,........pg.dropped.2......../2/true/false/1,r/3/false/false/1,s/4/false/true/0
-- end-expected
SELECT string_agg(attname || '/' || attnum::text || '/' || attisdropped::text || '/' || attislocal::text || '/' || attinhcount::text, ',' ORDER BY attnum) AS atts FROM pg_attribute WHERE attrelid = 'zw5x_ch'::regclass AND attnum > 0;

-- begin-expected
-- columns: relnatts
-- row: 4
-- end-expected
SELECT relnatts FROM pg_class WHERE relname = 'zw5x_ch';

DROP TABLE zw5x_ch;
DROP TABLE zw5x_pa;

CREATE TABLE zw5x_pt (k int, j int, v int) PARTITION BY RANGE (k);
CREATE TABLE zw5x_pt1 PARTITION OF zw5x_pt FOR VALUES FROM (0) TO (10);
ALTER TABLE zw5x_pt DROP COLUMN j;

-- begin-expected
-- columns: atts
-- row: k/1/false/false/1,........pg.dropped.2......../2/true/false/1,v/3/false/false/1
-- end-expected
SELECT string_agg(attname || '/' || attnum::text || '/' || attisdropped::text || '/' || attislocal::text || '/' || attinhcount::text, ',' ORDER BY attnum) AS atts FROM pg_attribute WHERE attrelid = 'zw5x_pt1'::regclass AND attnum > 0;

-- begin-expected
-- columns: partattrs
-- row: 1
-- end-expected
SELECT partattrs::text AS partattrs FROM pg_partitioned_table WHERE partrelid = 'zw5x_pt'::regclass;

DROP TABLE zw5x_pt;

-- ----------------------------------------------------------------------------
-- A constraint and an index written after the drop take the numbers as they are
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_u (a int, b varchar(5), c int, d int, e int);
ALTER TABLE zw5x_u DROP COLUMN b;
ALTER TABLE zw5x_u ADD CONSTRAINT zw5x_u_uc UNIQUE (d, c);
CREATE INDEX zw5x_u_ix ON zw5x_u (e) INCLUDE (d);

-- begin-expected
-- columns: conname | conkey
-- row: zw5x_u_uc | 4 3
-- end-expected
SELECT conname, array_to_string(conkey, ' ') AS conkey FROM pg_constraint WHERE conrelid = 'zw5x_u'::regclass ORDER BY conname;

-- The INCLUDE column's entry in indkey is an attribute number too.
-- begin-expected
-- columns: relname | indkey | indnkeyatts | indnatts
-- row: zw5x_u_ix | 5 4 | 1 | 2
-- row: zw5x_u_uc | 4 3 | 2 | 2
-- end-expected
SELECT c.relname, i.indkey::text AS indkey, i.indnkeyatts, i.indnatts FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE i.indrelid = 'zw5x_u'::regclass ORDER BY 1;

ALTER TABLE zw5x_u ADD COLUMN g int GENERATED ALWAYS AS (a * 2) STORED;

-- begin-expected
-- columns: adnum | expr
-- row: 6 | (a * 2)
-- end-expected
SELECT adnum, pg_get_expr(adbin, adrelid) AS expr FROM pg_attrdef WHERE adrelid = 'zw5x_u'::regclass ORDER BY adnum;

DROP TABLE zw5x_u;

-- ----------------------------------------------------------------------------
-- CASCADE leaves a tombstone for each column it takes
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_g (a int, b int GENERATED ALWAYS AS (a * 2) STORED, c int, d int);
ALTER TABLE zw5x_g DROP COLUMN a CASCADE;

-- begin-expected
-- columns: atts
-- row: ........pg.dropped.1........:1:true,........pg.dropped.2........:2:true,c:3:false,d:4:false
-- end-expected
SELECT string_agg(attname || ':' || attnum || ':' || attisdropped::text, ',' ORDER BY attnum) AS atts FROM pg_attribute WHERE attrelid = 'zw5x_g'::regclass AND attnum > 0;

-- begin-expected
-- columns: relnatts
-- row: 4
-- end-expected
SELECT relnatts FROM pg_class WHERE relname = 'zw5x_g';

ALTER TABLE zw5x_g ADD COLUMN e int;

-- begin-expected
-- columns: atts
-- row: c:3,d:4,e:5
-- end-expected
SELECT string_agg(attname || ':' || attnum, ',' ORDER BY attnum) AS atts FROM pg_attribute WHERE attrelid = 'zw5x_g'::regclass AND attnum > 0 AND NOT attisdropped;

DROP TABLE zw5x_g;
