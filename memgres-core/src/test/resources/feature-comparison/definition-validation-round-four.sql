-- A materialized view holds its rows, so it is sampled as a table is.
CREATE TABLE zzc5_ts_t (a int, v text);
INSERT INTO zzc5_ts_t VALUES (1,'x'),(2,'y');
CREATE MATERIALIZED VIEW zzc5_ts_m AS SELECT a, v FROM zzc5_ts_t;
CREATE VIEW zzc5_ts_v AS SELECT a, v FROM zzc5_ts_t;

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM zzc5_ts_m TABLESAMPLE SYSTEM (100);

-- begin-expected
-- columns: count
-- row: 2
-- end-expected
SELECT count(*) FROM zzc5_ts_m TABLESAMPLE BERNOULLI (100);

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM zzc5_ts_m TABLESAMPLE SYSTEM (0);

-- begin-expected
-- columns: a|v
-- row: 1|x
-- row: 2|y
-- end-expected
SELECT a, v FROM zzc5_ts_m TABLESAMPLE SYSTEM (100) ORDER BY a;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: TABLESAMPLE clause can only be applied to tables and materialized views
-- end-expected-error
SELECT count(*) FROM zzc5_ts_v TABLESAMPLE SYSTEM (100);

DROP VIEW zzc5_ts_v;
DROP MATERIALIZED VIEW zzc5_ts_m;
DROP TABLE zzc5_ts_t;

-- A column DEFAULT is judged where the column is defined.
-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type integer but default expression is of type text
-- end-expected-error
CREATE TABLE zzc5_d1 (a text, b int DEFAULT 'abc'::text);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type date but default expression is of type integer
-- end-expected-error
CREATE TABLE zzc5_d2 (a int, b date DEFAULT 1);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type integer but default expression is of type boolean
-- end-expected-error
CREATE TABLE zzc5_d3 (a int, b int DEFAULT true);

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'zzc5\_d%';

-- The pairs an assignment cast exists for are taken; the value is the insert's business.
CREATE TABLE zzc5_d4 (a text DEFAULT 5, b numeric DEFAULT 1, c int DEFAULT 1.5,
    d int DEFAULT '5', e timestamp DEFAULT now(), f int DEFAULT 2147483648, g money DEFAULT 3);

-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM information_schema.tables WHERE table_name = 'zzc5_d4';

DROP TABLE zzc5_d4;

-- A typed table's shape is its type's: no column of it may be dropped.
CREATE TYPE zzc5_ct AS (x int, y text);
CREATE TABLE zzc5_of OF zzc5_ct (x WITH OPTIONS NOT NULL, y WITH OPTIONS DEFAULT 'dd');

-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot drop column from typed table
-- end-expected-error
ALTER TABLE zzc5_of DROP COLUMN y;

-- The refusal comes before the column is looked up, so a missing name gets it too.
-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot drop column from typed table
-- end-expected-error
ALTER TABLE zzc5_of DROP COLUMN IF EXISTS nosuch;

-- begin-expected
-- columns: column_name|is_nullable|column_default
-- row: x|NO|
-- row: y|YES|'dd'::text
-- end-expected
SELECT column_name, is_nullable, column_default FROM information_schema.columns
WHERE table_name = 'zzc5_of' ORDER BY ordinal_position;

DROP TABLE zzc5_of;
DROP TYPE zzc5_ct;

-- A view column's default is filed against the view, not against the relation underneath.
CREATE TABLE zzc5_k1 (i int DEFAULT 7, w text);
CREATE VIEW zzc5_k1v AS SELECT i, w FROM zzc5_k1;
ALTER VIEW zzc5_k1v ALTER COLUMN w SET DEFAULT 'vv';

-- begin-expected
-- columns: rel|adnum|def
-- row: zzc5_k1|1|7
-- row: zzc5_k1v|2|'vv'::text
-- end-expected
SELECT adrelid::regclass::text AS rel, adnum, pg_get_expr(adbin, adrelid) AS def
FROM pg_attrdef WHERE adrelid IN ('zzc5_k1'::regclass, 'zzc5_k1v'::regclass) ORDER BY 1, 2;

ALTER VIEW zzc5_k1v ALTER COLUMN w DROP DEFAULT;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM pg_attrdef WHERE adrelid = 'zzc5_k1v'::regclass;

DROP VIEW zzc5_k1v;
DROP TABLE zzc5_k1;

-- pg_get_viewdef prints the analysed query, not the text it was written as.
CREATE TABLE zzc5_y1 (i int, v text);
CREATE VIEW zzc5_y1v AS SELECT * FROM zzc5_y1;

-- begin-expected
-- columns: d
-- row: [ SELECT i,~    v~   FROM zzc5_y1;]
-- end-expected
SELECT '[' || replace(pg_get_viewdef('zzc5_y1v'::regclass, true), chr(10), '~') || ']' AS d;

DROP VIEW zzc5_y1v;
DROP TABLE zzc5_y1;

CREATE TABLE zzc5_y2 (v text, w text);
CREATE VIEW zzc5_y2v AS SELECT upper(v) AS uv, w AS wv, 'v' AS lit FROM zzc5_y2 WHERE v = 'v' ORDER BY w;
ALTER TABLE zzc5_y2 RENAME COLUMN v TO v2;

-- begin-expected
-- columns: d
-- row: [ SELECT upper(v2) AS uv,~    w AS wv,~    'v'::text AS lit~   FROM zzc5_y2~  WHERE v2 = 'v'::text~  ORDER BY w;]
-- end-expected
SELECT '[' || replace(pg_get_viewdef('zzc5_y2v'::regclass, true), chr(10), '~') || ']' AS d;

DROP VIEW zzc5_y2v;
DROP TABLE zzc5_y2;

CREATE TABLE zzc5_qa (x int);
CREATE TABLE zzc5_qb (y int);
CREATE VIEW zzc5_qv AS SELECT a.x, b.y FROM zzc5_qa a, zzc5_qb b WHERE a.x = b.y;

-- begin-expected
-- columns: d
-- row: [ SELECT a.x,~    b.y~   FROM zzc5_qa a,~    zzc5_qb b~  WHERE a.x = b.y;]
-- end-expected
SELECT '[' || replace(pg_get_viewdef('zzc5_qv'::regclass, true), chr(10), '~') || ']' AS d;

DROP VIEW zzc5_qv;
DROP TABLE zzc5_qa;
DROP TABLE zzc5_qb;

-- What depends on a type is named in the order the dependencies were recorded.
CREATE TYPE zzc5_ct5 AS (x int, y text);
CREATE TABLE zzc5_tz OF zzc5_ct5;
CREATE TABLE zzc5_ta OF zzc5_ct5;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop type zzc5_ct5 because other objects depend on it
-- end-expected-error
DROP TYPE zzc5_ct5;

DROP TABLE zzc5_tz;
DROP TABLE zzc5_ta;
DROP TYPE zzc5_ct5;