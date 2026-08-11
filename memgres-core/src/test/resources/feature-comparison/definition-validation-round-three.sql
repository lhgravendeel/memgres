-- ============================================================================
-- A view column default belongs to the view, not to the relation underneath
-- ============================================================================
CREATE TABLE zzw4c_z1 (i int, w text);
CREATE VIEW zzw4c_z1v AS SELECT i, w FROM zzw4c_z1;
ALTER VIEW zzw4c_z1v ALTER COLUMN w SET DEFAULT 'vv';
INSERT INTO zzw4c_z1 (i) VALUES (1);
INSERT INTO zzw4c_z1v (i) VALUES (2);

-- begin-expected
-- columns: i, w
-- row: 1|null
-- row: 2|vv
-- end-expected
SELECT i, w FROM zzw4c_z1 ORDER BY i;

-- begin-expected
-- columns: def
-- row: null
-- end-expected
SELECT column_default AS def FROM information_schema.columns WHERE table_name = 'zzw4c_z1' AND column_name = 'w';

-- begin-expected
-- columns: def
-- row: 'vv'::text
-- end-expected
SELECT column_default AS def FROM information_schema.columns WHERE table_name = 'zzw4c_z1v' AND column_name = 'w';

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nope" of relation "zzw4c_z1v" does not exist
-- end-expected-error
ALTER VIEW zzw4c_z1v ALTER COLUMN nope SET DEFAULT 'q';

DROP VIEW zzw4c_z1v;
DROP TABLE zzw4c_z1;

-- ============================================================================
-- Renaming a base column rewrites the references and nothing else
-- ============================================================================
CREATE TABLE zzw4c_q1 (v text, x int);
INSERT INTO zzw4c_q1 VALUES ('hello', 1);
CREATE VIEW zzw4c_q1v AS SELECT v, 'v'::text AS lit, x FROM zzw4c_q1;
ALTER TABLE zzw4c_q1 RENAME COLUMN v TO v2;

-- begin-expected
-- columns: v, lit, x
-- row: hello|v|1
-- end-expected
SELECT v, lit, x FROM zzw4c_q1v;

-- begin-expected
-- columns: kept
-- row: true
-- end-expected
SELECT (pg_get_viewdef('zzw4c_q1v'::regclass) LIKE '%''v''::text%') AS kept;

DROP VIEW zzw4c_q1v;
DROP TABLE zzw4c_q1;

CREATE TABLE zzw4c_qa (v text);
CREATE TABLE zzw4c_qb (v text);
INSERT INTO zzw4c_qa VALUES ('A');
INSERT INTO zzw4c_qb VALUES ('B');
CREATE VIEW zzw4c_qabv AS SELECT zzw4c_qa.v AS av, zzw4c_qb.v AS bv FROM zzw4c_qa, zzw4c_qb;
ALTER TABLE zzw4c_qa RENAME COLUMN v TO v2;

-- begin-expected
-- columns: av, bv
-- row: A|B
-- end-expected
SELECT av, bv FROM zzw4c_qabv;

-- begin-expected
-- columns: kept
-- row: true
-- end-expected
SELECT (pg_get_viewdef('zzw4c_qabv'::regclass) LIKE '%zzw4c_qb.v %') AS kept;

DROP VIEW zzw4c_qabv;
DROP TABLE zzw4c_qa;
DROP TABLE zzw4c_qb;

CREATE TABLE zzw4c_r1 (i int, v text);
INSERT INTO zzw4c_r1 VALUES (1, 'a');
CREATE VIEW zzw4c_r1v AS SELECT i, v FROM zzw4c_r1 WHERE v = 'a';
CREATE VIEW zzw4c_r2v AS SELECT i, v FROM zzw4c_r1v;
ALTER TABLE zzw4c_r1 RENAME COLUMN v TO v2;

-- begin-expected
-- columns: v
-- row: a
-- end-expected
SELECT v FROM zzw4c_r2v;

DROP VIEW zzw4c_r2v;
DROP VIEW zzw4c_r1v;
DROP TABLE zzw4c_r1;

-- ============================================================================
-- A generation expression is coerced to the column's type where it is written
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type integer but default expression is of type text
-- end-expected-error
CREATE TABLE zzw4c_gg1 (a int, b int GENERATED ALWAYS AS ('abc'::text) STORED);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type date but default expression is of type integer
-- end-expected-error
CREATE TABLE zzw4c_gg2 (a int, b date GENERATED ALWAYS AS (1) STORED);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type integer but default expression is of type text
-- end-expected-error
CREATE TABLE zzw4c_gg3 (a text, b int GENERATED ALWAYS AS (a) STORED);

CREATE TABLE zzw4c_gg4 (a int, b text GENERATED ALWAYS AS (a) STORED);
INSERT INTO zzw4c_gg4 (a) VALUES (7);

-- begin-expected
-- columns: b
-- row: 7
-- end-expected
SELECT b FROM zzw4c_gg4;

DROP TABLE zzw4c_gg4;

-- ============================================================================
-- An exclusion operator has to be a member of the index's operator family
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42809
-- message-like: operator <>(integer,integer) is not a member of operator family "integer_ops"
-- end-expected-error
CREATE TABLE zzw4c_e3 (a int, EXCLUDE (a WITH <>));

-- begin-expected-error
-- sqlstate: 42809
-- message-like: operator <>(text,text) is not a member of operator family "text_ops"
-- end-expected-error
CREATE TABLE zzw4c_e3 (a varchar(10), EXCLUDE (a WITH <>));

CREATE TABLE zzw4c_e3 (a int, EXCLUDE (a WITH =));
DROP TABLE zzw4c_e3;

-- ============================================================================
-- LIKE copies a row's shape, and a stand-alone composite type has one
-- ============================================================================
CREATE TYPE zzw4c_lct AS (m int, n text);
CREATE TABLE zzw4c_lk5 (LIKE zzw4c_lct);

-- begin-expected
-- columns: cols
-- row: m,n
-- end-expected
SELECT string_agg(column_name, ',' ORDER BY ordinal_position) AS cols FROM information_schema.columns WHERE table_name = 'zzw4c_lk5';

DROP TABLE zzw4c_lk5;
DROP TYPE zzw4c_lct;

-- ============================================================================
-- A typed table takes per-column options and keeps its type alive
-- ============================================================================
CREATE TYPE zzw4c_ct3 AS (x int, y text);
CREATE TABLE zzw4c_oa OF zzw4c_ct3 (x WITH OPTIONS NOT NULL);
CREATE TABLE zzw4c_ob OF zzw4c_ct3 (PRIMARY KEY (x), y WITH OPTIONS DEFAULT 'd');

-- begin-expected
-- columns: n
-- row: NO
-- end-expected
SELECT is_nullable AS n FROM information_schema.columns WHERE table_name = 'zzw4c_oa' AND column_name = 'x';

-- begin-expected
-- columns: d
-- row: 'd'::text
-- end-expected
SELECT column_default AS d FROM information_schema.columns WHERE table_name = 'zzw4c_ob' AND column_name = 'y';

INSERT INTO zzw4c_ob (x) VALUES (1);

-- begin-expected
-- columns: x, y
-- row: 1|d
-- end-expected
SELECT x, y FROM zzw4c_ob;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "z" does not exist
-- end-expected-error
CREATE TABLE zzw4c_od OF zzw4c_ct3 (z WITH OPTIONS NOT NULL);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot add column to typed table
-- end-expected-error
ALTER TABLE zzw4c_oa ADD COLUMN z int;

DROP TABLE zzw4c_ob;
DROP TABLE zzw4c_oa;
DROP TYPE zzw4c_ct3;

-- ============================================================================
-- SET STATISTICS reads a signed integer constant and nothing else
-- ============================================================================
CREATE TABLE zzw4c_st (a int, b int);
ALTER TABLE zzw4c_st ALTER COLUMN a SET STATISTICS +5;

-- begin-expected
-- columns: t
-- row: 5
-- end-expected
SELECT attstattarget AS t FROM pg_attribute WHERE attrelid = 'zzw4c_st'::regclass AND attname = 'a';

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "'5'"
-- end-expected-error
ALTER TABLE zzw4c_st ALTER COLUMN a SET STATISTICS '5';

DROP TABLE zzw4c_st;

-- ============================================================================
-- TABLESAMPLE applies to relations that hold their own rows
-- ============================================================================
CREATE TABLE zzw4c_ts (a int, b int);
INSERT INTO zzw4c_ts VALUES (1,1),(2,2);
CREATE VIEW zzw4c_tsv AS SELECT a FROM zzw4c_ts;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: TABLESAMPLE clause can only be applied to tables and materialized views
-- end-expected-error
SELECT count(*) AS n FROM zzw4c_tsv TABLESAMPLE BERNOULLI (100);

-- begin-expected-error
-- sqlstate: 2202H
-- message-like: sample percentage must be between 0 and 100
-- end-expected-error
SELECT count(*) AS n FROM zzw4c_ts TABLESAMPLE BERNOULLI (150);

-- begin-expected-error
-- sqlstate: 42704
-- message-like: tablesample method nosuchmethod does not exist
-- end-expected-error
SELECT count(*) AS n FROM zzw4c_ts TABLESAMPLE NOSUCHMETHOD (10);

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n FROM zzw4c_ts TABLESAMPLE BERNOULLI (100);

DROP VIEW zzw4c_tsv;
DROP TABLE zzw4c_ts;
