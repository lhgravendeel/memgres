-- source: review-2026-08.md
-- finding: Root cause 10: recorded DDL options are never made to mean anything
-- area: PREPARE, EXECUTE, DO and CALL
-- title: Root cause 10: recorded DDL options are never made to mean anything
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_ud_e AS ENUM ('a','bb','ccc');
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_ud_f(zz_ud_e) RETURNS int LANGUAGE sql IMMUTABLE AS $$ SELECT length($1::text) $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE CAST (zz_ud_e AS int) WITH FUNCTION zz_ud_f(zz_ud_e) AS IMPLICIT;
-- begin-expected
-- columns: int4:int4
-- row: 3
-- rowcount: 1
-- end-expected
SELECT 'ccc'::zz_ud_e::int;
-- begin-expected
-- ok: 0
-- end-expected
CREATE OPERATOR FAMILY zz_ud_fam USING btree;
-- begin-expected
-- ok: 0
-- end-expected
CREATE OPERATOR CLASS zz_ud_oc FOR TYPE int4 USING btree FAMILY zz_ud_fam AS
  OPERATOR 1 <, OPERATOR 2 <=, OPERATOR 3 =, OPERATOR 4 >=, OPERATOR 5 >,
  FUNCTION 1 btint4cmp(int4, int4);
-- begin-expected
-- columns: count:int4
-- row: 5
-- rowcount: 1
-- end-expected
SELECT count(*)::int FROM pg_amop WHERE amopfamily = (SELECT oid FROM pg_opfamily WHERE opfname = 'zz_ud_fam');
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
CLUSTER zz_t USING zz_t_pkey;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
CLUSTER zz_t USING zz_ix;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
SELECT c.relname::text, i.indisclustered FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
  WHERE i.indrelid='zz_t'::regclass ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
ALTER TABLE zz_t SET WITHOUT CLUSTER;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t" does not exist
-- end-expected-error
SELECT count(*)::int FROM pg_index WHERE indrelid='zz_t'::regclass AND indisclustered;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_a" does not exist
-- end-expected-error
CLUSTER zz_a USING zz_nosuchindex;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_a" does not exist
-- end-expected-error
CLUSTER zz_a USING zz_bix;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_nosuchtable" does not exist
-- end-expected-error
CLUSTER zz_nosuchtable USING zz_bix;
-- begin-expected-error
-- sqlstate: 42P17
-- message-like: source data type and target data type are the same
-- end-expected-error
CREATE CAST (int AS int) WITH INOUT;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_p (id int, r int) PARTITION BY RANGE (r);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_p1 PARTITION OF zz_p FOR VALUES FROM (0) TO (50);
-- begin-expected-error
-- sqlstate: 23514
-- message-like: no partition of relation "zz_p" found for row
-- end-expected-error
INSERT INTO zz_p SELECT g, g%100 FROM generate_series(1,200) g;
-- begin-expected
-- ok: 0
-- end-expected
ANALYZE zz_p;
-- begin-expected
-- columns: tablename:text | count:int4
-- rowcount: 0
-- end-expected
SELECT tablename::text, count(*)::int FROM pg_stats WHERE tablename LIKE 'zz\_p%' GROUP BY 1 ORDER BY 1;
