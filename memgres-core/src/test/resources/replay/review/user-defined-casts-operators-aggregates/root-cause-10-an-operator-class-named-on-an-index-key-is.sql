-- source: review-2026-08.md
-- finding: Root cause 10: an operator class named on an index key is looked up in a static table of built-in names, unqualified
-- area: User-defined casts, operators, aggregates, collations and extensions
-- title: Root cause 10: an operator class named on an index key is looked up in a static table of built-in names, unqualified
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_it (i int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE OPERATOR CLASS zz_oc FOR TYPE int4 USING btree AS
  OPERATOR 1 <, OPERATOR 3 =, FUNCTION 1 btint4cmp(int4, int4);
-- begin-expected
-- ok: 0
-- end-expected
CREATE INDEX zz_i ON zz_it (i zz_oc);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_ix (a int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE INDEX zz_i2 ON zz_ix (a pg_catalog.int4_ops);
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_class WHERE relname='zz_i2';
