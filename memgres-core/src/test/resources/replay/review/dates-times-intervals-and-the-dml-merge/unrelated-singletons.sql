-- source: review-2026-08.md
-- finding: Unrelated singletons
-- area: Dates, times, intervals — and the DML/MERGE/COPY findings filed with them
-- title: Unrelated singletons
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_f1 (a int, b text);
-- begin-expected-error
-- sqlstate: 42601
-- message-like: VALUES lists must all be the same length
-- end-expected-error
INSERT INTO zz_f1 VALUES (1, 'x'), (2);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_cte (id int, v text);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_cte VALUES (1,'a'),(2,'b');
-- begin-expected
-- columns: id:int4
-- rowcount: 0
-- end-expected
WITH a AS (UPDATE zz_cte SET v = 'upd' WHERE id = 1 RETURNING id)
DELETE FROM zz_cte WHERE id IN (SELECT id FROM a) RETURNING id;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_addr AS (street text, city text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_ct (id int, a zz_addr);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_ct VALUES (1, ROW('Main St','Ede'));
-- begin-expected
-- ok: 1
-- end-expected
UPDATE zz_ct SET a.street = 'One, Two';
-- begin-expected
-- columns: street:text | city:text
-- row: One, Two | Ede
-- rowcount: 1
-- end-expected
SELECT (a).street, (a).city FROM zz_ct;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_ts(id int, body text, v tsvector);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_ts_tsv BEFORE INSERT OR UPDATE ON zz_ts
  FOR EACH ROW EXECUTE FUNCTION tsvector_update_trigger(v, 'pg_catalog.english', body);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_ts VALUES (1, 'the quick brown foxes');
-- begin-expected
-- columns: v:text
-- row: 'brown':3 'fox':4 'quick':2
-- rowcount: 1
-- end-expected
SELECT v::text FROM zz_ts;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_f6() RETURNS text LANGUAGE sql AS $$ SELECT 'COLLATE nosuchcollation_zz' $$;
