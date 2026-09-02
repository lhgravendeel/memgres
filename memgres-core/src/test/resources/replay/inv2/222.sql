-- source: investigation-2026-08.md
-- finding: 222
-- title: the trigger set is looked up once from the name written in the statement, before the row is routed; the leaf partition supplies only TG_TABLE_NAME, and a cross-
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_q (id int, k int NOT NULL) PARTITION BY RANGE (k);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_qa PARTITION OF zz_q FOR VALUES FROM (0) TO (100);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_lf() does not exist
-- end-expected-error
CREATE TRIGGER zz_qt BEFORE INSERT OR UPDATE OR DELETE ON zz_qa FOR EACH ROW EXECUTE FUNCTION zz_lf();
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_q VALUES (1, 10);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_lg" does not exist
-- end-expected-error
SELECT t FROM zz_lg ORDER BY seq;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_r (id int, k int NOT NULL) PARTITION BY RANGE (k);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_r1 PARTITION OF zz_r FOR VALUES FROM (0) TO (100);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_r2 PARTITION OF zz_r FOR VALUES FROM (100) TO (200);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_f2() does not exist
-- end-expected-error
CREATE TRIGGER zz_rt BEFORE INSERT OR UPDATE OR DELETE ON zz_r FOR EACH ROW EXECUTE FUNCTION zz_f2();
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_r VALUES (1, 10);
-- begin-expected
-- ok: 1
-- end-expected
UPDATE zz_r SET k = 150 WHERE id = 1;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_lg2" does not exist
-- end-expected-error
SELECT t FROM zz_lg2 ORDER BY seq;
