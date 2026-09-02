-- source: review-2026-08.md
-- finding: Root cause 4: the trigger set is looked up from the name written in the statement, before the row is routed
-- area: SQL-level cursors
-- title: Root cause 4: the trigger set is looked up from the name written in the statement, before the row is routed
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_lg (seq serial, t text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_lf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
  INSERT INTO zz_lg (t) VALUES (TG_WHEN||'/'||TG_OP||'/'||TG_TABLE_NAME);
  IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW; END $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_q (id int, k int NOT NULL) PARTITION BY RANGE (k);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_qa PARTITION OF zz_q FOR VALUES FROM (0) TO (100);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_qt BEFORE INSERT OR UPDATE OR DELETE ON zz_qa FOR EACH ROW EXECUTE FUNCTION zz_lf();
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_q VALUES (1, 10);
-- begin-expected
-- columns: t:text
-- row: BEFORE/INSERT/zz_qa
-- rowcount: 1
-- end-expected
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
