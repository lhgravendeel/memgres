-- source: review-2026-08.md
-- finding: Root cause 11: trigger and rule definitions are stored without being analysed
-- area: Joins, CTEs, subqueries — and the DDL that came with them
-- title: Root cause 11: trigger and rule definitions are stored without being analysed
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t1 (i int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_v1 AS SELECT i FROM zz_t1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_f1() RETURNS trigger AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_tg1 AFTER INSERT ON zz_v1 FOR EACH STATEMENT EXECUTE FUNCTION zz_f1();
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t3" does not exist
-- end-expected-error
CREATE TRIGGER zz_tg3 AFTER INSERT ON zz_t3 FOR EACH ROW WHEN (NEW.ctid IS NOT NULL) EXECUTE FUNCTION zz_f1();
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_s4;
-- begin-expected-error
-- sqlstate: 42809
-- message-like: relation "zz_s4" cannot have triggers
-- end-expected-error
CREATE TRIGGER zz_tg4 BEFORE INSERT ON zz_s4 FOR EACH ROW EXECUTE FUNCTION zz_f1();
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t5" does not exist
-- end-expected-error
CREATE CONSTRAINT TRIGGER zz_tg5c AFTER INSERT ON zz_t5 DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION zz_f1();
-- begin-expected
-- columns: ?column?:bool
-- rowcount: 0
-- end-expected
SELECT tgconstraint <> 0 FROM pg_trigger WHERE tgname='zz_tg5c';
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_constraint WHERE conname='zz_tg5c';
-- begin-expected-error
-- sqlstate: 42704
-- message-like: constraint "zz_tg5c" does not exist
-- end-expected-error
SET CONSTRAINTS zz_tg5c IMMEDIATE;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "REFERENCING"
-- end-expected-error
CREATE CONSTRAINT TRIGGER zz_tg5a AFTER INSERT ON zz_t5 REFERENCING NEW TABLE AS nt FOR EACH ROW EXECUTE FUNCTION zz_f1();
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: CREATE OR REPLACE CONSTRAINT TRIGGER is not supported
-- end-expected-error
CREATE OR REPLACE CONSTRAINT TRIGGER zz_tg5b AFTER INSERT ON zz_t5 FOR EACH ROW EXECUTE FUNCTION zz_f1();
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t6" does not exist
-- end-expected-error
CREATE TRIGGER zz_tg6a AFTER INSERT ON zz_t6 REFERENCING NEW TABLE AS nt NEW TABLE AS nt2 FOR EACH STATEMENT EXECUTE FUNCTION zz_f1();
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t6" does not exist
-- end-expected-error
CREATE TRIGGER zz_tg6b AFTER INSERT OR UPDATE ON zz_t6 REFERENCING NEW TABLE AS nt FOR EACH STATEMENT EXECUTE FUNCTION zz_f1();
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t6" does not exist
-- end-expected-error
CREATE TRIGGER zz_tg6c AFTER UPDATE OF a ON zz_t6 REFERENCING NEW TABLE AS nt FOR EACH STATEMENT EXECUTE FUNCTION zz_f1();
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_t10" does not exist
-- end-expected-error
CREATE TRIGGER zz_tg10 AFTER UPDATE OF w ON zz_t10 FOR EACH ROW EXECUTE FUNCTION zz_f10();
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zz_f10() does not exist
-- end-expected-error
DROP FUNCTION zz_f10();
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_trigger WHERE tgname='zz_tg10';
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_p12 (i int) PARTITION BY RANGE (i);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_p12a PARTITION OF zz_p12 FOR VALUES FROM (0) TO (100);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_tg12p AFTER INSERT ON zz_p12 FOR EACH ROW EXECUTE FUNCTION zz_f1();
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_trigger WHERE tgrelid='zz_p12a'::regclass AND NOT tgisinternal;
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ROW triggers with transition tables are not supported on partitions
-- end-expected-error
CREATE TRIGGER zz_tg12t AFTER INSERT ON zz_p12a REFERENCING NEW TABLE AS nt FOR EACH ROW EXECUTE FUNCTION zz_f1();
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_r6" does not exist
-- end-expected-error
CREATE RULE zz_r6_a AS ON UPDATE TO zz_r6 WHERE zz_r6other.x > 0 DO ALSO INSERT INTO zz_r6log VALUES ('x');
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_r6" does not exist
-- end-expected-error
CREATE RULE zz_r6_b AS ON UPDATE TO zz_r6 WHERE OLD.nosuchcol > 0 DO ALSO INSERT INTO zz_r6log VALUES ('x');
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_r6v" does not exist
-- end-expected-error
CREATE RULE zz_r6_c AS ON INSERT TO zz_r6v DO ALSO INSERT INTO zz_r6log VALUES ('r') RETURNING zz_r6log.m;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "CREATE"
-- end-expected-error
CREATE RULE zz_r6_e AS ON INSERT TO zz_r6 DO ALSO CREATE TABLE zz_r6bad (i int);
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_r6v" does not exist
-- end-expected-error
CREATE RULE zz_r6_g AS ON SELECT TO zz_r6v DO INSTEAD SELECT i, v FROM zz_r6;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_r6m" does not exist
-- end-expected-error
CREATE RULE zz_r6_f AS ON INSERT TO zz_r6m DO INSTEAD NOTHING;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_r6v" does not exist
-- end-expected-error
DROP RULE "_RETURN" ON zz_r6v;
