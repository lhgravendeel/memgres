-- source: investigation-2026-08.md
-- finding: 129
-- title: Trigger and rule definitions are stored as written without being analysed — the shape checks that exist look only at table-vs-view, and no pg_constraint/pg_depe
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_f1() RETURNS trigger AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql;
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
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t1 (i int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_v1 AS SELECT i FROM zz_t1;
-- begin-expected-error
-- sqlstate: 42723
-- message-like: function "zz_f1" already exists with same argument types
-- end-expected-error
CREATE FUNCTION zz_f1() RETURNS trigger AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_tg1 AFTER INSERT ON zz_v1 FOR EACH STATEMENT EXECUTE FUNCTION zz_f1();
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t3 (i int);
-- begin-expected-error
-- sqlstate: 42723
-- message-like: function "zz_f1" already exists with same argument types
-- end-expected-error
CREATE FUNCTION zz_f1() RETURNS trigger AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_tg3 AFTER INSERT ON zz_t3 FOR EACH ROW WHEN (NEW.ctid IS NOT NULL) EXECUTE FUNCTION zz_f1();
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t5 (i int);
-- begin-expected-error
-- sqlstate: 42723
-- message-like: function "zz_f1" already exists with same argument types
-- end-expected-error
CREATE FUNCTION zz_f1() RETURNS trigger AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql;
-- begin-expected
-- ok: 0
-- end-expected
CREATE CONSTRAINT TRIGGER zz_tg5c AFTER INSERT ON zz_t5 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION zz_f1();
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT tgconstraint <> 0 FROM pg_trigger WHERE tgname='zz_tg5c';
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_constraint WHERE conname='zz_tg5c';
-- begin-expected
-- ok: 0
-- end-expected
SET CONSTRAINTS zz_tg5c IMMEDIATE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_s4;
-- begin-expected-error
-- sqlstate: 42723
-- message-like: function "zz_f1" already exists with same argument types
-- end-expected-error
CREATE FUNCTION zz_f1() RETURNS trigger AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql;
-- begin-expected-error
-- sqlstate: 42809
-- message-like: relation "zz_s4" cannot have triggers
-- end-expected-error
CREATE TRIGGER zz_tg4 BEFORE INSERT ON zz_s4 FOR EACH ROW EXECUTE FUNCTION zz_f1();
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_t5" already exists
-- end-expected-error
CREATE TABLE zz_t5 (i int);
-- begin-expected-error
-- sqlstate: 42723
-- message-like: function "zz_f1" already exists with same argument types
-- end-expected-error
CREATE FUNCTION zz_f1() RETURNS trigger AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql;
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
