-- ============================================================================
-- A rule is analysed where it is written
-- ============================================================================
-- PostgreSQL analyses a rule at CREATE RULE: the qualification's OLD and NEW
-- resolve against the ruled relation, and every call in the qualification and
-- in the actions is resolved by name and argument list. A rule that fails
-- either is not stored, so the relation stays writable.
DROP TABLE IF EXISTS zzw4d_qa CASCADE;
DROP TABLE IF EXISTS zzw4d_qb CASCADE;
CREATE TABLE zzw4d_qa (i int, j int);
CREATE TABLE zzw4d_qb (i int);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column new.nosuchcol2 does not exist
-- end-expected-error
CREATE RULE zzw4d_q5r AS ON INSERT TO zzw4d_qa WHERE NEW.nosuchcol2 > 0 DO ALSO INSERT INTO zzw4d_qb VALUES (1);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column old.nosuchcol3 does not exist
-- end-expected-error
CREATE RULE zzw4d_q11r AS ON UPDATE TO zzw4d_qa WHERE OLD.nosuchcol3 > 0 DO ALSO INSERT INTO zzw4d_qb VALUES (1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunc(integer) does not exist
-- end-expected-error
CREATE RULE zzw4d_q7r AS ON INSERT TO zzw4d_qa DO ALSO INSERT INTO zzw4d_qb VALUES (nosuchfunc(1));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunc(integer) does not exist
-- end-expected-error
CREATE RULE zzw4d_q8r AS ON INSERT TO zzw4d_qa WHERE nosuchfunc(1) > 0 DO ALSO INSERT INTO zzw4d_qb VALUES (1);

-- None of the four was stored.
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM pg_rules WHERE tablename = 'zzw4d_qa';

-- A rule whose qualification and action do resolve is stored and fires.
CREATE RULE zzw4d_q9r AS ON INSERT TO zzw4d_qa WHERE NEW.i > 0 DO ALSO INSERT INTO zzw4d_qb VALUES (abs(NEW.i));
INSERT INTO zzw4d_qa VALUES (5, 6);

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::text AS n FROM zzw4d_qa;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::text AS n FROM zzw4d_qb;

DROP TABLE IF EXISTS zzw4d_qa CASCADE;
DROP TABLE IF EXISTS zzw4d_qb CASCADE;

-- ============================================================================
-- SET CONSTRAINTS names a constraint trigger, and OR REPLACE cannot replace one
-- ============================================================================
-- A constraint trigger is a constraint as well as a trigger, so SET CONSTRAINTS
-- reaches it by name; its deferrability decides what may be asked of it. And
-- PostgreSQL has no way to replace one, which is 0A000 rather than an accepted
-- statement with OR REPLACE quietly dropped.
DROP TABLE IF EXISTS zzw4d_t5 CASCADE;
DROP FUNCTION IF EXISTS zzw4d_f1() CASCADE;
CREATE TABLE zzw4d_t5 (i int);
CREATE FUNCTION zzw4d_f1() RETURNS trigger AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql;
CREATE CONSTRAINT TRIGGER zzw4d_tg5c AFTER INSERT ON zzw4d_t5 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION zzw4d_f1();
CREATE CONSTRAINT TRIGGER zzw4d_tg5d AFTER INSERT ON zzw4d_t5 FOR EACH ROW EXECUTE FUNCTION zzw4d_f1();

SET CONSTRAINTS zzw4d_tg5c IMMEDIATE;
SET CONSTRAINTS zzw4d_tg5c DEFERRED;
SET CONSTRAINTS public.zzw4d_tg5c IMMEDIATE;
SET CONSTRAINTS zzw4d_tg5d IMMEDIATE;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: constraint "zzw4d_tg5d" is not deferrable
-- end-expected-error
SET CONSTRAINTS zzw4d_tg5d DEFERRED;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: constraint "zzw4d_nosuch" does not exist
-- end-expected-error
SET CONSTRAINTS zzw4d_nosuch IMMEDIATE;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: CREATE OR REPLACE CONSTRAINT TRIGGER is not supported
-- end-expected-error
CREATE OR REPLACE CONSTRAINT TRIGGER zzw4d_tg5b AFTER INSERT ON zzw4d_t5 FOR EACH ROW EXECUTE FUNCTION zzw4d_f1();

-- OR REPLACE on an ordinary trigger is the form PostgreSQL does support.
CREATE OR REPLACE TRIGGER zzw4d_tg5e AFTER INSERT ON zzw4d_t5 FOR EACH ROW EXECUTE FUNCTION zzw4d_f1();

-- The refused statement left nothing behind: two constraint triggers and one
-- ordinary one.
-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT count(*)::text AS n FROM pg_trigger WHERE tgrelid = 'zzw4d_t5'::regclass AND NOT tgisinternal;

DROP TABLE IF EXISTS zzw4d_t5 CASCADE;
DROP FUNCTION IF EXISTS zzw4d_f1() CASCADE;