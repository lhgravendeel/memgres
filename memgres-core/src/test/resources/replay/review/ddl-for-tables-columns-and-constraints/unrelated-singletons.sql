-- source: review-2026-08.md
-- finding: Unrelated singletons
-- area: DDL for tables, columns and constraints
-- title: Unrelated singletons
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_own (id int);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_own ENABLE ROW LEVEL SECURITY;
-- begin-expected
-- ok: 0
-- end-expected
CREATE POLICY zz_pol ON zz_own FOR SELECT USING (true);
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_role LOGIN;
-- begin-expected
-- ok: 0
-- end-expected
GRANT SELECT ON zz_own TO zz_role;
-- begin-expected
-- ok: 0
-- end-expected
SET ROLE zz_role;
-- begin-expected-error
-- sqlstate: 42501
-- message-like: must be owner of relation zz_own
-- end-expected-error
DROP POLICY zz_pol ON zz_own;
-- replay: the reproducer above changed the session; put it back
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_p (i int PRIMARY KEY);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_p VALUES (1);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_c (id int PRIMARY KEY, p int REFERENCES zz_p(i) DEFERRABLE INITIALLY IMMEDIATE);
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
SAVEPOINT s;
-- begin-expected
-- ok: 0
-- end-expected
SET CONSTRAINTS ALL DEFERRED;
-- begin-expected
-- ok: 0
-- end-expected
ROLLBACK TO s;
-- begin-expected-error
-- sqlstate: 23503
-- message-like: insert or update on table "zz_c" violates foreign key constraint "zz_c_p_fkey"
-- end-expected-error
INSERT INTO zz_c VALUES (1, 555);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_rls (a int);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_rls VALUES (0),(1);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_rls ENABLE ROW LEVEL SECURITY;
-- begin-expected
-- ok: 0
-- end-expected
CREATE POLICY zz_rp ON zz_rls FOR SELECT USING (100 / a > 0);
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_r2 LOGIN;
-- begin-expected
-- ok: 0
-- end-expected
GRANT SELECT ON zz_rls TO zz_r2;
-- begin-expected
-- ok: 0
-- end-expected
SET ROLE zz_r2;
-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT a FROM zz_rls ORDER BY a;
-- replay: the reproducer above changed the session; put it back
-- begin-expected
-- ok: 0
-- end-expected
RESET ROLE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_tp (a int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_tc () INHERITS (zz_tp);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_tp VALUES (1);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_tc VALUES (2);
-- begin-expected
-- columns: count:int8
-- row: 2
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_tp TABLESAMPLE BERNOULLI (100);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t5 (i int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_f5() RETURNS trigger AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "DEFERRABLE"
-- end-expected-error
CREATE TRIGGER zz_tg5d AFTER INSERT ON zz_t5 DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION zz_f5();
