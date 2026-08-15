-- A rule is analysed when it is written, and it is dropped with the relation it is on.
--   * CREATE RULE resolves every column its qualification and its actions name -- in a
--     target list, a WHERE, an ORDER BY, an UPDATE SET, a scalar subquery, a CTE, a join
--     condition, a DO INSTEAD action -- and refuses the statement with 42703 rather than
--     storing a rule that would break the next write to the relation.
--   * a column of the relation being ruled is reachable from the rule's own qualification
--     but not from inside an action, where only OLD. and NEW. reach it.
--   * a rule is created against the schema its relation was written with.
--   * dropping the relation, dropping a view, dropping the schema and rolling a drop back
--     all move the rule with it.
-- Every answer below was read off PostgreSQL 18.

-- setup
CREATE TABLE rcd_ra (i int, j int);
CREATE TABLE rcd_rb (i int, k int);

-- stmt 1: a column that resolves nowhere is refused, in every shape an action can take
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE RULE rcd_r1 AS ON INSERT TO rcd_ra DO ALSO INSERT INTO rcd_rb SELECT nosuchcol FROM rcd_rb;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE RULE rcd_r2 AS ON INSERT TO rcd_ra DO ALSO INSERT INTO rcd_rb SELECT i FROM rcd_rb WHERE nosuchcol > 1;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE RULE rcd_r3 AS ON INSERT TO rcd_ra DO ALSO INSERT INTO rcd_rb SELECT i FROM rcd_rb ORDER BY nosuchcol;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE RULE rcd_r4 AS ON DELETE TO rcd_ra DO ALSO DELETE FROM rcd_rb WHERE nosuchcol = 1;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE RULE rcd_r5 AS ON UPDATE TO rcd_ra DO ALSO UPDATE rcd_rb SET k = nosuchcol;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE RULE rcd_r6 AS ON UPDATE TO rcd_ra DO ALSO UPDATE rcd_rb SET k = 1 WHERE nosuchcol = 2;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE RULE rcd_r7 AS ON INSERT TO rcd_ra WHERE nosuchcol > 1 DO ALSO INSERT INTO rcd_rb VALUES (1, 2);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE RULE rcd_r8 AS ON INSERT TO rcd_ra DO ALSO INSERT INTO rcd_rb VALUES ((SELECT nosuchcol FROM rcd_rb), 2);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE RULE rcd_r9 AS ON INSERT TO rcd_ra DO ALSO WITH c AS (SELECT nosuchcol FROM rcd_rb) INSERT INTO rcd_rb SELECT 1, 2;

-- a qualified miss is named by the alias, unquoted
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column x.nosuchcol does not exist
-- end-expected-error
CREATE RULE rcd_r10 AS ON INSERT TO rcd_ra DO ALSO INSERT INTO rcd_rb SELECT x.i, 1 FROM rcd_rb x JOIN rcd_rb y ON x.nosuchcol = y.i;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE RULE rcd_r11 AS ON INSERT TO rcd_ra DO INSTEAD INSERT INTO rcd_rb SELECT nosuchcol FROM rcd_rb;

-- stmt 2: the ruled relation's own column does not reach inside an action
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "j" does not exist
-- end-expected-error
CREATE RULE rcd_r12 AS ON INSERT TO rcd_ra DO ALSO INSERT INTO rcd_rb VALUES (j, 2);

-- none of them was stored, so the relation is still writable
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_rules WHERE tablename = 'rcd_ra';

INSERT INTO rcd_ra VALUES (1, 2);

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM rcd_ra;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM rcd_rb;

-- stmt 3: shapes that supply their own names are accepted and fire
CREATE RULE rcd_q2 AS ON INSERT TO rcd_ra DO ALSO WITH c AS (SELECT i AS z FROM rcd_rb) INSERT INTO rcd_rb SELECT z, 1 FROM c;
CREATE RULE rcd_q3 AS ON INSERT TO rcd_ra DO ALSO INSERT INTO rcd_rb SELECT s.v, 1 FROM (SELECT 7 AS v) s;
CREATE RULE rcd_q4 AS ON INSERT TO rcd_ra DO ALSO INSERT INTO rcd_rb SELECT g, 1 FROM generate_series(1, 3) AS g;
CREATE RULE rcd_q5 AS ON INSERT TO rcd_ra DO ALSO INSERT INTO rcd_rb SELECT i AS zz, 1 FROM rcd_rb ORDER BY zz;
CREATE RULE rcd_q6 AS ON INSERT TO rcd_ra DO ALSO INSERT INTO rcd_rb SELECT t.i, 1 FROM rcd_rb t WHERE t.k > 0;
CREATE RULE rcd_q7 AS ON INSERT TO rcd_ra WHERE new.i > 1 DO ALSO INSERT INTO rcd_rb VALUES (new.i, 2);
-- the rule's own qualification does reach the ruled relation's columns unqualified
CREATE RULE rcd_q8 AS ON INSERT TO rcd_ra WHERE j > 1 DO ALSO INSERT INTO rcd_rb VALUES (9, 9);

-- begin-expected
-- columns: rulename
-- row: rcd_q2
-- row: rcd_q3
-- row: rcd_q4
-- row: rcd_q5
-- row: rcd_q6
-- row: rcd_q7
-- row: rcd_q8
-- end-expected
SELECT rulename FROM pg_rules WHERE tablename = 'rcd_ra' ORDER BY rulename;

DELETE FROM rcd_ra;
DELETE FROM rcd_rb;
INSERT INTO rcd_ra VALUES (5, 6);

-- begin-expected
-- columns: n
-- row: 18
-- end-expected
SELECT count(*) AS n FROM rcd_rb;

DROP TABLE rcd_ra CASCADE;
DROP TABLE rcd_rb CASCADE;

-- stmt 4: a near miss on the ruled relation is refused, not stored
CREATE TABLE rcd_c (x int, y int);
CREATE TABLE rcd_b (i int, k int);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "xx" does not exist
-- end-expected-error
CREATE RULE rcd_h2 AS ON INSERT TO rcd_c WHERE xx > 1 DO ALSO INSERT INTO rcd_b VALUES (1, 2);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column new.xx does not exist
-- end-expected-error
CREATE RULE rcd_h3 AS ON INSERT TO rcd_c DO ALSO INSERT INTO rcd_b VALUES (new.xx, 2);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column t.ii does not exist
-- end-expected-error
CREATE RULE rcd_p4 AS ON INSERT TO rcd_c DO ALSO INSERT INTO rcd_b SELECT t.ii, 1 FROM rcd_b t;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_rules WHERE tablename = 'rcd_c';

DROP TABLE rcd_c CASCADE;
DROP TABLE rcd_b CASCADE;

-- stmt 5: a rule on a schema-qualified relation is created and fires
CREATE SCHEMA rcd_sq;
CREATE TABLE rcd_sq.t (i int);
CREATE TABLE rcd_sq.l (m text);
CREATE RULE rcd_rsq AS ON INSERT TO rcd_sq.t DO ALSO INSERT INTO rcd_sq.l VALUES ('q');
INSERT INTO rcd_sq.t VALUES (1);

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM rcd_sq.l;

-- the action is analysed against the schema it was written with
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE RULE rcd_rsq2 AS ON INSERT TO rcd_sq.t DO ALSO INSERT INTO rcd_sq.l SELECT nosuchcol FROM rcd_sq.l;

-- a schema that is not there is what a missing-relation refusal is about
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "rcd_nosuchschema" does not exist
-- end-expected-error
CREATE RULE rcd_rsq3 AS ON INSERT TO rcd_nosuchschema.t DO INSTEAD NOTHING;

DROP SCHEMA rcd_sq CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_rules WHERE rulename LIKE 'rcd!_rsq%' ESCAPE '!';

-- stmt 6: dropping a view drops the view's own rule
CREATE TABLE rcd_db (i int);
CREATE VIEW rcd_dv AS SELECT i FROM rcd_db;
CREATE RULE rcd_rdv AS ON INSERT TO rcd_dv DO INSTEAD INSERT INTO rcd_db VALUES (NEW.i);
DROP VIEW rcd_dv;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_rules WHERE rulename = 'rcd_rdv';

CREATE VIEW rcd_dv AS SELECT i FROM rcd_db;
CREATE RULE rcd_rdv AS ON INSERT TO rcd_dv DO INSTEAD INSERT INTO rcd_db VALUES (NEW.i);
DROP VIEW rcd_dv CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_rules WHERE rulename = 'rcd_rdv';

DROP TABLE rcd_db;

-- stmt 7: a rolled-back drop brings the rules back, and they fire again
CREATE TABLE rcd_ut (i int);
CREATE TABLE rcd_ul (m text);
CREATE RULE rcd_rut AS ON INSERT TO rcd_ut DO ALSO INSERT INTO rcd_ul VALUES ('x');

BEGIN;

DROP TABLE rcd_ut;

ROLLBACK;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM pg_rules WHERE rulename = 'rcd_rut';

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT relhasrules AS r FROM pg_class WHERE relname = 'rcd_ut';

INSERT INTO rcd_ut VALUES (1);

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM rcd_ul;

DROP TABLE rcd_ut CASCADE;
DROP TABLE rcd_ul CASCADE;

-- the same for a view
CREATE TABLE rcd_uvb (i int);
CREATE VIEW rcd_uv AS SELECT i FROM rcd_uvb;
CREATE RULE rcd_ruv AS ON INSERT TO rcd_uv DO INSTEAD INSERT INTO rcd_uvb VALUES (NEW.i);

BEGIN;

DROP VIEW rcd_uv;

ROLLBACK;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM pg_rules WHERE rulename = 'rcd_ruv';

INSERT INTO rcd_uv VALUES (7);

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM rcd_uvb;

DROP VIEW rcd_uv CASCADE;
DROP TABLE rcd_uvb CASCADE;

-- stmt 8: DROP SCHEMA CASCADE takes the rules of the relations in it
CREATE SCHEMA rcd_sc;
CREATE TABLE rcd_sc.rcd_sct (i int);
CREATE TABLE rcd_sc.rcd_scl (m text);
CREATE RULE rcd_rsc AS ON INSERT TO rcd_sc.rcd_sct DO ALSO INSERT INTO rcd_sc.rcd_scl VALUES ('z');
DROP SCHEMA rcd_sc CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_rules WHERE rulename = 'rcd_rsc';

-- a relation created under one of those names afterwards starts with none
CREATE TABLE rcd_sct (i int);

-- begin-expected
-- columns: r
-- row: f
-- end-expected
SELECT relhasrules AS r FROM pg_class WHERE relname = 'rcd_sct';

-- cleanup
DROP TABLE rcd_sct;
