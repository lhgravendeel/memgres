
-- A rule is analysed while it is being written, not when it fires. PostgreSQL resolves every
-- relation, column and call the rule names -- in its qualification and in each of its actions --
-- as part of CREATE RULE, so a mistake is reported by the statement that made it. Storing the
-- text unread left the mistake for whoever wrote to the ruled relation next, and until the rule
-- was dropped nothing could be written to that relation at all.

DROP TABLE IF EXISTS zzw5d_wa CASCADE;
DROP TABLE IF EXISTS zzw5d_wb CASCADE;
CREATE TABLE zzw5d_wa (i int, j int);
CREATE TABLE zzw5d_wb (i int);

-- A call in the rule's own qualification.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunc2(integer) does not exist
-- end-expected-error
CREATE RULE zzw5d_w1r AS ON INSERT TO zzw5d_wa WHERE nosuchfunc2(NEW.i) DO ALSO INSERT INTO zzw5d_wb VALUES (1);

-- A call in an INSERT ... SELECT action's select list.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunc3(integer) does not exist
-- end-expected-error
CREATE RULE zzw5d_w3r AS ON INSERT TO zzw5d_wa DO ALSO INSERT INTO zzw5d_wb SELECT nosuchfunc3(i) FROM zzw5d_wb;

-- A call on the right of an UPDATE action's assignment.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunc4(integer) does not exist
-- end-expected-error
CREATE RULE zzw5d_w4r AS ON INSERT TO zzw5d_wa DO ALSO UPDATE zzw5d_wb SET i = nosuchfunc4(i);

-- A call in a DELETE action's WHERE.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunc5(integer) does not exist
-- end-expected-error
CREATE RULE zzw5d_w5r AS ON INSERT TO zzw5d_wa DO ALSO DELETE FROM zzw5d_wb WHERE nosuchfunc5(i) > 0;

-- A call inside an action's own WITH clause.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunc6(integer) does not exist
-- end-expected-error
CREATE RULE zzw5d_w6r AS ON INSERT TO zzw5d_wa DO ALSO INSERT INTO zzw5d_wb WITH q AS (SELECT nosuchfunc6(i) AS v FROM zzw5d_wb) SELECT v FROM q;

-- A call's arguments are resolved before the call itself, so the inner name is the one reported.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchinner(integer) does not exist
-- end-expected-error
CREATE RULE zzw5d_w7r AS ON INSERT TO zzw5d_wa DO ALSO INSERT INTO zzw5d_wb VALUES (nosuchouter(nosuchinner(1)));

-- An argument list no signature of that name takes is a function that does not exist.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function abs(integer, integer) does not exist
-- end-expected-error
CREATE RULE zzw5d_w8r AS ON INSERT TO zzw5d_wa DO ALSO INSERT INTO zzw5d_wb VALUES (abs(NEW.i, NEW.j));

-- An UPDATE action's assignment target is matched against the relation it writes to.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchtarget" of relation "zzw5d_wb" does not exist
-- end-expected-error
CREATE RULE zzw5d_w9r AS ON INSERT TO zzw5d_wa DO ALSO UPDATE zzw5d_wb SET nosuchtarget = 1;

-- An alias on the action does not change the relation the message names.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchtarget" of relation "zzw5d_wb" does not exist
-- end-expected-error
CREATE RULE zzw5d_war AS ON INSERT TO zzw5d_wa DO ALSO UPDATE zzw5d_wb AS z SET nosuchtarget = 1;

-- An INSERT action's column list likewise.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" of relation "zzw5d_wb" does not exist
-- end-expected-error
CREATE RULE zzw5d_wbr AS ON INSERT TO zzw5d_wa DO ALSO INSERT INTO zzw5d_wb (nosuchcol) VALUES (1);

-- A schema qualifier on the action does not change it either.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" of relation "zzw5d_wb" does not exist
-- end-expected-error
CREATE RULE zzw5d_wcr AS ON INSERT TO zzw5d_wa DO ALSO INSERT INTO public.zzw5d_wb (nosuchcol) VALUES (1);

-- The relation an action names outranks everything the action says about it.
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zzw5d_nosuchrel" does not exist
-- end-expected-error
CREATE RULE zzw5d_wdr AS ON INSERT TO zzw5d_wa DO ALSO INSERT INTO zzw5d_nosuchrel (nosuchcol) VALUES (1);

-- An INSERT's column list is matched before anything it is handed is read.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" of relation "zzw5d_wb" does not exist
-- end-expected-error
CREATE RULE zzw5d_wer AS ON INSERT TO zzw5d_wa DO ALSO INSERT INTO zzw5d_wb (nosuchcol) VALUES (nosuchfuncb(1));

-- An UPDATE's assignments are read before its targets are matched.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunca(integer) does not exist
-- end-expected-error
CREATE RULE zzw5d_wfr AS ON INSERT TO zzw5d_wa DO ALSO UPDATE zzw5d_wb SET nosuchtarget = nosuchfunca(i);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column new.nosuchx does not exist
-- end-expected-error
CREATE RULE zzw5d_wgr AS ON INSERT TO zzw5d_wa DO ALSO UPDATE zzw5d_wb SET nosuchtarget = NEW.nosuchx;

-- The rule's own qualification is read before any of its actions.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column new.nosuchw does not exist
-- end-expected-error
CREATE RULE zzw5d_whr AS ON INSERT TO zzw5d_wa WHERE NEW.nosuchw > 0 DO ALSO INSERT INTO zzw5d_wb (nosuchcol) VALUES (1);

-- Not one of them was stored, so the ruled relation is still writable.
-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::text AS cnt FROM pg_rules WHERE tablename = 'zzw5d_wa';

INSERT INTO zzw5d_wa VALUES (5, 6);

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::text AS cnt FROM zzw5d_wa;

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::text AS cnt FROM zzw5d_wb;

-- A rule whose every name resolves is stored and fires.
CREATE RULE zzw5d_wokr AS ON INSERT TO zzw5d_wa DO ALSO INSERT INTO zzw5d_wb (i) VALUES (NEW.i);
INSERT INTO zzw5d_wa VALUES (7, 8);

-- begin-expected
-- columns: i
-- row: 7
-- end-expected
SELECT i::text AS i FROM zzw5d_wb ORDER BY 1;

DROP TABLE zzw5d_wa CASCADE;
DROP TABLE zzw5d_wb CASCADE;

-- A rule belongs to the relation it is written on, so dropping the relation drops it. Leaving it
-- registered left pg_rules describing a rule on a relation that was no longer there.

DROP TABLE IF EXISTS zzw5d_ra CASCADE;
DROP TABLE IF EXISTS zzw5d_rb CASCADE;
CREATE TABLE zzw5d_ra (i int);
CREATE TABLE zzw5d_rb (i int);
CREATE RULE zzw5d_r1r AS ON INSERT TO zzw5d_ra DO ALSO INSERT INTO zzw5d_rb VALUES (NEW.i);

-- begin-expected
-- columns: rulename
-- row: zzw5d_r1r
-- end-expected
SELECT rulename FROM pg_rules WHERE tablename = 'zzw5d_ra' ORDER BY 1;

DROP TABLE zzw5d_ra CASCADE;

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::text AS cnt FROM pg_rules WHERE tablename IN ('zzw5d_ra','zzw5d_rb');

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::text AS cnt FROM pg_class WHERE relname IN ('zzw5d_ra','zzw5d_rb');

-- A relation created under the name afterwards starts with no rules at all.
CREATE TABLE zzw5d_ra (i int);

-- begin-expected
-- columns: hasrules
-- row: false
-- end-expected
SELECT relhasrules::text AS hasrules FROM pg_class WHERE relname = 'zzw5d_ra';

DROP TABLE zzw5d_ra CASCADE;
DROP TABLE zzw5d_rb CASCADE;

-- A COMMIT of a transaction block that has already failed discards everything the block wrote:
-- PostgreSQL runs it as a rollback and answers it with the ROLLBACK command tag.

DROP TABLE IF EXISTS zzw5d_qa CASCADE;
CREATE TABLE zzw5d_qa (i int);
BEGIN;
INSERT INTO zzw5d_qa VALUES (1);

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
SELECT 1/0;

COMMIT;

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::text AS cnt FROM zzw5d_qa;

DROP TABLE zzw5d_qa CASCADE;