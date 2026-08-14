-- Inside a rule the ruled relation is not a name: its rows are old and new.
--
-- PostgreSQL builds the rule's range table out of the relation twice, entered as "old" and "new",
-- so the relation's own name is an entry that cannot be reached: WHERE t.i <> 0 on a rule over t
-- is 42P01 'invalid reference to FROM-clause entry for table "t"', with the hint that old is what
-- was meant. A name that is not in the range table at all is the other error, 'missing FROM-clause
-- entry for table "other"', with no hint to give.
--
-- memgres accepted all of it -- the qualification, the action, old written on an INSERT rule --
-- and stored a rule whose body named a row nothing would bind. What must keep working is the
-- action that reads the relation through a FROM of its own, and every rule that names old and new
-- where its event has them.

-- setup
DROP VIEW IF EXISTS zzt4c_rv CASCADE;
DROP TABLE IF EXISTS zzt4c_rt, zzt4c_ro CASCADE;
DROP SCHEMA IF EXISTS zzt4c_rs CASCADE;

CREATE TABLE zzt4c_rt (i int, j text);
CREATE TABLE zzt4c_ro (i int, j text);
CREATE VIEW zzt4c_rv AS SELECT i AS total, j AS caption FROM zzt4c_rt;
CREATE SCHEMA zzt4c_rs;
CREATE TABLE zzt4c_rs.zzt4c_far (i int);

-- ---------------------------------------------------------------------------
-- 1. The relation's own name in a qualification
-- ---------------------------------------------------------------------------

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "zzt4c_rt"
-- end-expected-error
CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_rt WHERE zzt4c_rt.i <> 0 DO ALSO NOTHING;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "zzt4c_rt"
-- end-expected-error
CREATE RULE zzt4c_r AS ON DELETE TO zzt4c_rt WHERE zzt4c_rt.i <> 0 DO ALSO NOTHING;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "zzt4c_rt"
-- end-expected-error
CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_rt WHERE zzt4c_rt.i <> 0 DO INSTEAD NOTHING;

-- An INSERT rule has the entry too, though it does not put it in scope.
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "zzt4c_rt"
-- end-expected-error
CREATE RULE zzt4c_r AS ON INSERT TO zzt4c_rt WHERE zzt4c_rt.i <> 0 DO ALSO NOTHING;

-- The relation is named as written, whatever the writer put in front of it.
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "zzt4c_rt"
-- end-expected-error
CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_rt WHERE public.zzt4c_rt.i <> 0 DO ALSO NOTHING;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "zzt4c_rt"
-- end-expected-error
CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_rt WHERE "zzt4c_rt".i <> 0 DO ALSO NOTHING;

-- A column the relation does not hold is still the FROM-clause entry, not a missing column.
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "zzt4c_rt"
-- end-expected-error
CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_rt WHERE zzt4c_rt.nope <> 0 DO ALSO NOTHING;

-- A rule on a relation of another schema reads the same way.
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "zzt4c_far"
-- end-expected-error
CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_rs.zzt4c_far
  WHERE zzt4c_rs.zzt4c_far.i <> 0 DO ALSO NOTHING;

-- A view is a relation like any other.
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "zzt4c_rv"
-- end-expected-error
CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_rv WHERE zzt4c_rv.total <> 0 DO INSTEAD NOTHING;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "zzt4c_rv"
-- end-expected-error
CREATE RULE zzt4c_r AS ON INSERT TO zzt4c_rv WHERE zzt4c_rv.total <> 0 DO INSTEAD NOTHING;

-- ---------------------------------------------------------------------------
-- 2. The relation's own name in an action, for every event
-- ---------------------------------------------------------------------------

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "zzt4c_rt"
-- end-expected-error
CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_rt
  DO ALSO INSERT INTO zzt4c_ro VALUES (zzt4c_rt.i, 'x');

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "zzt4c_rt"
-- end-expected-error
CREATE RULE zzt4c_r AS ON INSERT TO zzt4c_rt
  DO ALSO INSERT INTO zzt4c_ro VALUES (zzt4c_rt.i, 'x');

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "zzt4c_rt"
-- end-expected-error
CREATE RULE zzt4c_r AS ON DELETE TO zzt4c_rt
  DO ALSO INSERT INTO zzt4c_ro VALUES (zzt4c_rt.i, 'x');

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "zzt4c_rt"
-- end-expected-error
CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_rt
  DO INSTEAD INSERT INTO zzt4c_ro VALUES (zzt4c_rt.i, 'x');

-- Wherever in the action it stands.
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "zzt4c_rt"
-- end-expected-error
CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_rt
  DO ALSO UPDATE zzt4c_ro SET j = 'x' WHERE zzt4c_rt.i = 1;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "zzt4c_rt"
-- end-expected-error
CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_rt DO ALSO SELECT zzt4c_rt.i;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "zzt4c_rv"
-- end-expected-error
CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_rv
  DO INSTEAD INSERT INTO zzt4c_ro VALUES (zzt4c_rv.total, 'x');

-- An action that gives the relation a FROM of its own resolves the name there.
CREATE RULE zzt4c_rr2 AS ON UPDATE TO zzt4c_rt
  DO ALSO INSERT INTO zzt4c_ro SELECT zzt4c_rt.i, 'x' FROM zzt4c_rt;

-- But a FROM that renames it puts the written name out of reach again.
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "zzt4c_rt"
-- end-expected-error
CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_rt
  DO ALSO INSERT INTO zzt4c_ro SELECT zzt4c_rt.i, 'x' FROM zzt4c_rt q;

-- ---------------------------------------------------------------------------
-- 3. A relation that is in no range table at all is missing, not out of reach
-- ---------------------------------------------------------------------------

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "zzt4c_ro"
-- end-expected-error
CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_rt WHERE zzt4c_ro.i <> 0 DO ALSO NOTHING;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "zzt4c_ro"
-- end-expected-error
CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_rt WHERE public.zzt4c_ro.i <> 0 DO ALSO NOTHING;

-- The schema written does not hold the relation, so the name resolves to nothing.
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "zzt4c_rt"
-- end-expected-error
CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_rt WHERE zzt4c_rs.zzt4c_rt.i <> 0 DO ALSO NOTHING;

-- Nor does the schema exist.
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "zzt4c_ro"
-- end-expected-error
CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_rt
  WHERE zzt4c_nosuchschema.zzt4c_ro.i <> 0 DO ALSO NOTHING;

-- The rule is on a relation the search path does not reach, so its bare name is missing too.
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "zzt4c_far"
-- end-expected-error
CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_rs.zzt4c_far WHERE zzt4c_far.i <> 0 DO ALSO NOTHING;

-- ---------------------------------------------------------------------------
-- 4. old on INSERT and new on DELETE: the entry is there, the scope is not
-- ---------------------------------------------------------------------------

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "old"
-- end-expected-error
CREATE RULE zzt4c_r AS ON INSERT TO zzt4c_rt WHERE old.i <> 0 DO ALSO NOTHING;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "new"
-- end-expected-error
CREATE RULE zzt4c_r AS ON DELETE TO zzt4c_rt WHERE new.i <> 0 DO ALSO NOTHING;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "old"
-- end-expected-error
CREATE RULE zzt4c_r AS ON INSERT TO zzt4c_rv WHERE old.total <> 0 DO INSTEAD NOTHING;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "new"
-- end-expected-error
CREATE RULE zzt4c_r AS ON DELETE TO zzt4c_rv WHERE new.total <> 0 DO INSTEAD NOTHING;

-- In an action the same reference is refused for what the rule's event is, not for scope.
-- begin-expected-error
-- sqlstate: 42P17
-- message-like: ON INSERT rule cannot use OLD
-- end-expected-error
CREATE RULE zzt4c_r AS ON INSERT TO zzt4c_rt DO ALSO INSERT INTO zzt4c_ro VALUES (old.i, 'x');

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: ON DELETE rule cannot use NEW
-- end-expected-error
CREATE RULE zzt4c_r AS ON DELETE TO zzt4c_rt DO ALSO INSERT INTO zzt4c_ro VALUES (new.i, 'x');

-- ---------------------------------------------------------------------------
-- 5. The rules that name their rows properly are stored, and they fire
-- ---------------------------------------------------------------------------

CREATE RULE zzt4c_rr1 AS ON UPDATE TO zzt4c_rt
  DO ALSO INSERT INTO zzt4c_ro VALUES (old.i, new.j);
CREATE RULE zzt4c_rr3 AS ON DELETE TO zzt4c_rt
  DO ALSO INSERT INTO zzt4c_ro VALUES (old.i, 'gone');
CREATE RULE zzt4c_rr4 AS ON UPDATE TO zzt4c_rt WHERE old.i <> new.i
  DO ALSO INSERT INTO zzt4c_ro VALUES (new.i, 'moved');

-- Every rule refused above left nothing behind; these four are all the relation carries.
-- begin-expected
-- columns: rulename
-- row: zzt4c_rr1
-- row: zzt4c_rr2
-- row: zzt4c_rr3
-- row: zzt4c_rr4
-- end-expected
SELECT rulename FROM pg_rules WHERE tablename = 'zzt4c_rt' ORDER BY 1;

INSERT INTO zzt4c_rt VALUES (1, 'one');
UPDATE zzt4c_rt SET j = 'two' WHERE i = 1;

-- begin-expected
-- columns: i, j
-- row: 1, two
-- row: 1, x
-- end-expected
SELECT i, j FROM zzt4c_ro ORDER BY j;

UPDATE zzt4c_rt SET i = 2 WHERE i = 1;

-- begin-expected
-- columns: i, j
-- row: 2, moved
-- end-expected
SELECT i, j FROM zzt4c_ro WHERE j = 'moved';

DELETE FROM zzt4c_rt;

-- begin-expected
-- columns: i, j
-- row: 2, gone
-- end-expected
SELECT i, j FROM zzt4c_ro WHERE j = 'gone';

-- cleanup
DROP VIEW IF EXISTS zzt4c_rv CASCADE;
DROP TABLE IF EXISTS zzt4c_rt, zzt4c_ro CASCADE;
DROP TABLE IF EXISTS zzt4c_rs.zzt4c_far CASCADE;
DROP SCHEMA IF EXISTS zzt4c_rs CASCADE;
