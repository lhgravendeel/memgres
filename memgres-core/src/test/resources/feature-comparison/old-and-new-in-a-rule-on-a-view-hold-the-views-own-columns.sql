-- A rule on a view reads old and new as the view's columns, not the columns behind it.
--
-- A view selecting amount AS total holds total, so old.amount is 42703 'column old.amount does not
-- exist' inside a rule on that view, and old.totl is the same error carrying the near miss as a
-- hint. An unqualified name is read against whichever rows the event puts in scope: on DELETE only
-- old is there, on INSERT only new, and on UPDATE both -- so a column the view does hold is 42702
-- 'column reference "total" is ambiguous' there and resolves on the other two.
--
-- memgres skipped this check for views entirely, so a rule naming a column of the table under the
-- view was stored and only failed when it fired.

-- setup
DROP VIEW IF EXISTS zzt4c_vv CASCADE;
DROP TABLE IF EXISTS zzt4c_vbase, zzt4c_vo CASCADE;

CREATE TABLE zzt4c_vbase (amount int, note text);
CREATE TABLE zzt4c_vo (i int, j text);
CREATE VIEW zzt4c_vv AS SELECT amount AS total, note AS caption FROM zzt4c_vbase;

-- ---------------------------------------------------------------------------
-- 1. A column the table has and the view does not
-- ---------------------------------------------------------------------------

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column old.amount does not exist
-- end-expected-error
CREATE RULE zzt4c_v AS ON UPDATE TO zzt4c_vv WHERE old.amount <> 0 DO INSTEAD NOTHING;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column new.amount does not exist
-- end-expected-error
CREATE RULE zzt4c_v AS ON UPDATE TO zzt4c_vv WHERE new.amount <> 0 DO INSTEAD NOTHING;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column old.note does not exist
-- end-expected-error
CREATE RULE zzt4c_v AS ON DELETE TO zzt4c_vv WHERE old.note <> '' DO INSTEAD NOTHING;

-- A near miss of a column the view does hold is the same error, with the name it nearly wrote.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column old.totl does not exist
-- end-expected-error
CREATE RULE zzt4c_v AS ON UPDATE TO zzt4c_vv WHERE old.totl <> 0 DO INSTEAD NOTHING;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column new.totl does not exist
-- end-expected-error
CREATE RULE zzt4c_v AS ON UPDATE TO zzt4c_vv WHERE new.totl <> 0 DO INSTEAD NOTHING;

-- An action is read the same way as a qualification.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column old.amount does not exist
-- end-expected-error
CREATE RULE zzt4c_v AS ON UPDATE TO zzt4c_vv
  DO INSTEAD INSERT INTO zzt4c_vo VALUES (old.amount, 'x');

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column new.totl does not exist
-- end-expected-error
CREATE RULE zzt4c_v AS ON UPDATE TO zzt4c_vv
  DO INSTEAD INSERT INTO zzt4c_vo VALUES (new.totl, 'x');

-- ---------------------------------------------------------------------------
-- 2. An unqualified name, read against the rows the event holds
-- ---------------------------------------------------------------------------

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "amount" does not exist
-- end-expected-error
CREATE RULE zzt4c_v AS ON DELETE TO zzt4c_vv WHERE amount <> 0 DO INSTEAD NOTHING;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nope" does not exist
-- end-expected-error
CREATE RULE zzt4c_v AS ON INSERT TO zzt4c_vv WHERE nope <> 0 DO INSTEAD NOTHING;

-- On UPDATE both rows are in scope, so a column the view does hold is the ambiguous one.
-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "total" is ambiguous
-- end-expected-error
CREATE RULE zzt4c_v AS ON UPDATE TO zzt4c_vv WHERE total <> 0 DO INSTEAD NOTHING;

-- ---------------------------------------------------------------------------
-- 3. The view's own columns resolve, in every event that holds their row
-- ---------------------------------------------------------------------------

CREATE RULE zzt4c_vd AS ON DELETE TO zzt4c_vv WHERE total <> 0 DO INSTEAD NOTHING;
CREATE RULE zzt4c_vi AS ON INSERT TO zzt4c_vv WHERE total <> 0 DO INSTEAD NOTHING;
CREATE RULE zzt4c_vq AS ON DELETE TO zzt4c_vv WHERE old.total <> 0 DO INSTEAD NOTHING;
CREATE RULE zzt4c_vu AS ON UPDATE TO zzt4c_vv
  DO INSTEAD INSERT INTO zzt4c_vo VALUES (new.total, old.caption);

-- begin-expected
-- columns: rulename
-- row: zzt4c_vd
-- row: zzt4c_vi
-- row: zzt4c_vq
-- row: zzt4c_vu
-- end-expected
SELECT rulename FROM pg_rules WHERE tablename = 'zzt4c_vv' ORDER BY 1;

INSERT INTO zzt4c_vbase VALUES (3, 'three');
UPDATE zzt4c_vv SET total = 5;

-- The action wrote the view's old and new, and the DO INSTEAD kept the update off the table.
-- begin-expected
-- columns: i, j
-- row: 5, three
-- end-expected
SELECT i, j FROM zzt4c_vo ORDER BY 1;

-- begin-expected
-- columns: amount, note
-- row: 3, three
-- end-expected
SELECT amount, note FROM zzt4c_vbase ORDER BY 1;

-- cleanup
DROP VIEW IF EXISTS zzt4c_vv CASCADE;
DROP TABLE IF EXISTS zzt4c_vbase, zzt4c_vo CASCADE;
