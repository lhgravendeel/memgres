-- ============================================================================
-- The names a stored expression may write
--
-- A CHECK, a generation expression and an ALTER COLUMN ... USING are resolved
-- against the one relation they belong to and nothing else. A qualifier that
-- names that relation resolves; anything else is a table the statement never
-- named, and is refused as a missing FROM-clause entry. A composite column is
-- no exception: c.x is read as a relation c, and (c).x is how the field is
-- reached. Every value here was read off PostgreSQL 18.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- A USING expression is resolved before the column being retyped
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_u (a int, s text);

-- No relation clause, because the name was never looked up against one.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" does not exist
-- end-expected-error
ALTER TABLE zw5x_u ALTER COLUMN nosuch TYPE serial USING nosuch::int;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "other" does not exist
-- end-expected-error
ALTER TABLE zw5x_u ALTER COLUMN nosuch TYPE int USING other::int;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" does not exist
-- end-expected-error
ALTER TABLE zw5x_u ALTER COLUMN a TYPE int USING nosuch::int;

-- With nothing wrong in the expression, the column being retyped is reached,
-- and that complaint does name the relation.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" of relation "zw5x_u" does not exist
-- end-expected-error
ALTER TABLE zw5x_u ALTER COLUMN nosuch TYPE int USING a::int;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" of relation "zw5x_u" does not exist
-- end-expected-error
ALTER TABLE zw5x_u ALTER COLUMN nosuch TYPE int USING 1;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "serial" does not exist
-- end-expected-error
ALTER TABLE zw5x_u ALTER COLUMN a TYPE serial USING a::int;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zw5x_nosuchtable" does not exist
-- end-expected-error
ALTER TABLE zw5x_nosuchtable ALTER COLUMN nosuch TYPE int USING nosuch::int;

-- A qualifier names the relation or nothing at all.
ALTER TABLE zw5x_u ALTER COLUMN a TYPE int USING zw5x_u.a::int;
ALTER TABLE zw5x_u ALTER COLUMN a TYPE int USING public.zw5x_u.a::int;

-- Named as it was written, and without quotes.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column zw5x_u.nosuch does not exist
-- end-expected-error
ALTER TABLE zw5x_u ALTER COLUMN a TYPE int USING zw5x_u.nosuch::int;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "zzz"
-- end-expected-error
ALTER TABLE zw5x_u ALTER COLUMN a TYPE int USING zzz.a::int;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "zzz"
-- end-expected-error
ALTER TABLE zw5x_u ALTER COLUMN a TYPE int USING (zzz.a + nosuch);

-- Names are resolved left to right.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" does not exist
-- end-expected-error
ALTER TABLE zw5x_u ALTER COLUMN a TYPE int USING (nosuch + zzz.a);

DROP TABLE zw5x_u;

-- ----------------------------------------------------------------------------
-- On a relation in another schema, a qualifier may name either part
-- ----------------------------------------------------------------------------
CREATE SCHEMA zw5x_s2;
CREATE TABLE zw5x_s2.zw5x_w (a int, s text);
ALTER TABLE zw5x_s2.zw5x_w ALTER COLUMN a TYPE int USING zw5x_w.a::int;
ALTER TABLE zw5x_s2.zw5x_w ALTER COLUMN a TYPE int USING zw5x_s2.zw5x_w.a::int;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "zw5x_w"
-- end-expected-error
ALTER TABLE zw5x_s2.zw5x_w ALTER COLUMN a TYPE int USING nosuchschema.zw5x_w.a::int;

-- The match is on the name as written: an unquoted qualifier is folded to
-- lower case and a quoted one is not.
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "ZW5X_W"
-- end-expected-error
ALTER TABLE zw5x_s2.zw5x_w ADD CONSTRAINT zw5x_k CHECK ("ZW5X_W".a > 0);

ALTER TABLE zw5x_s2.zw5x_w ADD CONSTRAINT zw5x_k CHECK ("zw5x_w".a > 0);

DROP TABLE zw5x_s2.zw5x_w;
DROP SCHEMA zw5x_s2;

-- ----------------------------------------------------------------------------
-- A CHECK goes by the same rule, and is judged before the row it would refuse
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_k (a int, s text);
ALTER TABLE zw5x_k ADD CONSTRAINT zw5x_k1 CHECK (zw5x_k.a > 0);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: violates check constraint "zw5x_k1"
-- end-expected-error
INSERT INTO zw5x_k VALUES (-1);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column zw5x_k.nosuch does not exist
-- end-expected-error
ALTER TABLE zw5x_k ADD CONSTRAINT zw5x_k2 CHECK (zw5x_k.nosuch > 0);

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "zzz"
-- end-expected-error
ALTER TABLE zw5x_k ADD CONSTRAINT zw5x_k3 CHECK (zzz.a > 0);

-- NOT VALID and NOT ENFORCED defer the rows, not the names.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" does not exist
-- end-expected-error
ALTER TABLE zw5x_k ADD CONSTRAINT zw5x_k4 CHECK (nosuch > 0) NOT VALID;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "zzz"
-- end-expected-error
ALTER TABLE zw5x_k ADD CONSTRAINT zw5x_k5 CHECK (zzz.a > 0) NOT VALID;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" does not exist
-- end-expected-error
ALTER TABLE zw5x_k ADD CONSTRAINT zw5x_k6 CHECK (nosuch > 0) NOT ENFORCED;

-- The names are settled before the predicate is asked to be a boolean.
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "zzz"
-- end-expected-error
ALTER TABLE zw5x_k ADD CONSTRAINT zw5x_k7 CHECK (zzz.a);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of CHECK must be type boolean, not type integer
-- end-expected-error
ALTER TABLE zw5x_k ADD CONSTRAINT zw5x_k8 CHECK (a);

-- s is a column of the relation, and a qualifier is still read as a relation.
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "s"
-- end-expected-error
ALTER TABLE zw5x_k ADD CONSTRAINT zw5x_k9 CHECK (s.x > 0);

-- Only the one that resolved was stored.
-- begin-expected
-- columns: cons
-- row: zw5x_k1
-- end-expected
SELECT string_agg(conname, ',' ORDER BY conname) AS cons FROM pg_constraint WHERE conrelid = 'zw5x_k'::regclass;

DROP TABLE zw5x_k;

-- The same rule inside CREATE TABLE.
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "zzz"
-- end-expected-error
CREATE TABLE zw5x_k2 (a int, CHECK (zzz.a > 0));

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column zw5x_k3.nosuch does not exist
-- end-expected-error
CREATE TABLE zw5x_k3 (a int, CHECK (zw5x_k3.nosuch > 0));

-- ----------------------------------------------------------------------------
-- A composite field is reached by writing the column in parentheses
-- ----------------------------------------------------------------------------
CREATE TYPE zw5x_ct AS (x int, y text);
CREATE TABLE zw5x_cc (a int, c zw5x_ct);
ALTER TABLE zw5x_cc ADD CONSTRAINT zw5x_m1 CHECK ((c).x > 0);
ALTER TABLE zw5x_cc ALTER COLUMN a TYPE int USING (c).x;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "c"
-- end-expected-error
ALTER TABLE zw5x_cc ADD CONSTRAINT zw5x_m2 CHECK (c.x > 0);

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "c"
-- end-expected-error
ALTER TABLE zw5x_cc ALTER COLUMN a TYPE int USING c.x;

-- A three-part name is (schema, table, column), so the middle part is the
-- relation.
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "c"
-- end-expected-error
ALTER TABLE zw5x_cc ADD CONSTRAINT zw5x_m3 CHECK (zw5x_cc.c.x > 0);

DROP TABLE zw5x_cc;
DROP TYPE zw5x_ct;

-- ----------------------------------------------------------------------------
-- A generation expression goes by the same rule
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_gg (a int);

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "zzz"
-- end-expected-error
ALTER TABLE zw5x_gg ADD COLUMN g1 int GENERATED ALWAYS AS (zzz.a) STORED;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column zw5x_gg.nosuch does not exist
-- end-expected-error
ALTER TABLE zw5x_gg ADD COLUMN g2 int GENERATED ALWAYS AS (zw5x_gg.nosuch) STORED;

ALTER TABLE zw5x_gg ADD COLUMN g3 int GENERATED ALWAYS AS (zw5x_gg.a) STORED;
INSERT INTO zw5x_gg (a) VALUES (5);

-- begin-expected
-- columns: a | g3
-- row: 5 | 5
-- end-expected
SELECT a, g3 FROM zw5x_gg;

DROP TABLE zw5x_gg;
