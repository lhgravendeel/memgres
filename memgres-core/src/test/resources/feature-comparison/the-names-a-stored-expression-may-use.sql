-- ============================================================================
-- A stored expression may name the relation being defined
--
-- PostgreSQL stores a reference to the relation rather than the name that was
-- written, so a qualifier is judged where it is written and never reaches the
-- stored tree. A qualifier naming anything else is a relation the statement
-- never mentioned, and that is what it is told.
-- ============================================================================
CREATE TABLE zze6gd_q (a int, b int GENERATED ALWAYS AS (zze6gd_q.a * 2) STORED);
INSERT INTO zze6gd_q (a) VALUES (5);

-- begin-expected
-- columns: a | b
-- row: 5 | 10
-- end-expected
SELECT a, b FROM zze6gd_q;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "nosuchrel"
-- end-expected-error
CREATE TABLE zze6gd_q2 (a int, b int GENERATED ALWAYS AS (nosuchrel.a) STORED);

-- Another relation's name, even one that exists, is not in scope either.
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "zze6gd_q"
-- end-expected-error
CREATE TABLE zze6gd_q3 (a int, b int GENERATED ALWAYS AS (zze6gd_q.a) STORED);

-- A qualified reference to a generated column is still a generated column.
-- begin-expected-error
-- sqlstate: 42P17
-- message-like: cannot use generated column "g" in column generation expression
-- detail-like: A generated column cannot reference another generated column.
-- end-expected-error
CREATE TABLE zze6gd_q4 (a int, g int GENERATED ALWAYS AS (zze6gd_q4.g) STORED);

DROP TABLE zze6gd_q;

-- The relation's own name reaches it wherever a definition is stored: in a
-- CHECK, in an index key, in an index predicate and in a USING clause.
CREATE TABLE zze6gd_r (a int, nosuch int, CHECK (zze6gd_r.a > 0));
CREATE INDEX zze6gd_r_i ON zze6gd_r (a) WHERE zze6gd_r.a > 0;
CREATE INDEX zze6gd_r_j ON zze6gd_r ((zze6gd_r.a + 1));
ALTER TABLE zze6gd_r ALTER COLUMN a TYPE bigint USING zze6gd_r.a;

-- begin-expected
-- columns: t
-- row: bigint
-- end-expected
SELECT format_type(atttypid, atttypmod) AS t FROM pg_attribute
  WHERE attrelid = 'zze6gd_r'::regclass AND attname = 'a';

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "nosuchrel"
-- end-expected-error
CREATE INDEX zze6gd_r_k ON zze6gd_r (a) WHERE nosuchrel.a > 0;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "nosuchrel"
-- end-expected-error
CREATE INDEX zze6gd_r_l ON zze6gd_r ((nosuchrel.a + 1));

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "nosuchrel"
-- end-expected-error
ALTER TABLE zze6gd_r ADD CONSTRAINT zze6gd_r_c CHECK (nosuchrel.a > 0);

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "nosuchrel"
-- end-expected-error
ALTER TABLE zze6gd_r ALTER COLUMN a TYPE int USING nosuchrel.a;

-- ============================================================================
-- A name that is nearly one of the relation's own is offered as a hint
--
-- The one relation the definition is stored on is the whole of what was in
-- scope, so PostgreSQL offers a column of it spelled almost the same way --
-- qualified, as it writes every such suggestion.
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchh" does not exist
-- hint-like: Perhaps you meant to reference the column "zze6gd_r.nosuch".
-- end-expected-error
ALTER TABLE zze6gd_r ADD CONSTRAINT zze6gd_r_c2 CHECK (nosuchh > 0);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchh" does not exist
-- hint-like: Perhaps you meant to reference the column "zze6gd_r.nosuch".
-- end-expected-error
ALTER TABLE zze6gd_r ADD COLUMN gg int GENERATED ALWAYS AS (nosuchh) STORED;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchh" does not exist
-- hint-like: Perhaps you meant to reference the column "zze6gd_r.nosuch".
-- end-expected-error
ALTER TABLE zze6gd_r ALTER COLUMN a TYPE int USING nosuchh;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchh" does not exist
-- hint-like: Perhaps you meant to reference the column "zze6gd_r.nosuch".
-- end-expected-error
CREATE INDEX zze6gd_r_m ON zze6gd_r (a) WHERE nosuchh > 0;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchh" does not exist
-- hint-like: Perhaps you meant to reference the column "zze6gd_r.nosuch".
-- end-expected-error
CREATE INDEX zze6gd_r_n ON zze6gd_r ((nosuchh + 1));

-- Nothing the refusals turned away was created.
-- begin-expected
-- columns: indexname
-- row: zze6gd_r_i
-- row: zze6gd_r_j
-- end-expected
SELECT indexname FROM pg_indexes WHERE tablename = 'zze6gd_r' ORDER BY 1;

INSERT INTO zze6gd_r VALUES (3, 4);

-- begin-expected
-- columns: a | nosuch
-- row: 3 | 4
-- end-expected
SELECT a, nosuch FROM zze6gd_r;

DROP TABLE zze6gd_r;

-- A DEFAULT names no column at all, its own relation's included.
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use column reference in DEFAULT expression
-- end-expected-error
CREATE TABLE zze6gd_gt (a int DEFAULT zze6gd_gt.a);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use column reference in DEFAULT expression
-- end-expected-error
CREATE TABLE zze6gd_gu (a int, b int DEFAULT nosuchrel.a);

-- ============================================================================
-- A function a stored expression names has to exist when it is written
--
-- PostgreSQL resolves the name with the argument types it worked out, so the
-- complaint says which signature it looked for. A literal written with no type
-- of its own is still of type unknown and is named as one.
-- ============================================================================
CREATE TABLE zze6gd_f (a int, b text);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunc(integer) does not exist
-- hint-like: No function matches the given name and argument types. You might need to add explicit type casts.
-- end-expected-error
ALTER TABLE zze6gd_f ADD CONSTRAINT zze6gd_f_c CHECK (nosuchfunc(a) > 0);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunc(unknown) does not exist
-- end-expected-error
ALTER TABLE zze6gd_f ADD CONSTRAINT zze6gd_f_c2 CHECK (nosuchfunc('x') > 0);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunc(integer, unknown) does not exist
-- end-expected-error
ALTER TABLE zze6gd_f ADD CONSTRAINT zze6gd_f_c3 CHECK (nosuchfunc(a, 'x') > 0);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunc() does not exist
-- end-expected-error
ALTER TABLE zze6gd_f ADD CONSTRAINT zze6gd_f_c4 CHECK (nosuchfunc() > 0);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_catalog.nosuchfunc(integer) does not exist
-- end-expected-error
ALTER TABLE zze6gd_f ADD CONSTRAINT zze6gd_f_c5 CHECK (pg_catalog.nosuchfunc(a) > 0);

-- Every place PostgreSQL stores an expression resolves it the same way.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunc(integer) does not exist
-- end-expected-error
CREATE TABLE zze6gd_f2 (a int, CHECK (nosuchfunc(a) > 0));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunc(integer) does not exist
-- end-expected-error
ALTER TABLE zze6gd_f ALTER COLUMN a TYPE int USING nosuchfunc(a);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunc(integer) does not exist
-- end-expected-error
ALTER TABLE zze6gd_f ADD COLUMN g int GENERATED ALWAYS AS (nosuchfunc(a)) STORED;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunc(integer) does not exist
-- end-expected-error
CREATE TABLE zze6gd_f3 (a int, g int GENERATED ALWAYS AS (nosuchfunc(a)) STORED);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunc(integer) does not exist
-- end-expected-error
CREATE TABLE zze6gd_f4 (a int DEFAULT nosuchfunc(1));

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunc(integer) does not exist
-- end-expected-error
ALTER TABLE zze6gd_f ADD COLUMN h int DEFAULT nosuchfunc(1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunc(integer) does not exist
-- end-expected-error
ALTER TABLE zze6gd_f ALTER COLUMN a SET DEFAULT nosuchfunc(1);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunc(integer) does not exist
-- end-expected-error
CREATE INDEX zze6gd_f_i ON zze6gd_f (a) WHERE nosuchfunc(a) > 0;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunc(integer) does not exist
-- end-expected-error
CREATE INDEX zze6gd_f_j ON zze6gd_f ((nosuchfunc(a)));

-- A call of one argument whose name is a type is a cast written the other way
-- round, and PostgreSQL reads it as one when it finds no function of that name.
-- It is the one argument that makes it a cast.
CREATE DOMAIN zze6gd_dom AS int;
ALTER TABLE zze6gd_f ADD CONSTRAINT zze6gd_f_d1 CHECK (zze6gd_dom(a) > 0);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zze6gd_dom(integer, text) does not exist
-- end-expected-error
ALTER TABLE zze6gd_f ADD CONSTRAINT zze6gd_f_d2 CHECK (zze6gd_dom(a, b) > 0);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function zze6gd_dom() does not exist
-- end-expected-error
ALTER TABLE zze6gd_f ADD CONSTRAINT zze6gd_f_d3 CHECK (zze6gd_dom() > 0);

-- A function that does exist is left alone wherever a definition names it.
CREATE FUNCTION zze6gd_fn(int) RETURNS int AS 'SELECT $1' LANGUAGE sql IMMUTABLE;
ALTER TABLE zze6gd_f ADD CONSTRAINT zze6gd_f_d4 CHECK (zze6gd_fn(a) > 0);
CREATE INDEX zze6gd_f_fi ON zze6gd_f ((zze6gd_fn(a)));
CREATE INDEX zze6gd_f_k ON zze6gd_f (lower(b));
CREATE INDEX zze6gd_f_l ON zze6gd_f (a) WHERE upper(b) > 'A';
ALTER TABLE zze6gd_f ADD CONSTRAINT zze6gd_f_ok CHECK (length(b) < 20);
ALTER TABLE zze6gd_f ADD COLUMN gg text GENERATED ALWAYS AS (upper(b)) STORED;
INSERT INTO zze6gd_f (a, b) VALUES (1, 'ab');

-- begin-expected
-- columns: a | gg
-- row: 1 | AB
-- end-expected
SELECT a, gg FROM zze6gd_f;

-- begin-expected
-- columns: conname
-- row: zze6gd_f_d1
-- row: zze6gd_f_d4
-- row: zze6gd_f_ok
-- end-expected
SELECT conname FROM pg_constraint WHERE conrelid = 'zze6gd_f'::regclass ORDER BY 1;

DROP TABLE zze6gd_f;
DROP FUNCTION zze6gd_fn(int);
DROP DOMAIN zze6gd_dom;

-- ============================================================================
-- What a CHECK is refused for is decided by where the faults are written
--
-- PostgreSQL transforms the expression as it walks it, settling every name and
-- every call at the node it stands at, so the same two faults the other way
-- round get the other complaint.
-- ============================================================================
CREATE TABLE zze6gd_o (a int, nosuch int);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- hint-like: Perhaps you meant to reference the column "zze6gd_o.nosuch".
-- end-expected-error
ALTER TABLE zze6gd_o ADD CONSTRAINT zze6gd_o_c1 CHECK (nosuchcol > 0 AND (SELECT true));

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in check constraint
-- end-expected-error
ALTER TABLE zze6gd_o ADD CONSTRAINT zze6gd_o_c2 CHECK ((SELECT true) AND nosuchcol > 0);

-- An aggregate is the same: it is refused where it stands, and a name written
-- before it is reached first.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
ALTER TABLE zze6gd_o ADD CONSTRAINT zze6gd_o_c3 CHECK (nosuchcol > 0 AND count(a) > 0);

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in check constraints
-- end-expected-error
ALTER TABLE zze6gd_o ADD CONSTRAINT zze6gd_o_c4 CHECK (count(a) > 0 AND nosuchcol > 0);

-- And so is a call naming no function.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunc(integer) does not exist
-- end-expected-error
ALTER TABLE zze6gd_o ADD CONSTRAINT zze6gd_o_c5 CHECK (nosuchfunc(a) > 0 AND (SELECT true));

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in check constraint
-- end-expected-error
ALTER TABLE zze6gd_o ADD CONSTRAINT zze6gd_o_c6 CHECK ((SELECT true) AND nosuchfunc(a) > 0);

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunc(integer) does not exist
-- end-expected-error
ALTER TABLE zze6gd_o ADD CONSTRAINT zze6gd_o_c7 CHECK (nosuchfunc(a) > 0 AND nosuchcol > 0);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
ALTER TABLE zze6gd_o ADD CONSTRAINT zze6gd_o_c8 CHECK (nosuchcol > 0 AND nosuchfunc(a) > 0);

-- The arguments of a call are settled before the call itself.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
ALTER TABLE zze6gd_o ADD CONSTRAINT zze6gd_o_c9 CHECK (nosuchfunc(nosuchcol) > 0);

-- The same reading in a CREATE TABLE's own CHECK clause, where the hint names
-- the relation the statement is defining.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- hint-like: Perhaps you meant to reference the column "zze6gd_o2.nosuch".
-- end-expected-error
CREATE TABLE zze6gd_o2 (a int, nosuch int, CHECK (nosuchcol > 0 AND (SELECT true)));

-- A subquery is refused wherever it stands, in every shape it has.
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in check constraint
-- end-expected-error
ALTER TABLE zze6gd_o ADD CONSTRAINT zze6gd_o_s2 CHECK (a IN (SELECT 1));

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in check constraint
-- end-expected-error
ALTER TABLE zze6gd_o ADD CONSTRAINT zze6gd_o_s3 CHECK (EXISTS (SELECT 1));

-- Nothing that was accepted before is refused now.
ALTER TABLE zze6gd_o ADD CONSTRAINT zze6gd_o_ok CHECK (a > 0 AND nosuch IS NOT NULL);
INSERT INTO zze6gd_o VALUES (1, 2);

-- begin-expected
-- columns: a | nosuch
-- row: 1 | 2
-- end-expected
SELECT a, nosuch FROM zze6gd_o;

-- begin-expected
-- columns: conname
-- row: zze6gd_o_ok
-- end-expected
SELECT conname FROM pg_constraint WHERE conrelid = 'zze6gd_o'::regclass ORDER BY 1;

DROP TABLE zze6gd_o;
