-- ============================================================================
-- ALTER TABLE ... ADD COLUMN judges its DEFAULT before it builds the column
-- ============================================================================

CREATE SEQUENCE zzt9x_sq;
CREATE TABLE zzt9x_ac (k int);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "c1" is of type integer but default expression is of type text
-- end-expected-error
ALTER TABLE zzt9x_ac ADD COLUMN c1 int DEFAULT 'abc'::text;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "c2" is of type boolean but default expression is of type integer
-- end-expected-error
ALTER TABLE zzt9x_ac ADD COLUMN c2 bool DEFAULT 1;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "c3" is of type integer but default expression is of type timestamp with time zone
-- end-expected-error
ALTER TABLE zzt9x_ac ADD COLUMN c3 int DEFAULT now();

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "c5" is of type integer but default expression is of type boolean
-- end-expected-error
ALTER TABLE zzt9x_ac ADD COLUMN c5 int DEFAULT true;

-- stmt: a bare string literal is still of type unknown, so a bad one is bad input rather
-- than a mismatch
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "abc"
-- end-expected-error
ALTER TABLE zzt9x_ac ADD COLUMN c4 int DEFAULT 'abc';

-- stmt: the pairs PostgreSQL has an assignment cast for stand
ALTER TABLE zzt9x_ac ADD COLUMN c6 date DEFAULT now();
ALTER TABLE zzt9x_ac ADD COLUMN c7 text DEFAULT 1;
ALTER TABLE zzt9x_ac ADD COLUMN c8 int DEFAULT random();
ALTER TABLE zzt9x_ac ADD COLUMN c9 int DEFAULT nextval('zzt9x_sq');

-- stmt: nothing a refusal named was built
-- begin-expected
-- columns: cols
-- row: k:integer,c6:date,c7:text,c8:integer,c9:integer
-- end-expected
SELECT string_agg(attname||':'||atttypid::regtype::text, ',' ORDER BY attnum) AS cols
  FROM pg_attribute WHERE attrelid = 'zzt9x_ac'::regclass AND attnum > 0 AND NOT attisdropped;

DROP TABLE zzt9x_ac;
DROP SEQUENCE zzt9x_sq;

-- stmt: the refusals that came first still come first
CREATE TABLE zzt9x_ad2 (k int);

-- begin-expected-error
-- sqlstate: 42701
-- message-like: column "k" of relation "zzt9x_ad2" already exists
-- end-expected-error
ALTER TABLE zzt9x_ad2 ADD COLUMN k int DEFAULT 'x'::text;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "nosuchtype" does not exist
-- end-expected-error
ALTER TABLE zzt9x_ad2 ADD COLUMN z nosuchtype DEFAULT 'x'::text;

-- begin-expected-error
-- sqlstate: 42701
-- message-like: column name "ctid" conflicts with a system column name
-- end-expected-error
ALTER TABLE zzt9x_ad2 ADD COLUMN ctid int DEFAULT 'x'::text;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in DEFAULT expression
-- end-expected-error
ALTER TABLE zzt9x_ad2 ADD COLUMN z int DEFAULT (SELECT 1);

-- stmt: but a column-level PRIMARY KEY does not: the default is judged first
-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "z" is of type integer but default expression is of type text
-- end-expected-error
ALTER TABLE zzt9x_ad2 ADD COLUMN z int PRIMARY KEY DEFAULT 'x'::text;

-- begin-expected
-- columns: cols
-- row: k
-- end-expected
SELECT string_agg(attname, ',' ORDER BY attnum) AS cols
  FROM pg_attribute WHERE attrelid = 'zzt9x_ad2'::regclass AND attnum > 0 AND NOT attisdropped;

DROP TABLE zzt9x_ad2;

-- ============================================================================
-- The column's type is named the way the catalogue names it, for every kind of
-- expression a DEFAULT can be
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type integer but default expression is of type timestamp with time zone
-- end-expected-error
CREATE TABLE zzt9x_n1 (b int DEFAULT now());

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type integer but default expression is of type timestamp with time zone
-- end-expected-error
CREATE TABLE zzt9x_n2 (b int DEFAULT current_timestamp);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type integer but default expression is of type date
-- end-expected-error
CREATE TABLE zzt9x_n3 (b int DEFAULT current_date);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type integer but default expression is of type time with time zone
-- end-expected-error
CREATE TABLE zzt9x_n4 (b int DEFAULT current_time);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type integer but default expression is of type timestamp without time zone
-- end-expected-error
CREATE TABLE zzt9x_n5 (b int DEFAULT localtimestamp);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type integer but default expression is of type time without time zone
-- end-expected-error
CREATE TABLE zzt9x_n6 (b int DEFAULT localtime);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type integer but default expression is of type name
-- end-expected-error
CREATE TABLE zzt9x_nb (b int DEFAULT current_user);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type integer but default expression is of type name
-- end-expected-error
CREATE TABLE zzt9x_nc (b int DEFAULT session_user);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type integer but default expression is of type name
-- end-expected-error
CREATE TABLE zzt9x_nd (b int DEFAULT current_role);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type integer but default expression is of type name
-- end-expected-error
CREATE TABLE zzt9x_ne (b int DEFAULT current_catalog);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type integer but default expression is of type timestamp with time zone
-- end-expected-error
CREATE TABLE zzt9x_n7 (b int DEFAULT clock_timestamp());

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type integer but default expression is of type text
-- end-expected-error
CREATE TABLE zzt9x_n8 (b int DEFAULT upper('x'));

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type integer but default expression is of type text
-- end-expected-error
CREATE TABLE zzt9x_n9 (b int DEFAULT version());

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type integer but default expression is of type uuid
-- end-expected-error
CREATE TABLE zzt9x_na (b int DEFAULT gen_random_uuid());

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type integer but default expression is of type boolean
-- end-expected-error
CREATE TABLE zzt9x_nf (b int DEFAULT 1 = 1);

-- stmt: a subquery is refused before its type is ever reached
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in DEFAULT expression
-- end-expected-error
CREATE TABLE zzt9x_nk (b int DEFAULT (SELECT 1));

-- stmt: the same rule on columns that are not numbers
-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type boolean but default expression is of type timestamp with time zone
-- end-expected-error
CREATE TABLE zzt9x_ng (b bool DEFAULT now());

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type uuid but default expression is of type timestamp with time zone
-- end-expected-error
CREATE TABLE zzt9x_nh (b uuid DEFAULT now());

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type jsonb but default expression is of type integer
-- end-expected-error
CREATE TABLE zzt9x_ni (b jsonb DEFAULT 1);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type bytea but default expression is of type integer
-- end-expected-error
CREATE TABLE zzt9x_nl (b bytea DEFAULT 1);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type inet but default expression is of type integer
-- end-expected-error
CREATE TABLE zzt9x_nm (b inet DEFAULT 1);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type date but default expression is of type integer
-- end-expected-error
CREATE TABLE zzt9x_nj (b date DEFAULT length('abc'));

-- stmt: not one of them was built
-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::int AS cnt FROM pg_class WHERE relname LIKE 'zzt9x_n%' AND relkind = 'r';

-- ============================================================================
-- The defaults PostgreSQL accepts are still accepted, and still compute
-- ============================================================================

CREATE TABLE zzt9x_ok (a int DEFAULT length('abc'), b int DEFAULT 1+1,
                       c int DEFAULT 1.5*2, e date DEFAULT now(),
                       f timestamp DEFAULT current_timestamp,
                       g timestamptz DEFAULT current_date,
                       h text DEFAULT 1, i money DEFAULT 0,
                       j int DEFAULT extract(year from now()),
                       k int DEFAULT NULL, l int DEFAULT pi());
INSERT INTO zzt9x_ok (k) VALUES (1);

-- begin-expected
-- columns: a | b | c | h | l | k
-- row: 3 | 2 | 3 | 1 | 3 | 1
-- end-expected
SELECT a, b, c, h, l, k FROM zzt9x_ok;

-- begin-expected
-- columns: today | this_year
-- row: t | t
-- end-expected
SELECT e = current_date AS today, j = extract(year from now())::int AS this_year FROM zzt9x_ok;

DROP TABLE zzt9x_ok;

-- stmt: a value function is not a condition either
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of CHECK must be type boolean, not type date
-- end-expected-error
CREATE TABLE zzt9x_bc (a int CHECK (current_date));

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of CHECK must be type boolean, not type time without time zone
-- end-expected-error
CREATE TABLE zzt9x_be (a int CHECK (localtime));

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of CHECK must be type boolean, not type name
-- end-expected-error
CREATE TABLE zzt9x_bd (a int CHECK (current_user));

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of CHECK must be type boolean, not type timestamp with time zone
-- end-expected-error
CREATE TABLE zzt9x_bf (a int CHECK (now()));

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::int AS cnt FROM pg_class WHERE relname LIKE 'zzt9x_b%' AND relkind = 'r';

-- ============================================================================
-- A typed table's columns are the type's, and may not be renamed or retyped
-- ============================================================================

CREATE TYPE zzt9x_ct AS (x int, y text);
CREATE TABLE zzt9x_of OF zzt9x_ct;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot rename column of typed table
-- end-expected-error
ALTER TABLE zzt9x_of RENAME COLUMN y TO z;

-- stmt: the gate is the first thing the rename does, so a column that does not exist
-- answers the same way
-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot rename column of typed table
-- end-expected-error
ALTER TABLE zzt9x_of RENAME COLUMN nosuch TO z;

-- stmt: and so does a rename to the name the column already has
-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot rename column of typed table
-- end-expected-error
ALTER TABLE zzt9x_of RENAME COLUMN x TO x;

-- begin-expected
-- columns: cols
-- row: x,y
-- end-expected
SELECT string_agg(attname, ',' ORDER BY attnum) AS cols
  FROM pg_attribute WHERE attrelid = 'zzt9x_of'::regclass AND attnum > 0 AND NOT attisdropped;

-- stmt: the table itself can still be renamed
ALTER TABLE zzt9x_of RENAME TO zzt9x_of9;
ALTER TABLE zzt9x_of9 RENAME TO zzt9x_of;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot alter column type of typed table
-- end-expected-error
ALTER TABLE zzt9x_of ALTER COLUMN x TYPE bigint;

-- stmt: even a retype to the type the column already has
-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot alter column type of typed table
-- end-expected-error
ALTER TABLE zzt9x_of ALTER COLUMN y TYPE text;

-- stmt: and one to a type that does not exist, and to the serial shorthand
-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot alter column type of typed table
-- end-expected-error
ALTER TABLE zzt9x_of ALTER COLUMN x TYPE nosuchtype;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot alter column type of typed table
-- end-expected-error
ALTER TABLE zzt9x_of ALTER COLUMN x TYPE serial;

-- stmt: here, unlike the rename, the column lookup comes first
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" of relation "zzt9x_of" does not exist
-- end-expected-error
ALTER TABLE zzt9x_of ALTER COLUMN nosuch TYPE text;

-- stmt: what a typed table's column does accept
ALTER TABLE zzt9x_of ALTER COLUMN x SET DEFAULT 1;
ALTER TABLE zzt9x_of ALTER COLUMN x SET NOT NULL;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot add column to typed table
-- end-expected-error
ALTER TABLE zzt9x_of ADD COLUMN q int;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot drop column from typed table
-- end-expected-error
ALTER TABLE zzt9x_of DROP COLUMN x;

-- stmt: the table still reads as its type does
-- begin-expected
-- columns: cols
-- row: x:integer,y:text
-- end-expected
SELECT string_agg(attname||':'||atttypid::regtype::text, ',' ORDER BY attnum) AS cols
  FROM pg_attribute WHERE attrelid = 'zzt9x_of'::regclass AND attnum > 0 AND NOT attisdropped;

-- begin-expected
-- columns: cols
-- row: x:integer,y:text
-- end-expected
SELECT string_agg(attname||':'||atttypid::regtype::text, ',' ORDER BY attnum) AS cols
  FROM pg_attribute WHERE attrelid = 'zzt9x_ct'::regclass AND attnum > 0 AND NOT attisdropped;

-- stmt: an ordinary table takes both statements
CREATE TABLE zzt9x_pl (x int, y text);
ALTER TABLE zzt9x_pl RENAME COLUMN y TO z;
ALTER TABLE zzt9x_pl ALTER COLUMN x TYPE bigint;

-- begin-expected
-- columns: cols
-- row: x:bigint,z:text
-- end-expected
SELECT string_agg(attname||':'||atttypid::regtype::text, ',' ORDER BY attnum) AS cols
  FROM pg_attribute WHERE attrelid = 'zzt9x_pl'::regclass AND attnum > 0 AND NOT attisdropped;

DROP TABLE zzt9x_pl;
DROP TABLE zzt9x_of;
DROP TYPE zzt9x_ct;
