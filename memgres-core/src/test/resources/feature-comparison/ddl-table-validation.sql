-- ============================================================================
-- Feature Comparison: definition-time validation of tables and columns
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Covers the checks PostgreSQL makes before it will record a table or column
-- definition, and two valid statements that have to be accepted:
--   1.  A column name that would shadow a system column (42701)
--   2.  DEFAULT expressions: subquery, column reference, wrong type
--   3.  Inheritance: dropping/renaming an inherited column, ALTER TABLE ONLY,
--       INHERIT column matching, NO INHERIT on a non-parent
--   4.  DROP NOT NULL on a primary-key column (42P16)
--   5.  A CHECK or POLICY expression that is not a predicate (42804)
--   6.  COLLATE on a type that carries no collation (42804)
--   7.  Identity: column type, competing DEFAULT, DROP IDENTITY, ADD GENERATED
--   8.  Keys on a VIRTUAL generated column (0A000)
--   9.  ALTER COLUMN ... DROP EXPRESSION
--   10. SET STORAGE / SET COMPRESSION option and type validation
--   11. SET CONSTRAINTS naming a constraint that does not exist (42704)
--   12. Temporary and permanent tables referencing each other
--   13. CREATE TABLE AS with a duplicate output column name (42701)
--   14. ALTER TABLE ... SET WITHOUT CLUSTER / WITHOUT OIDS
-- ============================================================================

-- ============================================================================
-- 1. System column names
-- ============================================================================

-- stmt: a column named xmax would shadow the system column of that name
-- begin-expected-error
-- sqlstate: 42701
-- message-like: column name "xmax" conflicts with a system column name
-- end-expected-error
CREATE TABLE dtc_sys (xmax int);

-- stmt: ctid likewise
-- begin-expected-error
-- sqlstate: 42701
-- message-like: column name "ctid" conflicts with a system column name
-- end-expected-error
CREATE TABLE dtc_sys (ctid int);

-- stmt: and tableoid
-- begin-expected-error
-- sqlstate: 42701
-- message-like: column name "tableoid" conflicts with a system column name
-- end-expected-error
CREATE TABLE dtc_sys (tableoid int);

-- stmt: a quoted name with capitals is a different identifier and is allowed
CREATE TABLE dtc_sysq ("XMAX" int);

-- stmt: oid has been an ordinary column name since PG 12
CREATE TABLE dtc_sysoid (oid int);

-- setup
CREATE TABLE dtc_sys (i int);

-- stmt: ADD COLUMN is checked the same way
-- begin-expected-error
-- sqlstate: 42701
-- message-like: column name "xmin" conflicts with a system column name
-- end-expected-error
ALTER TABLE dtc_sys ADD COLUMN xmin integer;

-- stmt: so is RENAME COLUMN
-- begin-expected-error
-- sqlstate: 42701
-- message-like: column name "tableoid" conflicts with a system column name
-- end-expected-error
ALTER TABLE dtc_sys RENAME COLUMN i TO tableoid;

-- stmt: renaming a column that is not there names the column, not the relation
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
ALTER TABLE dtc_sys RENAME COLUMN nosuchcol TO j;

-- stmt: an ordinary rename still works
ALTER TABLE dtc_sys RENAME COLUMN i TO j;

-- stmt: and an ordinary ADD COLUMN
ALTER TABLE dtc_sys ADD COLUMN k int;

-- ============================================================================
-- 2. DEFAULT expression validation
-- ============================================================================

-- setup
CREATE TABLE dtc_def (c1 int, c2 int, c3 text, c4 numeric);

-- stmt: a default that is not a value of the column's type
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "wrong_datatype"
-- end-expected-error
ALTER TABLE dtc_def ALTER COLUMN c1 SET DEFAULT 'wrong_datatype';

-- stmt: same for numeric
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type numeric: "zz"
-- end-expected-error
ALTER TABLE dtc_def ALTER COLUMN c4 SET DEFAULT 'zz';

-- stmt: a DEFAULT is evaluated with no query in scope, so a subquery cannot work
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in DEFAULT expression
-- end-expected-error
ALTER TABLE dtc_def ALTER COLUMN c1 SET DEFAULT (SELECT 1);

-- stmt: nor can a column reference
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use column reference in DEFAULT expression
-- end-expected-error
ALTER TABLE dtc_def ALTER COLUMN c1 SET DEFAULT c2;

-- stmt: qualified column reference, same answer
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use column reference in DEFAULT expression
-- end-expected-error
ALTER TABLE dtc_def ALTER COLUMN c1 SET DEFAULT dtc_def.c2;

-- stmt: a name that is no column at all is still a column reference
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use column reference in DEFAULT expression
-- end-expected-error
ALTER TABLE dtc_def ALTER COLUMN c1 SET DEFAULT nosuchcol;

-- stmt: an expression that already has a type is a mismatch, not bad input syntax
-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "c1" is of type integer but default expression is of type timestamp with time zone
-- end-expected-error
ALTER TABLE dtc_def ALTER COLUMN c1 SET DEFAULT now();

-- stmt
-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "c1" is of type integer but default expression is of type text
-- end-expected-error
ALTER TABLE dtc_def ALTER COLUMN c1 SET DEFAULT 'abc'::text;

-- stmt
-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "c1" is of type integer but default expression is of type date
-- end-expected-error
ALTER TABLE dtc_def ALTER COLUMN c1 SET DEFAULT CURRENT_DATE;

-- stmt: altering a column that is not there
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" of relation "dtc_def" does not exist
-- end-expected-error
ALTER TABLE dtc_def ALTER COLUMN nosuchcol SET DEFAULT 1;

-- stmt: ordinary defaults are unaffected
ALTER TABLE dtc_def ALTER COLUMN c1 SET DEFAULT 5;

-- stmt: a string literal that is a valid integer is fine
ALTER TABLE dtc_def ALTER COLUMN c1 SET DEFAULT '7';

-- stmt: text takes an integer literal
ALTER TABLE dtc_def ALTER COLUMN c3 SET DEFAULT 5;

-- stmt: and a timestamp
ALTER TABLE dtc_def ALTER COLUMN c3 SET DEFAULT now();

-- stmt: back to the integer literal for the insert below
ALTER TABLE dtc_def ALTER COLUMN c3 SET DEFAULT 5;

-- stmt: an arithmetic expression
ALTER TABLE dtc_def ALTER COLUMN c1 SET DEFAULT (1+2)*3;

-- stmt: a CASE expression
ALTER TABLE dtc_def ALTER COLUMN c1 SET DEFAULT CASE WHEN true THEN 1 ELSE 2 END;

-- stmt: a volatile function
ALTER TABLE dtc_def ALTER COLUMN c1 SET DEFAULT random();

-- stmt: NULL
ALTER TABLE dtc_def ALTER COLUMN c1 SET DEFAULT NULL;

-- stmt: DROP DEFAULT
ALTER TABLE dtc_def ALTER COLUMN c1 DROP DEFAULT;

-- stmt: the surviving text default is used
INSERT INTO dtc_def (c2) VALUES (1);

-- begin-expected
-- columns: c1|c2|c3
-- row: |1|5
-- end-expected
SELECT c1, c2, c3 FROM dtc_def;

-- stmt: CREATE TABLE checks the same things
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "zzz"
-- end-expected-error
CREATE TABLE dtc_defc (a int DEFAULT 'zzz');

-- stmt: subquery default at creation
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in DEFAULT expression
-- end-expected-error
CREATE TABLE dtc_defc (a int DEFAULT (SELECT 1));

-- stmt: column reference default at creation
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use column reference in DEFAULT expression
-- end-expected-error
CREATE TABLE dtc_defc (a int, b int DEFAULT a);

-- stmt: ordinary defaults at creation still work
CREATE TABLE dtc_defc (a int DEFAULT 3, b text DEFAULT 'x');

-- stmt
INSERT INTO dtc_defc DEFAULT VALUES;

-- begin-expected
-- columns: a|b
-- row: 3|x
-- end-expected
SELECT a, b FROM dtc_defc;

-- setup
CREATE TABLE dtc_defa (a int, b int DEFAULT 1);

-- stmt: ADD COLUMN rejects a subquery default too
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in DEFAULT expression
-- end-expected-error
ALTER TABLE dtc_defa ADD COLUMN c int DEFAULT (SELECT 1);

-- stmt: and a column reference
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use column reference in DEFAULT expression
-- end-expected-error
ALTER TABLE dtc_defa ADD COLUMN d int DEFAULT a;

-- stmt: an ordinary ADD COLUMN default
ALTER TABLE dtc_defa ADD COLUMN e int DEFAULT 4;

-- ============================================================================
-- 3. Inheritance
-- ============================================================================

-- setup
CREATE TABLE dtc_par (a int, b int);
CREATE TABLE dtc_chi (c int) INHERITS (dtc_par);

-- stmt: an inherited column belongs to the parent
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot drop inherited column "a"
-- end-expected-error
ALTER TABLE dtc_chi DROP COLUMN a;

-- stmt: and cannot be renamed on the child either
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot rename inherited column "a"
-- end-expected-error
ALTER TABLE dtc_chi RENAME COLUMN a TO z;

-- stmt: the child's own column can be renamed
ALTER TABLE dtc_chi RENAME COLUMN c TO cc;

-- stmt: and dropped
ALTER TABLE dtc_chi DROP COLUMN cc;

-- stmt: ONLY would leave the child without a column its parent has
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: column must be added to child tables too
-- end-expected-error
ALTER TABLE ONLY dtc_par ADD COLUMN d int;

-- stmt: renaming on the parent alone is refused for the same reason
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: inherited column "a" must be renamed in child tables too
-- end-expected-error
ALTER TABLE ONLY dtc_par RENAME COLUMN a TO aa;

-- stmt: without ONLY the column reaches the child
ALTER TABLE dtc_par ADD COLUMN e int;

-- begin-expected
-- columns: e
-- end-expected
SELECT e FROM dtc_chi;

-- stmt: a rename reaches the child too
ALTER TABLE dtc_par RENAME COLUMN e TO ee;

-- begin-expected
-- columns: ee
-- end-expected
SELECT ee FROM dtc_chi;

-- stmt: and a drop
ALTER TABLE dtc_par DROP COLUMN ee;

-- stmt: the child no longer has it
-- begin-expected-error
-- sqlstate: 42703
-- end-expected-error
SELECT ee FROM dtc_chi;

-- stmt: with ONLY the child keeps the column as its own
ALTER TABLE ONLY dtc_par DROP COLUMN b;

-- setup: a parent that still has both columns, and a table that lacks one of them
-- (dtc_par lost b to the ONLY drop above, so this case needs its own parent)
CREATE TABLE dtc_par2 (a int, b int);
CREATE TABLE dtc_inh1 (a int);

-- stmt
-- begin-expected-error
-- sqlstate: 42804
-- message-like: child table is missing column "b"
-- end-expected-error
ALTER TABLE dtc_inh1 INHERIT dtc_par2;

-- setup: a table whose shared column has a different type
CREATE TABLE dtc_inh2 (a text, b int);

-- stmt
-- begin-expected-error
-- sqlstate: 42804
-- message-like: has different type for column "a"
-- end-expected-error
ALTER TABLE dtc_inh2 INHERIT dtc_par;

-- setup: a table that does match
CREATE TABLE dtc_inh3 (a int, b int);

-- stmt
ALTER TABLE dtc_inh3 INHERIT dtc_par;

-- stmt: inheriting twice from the same parent
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: would be inherited from more than once
-- end-expected-error
ALTER TABLE dtc_inh3 INHERIT dtc_par;

-- stmt: detaching works
ALTER TABLE dtc_inh3 NO INHERIT dtc_par;

-- stmt: detaching again names a relation that is no longer a parent
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: is not a parent of relation "dtc_inh3"
-- end-expected-error
ALTER TABLE dtc_inh3 NO INHERIT dtc_par;

-- setup: the same rules hold for a partitioned parent
CREATE TABLE dtc_prt (i int, t text) PARTITION BY RANGE (i);
CREATE TABLE dtc_prt_p0 PARTITION OF dtc_prt FOR VALUES FROM (0) TO (10);

-- stmt
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: column must be added to child tables too
-- end-expected-error
ALTER TABLE ONLY dtc_prt ADD COLUMN extra2 text;

-- stmt: adding without ONLY reaches the partition
ALTER TABLE dtc_prt ADD COLUMN extra3 text;

-- begin-expected
-- columns: extra3
-- end-expected
SELECT extra3 FROM dtc_prt_p0;

-- stmt: a partition's inherited column cannot be dropped on its own
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot drop inherited column "t"
-- end-expected-error
ALTER TABLE dtc_prt_p0 DROP COLUMN t;

-- stmt: nor renamed
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot rename inherited column "t"
-- end-expected-error
ALTER TABLE dtc_prt_p0 RENAME COLUMN t TO tt;

-- ============================================================================
-- 4. DROP NOT NULL on a primary-key column
-- ============================================================================

-- setup
CREATE TABLE dtc_nn (i int PRIMARY KEY, j int NOT NULL, k int UNIQUE NOT NULL);

-- stmt: the primary key still rejects a null, so the catalog may not say otherwise
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: column "i" is in a primary key
-- end-expected-error
ALTER TABLE dtc_nn ALTER COLUMN i DROP NOT NULL;

-- stmt: an ordinary NOT NULL can be dropped
ALTER TABLE dtc_nn ALTER COLUMN j DROP NOT NULL;

-- stmt: so can one under a UNIQUE constraint
ALTER TABLE dtc_nn ALTER COLUMN k DROP NOT NULL;

-- stmt: a column that is not there
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" of relation "dtc_nn" does not exist
-- end-expected-error
ALTER TABLE dtc_nn ALTER COLUMN nosuchcol DROP NOT NULL;

-- setup: a composite primary key covers each of its columns
CREATE TABLE dtc_nn2 (i int, j int, PRIMARY KEY (i, j));

-- stmt
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: column "j" is in a primary key
-- end-expected-error
ALTER TABLE dtc_nn2 ALTER COLUMN j DROP NOT NULL;

-- ============================================================================
-- 5. Non-boolean CHECK and POLICY expressions
-- ============================================================================

-- setup
CREATE TABLE dtc_chk (i int, t text, n numeric, b boolean);

-- stmt
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of CHECK must be type boolean, not type integer
-- end-expected-error
ALTER TABLE dtc_chk ADD CONSTRAINT dtc_ck1 CHECK (i);

-- stmt
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of CHECK must be type boolean, not type text
-- end-expected-error
ALTER TABLE dtc_chk ADD CONSTRAINT dtc_ck2 CHECK (t);

-- stmt
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of CHECK must be type boolean, not type numeric
-- end-expected-error
ALTER TABLE dtc_chk ADD CONSTRAINT dtc_ck3 CHECK (n);

-- stmt: a cast names the type it casts to
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of CHECK must be type boolean, not type text
-- end-expected-error
ALTER TABLE dtc_chk ADD CONSTRAINT dtc_ck4 CHECK (i::text);

-- stmt: a predicate is fine
ALTER TABLE dtc_chk ADD CONSTRAINT dtc_ck5 CHECK (i > 0);

-- stmt: so is a boolean column
ALTER TABLE dtc_chk ADD CONSTRAINT dtc_ck6 CHECK (b);

-- stmt: NULL is of no type in particular
ALTER TABLE dtc_chk ADD CONSTRAINT dtc_ck7 CHECK (NULL);

-- stmt: a bare string literal is still unknown, and coerces to boolean
ALTER TABLE dtc_chk ADD CONSTRAINT dtc_ck8 CHECK ('t');

-- stmt: a policy is held to the same rule
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of POLICY must be type boolean, not type integer
-- end-expected-error
CREATE POLICY dtc_pol1 ON dtc_chk FOR SELECT USING (i);

-- stmt
CREATE POLICY dtc_pol2 ON dtc_chk FOR SELECT USING (i > 0);

-- stmt: a column-level CHECK at creation is checked too
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of CHECK must be type boolean, not type integer
-- end-expected-error
CREATE TABLE dtc_chk2 (i int CHECK (i));

-- stmt: and a table-level one
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of CHECK must be type boolean, not type integer
-- end-expected-error
CREATE TABLE dtc_chk2 (i int, CHECK (i));

-- ============================================================================
-- 6. COLLATE on a non-collatable type
-- ============================================================================

-- setup
CREATE TABLE dtc_col (i int, t text);

-- stmt
-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type bigint
-- end-expected-error
ALTER TABLE dtc_col ALTER COLUMN i TYPE bigint COLLATE "C";

-- stmt: a collatable target type is accepted
ALTER TABLE dtc_col ALTER COLUMN t TYPE varchar(20) COLLATE "C";

-- stmt
ALTER TABLE dtc_col ALTER COLUMN t TYPE text COLLATE "C";

-- stmt: at creation time as well
-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type integer
-- end-expected-error
CREATE TABLE dtc_col2 (i int COLLATE "C");

-- stmt
-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type date
-- end-expected-error
CREATE TABLE dtc_col2 (d date COLLATE "C");

-- stmt: the collatable types are unaffected
CREATE TABLE dtc_col2 (t text COLLATE "C", v varchar(4) COLLATE "C");

-- stmt
INSERT INTO dtc_col2 VALUES ('a', 'b');

-- begin-expected
-- columns: t|v
-- row: a|b
-- end-expected
SELECT t, v FROM dtc_col2;

-- ============================================================================
-- 7. Identity columns
-- ============================================================================

-- stmt: identity comes from a sequence, so the type has to be an integer type
-- begin-expected-error
-- sqlstate: 22023
-- message-like: identity column type must be smallint, integer, or bigint
-- end-expected-error
CREATE TABLE dtc_idt (i text GENERATED ALWAYS AS IDENTITY);

-- stmt
-- begin-expected-error
-- sqlstate: 22023
-- message-like: identity column type must be smallint, integer, or bigint
-- end-expected-error
CREATE TABLE dtc_idt (i numeric GENERATED ALWAYS AS IDENTITY);

-- stmt
-- begin-expected-error
-- sqlstate: 22023
-- message-like: identity column type must be smallint, integer, or bigint
-- end-expected-error
CREATE TABLE dtc_idt (i date GENERATED BY DEFAULT AS IDENTITY);

-- stmt: a DEFAULT and an identity both claim to supply the value
-- begin-expected-error
-- sqlstate: 42601
-- message-like: both default and identity specified for column "i" of table "dtc_idt"
-- end-expected-error
CREATE TABLE dtc_idt (i int GENERATED ALWAYS AS IDENTITY DEFAULT 1);

-- stmt: order does not matter
-- begin-expected-error
-- sqlstate: 42601
-- message-like: both default and identity specified for column "i" of table "dtc_idt"
-- end-expected-error
CREATE TABLE dtc_idt (i int DEFAULT 1 GENERATED ALWAYS AS IDENTITY);

-- setup
CREATE TABLE dtc_idt (i bigint GENERATED ALWAYS AS IDENTITY, j int);

-- stmt
INSERT INTO dtc_idt (j) VALUES (1);

-- begin-expected
-- columns: i|j
-- row: 1|1
-- end-expected
SELECT i, j FROM dtc_idt;

-- stmt: DROP IDENTITY on a column that has none says so
-- begin-expected-error
-- sqlstate: 55000
-- message-like: column "j" of relation "dtc_idt" is not an identity column
-- end-expected-error
ALTER TABLE dtc_idt ALTER COLUMN j DROP IDENTITY;

-- stmt: IF EXISTS makes that silent
ALTER TABLE dtc_idt ALTER COLUMN j DROP IDENTITY IF EXISTS;

-- stmt: but IF EXISTS does not cover a column that is not there
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" of relation "dtc_idt" does not exist
-- end-expected-error
ALTER TABLE dtc_idt ALTER COLUMN nosuchcol DROP IDENTITY IF EXISTS;

-- stmt: dropping a real identity works
ALTER TABLE dtc_idt ALTER COLUMN i DROP IDENTITY;

-- stmt: and it is gone afterwards
-- begin-expected-error
-- sqlstate: 55000
-- message-like: column "i" of relation "dtc_idt" is not an identity column
-- end-expected-error
ALTER TABLE dtc_idt ALTER COLUMN i DROP IDENTITY;

-- setup
CREATE TABLE dtc_idt2 (i int, j int NOT NULL, k int GENERATED ALWAYS AS IDENTITY);

-- stmt: identity fills every row, so the column may not still admit nulls
-- begin-expected-error
-- sqlstate: 55000
-- message-like: must be declared NOT NULL before identity can be added
-- end-expected-error
ALTER TABLE dtc_idt2 ALTER COLUMN i ADD GENERATED ALWAYS AS IDENTITY;

-- stmt: a column that already has one
-- begin-expected-error
-- sqlstate: 55000
-- message-like: column "k" of relation "dtc_idt2" is already an identity column
-- end-expected-error
ALTER TABLE dtc_idt2 ALTER COLUMN k ADD GENERATED ALWAYS AS IDENTITY;

-- stmt: a NOT NULL column takes one
ALTER TABLE dtc_idt2 ALTER COLUMN j ADD GENERATED ALWAYS AS IDENTITY;

-- stmt
INSERT INTO dtc_idt2 (i) VALUES (5);

-- begin-expected
-- columns: i|j|k
-- row: 5|1|1
-- end-expected
SELECT i, j, k FROM dtc_idt2;

-- setup: BY DEFAULT identity still accepts an explicit value
CREATE TABLE dtc_idt3 (i smallint GENERATED BY DEFAULT AS IDENTITY, j int);
INSERT INTO dtc_idt3 (j) VALUES (5);
INSERT INTO dtc_idt3 (i, j) VALUES (9, 6);

-- begin-expected
-- columns: i|j
-- row: 1|5
-- row: 9|6
-- end-expected
SELECT i, j FROM dtc_idt3 ORDER BY j;

-- ============================================================================
-- 8. Keys on a VIRTUAL generated column
-- ============================================================================

-- stmt: a virtual column is never stored, so an index has nothing to hold
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: primary keys on virtual generated columns are not supported
-- end-expected-error
CREATE TABLE dtc_vg (a int, b int GENERATED ALWAYS AS (a) VIRTUAL PRIMARY KEY);

-- stmt
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: unique constraints on virtual generated columns are not supported
-- end-expected-error
CREATE TABLE dtc_vg (a int, b int GENERATED ALWAYS AS (a) VIRTUAL UNIQUE);

-- stmt: a table-level key over one is refused the same way
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: primary keys on virtual generated columns are not supported
-- end-expected-error
CREATE TABLE dtc_vg (a int, b int GENERATED ALWAYS AS (a) VIRTUAL, PRIMARY KEY (b));

-- stmt: a STORED generated column can be a primary key
CREATE TABLE dtc_vg (a int, b int GENERATED ALWAYS AS (a) STORED PRIMARY KEY);

-- stmt
INSERT INTO dtc_vg (a) VALUES (3);

-- begin-expected
-- columns: a|b
-- row: 3|3
-- end-expected
SELECT a, b FROM dtc_vg;

-- stmt: a virtual generated column with no key on it is fine
CREATE TABLE dtc_vg2 (a int, b int GENERATED ALWAYS AS (a*2) VIRTUAL);

-- stmt
INSERT INTO dtc_vg2 (a) VALUES (3);

-- begin-expected
-- columns: a|b
-- row: 3|6
-- end-expected
SELECT a, b FROM dtc_vg2;

-- ============================================================================
-- 9. ALTER COLUMN ... DROP EXPRESSION
-- ============================================================================

-- setup
CREATE TABLE dtc_dex (a int, b int GENERATED ALWAYS AS (a*2) STORED, c int GENERATED ALWAYS AS (a+1) VIRTUAL);
INSERT INTO dtc_dex (a) VALUES (4);

-- stmt: the documented way to make a generated column ordinary
ALTER TABLE dtc_dex ALTER COLUMN b DROP EXPRESSION;

-- begin-expected
-- columns: a|b|c
-- row: 4|8|5
-- end-expected
SELECT a, b, c FROM dtc_dex;

-- stmt: a virtual column has no stored values to keep
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: DROP EXPRESSION is not supported for virtual generated columns
-- end-expected-error
ALTER TABLE dtc_dex ALTER COLUMN c DROP EXPRESSION;

-- stmt: dropping it a second time says there is nothing to drop
-- begin-expected-error
-- sqlstate: 55000
-- message-like: column "b" of relation "dtc_dex" is not a generated column
-- end-expected-error
ALTER TABLE dtc_dex ALTER COLUMN b DROP EXPRESSION;

-- stmt: IF EXISTS makes that silent
ALTER TABLE dtc_dex ALTER COLUMN b DROP EXPRESSION IF EXISTS;

-- stmt: but not for a column that is not there
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" of relation "dtc_dex" does not exist
-- end-expected-error
ALTER TABLE dtc_dex ALTER COLUMN nosuchcol DROP EXPRESSION IF EXISTS;

-- stmt: the column now takes a written value
INSERT INTO dtc_dex (a, b) VALUES (1, 9);

-- begin-expected
-- columns: a|b|c
-- row: 1|9|2
-- row: 4|8|5
-- end-expected
SELECT a, b, c FROM dtc_dex ORDER BY a;

-- stmt: the virtual column still rejects one
-- begin-expected-error
-- sqlstate: 428C9
-- end-expected-error
INSERT INTO dtc_dex (a, c) VALUES (2, 9);

-- begin-expected
-- columns: attname|attgenerated
-- row: a|
-- row: b|
-- row: c|v
-- end-expected
SELECT attname, attgenerated FROM pg_attribute WHERE attrelid='dtc_dex'::regclass AND attnum>0 ORDER BY attnum;

-- ============================================================================
-- 10. SET STORAGE and SET COMPRESSION
-- ============================================================================

-- setup
CREATE TABLE dtc_sto (i int, c text, n numeric, u uuid, ia int[]);

-- stmt
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid storage type "nonsense"
-- end-expected-error
ALTER TABLE dtc_sto ALTER COLUMN c SET STORAGE NONSENSE;

-- stmt: a fixed-length value is never stored out of line
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: column data type integer can only have storage PLAIN
-- end-expected-error
ALTER TABLE dtc_sto ALTER COLUMN i SET STORAGE EXTERNAL;

-- stmt
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: column data type uuid can only have storage PLAIN
-- end-expected-error
ALTER TABLE dtc_sto ALTER COLUMN u SET STORAGE MAIN;

-- stmt: PLAIN is always allowed
ALTER TABLE dtc_sto ALTER COLUMN i SET STORAGE PLAIN;

-- stmt: the varlena types take any of them
ALTER TABLE dtc_sto ALTER COLUMN c SET STORAGE EXTERNAL;

-- stmt
ALTER TABLE dtc_sto ALTER COLUMN c SET STORAGE MAIN;

-- stmt
ALTER TABLE dtc_sto ALTER COLUMN n SET STORAGE EXTENDED;

-- stmt
ALTER TABLE dtc_sto ALTER COLUMN ia SET STORAGE EXTERNAL;

-- stmt
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" of relation "dtc_sto" does not exist
-- end-expected-error
ALTER TABLE dtc_sto ALTER COLUMN nosuchcol SET STORAGE PLAIN;

-- stmt: only the built-in compression methods exist
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid compression method "nosuchmethod"
-- end-expected-error
ALTER TABLE dtc_sto ALTER COLUMN c SET COMPRESSION nosuchmethod;

-- stmt
ALTER TABLE dtc_sto ALTER COLUMN c SET COMPRESSION pglz;

-- stmt
ALTER TABLE dtc_sto ALTER COLUMN c SET COMPRESSION DEFAULT;

-- stmt
ALTER TABLE dtc_sto ALTER COLUMN n SET COMPRESSION pglz;

-- stmt: compression only applies to varlena values
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: column data type integer does not support compression
-- end-expected-error
ALTER TABLE dtc_sto ALTER COLUMN i SET COMPRESSION pglz;

-- stmt
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: column data type uuid does not support compression
-- end-expected-error
ALTER TABLE dtc_sto ALTER COLUMN u SET COMPRESSION pglz;

-- stmt
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" of relation "dtc_sto" does not exist
-- end-expected-error
ALTER TABLE dtc_sto ALTER COLUMN nosuchcol SET COMPRESSION pglz;

-- begin-expected
-- columns: attname|attstorage
-- row: i|p
-- row: c|m
-- row: n|x
-- row: u|p
-- row: ia|e
-- end-expected
SELECT attname, attstorage FROM pg_attribute WHERE attrelid='dtc_sto'::regclass AND attnum>0 ORDER BY attnum;

-- ============================================================================
-- 11. SET CONSTRAINTS
-- ============================================================================

-- stmt: a name that matches no constraint would silently do nothing
-- begin-expected-error
-- sqlstate: 42704
-- message-like: constraint "dtc_no_such_constraint" does not exist
-- end-expected-error
SET CONSTRAINTS dtc_no_such_constraint DEFERRED;

-- stmt: ALL never names one
SET CONSTRAINTS ALL DEFERRED;

-- stmt
SET CONSTRAINTS ALL IMMEDIATE;

-- setup
CREATE TABLE dtc_cons (i int, CONSTRAINT dtc_uq1 UNIQUE (i) DEFERRABLE);

-- stmt: a constraint that does exist
SET CONSTRAINTS dtc_uq1 DEFERRED;

-- stmt
SET CONSTRAINTS dtc_uq1 IMMEDIATE;

-- stmt: one bad name in a list is enough
-- begin-expected-error
-- sqlstate: 42704
-- message-like: constraint "dtc_no_such_constraint" does not exist
-- end-expected-error
SET CONSTRAINTS dtc_uq1, dtc_no_such_constraint IMMEDIATE;

-- ============================================================================
-- 12. Temporary and permanent tables
-- ============================================================================

-- stmt: ON COMMIT describes rows that live no longer than the session
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: ON COMMIT can only be used on temporary tables
-- end-expected-error
CREATE TABLE dtc_oc (i int) ON COMMIT DELETE ROWS;

-- stmt
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: ON COMMIT can only be used on temporary tables
-- end-expected-error
CREATE TABLE dtc_oc (i int) ON COMMIT PRESERVE ROWS;

-- setup
CREATE TABLE dtc_perm (i int PRIMARY KEY);

-- stmt: a temporary table cannot key into a permanent one
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: constraints on temporary tables may reference only temporary tables
-- end-expected-error
CREATE TEMP TABLE dtc_tmp0 (i int REFERENCES dtc_perm(i));

-- setup
CREATE TEMP TABLE dtc_tmp1 (i int PRIMARY KEY);

-- stmt: nor a permanent one into a temporary one
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: constraints on permanent tables may reference only permanent tables
-- end-expected-error
CREATE TABLE dtc_perm2 (i int REFERENCES dtc_tmp1(i));

-- stmt: a table-level foreign key is checked the same way
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: constraints on permanent tables may reference only permanent tables
-- end-expected-error
CREATE TABLE dtc_perm2 (i int, FOREIGN KEY (i) REFERENCES dtc_tmp1(i));

-- stmt: and so is ALTER TABLE ADD CONSTRAINT
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: constraints on permanent tables may reference only permanent tables
-- end-expected-error
ALTER TABLE dtc_perm ADD CONSTRAINT dtc_fk9 FOREIGN KEY (i) REFERENCES dtc_tmp1(i);

-- stmt: temp to temp is fine, ON COMMIT and all
CREATE TEMP TABLE dtc_tmp2 (i int REFERENCES dtc_tmp1(i)) ON COMMIT DELETE ROWS;

-- ============================================================================
-- 13. CREATE TABLE AS output column names
-- ============================================================================

-- stmt: the query's output names become the table's column names
-- begin-expected-error
-- sqlstate: 42701
-- message-like: column "x" specified more than once
-- end-expected-error
CREATE TABLE dtc_cta AS SELECT 1 AS x, 2 AS x;

-- stmt: distinct names are fine
CREATE TABLE dtc_cta AS SELECT 1 AS x, 2 AS y;

-- begin-expected
-- columns: x|y
-- row: 1|2
-- end-expected
SELECT x, y FROM dtc_cta;

-- ============================================================================
-- 14. SET WITHOUT CLUSTER / WITHOUT OIDS, and LIKE
-- ============================================================================

-- setup
CREATE TABLE dtc_misc (i int);

-- stmt: valid SQL about on-disk layout, with nothing to do here
ALTER TABLE dtc_misc SET WITHOUT CLUSTER;

-- stmt
ALTER TABLE dtc_misc SET WITHOUT OIDS;

-- stmt
ALTER TABLE dtc_misc SET (fillfactor = 70);

-- setup
CREATE TABLE dtc_like_src (a int, b text DEFAULT 'z');

-- stmt: the recognised LIKE options still work
CREATE TABLE dtc_like1 (LIKE dtc_like_src INCLUDING ALL);

-- stmt
INSERT INTO dtc_like1 (a) VALUES (1);

-- begin-expected
-- columns: a|b
-- row: 1|z
-- end-expected
SELECT a, b FROM dtc_like1;

-- stmt
CREATE TABLE dtc_like2 (LIKE dtc_like_src INCLUDING DEFAULTS EXCLUDING INDEXES);

-- stmt
INSERT INTO dtc_like2 (a) VALUES (1);

-- begin-expected
-- columns: a|b
-- row: 1|z
-- end-expected
SELECT a, b FROM dtc_like2;
