-- ============================================================================
-- The type a column is declared with: what it is judged against, and the name
-- the refusal prints
--
-- A definition is measured against the column's type before the relation is
-- built -- a DEFAULT, a generation expression and a COLLATE clause alike --
-- and the type is named the way the session's search path would have the
-- reader write it: bare where the path reaches the schema holding it,
-- qualified where it does not. Every value here was read off PostgreSQL 18.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- The name a complaint prints is the one the search path would have you write
-- ----------------------------------------------------------------------------
CREATE SCHEMA zw5x_s;
CREATE DOMAIN zw5x_s.dom AS int;
CREATE DOMAIN zw5x_pub AS int;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type zw5x_s.dom but default expression is of type text
-- end-expected-error
CREATE TABLE zw5x_t1 (b zw5x_s.dom DEFAULT 'abc'::text);

-- A type the path reaches is named bare, however the writer wrote it.
-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type zw5x_pub but default expression is of type text
-- end-expected-error
CREATE TABLE zw5x_t5 (b public.zw5x_pub DEFAULT 'abc'::text);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type zw5x_pub but default expression is of type text
-- end-expected-error
CREATE TABLE zw5x_t6 (b zw5x_pub DEFAULT 'abc'::text);

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "dom" does not exist
-- end-expected-error
CREATE TABLE zw5x_p4 (b dom DEFAULT 'abc'::text);

SET search_path = zw5x_s, public;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type dom but default expression is of type text
-- end-expected-error
CREATE TABLE public.zw5x_p1 (b dom DEFAULT 'abc'::text);

-- Writing the qualifier does not make PostgreSQL print it.
-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type dom but default expression is of type text
-- end-expected-error
CREATE TABLE public.zw5x_p2 (b zw5x_s.dom DEFAULT 'abc'::text);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type zw5x_pub but default expression is of type text
-- end-expected-error
CREATE TABLE public.zw5x_p3 (b public.zw5x_pub DEFAULT 'abc'::text);

SET search_path = zw5x_s;

-- A path that leaves public out leaves public's own types qualified.
-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type public.zw5x_pub but default expression is of type text
-- end-expected-error
CREATE TABLE public.zw5x_p5 (b public.zw5x_pub DEFAULT 'abc'::text);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type dom but default expression is of type text
-- end-expected-error
CREATE TABLE public.zw5x_p6 (b zw5x_s.dom DEFAULT 'abc'::text);

RESET search_path;

-- A qualifier on a built-in is dropped, and the catalogue's own name printed.
-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type integer but default expression is of type text
-- end-expected-error
CREATE TABLE zw5x_t7 (b pg_catalog.int4 DEFAULT 'abc'::text);

-- A bare string literal is read with the base type's input function, so the
-- complaint names the base type and not the domain.
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "abc"
-- end-expected-error
CREATE TABLE zw5x_t3 (b zw5x_s.dom DEFAULT 'abc');

CREATE TABLE zw5x_t4 (b zw5x_s.dom DEFAULT 1);

-- begin-expected
-- columns: atts
-- row: b:zw5x_s.dom
-- end-expected
SELECT string_agg(attname || ':' || atttypid::regtype::text, ',' ORDER BY attnum) AS atts FROM pg_attribute WHERE attrelid = 'zw5x_t4'::regclass AND attnum > 0 AND NOT attisdropped;

DROP TABLE zw5x_t4;

-- ----------------------------------------------------------------------------
-- ADD COLUMN judges its generation expression against the column's type
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_g (k int, s text);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "c1" is of type integer but default expression is of type text
-- end-expected-error
ALTER TABLE zw5x_g ADD COLUMN c1 int GENERATED ALWAYS AS ('x'::text) STORED;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "c2" is of type boolean but default expression is of type integer
-- end-expected-error
ALTER TABLE zw5x_g ADD COLUMN c2 bool GENERATED ALWAYS AS (1) STORED;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "c4" is of type integer but default expression is of type text
-- end-expected-error
ALTER TABLE zw5x_g ADD COLUMN c4 int GENERATED ALWAYS AS (s) STORED;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "cd" is of type integer but default expression is of type boolean
-- end-expected-error
ALTER TABLE zw5x_g ADD COLUMN cd int GENERATED ALWAYS AS (1=1) STORED;

-- A virtual generated column is judged by the same rule.
-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "cg" is of type integer but default expression is of type text
-- end-expected-error
ALTER TABLE zw5x_g ADD COLUMN cg int GENERATED ALWAYS AS ('x'::text) VIRTUAL;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "abc"
-- end-expected-error
ALTER TABLE zw5x_g ADD COLUMN c5 int GENERATED ALWAYS AS ('abc') STORED;

-- Over a domain column, named the way the search path names it.
-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "cc" is of type zw5x_s.dom but default expression is of type text
-- end-expected-error
ALTER TABLE zw5x_g ADD COLUMN cc zw5x_s.dom GENERATED ALWAYS AS ('x'::text) STORED;

-- The check is the last of the ones ADD COLUMN already made: each of these is
-- a different fault in the same statement, and the one reached first is the
-- one reported.
-- begin-expected-error
-- sqlstate: 42P17
-- message-like: generation expression is not immutable
-- end-expected-error
ALTER TABLE zw5x_g ADD COLUMN c8 int GENERATED ALWAYS AS (now()) STORED;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
ALTER TABLE zw5x_g ADD COLUMN c9 int GENERATED ALWAYS AS (nosuchcol) STORED;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in column generation expression
-- end-expected-error
ALTER TABLE zw5x_g ADD COLUMN cb int GENERATED ALWAYS AS ((SELECT 1)) STORED;

-- begin-expected-error
-- sqlstate: 42701
-- message-like: column name "ctid" conflicts with a system column name
-- end-expected-error
ALTER TABLE zw5x_g ADD COLUMN ctid int GENERATED ALWAYS AS ('x'::text) STORED;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "nosuchtype" does not exist
-- end-expected-error
ALTER TABLE zw5x_g ADD COLUMN ce nosuchtype GENERATED ALWAYS AS ('x'::text) STORED;

-- begin-expected-error
-- sqlstate: 42701
-- message-like: column "k" of relation "zw5x_g" already exists
-- end-expected-error
ALTER TABLE zw5x_g ADD COLUMN k int GENERATED ALWAYS AS ('x'::text) STORED;

-- What the same statement goes on accepting.
ALTER TABLE zw5x_g ADD COLUMN c3 int GENERATED ALWAYS AS (k) STORED;
ALTER TABLE zw5x_g ADD COLUMN c6 text GENERATED ALWAYS AS (k) STORED;
ALTER TABLE zw5x_g ADD COLUMN c7 int GENERATED ALWAYS AS (k+1) STORED;

-- begin-expected
-- columns: atts
-- row: k:integer,s:text,c3:integer,c6:text,c7:integer
-- end-expected
SELECT string_agg(attname || ':' || atttypid::regtype::text, ',' ORDER BY attnum) AS atts FROM pg_attribute WHERE attrelid = 'zw5x_g'::regclass AND attnum > 0 AND NOT attisdropped;

INSERT INTO zw5x_g (k, s) VALUES (5, 'x');

-- begin-expected
-- columns: c3 | c6 | c7
-- row: 5 | 5 | 6
-- end-expected
SELECT c3, c6, c7 FROM zw5x_g;

DROP TABLE zw5x_g;

-- ----------------------------------------------------------------------------
-- ALTER COLUMN ... TYPE settles which column before it settles what type
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_a (x int, y text);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" of relation "zw5x_a" does not exist
-- end-expected-error
ALTER TABLE zw5x_a ALTER COLUMN nosuch TYPE serial;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" of relation "zw5x_a" does not exist
-- end-expected-error
ALTER TABLE zw5x_a ALTER COLUMN nosuch TYPE bigserial;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" of relation "zw5x_a" does not exist
-- end-expected-error
ALTER TABLE zw5x_a ALTER COLUMN nosuch TYPE nosuchtype;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" of relation "zw5x_a" does not exist
-- end-expected-error
ALTER TABLE zw5x_a ALTER COLUMN nosuch SET DATA TYPE serial;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" of relation "zw5x_a" does not exist
-- end-expected-error
ALTER TABLE zw5x_a ALTER COLUMN nosuch TYPE serial USING x;

-- Where the column is there, the type is what is at fault.
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "serial" does not exist
-- end-expected-error
ALTER TABLE zw5x_a ALTER COLUMN x TYPE serial;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "bigserial" does not exist
-- end-expected-error
ALTER TABLE zw5x_a ALTER COLUMN x TYPE bigserial;

-- And the relation is settled ahead of both.
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zw5x_nosuchtable" does not exist
-- end-expected-error
ALTER TABLE zw5x_nosuchtable ALTER COLUMN nosuch TYPE serial;

-- begin-expected
-- columns: atts
-- row: x:integer,y:text
-- end-expected
SELECT string_agg(attname || ':' || atttypid::regtype::text, ',' ORDER BY attnum) AS atts FROM pg_attribute WHERE attrelid = 'zw5x_a'::regclass AND attnum > 0 AND NOT attisdropped;

DROP TABLE zw5x_a;

-- ----------------------------------------------------------------------------
-- A COLLATE clause is judged against the type the column really has
-- ----------------------------------------------------------------------------
CREATE TYPE zw5x_en AS ENUM ('a','b');
CREATE TYPE zw5x_co AS (x int, y text);
CREATE TYPE zw5x_rg AS RANGE (subtype = int4);
CREATE DOMAIN zw5x_de AS zw5x_en;
CREATE DOMAIN zw5x_dt AS text;
CREATE TABLE zw5x_r (k int, s text, e zw5x_en);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type zw5x_en
-- end-expected-error
CREATE TABLE zw5x_r2 (a zw5x_en COLLATE "C");

-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type zw5x_en
-- end-expected-error
ALTER TABLE zw5x_r ADD COLUMN a1 zw5x_en COLLATE "C";

-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type zw5x_en
-- end-expected-error
ALTER TABLE zw5x_r ALTER COLUMN e TYPE zw5x_en COLLATE "C";

-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type zw5x_en[]
-- end-expected-error
ALTER TABLE zw5x_r ADD COLUMN a2 zw5x_en[] COLLATE "C";

-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type zw5x_co
-- end-expected-error
ALTER TABLE zw5x_r ADD COLUMN a3 zw5x_co COLLATE "C";

-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type zw5x_rg
-- end-expected-error
ALTER TABLE zw5x_r ADD COLUMN a5 zw5x_rg COLLATE "C";

-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type zw5x_rg
-- end-expected-error
ALTER TABLE zw5x_r ALTER COLUMN k TYPE zw5x_rg COLLATE "C";

-- A domain is named by its own name, whatever it was built over.
-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type zw5x_de
-- end-expected-error
ALTER TABLE zw5x_r ADD COLUMN a6 zw5x_de COLLATE "C";

-- A domain over a type that does carry a collation takes the clause.
ALTER TABLE zw5x_r ADD COLUMN a8 zw5x_dt COLLATE "C";

-- The clause is still judged last of the three questions PostgreSQL asks.
-- begin-expected-error
-- sqlstate: 42704
-- message-like: collation "nosuchcoll" for encoding "UTF8" does not exist
-- end-expected-error
ALTER TABLE zw5x_r ADD COLUMN a9 zw5x_en COLLATE nosuchcoll;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "nosuchtype" does not exist
-- end-expected-error
ALTER TABLE zw5x_r ADD COLUMN b2 nosuchtype COLLATE nosuchcoll;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" of relation "zw5x_r" does not exist
-- end-expected-error
ALTER TABLE zw5x_r ALTER COLUMN nosuch TYPE int COLLATE nosuchcoll;

-- A width the type cannot have is settled with the type, ahead of the name of
-- the collation.
-- begin-expected-error
-- sqlstate: 22023
-- message-like: length for type varchar cannot exceed 10485760
-- end-expected-error
ALTER TABLE zw5x_r ALTER COLUMN s TYPE varchar(20000000) COLLATE nosuchcoll;

CREATE TYPE zw5x_sh;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "zw5x_sh" is only a shell
-- end-expected-error
ALTER TABLE zw5x_r ALTER COLUMN k TYPE zw5x_sh COLLATE "C";

-- The types that never carried one are unmoved.
-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type bigint
-- end-expected-error
ALTER TABLE zw5x_r ALTER COLUMN k TYPE bigint COLLATE "C";

-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type integer[]
-- end-expected-error
ALTER TABLE zw5x_r ADD COLUMN n2 int[] COLLATE "C";

-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type int4range
-- end-expected-error
ALTER TABLE zw5x_r ADD COLUMN n4 int4range COLLATE "C";

ALTER TABLE zw5x_r ALTER COLUMN s TYPE varchar(20) COLLATE "C";
ALTER TABLE zw5x_r ALTER COLUMN s TYPE text COLLATE "C";

-- begin-expected
-- columns: atts
-- row: k:integer,s:text,e:zw5x_en,a8:zw5x_dt
-- end-expected
SELECT string_agg(attname || ':' || atttypid::regtype::text, ',' ORDER BY attnum) AS atts FROM pg_attribute WHERE attrelid = 'zw5x_r'::regclass AND attnum > 0 AND NOT attisdropped;

DROP TABLE zw5x_r;
DROP TYPE zw5x_sh;
DROP DOMAIN zw5x_de;
DROP DOMAIN zw5x_dt;
DROP TYPE zw5x_rg;
DROP TYPE zw5x_co;
DROP TYPE zw5x_en;

-- ----------------------------------------------------------------------------
-- A type named after a schema the path leaves out keeps the qualifier
-- ----------------------------------------------------------------------------
CREATE SCHEMA zw5x_sch;
CREATE TYPE zw5x_sch.zw5x_en2 AS ENUM ('p','q');
CREATE TYPE zw5x_en AS ENUM ('a','b');
CREATE TABLE zw5x_q (k int);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type zw5x_sch.zw5x_en2
-- end-expected-error
ALTER TABLE zw5x_q ADD COLUMN c1 zw5x_sch.zw5x_en2 COLLATE "C";

SET search_path = zw5x_sch, public;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type zw5x_en2
-- end-expected-error
ALTER TABLE public.zw5x_q ADD COLUMN c2 zw5x_en2 COLLATE "C";

-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type zw5x_en
-- end-expected-error
ALTER TABLE public.zw5x_q ADD COLUMN c3 public.zw5x_en COLLATE "C";

SET search_path = zw5x_sch;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type public.zw5x_en
-- end-expected-error
ALTER TABLE public.zw5x_q ADD COLUMN c4 public.zw5x_en COLLATE "C";

RESET search_path;
DROP TABLE zw5x_q;
DROP TYPE zw5x_sch.zw5x_en2;
DROP TYPE zw5x_en;
DROP SCHEMA zw5x_sch;
DROP DOMAIN zw5x_s.dom;
DROP DOMAIN zw5x_pub;
DROP SCHEMA zw5x_s;

-- ----------------------------------------------------------------------------
-- serial2, serial4 and serial8 are the shorthands their words stand for
-- ----------------------------------------------------------------------------
CREATE TABLE zw5x_s2 (a serial2, b serial4, c serial8);

-- begin-expected
-- columns: column_name | data_type | column_default | is_nullable
-- row: a | smallint | nextval('zw5x_s2_a_seq'::regclass) | NO
-- row: b | integer | nextval('zw5x_s2_b_seq'::regclass) | NO
-- row: c | bigint | nextval('zw5x_s2_c_seq'::regclass) | NO
-- end-expected
SELECT column_name, data_type, column_default, is_nullable FROM information_schema.columns WHERE table_name = 'zw5x_s2' ORDER BY ordinal_position;

-- begin-expected
-- columns: rels
-- row: zw5x_s2/r,zw5x_s2_a_seq/S,zw5x_s2_b_seq/S,zw5x_s2_c_seq/S
-- end-expected
SELECT string_agg(relname || '/' || relkind::text, ',' ORDER BY relname) AS rels FROM pg_class WHERE relname LIKE 'zw5x!_s2%' ESCAPE '!';

-- Each sequence is bounded by the integer type the shorthand stands for.
-- begin-expected
-- columns: seqs
-- row: zw5x_s2_a_seq/smallint/32767,zw5x_s2_b_seq/integer/2147483647,zw5x_s2_c_seq/bigint/9223372036854775807
-- end-expected
SELECT string_agg(c.relname || '/' || s.seqtypid::regtype::text || '/' || s.seqmax::text, ',' ORDER BY c.relname) AS seqs FROM pg_sequence s JOIN pg_class c ON c.oid = s.seqrelid WHERE c.relname LIKE 'zw5x!_s2%' ESCAPE '!';

INSERT INTO zw5x_s2 DEFAULT VALUES;

-- begin-expected
-- columns: a | b | c
-- row: 1 | 1 | 1
-- end-expected
SELECT a, b, c FROM zw5x_s2;

DROP TABLE zw5x_s2;

-- The shorthand means something only where a column's type is written.
CREATE TABLE zw5x_s3 (k int);
ALTER TABLE zw5x_s3 ADD COLUMN q serial2;

-- begin-expected
-- columns: cols
-- row: k/integer/null,q/smallint/nextval('zw5x_s3_q_seq'::regclass)
-- end-expected
SELECT string_agg(column_name || '/' || data_type || '/' || coalesce(column_default, 'null'), ',' ORDER BY ordinal_position) AS cols FROM information_schema.columns WHERE table_name = 'zw5x_s3';

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "serial2" does not exist
-- end-expected-error
ALTER TABLE zw5x_s3 ALTER COLUMN k TYPE serial2;

-- The column is settled before the type it is being retyped to.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" of relation "zw5x_s3" does not exist
-- end-expected-error
ALTER TABLE zw5x_s3 ALTER COLUMN nosuch TYPE serial2;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "serial4" does not exist
-- end-expected-error
SELECT CAST(1 AS serial4);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: array of serial is not implemented
-- end-expected-error
CREATE TABLE zw5x_s4 (a serial2[]);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: type modifier is not allowed for type "smallint"
-- end-expected-error
CREATE TABLE zw5x_s5 (a serial2(5));

-- A name written under a schema is a real type name, and pg_catalog holds no
-- serial2.
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "pg_catalog.serial2" does not exist
-- end-expected-error
CREATE TABLE zw5x_s6 (a pg_catalog.serial2);

-- The complaint names the integer type the column would have had.
-- begin-expected-error
-- sqlstate: 42804
-- message-like: collations are not supported by type smallint
-- end-expected-error
CREATE TABLE zw5x_s7 (a serial2 COLLATE "C");

DROP TABLE zw5x_s3;
