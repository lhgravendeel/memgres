-- ============================================================================
-- The type a column is changed to is any type, not only a built-in one
--
-- A domain, an enum and a composite are types in their own right, and the
-- column reads back as the one it was changed to. Without a USING clause the
-- old values have to reach the new type by an assignment cast, and for a domain
-- that is decided by the domain's base type.
-- ============================================================================
CREATE DOMAIN zze6gd_dom AS int;
CREATE DOMAIN zze6gd_dom2 AS zze6gd_dom;
CREATE TYPE zze6gd_enum AS ENUM ('a','b');
CREATE TYPE zze6gd_enum2 AS ENUM ('c');
CREATE TYPE zze6gd_comp AS (x int, y text);
CREATE TYPE zze6gd_comp2 AS (x int);

CREATE TABLE zze6gd_t1 (s text);
ALTER TABLE zze6gd_t1 ALTER COLUMN s TYPE zze6gd_dom USING s::int;

-- begin-expected
-- columns: attname | t | attnotnull
-- row: s | zze6gd_dom | f
-- end-expected
SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS t, a.attnotnull
  FROM pg_attribute a WHERE a.attrelid = 'zze6gd_t1'::regclass AND a.attnum > 0;

-- A domain is transparent for casting, so its base type is reachable again.
ALTER TABLE zze6gd_t1 ALTER COLUMN s TYPE int;

-- begin-expected
-- columns: t
-- row: integer
-- end-expected
SELECT format_type(atttypid, atttypmod) AS t FROM pg_attribute
  WHERE attrelid = 'zze6gd_t1'::regclass AND attnum > 0;

-- ...and an integer column reaches an integer domain with no USING clause,
-- because the domain's base type is what the assignment cast is judged against.
ALTER TABLE zze6gd_t1 ALTER COLUMN s TYPE zze6gd_dom;

-- begin-expected
-- columns: t
-- row: zze6gd_dom
-- end-expected
SELECT format_type(atttypid, atttypmod) AS t FROM pg_attribute
  WHERE attrelid = 'zze6gd_t1'::regclass AND attnum > 0;

DROP TABLE zze6gd_t1;

-- A domain over a domain is a type of its own, and the column reads back as
-- the one that was named.
CREATE TABLE zze6gd_t2 (s text);
ALTER TABLE zze6gd_t2 ALTER COLUMN s TYPE zze6gd_dom2 USING s::int;

-- begin-expected
-- columns: t
-- row: zze6gd_dom2
-- end-expected
SELECT format_type(atttypid, atttypmod) AS t FROM pg_attribute
  WHERE attrelid = 'zze6gd_t2'::regclass AND attnum > 0;

ALTER TABLE zze6gd_t2 ALTER COLUMN s TYPE zze6gd_dom;

-- begin-expected
-- columns: t
-- row: zze6gd_dom
-- end-expected
SELECT format_type(atttypid, atttypmod) AS t FROM pg_attribute
  WHERE attrelid = 'zze6gd_t2'::regclass AND attnum > 0;

DROP TABLE zze6gd_t2;

-- A text column does not reach an integer domain on its own, and PostgreSQL
-- puts the conversion the writer meant in the hint.
CREATE TABLE zze6gd_t4 (s text);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "s" cannot be cast automatically to type zze6gd_dom
-- hint-like: You might need to specify "USING s::zze6gd_dom".
-- end-expected-error
ALTER TABLE zze6gd_t4 ALTER COLUMN s TYPE zze6gd_dom;

-- begin-expected
-- columns: t
-- row: text
-- end-expected
SELECT format_type(atttypid, atttypmod) AS t FROM pg_attribute
  WHERE attrelid = 'zze6gd_t4'::regclass AND attnum > 0;

DROP TABLE zze6gd_t4;

-- ============================================================================
-- A composite is a type in its own right, and nothing casts into one on its own
-- ============================================================================
CREATE TABLE zze6gd_c1 (a text);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "a" cannot be cast automatically to type zze6gd_comp
-- hint-like: You might need to specify "USING a::zze6gd_comp".
-- end-expected-error
ALTER TABLE zze6gd_c1 ALTER COLUMN a TYPE zze6gd_comp;

ALTER TABLE zze6gd_c1 ALTER COLUMN a TYPE zze6gd_comp USING ROW(1, a)::zze6gd_comp;

-- begin-expected
-- columns: t
-- row: zze6gd_comp
-- end-expected
SELECT format_type(atttypid, atttypmod) AS t FROM pg_attribute
  WHERE attrelid = 'zze6gd_c1'::regclass AND attnum > 0;

-- The column is already that composite, so the retype has nothing to convert.
ALTER TABLE zze6gd_c1 ALTER COLUMN a TYPE zze6gd_comp;

-- Another composite is another type.
-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "a" cannot be cast automatically to type zze6gd_comp2
-- end-expected-error
ALTER TABLE zze6gd_c1 ALTER COLUMN a TYPE zze6gd_comp2;

-- A composite reaches a string type by an assignment cast, as every type does.
ALTER TABLE zze6gd_c1 ALTER COLUMN a TYPE varchar(4);

-- begin-expected
-- columns: t
-- row: character varying(4)
-- end-expected
SELECT format_type(atttypid, atttypmod) AS t FROM pg_attribute
  WHERE attrelid = 'zze6gd_c1'::regclass AND attnum > 0;

DROP TABLE zze6gd_c1;

CREATE TABLE zze6gd_c2 (a zze6gd_comp);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "a" cannot be cast automatically to type integer
-- hint-like: You might need to specify "USING a::integer".
-- end-expected-error
ALTER TABLE zze6gd_c2 ALTER COLUMN a TYPE int;

ALTER TABLE zze6gd_c2 ALTER COLUMN a TYPE text;

-- begin-expected
-- columns: t
-- row: text
-- end-expected
SELECT format_type(atttypid, atttypmod) AS t FROM pg_attribute
  WHERE attrelid = 'zze6gd_c2'::regclass AND attnum > 0;

DROP TABLE zze6gd_c2;

-- An enum is a type of its own too, and another enum is not it.
CREATE TABLE zze6gd_e1 (a zze6gd_enum);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "a" cannot be cast automatically to type zze6gd_enum2
-- hint-like: You might need to specify "USING a::zze6gd_enum2".
-- end-expected-error
ALTER TABLE zze6gd_e1 ALTER COLUMN a TYPE zze6gd_enum2;

-- The enum the column already carries is not a conversion at all, and a string
-- type is reached from an enum as it is from everything.
ALTER TABLE zze6gd_e1 ALTER COLUMN a TYPE zze6gd_enum;
ALTER TABLE zze6gd_e1 ALTER COLUMN a TYPE text;

-- begin-expected
-- columns: t
-- row: text
-- end-expected
SELECT format_type(atttypid, atttypmod) AS t FROM pg_attribute
  WHERE attrelid = 'zze6gd_e1'::regclass AND attnum > 0;

DROP TABLE zze6gd_e1;
DROP TYPE zze6gd_comp2;
DROP TYPE zze6gd_comp;
DROP TYPE zze6gd_enum2;
DROP TYPE zze6gd_enum;
DROP DOMAIN zze6gd_dom2;
DROP DOMAIN zze6gd_dom;

-- ============================================================================
-- What a retype leaves behind: the default, the constraints, the rows
-- ============================================================================
CREATE DOMAIN zze6gd_pos AS int CHECK (VALUE > 0);
CREATE DOMAIN zze6gd_dm AS int;
CREATE TYPE zze6gd_cm AS (x int);
CREATE TYPE zze6gd_em AS ENUM ('a');

CREATE TABLE zze6gd_k (a int DEFAULT 5);
CREATE INDEX zze6gd_kx ON zze6gd_k (a);
ALTER TABLE zze6gd_k ADD CONSTRAINT zze6gd_kck CHECK (a > 1);
INSERT INTO zze6gd_k VALUES (3);
ALTER TABLE zze6gd_k ALTER COLUMN a TYPE zze6gd_pos;

-- begin-expected
-- columns: attname | t | def
-- row: a | zze6gd_pos | 5
-- end-expected
SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS t,
       pg_get_expr(d.adbin, d.adrelid) AS def
  FROM pg_attribute a LEFT JOIN pg_attrdef d
    ON d.adrelid = a.attrelid AND d.adnum = a.attnum
 WHERE a.attrelid = 'zze6gd_k'::regclass AND a.attnum > 0;

-- begin-expected
-- columns: indexdef
-- row: CREATE INDEX zze6gd_kx ON public.zze6gd_k USING btree (a)
-- end-expected
SELECT indexdef FROM pg_indexes WHERE tablename = 'zze6gd_k';

-- The column is a column of the domain from here on, so what is written into
-- it is held to the domain's own rules as well as to the relation's.
-- begin-expected-error
-- sqlstate: 23514
-- message-like: value for domain zze6gd_pos violates check constraint "zze6gd_pos_check"
-- end-expected-error
INSERT INTO zze6gd_k VALUES (-4);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "zze6gd_k" violates check constraint "zze6gd_kck"
-- end-expected-error
INSERT INTO zze6gd_k VALUES (1);

-- begin-expected
-- columns: a
-- row: 3
-- end-expected
SELECT a FROM zze6gd_k ORDER BY 1;

DROP TABLE zze6gd_k;

-- A row the domain would not have taken stops the retype: the column never
-- holds a value no INSERT could have written into it.
CREATE TABLE zze6gd_k2 (a int);
INSERT INTO zze6gd_k2 VALUES (-2);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: value for domain zze6gd_pos violates check constraint "zze6gd_pos_check"
-- end-expected-error
ALTER TABLE zze6gd_k2 ALTER COLUMN a TYPE zze6gd_pos;

-- Nothing moved: the column is what it was and so is the row.
-- begin-expected
-- columns: t
-- row: integer
-- end-expected
SELECT format_type(atttypid, atttypmod) AS t FROM pg_attribute
  WHERE attrelid = 'zze6gd_k2'::regclass AND attnum > 0;

-- begin-expected
-- columns: a
-- row: -2
-- end-expected
SELECT a FROM zze6gd_k2;

DROP TABLE zze6gd_k2;

-- The retype reaches every descendant, as a retype to any type does.
CREATE TABLE zze6gd_p (a text);
CREATE TABLE zze6gd_ch () INHERITS (zze6gd_p);
ALTER TABLE zze6gd_p ALTER COLUMN a TYPE zze6gd_dm USING a::int;

-- begin-expected
-- columns: relname | t
-- row: zze6gd_ch | zze6gd_dm
-- row: zze6gd_p | zze6gd_dm
-- end-expected
SELECT c.relname, format_type(a.atttypid, a.atttypmod) AS t
  FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
 WHERE c.relname IN ('zze6gd_p','zze6gd_ch') AND a.attnum > 0 ORDER BY 1;

DROP TABLE zze6gd_ch;
DROP TABLE zze6gd_p;

-- A column of a domain is a column depending on that domain, however it came
-- to be one.
CREATE TABLE zze6gd_dep (a int);
ALTER TABLE zze6gd_dep ALTER COLUMN a TYPE zze6gd_dm;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop type zze6gd_dm because other objects depend on it
-- detail-like: column a of table zze6gd_dep depends on type zze6gd_dm
-- hint-like: Use DROP ... CASCADE to drop the dependent objects too.
-- end-expected-error
DROP DOMAIN zze6gd_dm;

DROP TABLE zze6gd_dep;

-- ============================================================================
-- No type a reader defined takes a type modifier
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42601
-- message-like: type modifier is not allowed for type "zze6gd_dm"
-- end-expected-error
CREATE TABLE zze6gd_m1 (a zze6gd_dm(5));

-- begin-expected-error
-- sqlstate: 42601
-- message-like: type modifier is not allowed for type "zze6gd_cm"
-- end-expected-error
CREATE TABLE zze6gd_m2 (a zze6gd_cm(3));

-- begin-expected-error
-- sqlstate: 42601
-- message-like: type modifier is not allowed for type "zze6gd_em"
-- end-expected-error
CREATE TABLE zze6gd_m3 (a zze6gd_em(3));

CREATE TABLE zze6gd_m4 (a int);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: type modifier is not allowed for type "zze6gd_dm"
-- end-expected-error
ALTER TABLE zze6gd_m4 ALTER COLUMN a TYPE zze6gd_dm(5);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: type modifier is not allowed for type "zze6gd_cm"
-- end-expected-error
ALTER TABLE zze6gd_m4 ADD COLUMN b zze6gd_cm(3);

-- A type nothing answers to is reported as that, and the column is settled
-- before the type is.
-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "zze6gd_nosuch" does not exist
-- end-expected-error
ALTER TABLE zze6gd_m4 ALTER COLUMN a TYPE zze6gd_nosuch;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" of relation "zze6gd_m4" does not exist
-- end-expected-error
ALTER TABLE zze6gd_m4 ALTER COLUMN nosuch TYPE zze6gd_dm;

DROP TABLE zze6gd_m4;
DROP TYPE zze6gd_em;
DROP TYPE zze6gd_cm;
DROP DOMAIN zze6gd_dm;
DROP DOMAIN zze6gd_pos;
