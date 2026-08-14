-- ============================================================================
-- The type a column reads back as
--
-- An array is a type of its own, and PostgreSQL asks of a DEFAULT written on an
-- array column exactly what it asks of one written on any other: is there a
-- coercion from what the expression produces to what the column holds? There is
-- one between two arrays precisely where there is one between their element
-- types, and none at all from a lone value to an array.
--
-- A boolean written into a string column goes through the cast PostgreSQL
-- registered between the two, which spells the value out in full. The single
-- letter is boolean's own output form, and that is reached only inside an array
-- or a composite, where the letter is what PostgreSQL writes there too.
--
-- A range, an enum and a domain are each a type in their own right, whatever a
-- given engine keeps their values in: a column declared as one is a column of
-- it, format_type names it, a column depending on it blocks a DROP of it, and a
-- retype from some other type into it needs a USING clause. An array of one is
-- a further type again, and the domain's own rules are every element's rules.
--
-- A domain has no operators of its own. PostgreSQL resolves an operator over
-- the type the domain is built on and records the reading down to it, so a
-- CHECK over a domain-typed column comes back holding that cast -- which is why
-- a CHECK written before the column was retyped to a domain reads back
-- differently afterwards, and reads back as it was again when the column stops
-- being a domain.
--
-- Every value below was read off PostgreSQL 18 before it was written down.
-- ============================================================================

-- ============================================================================
-- A DEFAULT that does not fit an array column
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "a" is of type integer[] but default expression is of type integer
-- end-expected-error
CREATE TABLE zzm3sd_ad (a int[] DEFAULT 1);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "a" is of type integer[] but default expression is of type boolean
-- end-expected-error
CREATE TABLE zzm3sd_ad (a int[] DEFAULT true);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "a" is of type integer[] but default expression is of type timestamp with time zone
-- end-expected-error
CREATE TABLE zzm3sd_ad (a int[] DEFAULT now());

-- an array of another element type is judged element against element
-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "a" is of type integer[] but default expression is of type text[]
-- end-expected-error
CREATE TABLE zzm3sd_ad (a int[] DEFAULT '{a}'::text[]);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "a" is of type text[] but default expression is of type integer
-- end-expected-error
CREATE TABLE zzm3sd_ad (a text[] DEFAULT 1);

-- a string type takes anything through its own reader, but a lone text value is
-- still not an array of them
-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "a" is of type text[] but default expression is of type text
-- end-expected-error
CREATE TABLE zzm3sd_ad (a text[] DEFAULT 'x'::text);

-- the array type is named without the modifier the column was declared with
-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "a" is of type character varying[] but default expression is of type integer
-- end-expected-error
CREATE TABLE zzm3sd_ad (a varchar(5)[] DEFAULT 1);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "a" is of type boolean[] but default expression is of type integer
-- end-expected-error
CREATE TABLE zzm3sd_ad (a bool[] DEFAULT 1);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "a" is of type date[] but default expression is of type integer
-- end-expected-error
CREATE TABLE zzm3sd_ad (a date[] DEFAULT 1);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "a" is of type numeric[] but default expression is of type integer
-- end-expected-error
CREATE TABLE zzm3sd_ad (a numeric[] DEFAULT 1);

-- a generation expression is judged by the same rule and in the same words
-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type integer[] but default expression is of type integer
-- end-expected-error
CREATE TABLE zzm3sd_ad (a int[], b int[] GENERATED ALWAYS AS (1) STORED);

-- and so is a column added later
CREATE TABLE zzm3sd_aa (a int);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type integer[] but default expression is of type integer
-- end-expected-error
ALTER TABLE zzm3sd_aa ADD COLUMN b int[] DEFAULT 1;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_class WHERE relname = 'zzm3sd_ad';

DROP TABLE zzm3sd_aa;

-- What an array column does take: a literal of no type of its own, an array
-- constructor, a cast to the column's own type, and an array whose elements
-- reach the column's element type by an assignment cast.
CREATE TABLE zzm3sd_ok (a int[] DEFAULT '{1,2}', b int[] DEFAULT '{1,2}'::int[], c int[] DEFAULT ARRAY[1,2], d text[] DEFAULT ARRAY[1,2], e text[] DEFAULT '{1,2}'::int[], f int8[] DEFAULT '{1}'::int4[], g int[] DEFAULT NULL, h int[] DEFAULT '{}');
INSERT INTO zzm3sd_ok DEFAULT VALUES;

-- begin-expected
-- columns: a | b | c | d | e | f | g | h
-- row: {1,2} | {1,2} | {1,2} | {1,2} | {1,2} | {1} | NULL | {}
-- end-expected
SELECT a, b, c, d, e, f, g, h FROM zzm3sd_ok;

DROP TABLE zzm3sd_ok;

CREATE TABLE zzm3sd_al (a int);
ALTER TABLE zzm3sd_al ADD COLUMN b int[] DEFAULT '{1,2}';
ALTER TABLE zzm3sd_al ADD COLUMN c text[] DEFAULT ARRAY['x'];
ALTER TABLE zzm3sd_al ADD COLUMN d int8[] DEFAULT '{7}'::int4[];
ALTER TABLE zzm3sd_al ALTER COLUMN b SET DEFAULT ARRAY[5, 6];

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type integer[] but default expression is of type integer
-- end-expected-error
ALTER TABLE zzm3sd_al ALTER COLUMN b SET DEFAULT 5;

INSERT INTO zzm3sd_al (a) VALUES (1);

-- begin-expected
-- columns: a | b | c | d
-- row: 1 | {5,6} | {x} | {7}
-- end-expected
SELECT a, b, c, d FROM zzm3sd_al;

DROP TABLE zzm3sd_al;

CREATE TABLE zzm3sd_al2 (a int[], b int[] GENERATED ALWAYS AS (a) STORED);
INSERT INTO zzm3sd_al2 (a) VALUES ('{4,5}');

-- begin-expected
-- columns: a | b
-- row: {4,5} | {4,5}
-- end-expected
SELECT a, b FROM zzm3sd_al2;

DROP TABLE zzm3sd_al2;

-- ============================================================================
-- A boolean read into a string column
-- ============================================================================

CREATE TABLE zzm3sd_b (a varchar, b text, c char(5), d varchar(4), e bool);
INSERT INTO zzm3sd_b (a,b,c,d,e) VALUES (true, true, true, true, true);

-- the blank-padded column is left out of this one: what it holds is 'true ',
-- and a trailing blank cannot be told from the separator here
-- begin-expected
-- columns: a | b | d | e
-- row: true | true | true | t
-- end-expected
SELECT a, b, d, e FROM zzm3sd_b;

-- a blank-padded character type is measured without its padding
-- begin-expected
-- columns: la | lb | lc | ld
-- row: 4 | 4 | 4 | 4
-- end-expected
SELECT length(a) AS la, length(b) AS lb, length(c) AS lc, length(d) AS ld FROM zzm3sd_b;

UPDATE zzm3sd_b SET a = false, b = false;

-- begin-expected
-- columns: a | b
-- row: false | false
-- end-expected
SELECT a, b FROM zzm3sd_b;

DROP TABLE zzm3sd_b;

-- a DEFAULT of a boolean on a string column reads back the same way
CREATE TABLE zzm3sd_bd (a varchar DEFAULT true, b text DEFAULT false, c varchar(4) DEFAULT true);
INSERT INTO zzm3sd_bd DEFAULT VALUES;

-- begin-expected
-- columns: a | b | c
-- row: true | false | true
-- end-expected
SELECT a, b, c FROM zzm3sd_bd;

DROP TABLE zzm3sd_bd;

-- and so does a retype out of boolean into one
CREATE TABLE zzm3sd_br (f bool);
INSERT INTO zzm3sd_br VALUES (true);
ALTER TABLE zzm3sd_br ALTER COLUMN f TYPE varchar;

-- begin-expected
-- columns: f
-- row: true
-- end-expected
SELECT f FROM zzm3sd_br;

DROP TABLE zzm3sd_br;

-- the letter is still what a boolean is written as inside a container, and an
-- explicit cast still spells it out
-- begin-expected
-- columns: a | b | c
-- row: {t,f} | true | false
-- end-expected
SELECT ARRAY[true,false] AS a, true::text AS b, false::text AS c;

-- ============================================================================
-- A column of a range type
-- ============================================================================

CREATE TYPE zzm3sd_r AS RANGE (subtype = int4);
CREATE TYPE zzm3sd_e AS ENUM ('a', 'b');
CREATE DOMAIN zzm3sd_d AS int;
CREATE TABLE zzm3sd_rt (a zzm3sd_r);
INSERT INTO zzm3sd_rt VALUES ('[1,3)');

-- begin-expected
-- columns: a | lo | hi
-- row: [1,3) | 1 | 3
-- end-expected
SELECT a, lower(a) AS lo, upper(a) AS hi FROM zzm3sd_rt;

-- the catalogue names the range, and reports the layout the range has
-- begin-expected
-- columns: attname | ft | attlen | attalign | attstorage
-- row: a | zzm3sd_r | -1 | i | x
-- end-expected
SELECT attname, format_type(atttypid, atttypmod) AS ft, attlen, attalign, attstorage FROM pg_attribute WHERE attrelid = 'zzm3sd_rt'::regclass AND attnum > 0 ORDER BY attnum;

-- begin-expected
-- columns: udt_name | data_type
-- row: zzm3sd_r | USER-DEFINED
-- end-expected
SELECT udt_name, data_type FROM information_schema.columns WHERE table_name = 'zzm3sd_rt';

-- a column of the range is a column depending on it
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop type zzm3sd_r because other objects depend on it
-- end-expected-error
DROP TYPE zzm3sd_r;

-- begin-expected
-- columns: attname | ft
-- row: a | zzm3sd_r
-- end-expected
SELECT attname, format_type(atttypid, atttypmod) AS ft FROM pg_attribute WHERE attrelid = 'zzm3sd_rt'::regclass AND attnum > 0 ORDER BY attnum;

DROP TABLE zzm3sd_rt;

-- and a text column does not reach one of the reader's own types without being
-- told how
CREATE TABLE zzm3sd_rc (a text, b text, c text);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "a" cannot be cast automatically to type zzm3sd_r
-- end-expected-error
ALTER TABLE zzm3sd_rc ALTER COLUMN a TYPE zzm3sd_r;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" cannot be cast automatically to type zzm3sd_e
-- end-expected-error
ALTER TABLE zzm3sd_rc ALTER COLUMN b TYPE zzm3sd_e;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "c" cannot be cast automatically to type zzm3sd_d
-- end-expected-error
ALTER TABLE zzm3sd_rc ALTER COLUMN c TYPE zzm3sd_d;

-- begin-expected
-- columns: attname | ft
-- row: a | text
-- row: b | text
-- row: c | text
-- end-expected
SELECT attname, format_type(atttypid, atttypmod) AS ft FROM pg_attribute WHERE attrelid = 'zzm3sd_rc'::regclass AND attnum > 0 ORDER BY attnum;

-- told how, it goes
ALTER TABLE zzm3sd_rc ALTER COLUMN a TYPE zzm3sd_r USING a::zzm3sd_r;

-- begin-expected
-- columns: attname | ft
-- row: a | zzm3sd_r
-- end-expected
SELECT attname, format_type(atttypid, atttypmod) AS ft FROM pg_attribute WHERE attrelid = 'zzm3sd_rc'::regclass AND attnum = 1;

DROP TABLE zzm3sd_rc;

-- ============================================================================
-- An array of a type the reader defined
-- ============================================================================

CREATE TABLE zzm3sd_ua (a zzm3sd_d[], b zzm3sd_e[], c zzm3sd_r[], d int[]);

-- begin-expected
-- columns: attname | ft
-- row: a | zzm3sd_d[]
-- row: b | zzm3sd_e[]
-- row: c | zzm3sd_r[]
-- row: d | integer[]
-- end-expected
SELECT attname, format_type(atttypid, atttypmod) AS ft FROM pg_attribute WHERE attrelid = 'zzm3sd_ua'::regclass AND attnum > 0 ORDER BY attnum;

-- and it holds a list of that type's values
INSERT INTO zzm3sd_ua (a, b) VALUES ('{1,2}', '{a,b}');

-- begin-expected
-- columns: a | b | a1 | b2 | na | nb
-- row: {1,2} | {a,b} | 1 | b | 2 | 2
-- end-expected
SELECT a, b, a[1] AS a1, b[2] AS b2, array_length(a,1) AS na, array_length(b,1) AS nb FROM zzm3sd_ua;

DROP TABLE zzm3sd_ua;

-- the domain's own rules are every element's rules
CREATE DOMAIN zzm3sd_p AS int CHECK (VALUE > 0);
CREATE DOMAIN zzm3sd_nn AS int NOT NULL;
CREATE TABLE zzm3sd_da (a zzm3sd_p[], b zzm3sd_nn[]);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: value for domain zzm3sd_p violates check constraint "zzm3sd_p_check"
-- end-expected-error
INSERT INTO zzm3sd_da (a) VALUES ('{-1}');

-- every element, however many dimensions deep
-- begin-expected-error
-- sqlstate: 23514
-- message-like: value for domain zzm3sd_p violates check constraint "zzm3sd_p_check"
-- end-expected-error
INSERT INTO zzm3sd_da (a) VALUES ('{{1,2},{3,-4}}');

-- begin-expected-error
-- sqlstate: 23502
-- message-like: domain zzm3sd_nn does not allow null values
-- end-expected-error
INSERT INTO zzm3sd_da (b) VALUES ('{NULL}');

-- a null array holds no value of the domain, so there is nothing to judge, and
-- a CHECK that answers NULL does not violate
INSERT INTO zzm3sd_da (a, b) VALUES ('{1,2}', NULL);
INSERT INTO zzm3sd_da (a) VALUES ('{NULL}');
INSERT INTO zzm3sd_da (a) VALUES ('{}');

-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT count(*)::int AS n FROM zzm3sd_da;

-- begin-expected
-- columns: a
-- row: {}
-- row: {1,2}
-- row: {NULL}
-- end-expected
SELECT a FROM zzm3sd_da ORDER BY 1;

DROP TABLE zzm3sd_da;
DROP DOMAIN zzm3sd_p;
DROP DOMAIN zzm3sd_nn;

-- what an array of one of those types does take
CREATE DOMAIN zzm3sd_dm AS int CHECK (VALUE > 0);
CREATE TYPE zzm3sd_en AS ENUM ('a', 'b', 'c');
CREATE TABLE zzm3sd_ar (a zzm3sd_dm[] DEFAULT '{1,2}', b zzm3sd_en[] DEFAULT '{a,b}', c int[] DEFAULT ARRAY[9]);
INSERT INTO zzm3sd_ar DEFAULT VALUES;

-- begin-expected
-- columns: a | b | c
-- row: {1,2} | {a,b} | {9}
-- end-expected
SELECT a, b, c FROM zzm3sd_ar;

-- begin-expected
-- columns: na | a2 | b1
-- row: 2 | 2 | a
-- end-expected
SELECT array_length(a, 1) AS na, a[2] AS a2, b[1] AS b1 FROM zzm3sd_ar;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM zzm3sd_ar WHERE 1 = ANY (a);

-- begin-expected
-- columns: u
-- row: a
-- row: b
-- end-expected
SELECT unnest(b) AS u FROM zzm3sd_ar ORDER BY 1;

INSERT INTO zzm3sd_ar (a, b, c) VALUES (ARRAY[3, 4]::zzm3sd_dm[], ARRAY['c']::zzm3sd_en[], ARRAY[1]);

-- begin-expected
-- columns: a | b | c
-- row: {3,4} | {c} | {1}
-- row: {1,2} | {a,b} | {9}
-- end-expected
SELECT a, b, c FROM zzm3sd_ar ORDER BY c;

DROP TABLE zzm3sd_ar;

CREATE TABLE zzm3sd_ar2 (a zzm3sd_en[]);
INSERT INTO zzm3sd_ar2 VALUES ('{a,c}');

-- begin-expected
-- columns: a
-- row: {a,c}
-- end-expected
SELECT a FROM zzm3sd_ar2;

DROP TABLE zzm3sd_ar2;
DROP TYPE zzm3sd_en;
DROP DOMAIN zzm3sd_dm;
DROP TYPE zzm3sd_e;
DROP DOMAIN zzm3sd_d;
DROP TYPE zzm3sd_r;

-- ============================================================================
-- What a CHECK over a domain-typed column says
-- ============================================================================

CREATE DOMAIN zzm3sd_di AS int;
CREATE DOMAIN zzm3sd_dt AS text;
CREATE DOMAIN zzm3sd_dn AS numeric(8,2);
CREATE DOMAIN zzm3sd_dv AS varchar(5);
CREATE TABLE zzm3sd_dc (a zzm3sd_di, b zzm3sd_dt, c zzm3sd_dn, d zzm3sd_dv, e int, f text);
ALTER TABLE zzm3sd_dc ADD CONSTRAINT zzm3sd_x1 CHECK (a > 1);
ALTER TABLE zzm3sd_dc ADD CONSTRAINT zzm3sd_x2 CHECK (b <> 'q');
ALTER TABLE zzm3sd_dc ADD CONSTRAINT zzm3sd_x3 CHECK (c > 1);
ALTER TABLE zzm3sd_dc ADD CONSTRAINT zzm3sd_x4 CHECK (d <> 'q');
ALTER TABLE zzm3sd_dc ADD CONSTRAINT zzm3sd_x5 CHECK (a IS NOT NULL);
ALTER TABLE zzm3sd_dc ADD CONSTRAINT zzm3sd_x6 CHECK (a = e);
ALTER TABLE zzm3sd_dc ADD CONSTRAINT zzm3sd_x7 CHECK (b = f);
ALTER TABLE zzm3sd_dc ADD CONSTRAINT zzm3sd_x8 CHECK (a + 1 > 2);
ALTER TABLE zzm3sd_dc ADD CONSTRAINT zzm3sd_x9 CHECK (length(b) > 1);
ALTER TABLE zzm3sd_dc ADD CONSTRAINT zzm3sd_xa CHECK (a BETWEEN 1 AND 9);
ALTER TABLE zzm3sd_dc ADD CONSTRAINT zzm3sd_xb CHECK (b LIKE 'a%');
ALTER TABLE zzm3sd_dc ADD CONSTRAINT zzm3sd_xc CHECK (a IN (1, 2));
ALTER TABLE zzm3sd_dc ADD CONSTRAINT zzm3sd_xf CHECK (-a < 0);

-- begin-expected
-- columns: conname | def
-- row: zzm3sd_x1 | CHECK (((a)::integer > 1))
-- row: zzm3sd_x2 | CHECK (((b)::text <> 'q'::text))
-- row: zzm3sd_x3 | CHECK (((c)::numeric > (1)::numeric))
-- row: zzm3sd_x4 | CHECK (((d)::text <> 'q'::text))
-- row: zzm3sd_x5 | CHECK ((a IS NOT NULL))
-- row: zzm3sd_x6 | CHECK (((a)::integer = e))
-- row: zzm3sd_x7 | CHECK (((b)::text = f))
-- row: zzm3sd_x8 | CHECK ((((a)::integer + 1) > 2))
-- row: zzm3sd_x9 | CHECK ((length((b)::text) > 1))
-- row: zzm3sd_xa | CHECK ((((a)::integer >= 1) AND ((a)::integer <= 9)))
-- row: zzm3sd_xb | CHECK (((b)::text ~~ 'a%'::text))
-- row: zzm3sd_xc | CHECK (((a)::integer = ANY (ARRAY[1, 2])))
-- row: zzm3sd_xf | CHECK (((- (a)::integer) < 0))
-- end-expected
SELECT conname, pg_get_constraintdef(oid) AS def FROM pg_constraint WHERE conrelid = 'zzm3sd_dc'::regclass ORDER BY conname;

-- the information schema carries the same text without the CHECK ( ) around it
-- begin-expected
-- columns: constraint_name | check_clause
-- row: zzm3sd_x2 | ((b)::text <> 'q'::text)
-- end-expected
SELECT constraint_name, check_clause FROM information_schema.check_constraints WHERE constraint_name = 'zzm3sd_x2';

DROP TABLE zzm3sd_dc;
DROP DOMAIN zzm3sd_dt;
DROP DOMAIN zzm3sd_dn;
DROP DOMAIN zzm3sd_dv;
DROP DOMAIN zzm3sd_di;

-- ============================================================================
-- A retype to a domain rewrites what the CHECKs on the column say
-- ============================================================================

CREATE DOMAIN zzm3sd_pos AS int CHECK (VALUE > 0);
CREATE TABLE zzm3sd_m (a int, b int, c int, d int, e int, f int, g int, h int);
ALTER TABLE zzm3sd_m ADD CONSTRAINT zzm3sd_k1 CHECK (a > 1);
ALTER TABLE zzm3sd_m ADD CONSTRAINT zzm3sd_k2 CHECK (b > 1);
ALTER TABLE zzm3sd_m ADD CONSTRAINT zzm3sd_k3 CHECK (c > 1);
ALTER TABLE zzm3sd_m ADD CONSTRAINT zzm3sd_k4 CHECK (d IS NOT NULL);
ALTER TABLE zzm3sd_m ADD CONSTRAINT zzm3sd_k5 CHECK (e + 1 > 2);
ALTER TABLE zzm3sd_m ADD CONSTRAINT zzm3sd_k6 CHECK (f > 1 AND g > 2);
ALTER TABLE zzm3sd_m ADD CONSTRAINT zzm3sd_k7 CHECK (h > 1);
ALTER TABLE zzm3sd_m ALTER COLUMN a TYPE bigint;
ALTER TABLE zzm3sd_m ALTER COLUMN b TYPE numeric;
ALTER TABLE zzm3sd_m ALTER COLUMN c TYPE zzm3sd_pos;
ALTER TABLE zzm3sd_m ALTER COLUMN d TYPE zzm3sd_pos;
ALTER TABLE zzm3sd_m ALTER COLUMN e TYPE zzm3sd_pos;
ALTER TABLE zzm3sd_m ALTER COLUMN f TYPE zzm3sd_pos;
ALTER TABLE zzm3sd_m ALTER COLUMN h TYPE smallint;

-- only the columns that became a domain carry a reading down to the base type;
-- a widening between built-ins leaves the operator where it was, and the one
-- whose operator gained a numeric operand puts the conversion on the constant
-- begin-expected
-- columns: conname | def
-- row: zzm3sd_k1 | CHECK ((a > 1))
-- row: zzm3sd_k2 | CHECK ((b > (1)::numeric))
-- row: zzm3sd_k3 | CHECK (((c)::integer > 1))
-- row: zzm3sd_k4 | CHECK ((d IS NOT NULL))
-- row: zzm3sd_k5 | CHECK ((((e)::integer + 1) > 2))
-- row: zzm3sd_k6 | CHECK ((((f)::integer > 1) AND (g > 2)))
-- row: zzm3sd_k7 | CHECK ((h > 1))
-- end-expected
SELECT conname, pg_get_constraintdef(oid) AS def FROM pg_constraint WHERE conrelid = 'zzm3sd_m'::regclass ORDER BY conname;

-- begin-expected
-- columns: attname | ft
-- row: a | bigint
-- row: b | numeric
-- row: c | zzm3sd_pos
-- row: d | zzm3sd_pos
-- row: e | zzm3sd_pos
-- row: f | zzm3sd_pos
-- row: g | integer
-- row: h | smallint
-- end-expected
SELECT attname, format_type(atttypid, atttypmod) AS ft FROM pg_attribute WHERE attrelid = 'zzm3sd_m'::regclass AND attnum > 0 ORDER BY attnum;

-- and both rules are enforced throughout: the domain's own, then the relation's
INSERT INTO zzm3sd_m VALUES (2, 2, 2, 2, 2, 2, 3, 2);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: value for domain zzm3sd_pos violates check constraint "zzm3sd_pos_check"
-- end-expected-error
INSERT INTO zzm3sd_m VALUES (2, 2, 0, 2, 2, 2, 3, 2);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "zzm3sd_m" violates check constraint "zzm3sd_k3"
-- end-expected-error
INSERT INTO zzm3sd_m VALUES (2, 2, 1, 2, 2, 2, 3, 2);

-- and the reading down goes again when the column stops being a domain
ALTER TABLE zzm3sd_m ALTER COLUMN c TYPE int;

-- begin-expected
-- columns: conname | def
-- row: zzm3sd_k3 | CHECK ((c > 1))
-- end-expected
SELECT conname, pg_get_constraintdef(oid) AS def FROM pg_constraint WHERE conrelid = 'zzm3sd_m'::regclass AND conname = 'zzm3sd_k3';

DROP TABLE zzm3sd_m;
DROP DOMAIN zzm3sd_pos;
