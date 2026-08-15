-- ============================================================================
-- TABLESAMPLE names a relation, and that relation may be qualified; REPEATABLE
-- is declared over double precision, so a fraction is a seed.
-- ============================================================================
DROP SCHEMA IF EXISTS zzc3_s CASCADE;
CREATE SCHEMA zzc3_s;
CREATE TABLE zzc3_s.zzc3_ts (a int);
INSERT INTO zzc3_s.zzc3_ts SELECT g FROM generate_series(1,20) g;

-- begin-expected
-- columns: cnt
-- row: 20
-- end-expected
SELECT count(*) AS cnt FROM zzc3_s.zzc3_ts TABLESAMPLE BERNOULLI (100);

-- begin-expected
-- columns: cnt
-- row: 20
-- end-expected
SELECT count(*) AS cnt FROM zzc3_s.zzc3_ts TABLESAMPLE BERNOULLI (100) REPEATABLE (1.5);

-- begin-expected-error
-- sqlstate: 2202G
-- message-like: TABLESAMPLE REPEATABLE parameter cannot be null
-- end-expected-error
SELECT count(*) FROM zzc3_s.zzc3_ts TABLESAMPLE BERNOULLI (100) REPEATABLE (NULL);

-- begin-expected-error
-- sqlstate: 2202H
-- message-like: TABLESAMPLE parameter cannot be null
-- end-expected-error
SELECT count(*) FROM zzc3_s.zzc3_ts TABLESAMPLE BERNOULLI (NULL);

DROP TABLE zzc3_s.zzc3_ts;
DROP SCHEMA zzc3_s;

-- ============================================================================
-- A sequence function opens a relation: the kind it found is what it complains
-- about, and only a name reaching nothing at all is a relation that is not there.
-- The argument is a relation name, so its quotes are identifier quoting.
-- ============================================================================
DROP TABLE IF EXISTS zzc3_ntq CASCADE;
CREATE TABLE zzc3_ntq (a int);
CREATE VIEW zzc3_ntqv AS SELECT * FROM zzc3_ntq;
CREATE INDEX zzc3_ntqi ON zzc3_ntq (a);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot open relation "zzc3_ntq"
-- detail-like: This operation is not supported for tables.
-- end-expected-error
SELECT nextval('zzc3_ntq');

-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot open relation "zzc3_ntq"
-- detail-like: This operation is not supported for tables.
-- end-expected-error
SELECT setval('zzc3_ntq', 1);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot open relation "zzc3_ntqv"
-- detail-like: This operation is not supported for views.
-- end-expected-error
SELECT nextval('zzc3_ntqv');

-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot open relation "zzc3_ntqi"
-- detail-like: This operation is not supported for indexes.
-- end-expected-error
SELECT nextval('zzc3_ntqi');

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zzc3_NOSUCH" does not exist
-- end-expected-error
SELECT nextval('"zzc3_NOSUCH"');

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zzc3_nosuch2" does not exist
-- end-expected-error
SELECT currval('ZZC3_NOSUCH2');

DROP VIEW zzc3_ntqv;
DROP TABLE zzc3_ntq;

-- ============================================================================
-- A domain carries every CHECK it was written with, under the names PostgreSQL
-- gives them; a name written twice leaves nothing created at all.
-- ============================================================================
DROP DOMAIN IF EXISTS zzc3_d10;
CREATE DOMAIN zzc3_d10 AS int CHECK (VALUE > 0) CHECK (VALUE < 10);

-- begin-expected
-- columns: names
-- row: zzc3_d10_check,zzc3_d10_check1
-- end-expected
SELECT string_agg(conname, ',' ORDER BY conname) AS names FROM pg_constraint WHERE contypid = 'zzc3_d10'::regtype;

-- begin-expected-error
-- sqlstate: 23514
-- message-like: value for domain zzc3_d10 violates check constraint "zzc3_d10_check"
-- end-expected-error
SELECT (-1)::zzc3_d10;

-- begin-expected-error
-- sqlstate: 23514
-- message-like: value for domain zzc3_d10 violates check constraint "zzc3_d10_check1"
-- end-expected-error
SELECT 11::zzc3_d10;

-- begin-expected
-- columns: v
-- row: 5
-- end-expected
SELECT (5::zzc3_d10)::text AS v;

-- begin-expected-error
-- sqlstate: 42710
-- message-like: constraint "k" for domain "zzc3_d11" already exists
-- end-expected-error
CREATE DOMAIN zzc3_d11 AS int CONSTRAINT k CHECK (VALUE > 0) CONSTRAINT k CHECK (VALUE < 9);

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "zzc3_d11" does not exist
-- end-expected-error
SELECT (-1)::zzc3_d11;

DROP DOMAIN zzc3_d10;

-- ============================================================================
-- LIKE writes its source's columns into the definition, so they clash with what
-- the definition writes and with what a second LIKE brings; and it copies a
-- relation that has columns, which a sequence has not.
-- ============================================================================
DROP TABLE IF EXISTS zzc3_lsrc CASCADE;
DROP SEQUENCE IF EXISTS zzc3_lseq;
CREATE TABLE zzc3_lsrc (a int, b text);
CREATE SEQUENCE zzc3_lseq;

-- begin-expected-error
-- sqlstate: 42701
-- message-like: column "a" specified more than once
-- end-expected-error
CREATE TABLE zzc3_l1 (a int, LIKE zzc3_lsrc);

-- begin-expected-error
-- sqlstate: 42701
-- message-like: column "a" specified more than once
-- end-expected-error
CREATE TABLE zzc3_l2 (LIKE zzc3_lsrc, a int);

-- begin-expected-error
-- sqlstate: 42701
-- message-like: column "a" specified more than once
-- end-expected-error
CREATE TABLE zzc3_l3 (LIKE zzc3_lsrc, LIKE zzc3_lsrc);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: relation "zzc3_lseq" is invalid in LIKE clause
-- detail-like: This operation is not supported for sequences.
-- end-expected-error
CREATE TABLE zzc3_l4 (LIKE zzc3_lseq);

-- a LIKE on its own still copies the columns
CREATE TABLE zzc3_l5 (LIKE zzc3_lsrc);

-- begin-expected
-- columns: cols
-- row: a,b
-- end-expected
SELECT string_agg(column_name, ',' ORDER BY ordinal_position) AS cols FROM information_schema.columns WHERE table_name = 'zzc3_l5';

DROP TABLE zzc3_l5;
DROP SEQUENCE zzc3_lseq;
DROP TABLE zzc3_lsrc;

-- ============================================================================
-- A typed table's columns are the composite type's attributes. The type has to
-- be a stand-alone one: a table's own row type is refused.
-- ============================================================================
DROP TABLE IF EXISTS zzc3_of CASCADE;
DROP TYPE IF EXISTS zzc3_ct CASCADE;
CREATE TYPE zzc3_ct AS (x int, y text);
CREATE TABLE zzc3_of OF zzc3_ct;

-- begin-expected
-- columns: cols
-- row: x,y
-- end-expected
SELECT string_agg(column_name, ',' ORDER BY ordinal_position) AS cols FROM information_schema.columns WHERE table_name = 'zzc3_of';

INSERT INTO zzc3_of VALUES (1,'a');

-- begin-expected
-- columns: x, y
-- row: 1|a
-- end-expected
SELECT x, y FROM zzc3_of;

CREATE TABLE zzc3_of2 OF zzc3_ct (PRIMARY KEY (x));

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*) AS cnt FROM pg_constraint WHERE conname = 'zzc3_of2_pkey' AND contype = 'p';

-- begin-expected-error
-- sqlstate: 42704
-- message-like: type "zzc3_nosuchtype" does not exist
-- end-expected-error
CREATE TABLE zzc3_of3 OF zzc3_nosuchtype;

CREATE TABLE zzc3_ofx (a int);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: type zzc3_ofx is the row type of another table
-- detail-like: A typed table must use a stand-alone composite type created with CREATE TYPE.
-- end-expected-error
CREATE TABLE zzc3_of4 OF zzc3_ofx;

DROP TABLE zzc3_ofx;
DROP TABLE zzc3_of2;
DROP TABLE zzc3_of;
DROP TYPE zzc3_ct;

-- ============================================================================
-- A type modifier belongs to the types that have one. PostgreSQL refuses the
-- rest in two voices: the words its grammar has a production for stop at the
-- parenthesis, and a type read as a plain name is looked up and found to have
-- no modifier of its own.
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "("
-- end-expected-error
CREATE TABLE zzc3_i5 (a int(5));

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "("
-- end-expected-error
CREATE TABLE zzc3_i6 (a boolean(3));

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "("
-- end-expected-error
SELECT 1::int(5);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: type modifier is not allowed for type "text"
-- end-expected-error
CREATE TABLE zzc3_i7 (a text(5));

-- begin-expected-error
-- sqlstate: 42601
-- message-like: type modifier is not allowed for type "int4"
-- end-expected-error
CREATE TABLE zzc3_i8 (a int4(5));

-- begin-expected-error
-- sqlstate: 42601
-- message-like: type modifier is not allowed for type "date"
-- end-expected-error
CREATE TABLE zzc3_i9 (a date(3));

-- the types that carry one are untouched
CREATE TABLE zzc3_i10 (a varchar(5), b numeric(10,2), c timestamp(3), d float(5), e bit(3));

-- begin-expected
-- columns: len
-- row: 5
-- end-expected
SELECT character_maximum_length AS len FROM information_schema.columns WHERE table_name = 'zzc3_i10' AND column_name = 'a';

DROP TABLE zzc3_i10;

-- ============================================================================
-- An exclusion constraint compares with the operator it names, and only with an
-- operator that is its own commutator. A generated column's expression and a
-- CHECK are both judged when the table is defined.
-- ============================================================================
DROP TABLE IF EXISTS zzc3_x6 CASCADE;
CREATE TABLE zzc3_x6 (a int, EXCLUDE USING gist (a WITH <>));
INSERT INTO zzc3_x6 VALUES (1);

-- begin-expected-error
-- sqlstate: 23P01
-- message-like: conflicting key value violates exclusion constraint "zzc3_x6_a_excl"
-- detail-like: Key (a)=(2) conflicts with existing key (a)=(1).
-- end-expected-error
INSERT INTO zzc3_x6 VALUES (2);

-- a value like the stored one is not excluded
INSERT INTO zzc3_x6 VALUES (1);

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*) AS cnt FROM zzc3_x6;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: operator <(integer,integer) is not commutative
-- detail-like: Only commutative operators can be used in exclusion constraints.
-- end-expected-error
CREATE TABLE zzc3_ex1 (a int, EXCLUDE USING btree (a WITH <));

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "abc"
-- end-expected-error
CREATE TABLE zzc3_gc1 (a int, b int GENERATED ALWAYS AS ('abc') STORED);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: set-returning functions are not allowed in check constraints
-- end-expected-error
CREATE TABLE zzc3_ck3 (a int CHECK (generate_series(1,a) > 0));

DROP TABLE zzc3_x6;

-- ============================================================================
-- A view publishes the names it was created with. Renaming a column of the
-- relation underneath renames what the view reads, not what it shows; and a
-- default written on a view column is a default the catalogue reports.
-- ============================================================================
DROP VIEW IF EXISTS zzc3_v1v;
DROP TABLE IF EXISTS zzc3_v1t CASCADE;
CREATE TABLE zzc3_v1t (i int, v text);
INSERT INTO zzc3_v1t VALUES (1,'a');
CREATE VIEW zzc3_v1v AS SELECT i, v FROM zzc3_v1t;
ALTER TABLE zzc3_v1t RENAME COLUMN v TO v2;

-- begin-expected
-- columns: v
-- row: a
-- end-expected
SELECT v FROM zzc3_v1v;

-- begin-expected
-- columns: cols
-- row: i,v
-- end-expected
SELECT string_agg(column_name, ',' ORDER BY ordinal_position) AS cols FROM information_schema.columns WHERE table_name = 'zzc3_v1v';

-- begin-expected
-- columns: aliased
-- row: true
-- end-expected
SELECT (pg_get_viewdef('zzc3_v1v'::regclass) LIKE '%v2 AS v%') AS aliased;

CREATE TABLE zzc3_v2t (i int, v text);
CREATE VIEW zzc3_v2v AS SELECT i, v FROM zzc3_v2t;
ALTER VIEW zzc3_v2v ALTER COLUMN v SET DEFAULT 'zz';

-- begin-expected
-- columns: def
-- row: 'zz'::text
-- end-expected
SELECT column_default AS def FROM information_schema.columns WHERE table_name = 'zzc3_v2v' AND column_name = 'v';

INSERT INTO zzc3_v2v (i) VALUES (1);

-- begin-expected
-- columns: i, v
-- row: 1|zz
-- end-expected
SELECT i, v FROM zzc3_v2t;

ALTER VIEW zzc3_v2v ALTER COLUMN v DROP DEFAULT;

-- begin-expected
-- columns: def
-- row: null
-- end-expected
SELECT column_default AS def FROM information_schema.columns WHERE table_name = 'zzc3_v2v' AND column_name = 'v';

DROP VIEW zzc3_v2v;
DROP TABLE zzc3_v2t;
DROP VIEW zzc3_v1v;
DROP TABLE zzc3_v1t;

-- ============================================================================
-- A collation named in a definition has to be one the database holds, an index
-- named by CLUSTER ON has to be one of the relation's own, and a deferrable key
-- cannot back a foreign key.
-- ============================================================================
DROP TABLE IF EXISTS zzc3_misc CASCADE;
CREATE TABLE zzc3_misc (a int);
CREATE INDEX zzc3_mi ON zzc3_misc (a);
CREATE TABLE zzc3_other (c int);
CREATE INDEX zzc3_oi ON zzc3_other (c);

-- begin-expected-error
-- sqlstate: 42704
-- message-like: collation "nosuch_collation" for encoding "UTF8" does not exist
-- end-expected-error
ALTER TABLE zzc3_misc ADD COLUMN e text COLLATE "nosuch_collation";

-- begin-expected
-- columns: cols
-- row: a
-- end-expected
SELECT string_agg(column_name, ',' ORDER BY ordinal_position) AS cols FROM information_schema.columns WHERE table_name = 'zzc3_misc';

-- begin-expected-error
-- sqlstate: 42704
-- message-like: index "zzc3_nosuchindex" for table "zzc3_misc" does not exist
-- end-expected-error
ALTER TABLE zzc3_misc CLUSTER ON zzc3_nosuchindex;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zzc3_oi" is not an index for table "zzc3_misc"
-- end-expected-error
ALTER TABLE zzc3_misc CLUSTER ON zzc3_oi;

-- its own index is accepted
ALTER TABLE zzc3_misc CLUSTER ON zzc3_mi;

CREATE TABLE zzc3_fu (d int UNIQUE DEFERRABLE);

-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot use a deferrable unique constraint for referenced table "zzc3_fu"
-- end-expected-error
CREATE TABLE zzc3_fc (p int REFERENCES zzc3_fu(d));

CREATE TABLE zzc3_fp (a int PRIMARY KEY DEFERRABLE);

-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot use a deferrable primary key for referenced table "zzc3_fp"
-- end-expected-error
CREATE TABLE zzc3_fq (b int REFERENCES zzc3_fp);

DROP TABLE zzc3_fp;
DROP TABLE zzc3_fu;
DROP TABLE zzc3_other;
DROP TABLE zzc3_misc;