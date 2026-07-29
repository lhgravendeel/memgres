-- ============================================================================
-- Feature Comparison: residual DDL validation, corrected
-- Target: PostgreSQL 18 vs Memgres
--
-- The second pass over the residual DDL checks, after each one was re-measured
-- against PostgreSQL 18 rather than assumed.
--
-- Most of what it pins is the dangerous direction: definitions PostgreSQL runs
-- and memgres was refusing -- an inlinable SQL function in a partition key, a
-- partition key expression containing a comma, the only name PostgreSQL gives a
-- NOT NULL constraint, a quoted index name differing only by case, CHECK (...)
-- NOT VALID written in a CREATE TABLE, a hash partition whose modulus divides
-- another's, and ALTER TABLE i RENAME TO j aimed at an index. The rest is the
-- ordinary kind: refusals PostgreSQL makes that memgres accepted, each paired
-- here with the neighbouring shapes that must keep being accepted.
-- ============================================================================

DROP TABLE IF EXISTS d12_pkq1, d12_pkq2, d12_pkq3, d12_pke1, d12_pke2, d12_pke3 CASCADE;
DROP TABLE IF EXISTS d12_pke4, d12_pke5, d12_pke6, d12_pke7 CASCADE;
DROP FUNCTION IF EXISTS d12_sqlstable(int);
DROP FUNCTION IF EXISTS d12_sqlvol(int);
DROP FUNCTION IF EXISTS d12_sqlimm(int);
DROP FUNCTION IF EXISTS d12_plstable(int);

-- ============================================================================
-- SECTION A: a partition key is judged by what it computes
-- ============================================================================

CREATE FUNCTION d12_sqlstable(int) RETURNS int LANGUAGE sql STABLE AS 'SELECT $1 + 1';
CREATE FUNCTION d12_sqlvol(int) RETURNS int LANGUAGE sql AS 'SELECT $1 + 1';
CREATE FUNCTION d12_sqlimm(int) RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT $1 + 1';
CREATE FUNCTION d12_plstable(int) RETURNS int LANGUAGE plpgsql STABLE AS 'BEGIN RETURN $1 + 1; END';

-- PostgreSQL inlines a SQL body before it judges the expression, so what the
-- declaration says about volatility never comes up.
CREATE TABLE d12_pkq1 (a int) PARTITION BY RANGE ((d12_sqlstable(a)));
CREATE TABLE d12_pkq2 (a int) PARTITION BY RANGE ((d12_sqlvol(a)));
CREATE TABLE d12_pkq3 (a int) PARTITION BY RANGE ((d12_sqlimm(a)));

-- A body that cannot be inlined is believed when it says it is not immutable.
-- begin-expected-error
-- sqlstate: 42P17
-- message-like: functions in partition key expression must be marked IMMUTABLE
-- end-expected-error
CREATE TABLE d12_pkq4 (a int) PARTITION BY RANGE ((d12_plstable(a)));

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: functions in partition key expression must be marked IMMUTABLE
-- end-expected-error
CREATE TABLE d12_pkq5 (a int) PARTITION BY RANGE ((random()::int + a));

-- A key expression may hold a comma of its own; splitting the list on every
-- comma read one call as two keys and reported half of it as a missing column.
CREATE TABLE d12_pke1 (ts timestamp) PARTITION BY RANGE ((date_trunc('month', ts)));
CREATE TABLE d12_pke2 (b int) PARTITION BY RANGE ((coalesce(b, 0)));
CREATE TABLE d12_pke3 (s text) PARTITION BY RANGE ((substr(s, 1, 1)));
CREATE TABLE d12_pke4 (d date) PARTITION BY RANGE ((date_part('year', d)));
CREATE TABLE d12_pke5 (a int, b int) PARTITION BY LIST ((greatest(a, b)));

-- ... and a key list that really is several keys still is.
CREATE TABLE d12_pke6 (a int, b int) PARTITION BY RANGE (a, b);
CREATE TABLE d12_pke7 (a int, b int) PARTITION BY RANGE ((a + b), b);

-- A key expression only reads the row's own columns.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE TABLE d12_pke8 (a int) PARTITION BY RANGE ((nosuchcol + 1));

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" named in partition key does not exist
-- end-expected-error
CREATE TABLE d12_pke9 (a int) PARTITION BY RANGE (nosuchcol);

-- ============================================================================
-- SECTION B: a NOT NULL constraint has a name, and it is the name commands ask for
-- ============================================================================

CREATE TABLE d12_nna (id int PRIMARY KEY, a int);
ALTER TABLE d12_nna ALTER COLUMN a SET NOT NULL;

-- begin-expected
-- columns: conname
-- row: d12_nna_a_not_null
-- row: d12_nna_id_not_null
-- end-expected
SELECT conname FROM pg_constraint WHERE conrelid = 'd12_nna'::regclass
  AND contype = 'n' ORDER BY conname;

-- That name is the one that can be dropped, and dropping it makes the column
-- nullable again.
ALTER TABLE d12_nna DROP CONSTRAINT d12_nna_a_not_null;

-- begin-expected
-- columns: is_nullable
-- row: YES
-- end-expected
SELECT is_nullable FROM information_schema.columns
  WHERE table_name = 'd12_nna' AND column_name = 'a';

INSERT INTO d12_nna (id) VALUES (1);

-- begin-expected-error
-- sqlstate: 42704
-- message-like: constraint "d12_nna_a_not_null" of relation "d12_nna" does not exist
-- end-expected-error
ALTER TABLE d12_nna DROP CONSTRAINT d12_nna_a_not_null;

-- A column declared NOT NULL carries exactly the same constraint.
CREATE TABLE d12_nnb (id int PRIMARY KEY, a int NOT NULL);
ALTER TABLE d12_nnb DROP CONSTRAINT d12_nnb_a_not_null;
INSERT INTO d12_nnb (id) VALUES (1);

-- A written name is the name the catalog shows.
CREATE TABLE d12_nnc (id int PRIMARY KEY, a int, b int NOT NULL);
ALTER TABLE d12_nnc ADD CONSTRAINT d12_nnc_a NOT NULL a;

-- begin-expected
-- columns: conname
-- row: d12_nnc_a
-- row: d12_nnc_b_not_null
-- row: d12_nnc_id_not_null
-- end-expected
SELECT conname FROM pg_constraint WHERE conrelid = 'd12_nnc'::regclass
  AND contype = 'n' ORDER BY conname;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: constraint "d12_nnc_a_not_null" of relation "d12_nnc" does not exist
-- end-expected-error
ALTER TABLE d12_nnc DROP CONSTRAINT d12_nnc_a_not_null;

ALTER TABLE d12_nnc DROP CONSTRAINT d12_nnc_a;
INSERT INTO d12_nnc (id, b) VALUES (1, 1);

-- Naming a constraint over a column that is already NOT NULL creates nothing:
-- PostgreSQL merges the declaration into the constraint already there.
CREATE TABLE d12_nnd (id int PRIMARY KEY, b int NOT NULL);
ALTER TABLE d12_nnd ADD CONSTRAINT d12_nnd_b NOT NULL b;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: constraint "d12_nnd_b" of relation "d12_nnd" does not exist
-- end-expected-error
ALTER TABLE d12_nnd DROP CONSTRAINT d12_nnd_b;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: constraint "d12_nnd_b" of relation "d12_nnd" does not exist
-- end-expected-error
ALTER TABLE d12_nnd ALTER CONSTRAINT d12_nnd_b NO INHERIT;

ALTER TABLE d12_nnd DROP CONSTRAINT d12_nnd_b_not_null;

-- The constraint can be renamed.
CREATE TABLE d12_nne (id int PRIMARY KEY, a int NOT NULL);
ALTER TABLE d12_nne RENAME CONSTRAINT d12_nne_a_not_null TO d12_nne_nn;

-- begin-expected
-- columns: conname
-- row: d12_nne_id_not_null
-- row: d12_nne_nn
-- end-expected
SELECT conname FROM pg_constraint WHERE conrelid = 'd12_nne'::regclass
  AND contype = 'n' ORDER BY conname;

ALTER TABLE d12_nne DROP CONSTRAINT d12_nne_nn;

-- Renaming the column leaves the constraint under the name it already had.
CREATE TABLE d12_nnf (id int PRIMARY KEY, a int NOT NULL);
ALTER TABLE d12_nnf RENAME COLUMN a TO z;

-- begin-expected
-- columns: conname
-- row: d12_nnf_a_not_null
-- row: d12_nnf_id_not_null
-- end-expected
SELECT conname FROM pg_constraint WHERE conrelid = 'd12_nnf'::regclass
  AND contype = 'n' ORDER BY conname;

-- A key column's NOT NULL belongs to the key.
CREATE TABLE d12_nng (id int PRIMARY KEY, a int);

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: column "id" is in a primary key
-- end-expected-error
ALTER TABLE d12_nng DROP CONSTRAINT d12_nng_id_not_null;

-- ============================================================================
-- SECTION C: ALTER DOMAIN changes shared state, so ROLLBACK has to put it back
-- ============================================================================

CREATE DOMAIN d12_dma AS int;
CREATE TABLE d12_dmat (id int PRIMARY KEY, a d12_dma);

BEGIN;
ALTER DOMAIN d12_dma SET NOT NULL;
ROLLBACK;
INSERT INTO d12_dmat VALUES (1, NULL);

DELETE FROM d12_dmat;
BEGIN;
ALTER DOMAIN d12_dma SET DEFAULT 7;
ROLLBACK;
INSERT INTO d12_dmat (id) VALUES (2);

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT a FROM d12_dmat WHERE id = 2;

BEGIN;
ALTER DOMAIN d12_dma ADD CONSTRAINT d12_dma_c CHECK (VALUE > 100);
ROLLBACK;
INSERT INTO d12_dmat VALUES (3, 5);

-- What was committed stands.
CREATE DOMAIN d12_dmb AS int;
CREATE TABLE d12_dmbt (id int PRIMARY KEY, a d12_dmb);
BEGIN;
ALTER DOMAIN d12_dmb SET NOT NULL;
COMMIT;

-- begin-expected-error
-- sqlstate: 23502
-- end-expected-error
INSERT INTO d12_dmbt VALUES (1, NULL);

ALTER DOMAIN d12_dmb DROP NOT NULL;
INSERT INTO d12_dmbt VALUES (2, NULL);

-- A rename really renames, and a type name is one name across kinds.
CREATE DOMAIN d12_dmc AS int;
CREATE TYPE d12_enum AS ENUM ('x');
CREATE TABLE d12_host (a int);

-- begin-expected-error
-- sqlstate: 42710
-- message-like: type "d12_enum" already exists
-- end-expected-error
ALTER DOMAIN d12_dmc RENAME TO d12_enum;

-- begin-expected-error
-- sqlstate: 42710
-- message-like: type "d12_host" already exists
-- end-expected-error
ALTER DOMAIN d12_dmc RENAME TO d12_host;

ALTER DOMAIN d12_dmc RENAME TO d12_dmc2;

-- begin-expected
-- columns: typname
-- row: d12_dmc2
-- end-expected
SELECT typname FROM pg_type WHERE typname = 'd12_dmc2';

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "d12_nosuch" does not exist
-- end-expected-error
ALTER DOMAIN d12_dmc2 SET SCHEMA d12_nosuch;

-- ============================================================================
-- SECTION D: a command aimed at the wrong kind says which kind it wanted
-- ============================================================================

CREATE TABLE d12_rft (id int PRIMARY KEY);
CREATE VIEW d12_rfv AS SELECT id FROM d12_rft;
CREATE SEQUENCE d12_rfs;
CREATE INDEX d12_rfi ON d12_rft (id);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "d12_rfv" is not a table or materialized view
-- end-expected-error
REFRESH MATERIALIZED VIEW d12_rfv;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "d12_rfs" is not a table or materialized view
-- end-expected-error
REFRESH MATERIALIZED VIEW d12_rfs;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "d12_rfi" is not a table or materialized view
-- end-expected-error
REFRESH MATERIALIZED VIEW d12_rfi;

-- A plain table gets as far as the materialized-view check and is refused there.
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: "d12_rft" is not a materialized view
-- end-expected-error
REFRESH MATERIALIZED VIEW d12_rft;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "d12_rfnosuch" does not exist
-- end-expected-error
REFRESH MATERIALIZED VIEW d12_rfnosuch;

CREATE MATERIALIZED VIEW d12_rfm AS SELECT id FROM d12_rft;
REFRESH MATERIALIZED VIEW d12_rfm;

-- CREATE OR REPLACE VIEW says what it cannot replace, rather than that the name
-- is taken -- which is what a plain CREATE says.
CREATE TABLE d12_crt (a int);
CREATE SEQUENCE d12_crs;
CREATE MATERIALIZED VIEW d12_crm AS SELECT 1 AS a;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "d12_crt" is not a view
-- end-expected-error
CREATE OR REPLACE VIEW d12_crt AS SELECT 1 AS a;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "d12_crs" is not a view
-- end-expected-error
CREATE OR REPLACE VIEW d12_crs AS SELECT 1 AS a;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "d12_crm" is not a view
-- end-expected-error
CREATE OR REPLACE VIEW d12_crm AS SELECT 1 AS a;

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "d12_crt" already exists
-- end-expected-error
CREATE VIEW d12_crt AS SELECT 1 AS a;

CREATE VIEW d12_crv AS SELECT 1 AS a;
CREATE OR REPLACE VIEW d12_crv AS SELECT 2 AS a;

-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT a FROM d12_crv;

-- ALTER TABLE reaches an index only for its name and its owner.
CREATE TABLE d12_ait (i int PRIMARY KEY, j int);
CREATE INDEX d12_aiidx ON d12_ait (j);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: ALTER action ADD COLUMN cannot be performed on relation "d12_aiidx"
-- end-expected-error
ALTER TABLE d12_aiidx ADD COLUMN z int;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: ALTER action DROP COLUMN cannot be performed on relation "d12_aiidx"
-- end-expected-error
ALTER TABLE d12_aiidx DROP COLUMN j;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot change schema of index "d12_aiidx"
-- end-expected-error
ALTER TABLE d12_aiidx SET SCHEMA public;

ALTER TABLE d12_aiidx RENAME TO d12_aiidx9;

-- begin-expected
-- columns: indexname
-- row: d12_aiidx9
-- row: d12_ait_pkey
-- end-expected
SELECT indexname FROM pg_indexes WHERE tablename = 'd12_ait' ORDER BY indexname;

ALTER TABLE d12_ait ADD COLUMN z int;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "d12_ainosuch" does not exist
-- end-expected-error
ALTER TABLE d12_ainosuch ADD COLUMN z int;

-- A rename onto a name another relation owns is refused.
CREATE TABLE d12_art (i int PRIMARY KEY, j int);
CREATE INDEX d12_aridx ON d12_art (j);

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "d12_art" already exists
-- end-expected-error
ALTER INDEX d12_aridx RENAME TO d12_art;

ALTER INDEX d12_aridx RENAME TO d12_aridx2;

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "d12_aridx2" already exists
-- end-expected-error
ALTER TABLE d12_art RENAME TO d12_aridx2;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "d12_arnosuch" does not exist
-- end-expected-error
ALTER INDEX d12_arnosuch RENAME TO d12_arx;

ALTER INDEX IF EXISTS d12_arnosuch RENAME TO d12_arx;

-- A role is not renamed onto another role.
DROP ROLE IF EXISTS d12_rna;
DROP ROLE IF EXISTS d12_rnb;
DROP ROLE IF EXISTS d12_rnc;
CREATE ROLE d12_rna;
CREATE ROLE d12_rnb;

-- begin-expected-error
-- sqlstate: 42710
-- message-like: role "d12_rnb" already exists
-- end-expected-error
ALTER ROLE d12_rna RENAME TO d12_rnb;

ALTER ROLE d12_rna RENAME TO d12_rnc;

-- begin-expected
-- columns: rolname
-- row: d12_rnb
-- row: d12_rnc
-- end-expected
SELECT rolname FROM pg_roles WHERE rolname IN ('d12_rna','d12_rnb','d12_rnc') ORDER BY rolname;

DROP ROLE d12_rnb;
DROP ROLE d12_rnc;

-- ============================================================================
-- SECTION E: an index name belongs to one index, spelling included
-- ============================================================================

CREATE TABLE d12_ixt (b int PRIMARY KEY);
CREATE INDEX d12_ixmixedcase ON d12_ixt (b);
CREATE INDEX "d12_ixMixedCase" ON d12_ixt (b);

-- begin-expected
-- columns: indexname
-- row: d12_ixMixedCase
-- row: d12_ixmixedcase
-- row: d12_ixt_pkey
-- end-expected
SELECT indexname FROM pg_indexes WHERE tablename = 'd12_ixt' ORDER BY indexname;

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "d12_ixmixedcase" already exists
-- end-expected-error
CREATE INDEX d12_ixmixedcase ON d12_ixt (b);

DROP INDEX "d12_ixMixedCase";
DROP INDEX d12_ixmixedcase;

-- The index a key made for itself owns its name too.
CREATE TABLE d12_imt (id serial PRIMARY KEY, a int UNIQUE);

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "d12_imt_pkey" already exists
-- end-expected-error
CREATE TABLE d12_imt_pkey (x int);

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "d12_imt_a_key" already exists
-- end-expected-error
CREATE TABLE d12_imt_a_key (x int);

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop index d12_imt_pkey because constraint d12_imt_pkey on table d12_imt requires it
-- end-expected-error
DROP INDEX d12_imt_pkey;

-- An index somebody wrote a CREATE INDEX for is theirs to drop.
CREATE UNIQUE INDEX d12_imwritten ON d12_imt (a);
DROP INDEX d12_imwritten;

-- An unqualified DROP INDEX only reaches the schemas the search path shows.
CREATE SCHEMA d12_iqs;
CREATE TABLE d12_iqs.t (a int PRIMARY KEY, b int);
CREATE INDEX d12_iqidx ON d12_iqs.t (b);

-- begin-expected-error
-- sqlstate: 42704
-- message-like: index "d12_iqidx" does not exist
-- end-expected-error
DROP INDEX d12_iqidx;

DROP INDEX d12_iqs.d12_iqidx;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: index "d12_iqnosuch" does not exist
-- end-expected-error
DROP INDEX d12_iqnosuch;

DROP INDEX IF EXISTS d12_iqnosuch;

-- A qualified name is checked against its own schema, not the session's.
CREATE SCHEMA d12_nsq;
CREATE VIEW d12_nsq.thing AS SELECT 1 AS a;

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "thing" already exists
-- end-expected-error
CREATE TABLE d12_nsq.thing (a int);

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "thing" already exists
-- end-expected-error
CREATE SEQUENCE d12_nsq.thing;

CREATE TABLE d12_nsq.other (a int);

-- ============================================================================
-- SECTION F: a multi-action ALTER TABLE settles one shape, or leaves none
-- ============================================================================

CREATE TABLE d12_mat (id int PRIMARY KEY, d int DEFAULT 4, c int NOT NULL DEFAULT 0);

-- DROP DEFAULT is an earlier pass than SET DEFAULT, so the written order loses.
ALTER TABLE d12_mat ALTER COLUMN d SET DEFAULT 11, ALTER COLUMN d DROP DEFAULT;

-- begin-expected
-- columns: column_default
-- row: 11
-- end-expected
SELECT column_default FROM information_schema.columns
  WHERE table_name = 'd12_mat' AND column_name = 'd';

ALTER TABLE d12_mat ALTER COLUMN c SET NOT NULL, ALTER COLUMN c DROP NOT NULL;

-- begin-expected
-- columns: is_nullable
-- row: NO
-- end-expected
SELECT is_nullable FROM information_schema.columns
  WHERE table_name = 'd12_mat' AND column_name = 'c';

ALTER TABLE d12_mat ALTER COLUMN d DROP DEFAULT, ALTER COLUMN d SET DEFAULT 12;

-- begin-expected
-- columns: column_default
-- row: 12
-- end-expected
SELECT column_default FROM information_schema.columns
  WHERE table_name = 'd12_mat' AND column_name = 'd';

-- A refused multi-action ALTER leaves the table as it was.
CREATE TABLE d12_mau (id int PRIMARY KEY, z int);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "z" does not exist
-- end-expected-error
ALTER TABLE d12_mau ADD CONSTRAINT d12_mau_c CHECK (z IS NULL OR z > 0), DROP COLUMN z;

-- begin-expected
-- columns: column_name
-- row: id
-- row: z
-- end-expected
SELECT column_name FROM information_schema.columns
  WHERE table_name = 'd12_mau' ORDER BY column_name;

CREATE TABLE d12_mav (id int PRIMARY KEY, z int);
INSERT INTO d12_mav VALUES (1, NULL);

-- begin-expected-error
-- sqlstate: 23502
-- message-like: column "z" of relation "d12_mav" contains null values
-- end-expected-error
ALTER TABLE d12_mav ADD COLUMN y int, ALTER COLUMN z SET NOT NULL;

-- begin-expected
-- columns: column_name
-- row: id
-- row: z
-- end-expected
SELECT column_name FROM information_schema.columns
  WHERE table_name = 'd12_mav' ORDER BY column_name;

-- An ordinary multi-action ALTER still runs, including a constraint written
-- before the column it reads.
CREATE TABLE d12_maw (id int PRIMARY KEY, a int, b int);
ALTER TABLE d12_maw ADD COLUMN c int, ALTER COLUMN a SET DEFAULT 3, ALTER COLUMN b SET NOT NULL;
ALTER TABLE d12_maw DROP COLUMN c, ADD COLUMN e int DEFAULT 9;
ALTER TABLE d12_maw ADD CONSTRAINT d12_maw_ck CHECK (f > 0), ADD COLUMN f int;

-- begin-expected
-- columns: column_name
-- row: a
-- row: b
-- row: e
-- row: f
-- row: id
-- end-expected
SELECT column_name FROM information_schema.columns
  WHERE table_name = 'd12_maw' ORDER BY column_name;

-- ============================================================================
-- SECTION G: what is stored decides whether a new rule can hold
-- ============================================================================

CREATE TABLE d12_ext (id int PRIMARY KEY);
INSERT INTO d12_ext VALUES (1);

-- DEFAULT NULL fills the rows with nothing, so it is no default at all.
-- begin-expected-error
-- sqlstate: 23502
-- message-like: column "c" of relation "d12_ext" contains null values
-- end-expected-error
ALTER TABLE d12_ext ADD COLUMN c int NOT NULL DEFAULT NULL;

-- begin-expected-error
-- sqlstate: 23502
-- message-like: column "d" of relation "d12_ext" contains null values
-- end-expected-error
ALTER TABLE d12_ext ADD COLUMN d int NOT NULL;

ALTER TABLE d12_ext ADD COLUMN e int NOT NULL DEFAULT 3;

-- begin-expected
-- columns: e
-- row: 3
-- end-expected
SELECT e FROM d12_ext;

-- On an empty table there are no rows to contradict it.
CREATE TABLE d12_exu (id int PRIMARY KEY);
ALTER TABLE d12_exu ADD COLUMN c int NOT NULL DEFAULT NULL;

-- A retype may not break what is declared over the column.
CREATE TABLE d12_exw (id int PRIMARY KEY, a text);
INSERT INTO d12_exw VALUES (1,'1'),(2,'01');
CREATE UNIQUE INDEX d12_exw_uidx ON d12_exw (a);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: could not create unique index "d12_exw_uidx"
-- end-expected-error
ALTER TABLE d12_exw ALTER COLUMN a TYPE int USING a::int;

-- The statement is refused whole, so the old values are still there.
-- begin-expected
-- columns: a
-- row: 1
-- row: 01
-- end-expected
SELECT a FROM d12_exw ORDER BY id;

CREATE TABLE d12_exx (id int PRIMARY KEY, a text NOT NULL);
INSERT INTO d12_exx VALUES (1,'1');

-- begin-expected-error
-- sqlstate: 23502
-- message-like: column "a" of relation "d12_exx" contains null values
-- end-expected-error
ALTER TABLE d12_exx ALTER COLUMN a TYPE int USING NULL;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT a FROM d12_exx;

-- A conversion that keeps the values distinct is accepted.
CREATE TABLE d12_exy (id int PRIMARY KEY, a text);
INSERT INTO d12_exy VALUES (1,'1'),(2,'2');
CREATE UNIQUE INDEX d12_exy_uidx ON d12_exy (a);
ALTER TABLE d12_exy ALTER COLUMN a TYPE int USING a::int;

-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- end-expected
SELECT a FROM d12_exy ORDER BY id;

-- RESTART is an identity action; a serial column's sequence is a default.
CREATE TABLE d12_idt (id int GENERATED BY DEFAULT AS IDENTITY, w int, s serial);

-- begin-expected-error
-- sqlstate: 55000
-- message-like: column "w" of relation "d12_idt" is not an identity column
-- end-expected-error
ALTER TABLE d12_idt ALTER COLUMN w RESTART;

-- begin-expected-error
-- sqlstate: 55000
-- message-like: column "s" of relation "d12_idt" is not an identity column
-- end-expected-error
ALTER TABLE d12_idt ALTER COLUMN s RESTART;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" of relation "d12_idt" does not exist
-- end-expected-error
ALTER TABLE d12_idt ALTER COLUMN nosuch RESTART;

ALTER TABLE d12_idt ALTER COLUMN id RESTART;
ALTER TABLE d12_idt ALTER COLUMN id RESTART WITH 50;

-- ============================================================================
-- SECTION H: a definition that contradicts itself is refused for what it says
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: multiple primary keys for table "d12_pktwo" are not allowed
-- end-expected-error
CREATE TABLE d12_pktwo (a int PRIMARY KEY, b int PRIMARY KEY);

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: multiple primary keys for table "d12_pktwo2" are not allowed
-- end-expected-error
CREATE TABLE d12_pktwo2 (a int PRIMARY KEY, b int, PRIMARY KEY (b));

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: multiple primary keys for table "d12_pktwo3" are not allowed
-- end-expected-error
CREATE TABLE d12_pktwo3 (a int, b int, PRIMARY KEY (a), PRIMARY KEY (b));

-- One key over several columns is one key.
CREATE TABLE d12_pkone (a int, b int, PRIMARY KEY (a, b));
CREATE TABLE d12_pkone2 (a int PRIMARY KEY, b int UNIQUE);
CREATE TABLE d12_pkone3 (a int PRIMARY KEY, b int);

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: multiple primary keys for table "d12_pkone3" are not allowed
-- end-expected-error
ALTER TABLE d12_pkone3 ADD PRIMARY KEY (b);

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: multiple primary keys for table "d12_pkone3" are not allowed
-- end-expected-error
ALTER TABLE d12_pkone3 ADD CONSTRAINT d12_pkone3_k PRIMARY KEY (b);

-- NOT VALID written in a CREATE TABLE has nothing to defer, so the word is
-- spare rather than a syntax error.
CREATE TABLE d12_nva (id int PRIMARY KEY, a int, CONSTRAINT d12_nva_c CHECK (a > 0) NOT VALID);
CREATE TABLE d12_nvb (id int PRIMARY KEY, a int, CONSTRAINT d12_nvb_n NOT NULL a NOT VALID);

-- A key is enforced by an index built at once, so there is nothing to defer.
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: UNIQUE constraints cannot be marked NOT VALID
-- end-expected-error
CREATE TABLE d12_nvc (id int PRIMARY KEY, a int, CONSTRAINT d12_nvc_u UNIQUE (a) NOT VALID);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: PRIMARY KEY constraints cannot be marked NOT VALID
-- end-expected-error
CREATE TABLE d12_nvd (id int, a int, CONSTRAINT d12_nvd_p PRIMARY KEY (a) NOT VALID);

CREATE TABLE d12_nve (id int PRIMARY KEY, a int);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: UNIQUE constraints cannot be marked NOT VALID
-- end-expected-error
ALTER TABLE d12_nve ADD CONSTRAINT d12_nve_u UNIQUE (a) NOT VALID;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: PRIMARY KEY constraints cannot be marked NOT VALID
-- end-expected-error
ALTER TABLE d12_nve ADD CONSTRAINT d12_nve_p PRIMARY KEY (a) NOT VALID;

ALTER TABLE d12_nve ADD CONSTRAINT d12_nve_c CHECK (a > 0) NOT VALID;

-- ADD ATTRIBUTE has no IF NOT EXISTS; DROP ATTRIBUTE has IF EXISTS.
CREATE TYPE d12_att AS (a int, b text);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "NOT"
-- end-expected-error
ALTER TYPE d12_att ADD ATTRIBUTE IF NOT EXISTS c int;

ALTER TYPE d12_att ADD ATTRIBUTE c int;
ALTER TYPE d12_att DROP ATTRIBUTE IF EXISTS c;
ALTER TYPE d12_att DROP ATTRIBUTE IF EXISTS nosuch;

-- ============================================================================
-- SECTION I: a partition attaches to a partitioned parent, over a free slot
-- ============================================================================

CREATE TABLE d12_poplain (i int);

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: "d12_poplain" is not partitioned
-- end-expected-error
CREATE TABLE d12_pox PARTITION OF d12_poplain FOR VALUES FROM (1) TO (2);

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: "d12_poplain" is not partitioned
-- end-expected-error
CREATE TABLE d12_poy PARTITION OF d12_poplain DEFAULT;

CREATE TABLE d12_pop (i int) PARTITION BY RANGE (i);
CREATE TABLE d12_pop1 PARTITION OF d12_pop FOR VALUES FROM (1) TO (2);

-- A hash modulus only has to divide, or be divided by, the ones already there.
CREATE TABLE d12_hpt (i int) PARTITION BY HASH (i);
CREATE TABLE d12_hp1 PARTITION OF d12_hpt FOR VALUES WITH (MODULUS 4, REMAINDER 1);

-- 2 divides 4, and remainder 0 modulo 2 never meets remainder 1 modulo 4.
CREATE TABLE d12_hp2 PARTITION OF d12_hpt FOR VALUES WITH (MODULUS 2, REMAINDER 0);

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: partition "d12_hp3" would overlap partition
-- end-expected-error
CREATE TABLE d12_hp3 PARTITION OF d12_hpt FOR VALUES WITH (MODULUS 2, REMAINDER 1);

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: every hash partition modulus must be a factor of the next larger modulus
-- end-expected-error
CREATE TABLE d12_hp4 PARTITION OF d12_hpt FOR VALUES WITH (MODULUS 3, REMAINDER 0);

CREATE TABLE d12_hp5 PARTITION OF d12_hpt FOR VALUES WITH (MODULUS 8, REMAINDER 3);
CREATE TABLE d12_hp6 PARTITION OF d12_hpt FOR VALUES WITH (MODULUS 8, REMAINDER 7);

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: remainder for hash partition must be less than modulus
-- end-expected-error
CREATE TABLE d12_hp7 PARTITION OF d12_hpt FOR VALUES WITH (MODULUS 4, REMAINDER 4);

-- ============================================================================
-- SECTION J: LIKE copies the columns; the rest travels when it is asked for
-- ============================================================================

CREATE TABLE d12_lksrc (id int PRIMARY KEY, a int UNIQUE);
CREATE TABLE d12_lkplain (LIKE d12_lksrc);
INSERT INTO d12_lkplain VALUES (1, 1);
INSERT INTO d12_lkplain VALUES (1, 1);

-- Only the NOT NULL the key implied travels with the column.
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_constraint WHERE conrelid = 'd12_lkplain'::regclass;

CREATE TABLE d12_lkcons (LIKE d12_lksrc INCLUDING CONSTRAINTS);
INSERT INTO d12_lkcons VALUES (1, 1);
INSERT INTO d12_lkcons VALUES (1, 1);

-- INCLUDING INDEXES is what brings the key, under the new table's own name.
CREATE TABLE d12_lkidx (LIKE d12_lksrc INCLUDING INDEXES);
INSERT INTO d12_lkidx VALUES (1, 1);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "d12_lkidx_pkey"
-- end-expected-error
INSERT INTO d12_lkidx VALUES (1, 1);

CREATE TABLE d12_lkall (LIKE d12_lksrc INCLUDING ALL);
INSERT INTO d12_lkall VALUES (1, 1);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "d12_lkall_pkey"
-- end-expected-error
INSERT INTO d12_lkall VALUES (1, 1);

-- ============================================================================
-- SECTION K: a materialized view is a relation others depend on
-- ============================================================================

CREATE TABLE d12_mvb (id int PRIMARY KEY);
INSERT INTO d12_mvb VALUES (1);
CREATE MATERIALIZED VIEW d12_mvm AS SELECT id FROM d12_mvb;
CREATE VIEW d12_mvv AS SELECT id FROM d12_mvm;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop materialized view d12_mvm because other objects depend on it
-- end-expected-error
DROP MATERIALIZED VIEW d12_mvm;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table d12_mvb because other objects depend on it
-- end-expected-error
DROP TABLE d12_mvb;

DROP TABLE d12_mvb CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM information_schema.views WHERE table_name = 'd12_mvv';

CREATE TABLE d12_mvb2 (id int PRIMARY KEY);
CREATE MATERIALIZED VIEW d12_mvm2 AS SELECT id FROM d12_mvb2;
CREATE VIEW d12_mvv2 AS SELECT id FROM d12_mvm2;
DROP MATERIALIZED VIEW d12_mvm2 CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM information_schema.views WHERE table_name = 'd12_mvv2';

-- Nothing reading it means nothing blocking it.
CREATE MATERIALIZED VIEW d12_mvm3 AS SELECT id FROM d12_mvb2;
DROP MATERIALIZED VIEW d12_mvm3;

-- A blocked drop names the relation the way it can be reached.
CREATE SCHEMA d12_dqs;
CREATE TABLE d12_dqs.shared (id int PRIMARY KEY);
CREATE VIEW d12_dqs.v AS SELECT id FROM d12_dqs.shared;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table d12_dqs.shared because other objects depend on it
-- end-expected-error
DROP TABLE d12_dqs.shared;

CREATE TABLE d12_dqpub (id int PRIMARY KEY);
CREATE VIEW d12_dqpubv AS SELECT id FROM d12_dqpub;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table d12_dqpub because other objects depend on it
-- end-expected-error
DROP TABLE d12_dqpub;

-- ============================================================================
-- SECTION L: moving an object needs somewhere to move it to
-- ============================================================================

CREATE SEQUENCE d12_ssseq;
CREATE TYPE d12_sstype AS (a int);
CREATE TABLE d12_sstab (a int);

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "d12_ssnosuch" does not exist
-- end-expected-error
ALTER SEQUENCE d12_ssseq SET SCHEMA d12_ssnosuch;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "d12_ssnosuch" does not exist
-- end-expected-error
ALTER TYPE d12_sstype SET SCHEMA d12_ssnosuch;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "d12_ssnosuch" does not exist
-- end-expected-error
ALTER TABLE d12_sstab SET SCHEMA d12_ssnosuch;

CREATE SCHEMA d12_ssok;
ALTER SEQUENCE d12_ssseq SET SCHEMA d12_ssok;
ALTER TYPE d12_sstype SET SCHEMA d12_ssok;
ALTER TABLE d12_sstab SET SCHEMA d12_ssok;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM information_schema.tables
  WHERE table_schema = 'd12_ssok' AND table_name = 'd12_sstab';

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP TABLE IF EXISTS d12_pkq1, d12_pkq2, d12_pkq3 CASCADE;
DROP TABLE IF EXISTS d12_pke1, d12_pke2, d12_pke3, d12_pke4, d12_pke5, d12_pke6, d12_pke7 CASCADE;
DROP FUNCTION IF EXISTS d12_sqlstable(int);
DROP FUNCTION IF EXISTS d12_sqlvol(int);
DROP FUNCTION IF EXISTS d12_sqlimm(int);
DROP FUNCTION IF EXISTS d12_plstable(int);
DROP TABLE IF EXISTS d12_nna, d12_nnb, d12_nnc, d12_nnd, d12_nne, d12_nnf, d12_nng CASCADE;
DROP TABLE IF EXISTS d12_dmat, d12_dmbt, d12_host CASCADE;
DROP DOMAIN IF EXISTS d12_dma, d12_dmb, d12_dmc2 CASCADE;
DROP TYPE IF EXISTS d12_enum CASCADE;
DROP MATERIALIZED VIEW IF EXISTS d12_rfm, d12_crm CASCADE;
DROP VIEW IF EXISTS d12_rfv, d12_crv CASCADE;
DROP SEQUENCE IF EXISTS d12_rfs, d12_crs CASCADE;
DROP TABLE IF EXISTS d12_rft, d12_crt, d12_ait, d12_art CASCADE;
DROP TABLE IF EXISTS d12_ixt, d12_imt CASCADE;
DROP TABLE IF EXISTS d12_iqs.t CASCADE;
DROP VIEW IF EXISTS d12_nsq.thing CASCADE;
DROP TABLE IF EXISTS d12_nsq.other CASCADE;
DROP VIEW IF EXISTS d12_dqs.v CASCADE;
DROP TABLE IF EXISTS d12_dqs.shared CASCADE;
DROP TABLE IF EXISTS d12_ssok.d12_sstab CASCADE;
DROP SEQUENCE IF EXISTS d12_ssok.d12_ssseq CASCADE;
DROP SCHEMA IF EXISTS d12_iqs CASCADE;
DROP SCHEMA IF EXISTS d12_nsq CASCADE;
DROP SCHEMA IF EXISTS d12_dqs CASCADE;
DROP SCHEMA IF EXISTS d12_ssok CASCADE;
DROP TABLE IF EXISTS d12_mat, d12_mau, d12_mav, d12_maw CASCADE;
DROP TABLE IF EXISTS d12_ext, d12_exu, d12_exw, d12_exx, d12_exy, d12_idt CASCADE;
DROP TABLE IF EXISTS d12_pkone, d12_pkone2, d12_pkone3 CASCADE;
DROP TABLE IF EXISTS d12_nva, d12_nvb, d12_nve CASCADE;
DROP TYPE IF EXISTS d12_att CASCADE;
DROP TABLE IF EXISTS d12_poplain, d12_pop, d12_hpt CASCADE;
DROP TABLE IF EXISTS d12_lkplain, d12_lkcons, d12_lkidx, d12_lkall, d12_lksrc CASCADE;
DROP TABLE IF EXISTS d12_mvb2 CASCADE;
DROP TABLE IF EXISTS d12_dqpub CASCADE;
DROP VIEW IF EXISTS d12_dqpubv CASCADE;
DROP SEQUENCE IF EXISTS d12_ssseq CASCADE;
DROP TYPE IF EXISTS d12_sstype CASCADE;
DROP TABLE IF EXISTS d12_sstab CASCADE;
