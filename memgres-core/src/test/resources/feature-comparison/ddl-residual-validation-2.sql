-- The checks PostgreSQL 18 makes while a definition is being written, and the PG 18 spellings a
-- migration may use. Each refusal here is paired with the ordinary shapes around it, because a
-- rule that fires on a definition PostgreSQL accepts costs more than the permissiveness it removes.

-- ---------------------------------------------------------------------------------------------
-- A retype rewrites every stored value, so what was already declared over the column has to hold
-- over the new values: a unique key may not end up with two rows on one value, a CHECK may not end
-- up with a row that fails it, and the default has to survive the cast.

DROP TABLE IF EXISTS drv_pk CASCADE;
CREATE TABLE drv_pk (a numeric PRIMARY KEY);
INSERT INTO drv_pk VALUES (1.0),(1.4);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: could not create unique index
-- end-expected-error
ALTER TABLE drv_pk ALTER COLUMN a TYPE int;

-- The refused ALTER left the column and its rows exactly as they were.
-- begin-expected
-- columns: a
-- row: 1.0
-- row: 1.4
-- end-expected
SELECT a FROM drv_pk ORDER BY 1;

-- The same retype over values that stay distinct is accepted and rewrites them.
DROP TABLE IF EXISTS drv_pk_ok CASCADE;
CREATE TABLE drv_pk_ok (a numeric PRIMARY KEY);
INSERT INTO drv_pk_ok VALUES (1.0),(2.4);
ALTER TABLE drv_pk_ok ALTER COLUMN a TYPE int;

-- begin-expected
-- columns: a
-- row: 1
-- row: 2
-- end-expected
SELECT a FROM drv_pk_ok ORDER BY 1;

-- A UNIQUE constraint blocks the same collision a primary key does.
DROP TABLE IF EXISTS drv_uq CASCADE;
CREATE TABLE drv_uq (a numeric UNIQUE);
INSERT INTO drv_uq VALUES (1.0),(1.4);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: could not create unique index
-- end-expected-error
ALTER TABLE drv_uq ALTER COLUMN a TYPE int;

-- A CHECK constraint is re-run over the rewritten values.
DROP TABLE IF EXISTS drv_ck CASCADE;
CREATE TABLE drv_ck (a numeric, CONSTRAINT drv_ckc CHECK (a < 0.9));
INSERT INTO drv_ck VALUES (0.6);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: check constraint "drv_ckc" of relation "drv_ck" is violated by some row
-- end-expected-error
ALTER TABLE drv_ck ALTER COLUMN a TYPE int;

-- begin-expected
-- columns: a
-- row: 0.6
-- end-expected
SELECT a FROM drv_ck ORDER BY 1;

-- A CHECK the rewritten values still satisfy is no obstacle.
DROP TABLE IF EXISTS drv_ck_ok CASCADE;
CREATE TABLE drv_ck_ok (a numeric CHECK (a >= 0));
INSERT INTO drv_ck_ok VALUES (1.2),(3.7);
ALTER TABLE drv_ck_ok ALTER COLUMN a TYPE int;

-- begin-expected
-- columns: a
-- row: 1
-- row: 4
-- end-expected
SELECT a FROM drv_ck_ok ORDER BY 1;

-- A constraint added NOT VALID was never checked against the rows, and a retype does not start.
DROP TABLE IF EXISTS drv_nv CASCADE;
CREATE TABLE drv_nv (a numeric);
INSERT INTO drv_nv VALUES (0.6);
ALTER TABLE drv_nv ADD CONSTRAINT drv_nvc CHECK (a < 0.9) NOT VALID;
ALTER TABLE drv_nv ALTER COLUMN a TYPE int;

-- A default that cannot be cast to the new type refuses the whole ALTER, so the column never ends
-- up advertising a default the next INSERT would choke on.
DROP TABLE IF EXISTS drv_df CASCADE;
CREATE TABLE drv_df (a text DEFAULT 'abc');

-- begin-expected-error
-- sqlstate: 42804
-- message-like: default for column "a" cannot be cast automatically to type integer
-- end-expected-error
ALTER TABLE drv_df ALTER COLUMN a TYPE int USING 0;

-- A USING clause does not excuse the default either.
DROP TABLE IF EXISTS drv_df2 CASCADE;
CREATE TABLE drv_df2 (a text DEFAULT '7');

-- begin-expected-error
-- sqlstate: 42804
-- message-like: default for column "a" cannot be cast automatically to type integer
-- end-expected-error
ALTER TABLE drv_df2 ALTER COLUMN a TYPE int USING a::int;

-- Defaults that do cast are untouched: within a type category, or to a string type.
DROP TABLE IF EXISTS drv_df_ok CASCADE;
CREATE TABLE drv_df_ok (a int DEFAULT 3, b text DEFAULT 'x', c numeric DEFAULT 1.5,
                        d timestamp DEFAULT now());
ALTER TABLE drv_df_ok ALTER COLUMN a TYPE bigint;
ALTER TABLE drv_df_ok ALTER COLUMN b TYPE varchar(5);
ALTER TABLE drv_df_ok ALTER COLUMN c TYPE int;
ALTER TABLE drv_df_ok ALTER COLUMN d TYPE date;

-- A serial column keeps its nextval default across a widening.
DROP TABLE IF EXISTS drv_ser CASCADE;
CREATE TABLE drv_ser (id serial PRIMARY KEY, v int);
ALTER TABLE drv_ser ALTER COLUMN id TYPE bigint;

-- A column with no default at all retypes under an explicit USING.
DROP TABLE IF EXISTS drv_nodf CASCADE;
CREATE TABLE drv_nodf (a text);
ALTER TABLE drv_nodf ALTER COLUMN a TYPE int USING 0;

-- ---------------------------------------------------------------------------------------------
-- PG 18 lets VACUUM and ANALYZE say ONLY before the relation, meaning the relation itself and not
-- its inheritance children. Read as a relation name it named one that was never there.

DROP TABLE IF EXISTS drv_va CASCADE;
CREATE TABLE drv_va (id int PRIMARY KEY, v int);
VACUUM ONLY drv_va;
ANALYZE ONLY drv_va;
VACUUM ANALYZE ONLY drv_va;
VACUUM VERBOSE ONLY drv_va;
ANALYZE ONLY drv_va (v);
VACUUM ANALYZE ONLY drv_va (v);

-- Without ONLY they behave as they always did, and a relation that is not there is still reported.
VACUUM drv_va;
ANALYZE drv_va;
VACUUM;
ANALYZE;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "drv_nosuch" does not exist
-- end-expected-error
VACUUM ONLY drv_nosuch;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "drv_nosuch" does not exist
-- end-expected-error
ANALYZE ONLY drv_nosuch;

-- ONLY is a reserved word, so it can never be the relation it was meant to qualify.
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
VACUUM ONLY;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at end of input
-- end-expected-error
ANALYZE ONLY;

-- Quoted, it is an ordinary name again.
DROP TABLE IF EXISTS "only" CASCADE;
CREATE TABLE "only" (i int PRIMARY KEY);
VACUUM "only";
ANALYZE "only";
VACUUM ONLY "only";
DROP TABLE "only";

-- ---------------------------------------------------------------------------------------------
-- A temporal key is unique per period rather than outright. The PRIMARY KEY spelling was already
-- read; the UNIQUE spelling is the same key and has to be read the same way. The scalar part is
-- compared by a GiST index, which needs btree_gist to know how to compare an integer.

CREATE EXTENSION IF NOT EXISTS btree_gist;
DROP TABLE IF EXISTS drv_wo CASCADE;
CREATE TABLE drv_wo (id int, valid_at daterange,
                     CONSTRAINT drv_woc UNIQUE (id, valid_at WITHOUT OVERLAPS));
INSERT INTO drv_wo VALUES (1,'[2020-01-01,2021-01-01)');

-- begin-expected-error
-- sqlstate: 23P01
-- message-like: conflicting key value violates exclusion constraint "drv_woc"
-- end-expected-error
INSERT INTO drv_wo VALUES (1,'[2020-06-01,2022-01-01)');

-- Periods that merely touch do not overlap, and a different key never conflicts.
INSERT INTO drv_wo VALUES (1,'[2021-01-01,2022-01-01)');
INSERT INTO drv_wo VALUES (2,'[2020-06-01,2022-01-01)');

-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT count(*) AS n FROM drv_wo;

-- The unnamed form reads the same; only the name the two engines pick for the key differs.
DROP TABLE IF EXISTS drv_wo2 CASCADE;
CREATE TABLE drv_wo2 (id int, valid_at daterange, UNIQUE (id, valid_at WITHOUT OVERLAPS));
INSERT INTO drv_wo2 VALUES (1,'[2020-01-01,2021-01-01)');
INSERT INTO drv_wo2 VALUES (1,'[2021-01-01,2022-01-01)');

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n FROM drv_wo2;

-- An ordinary UNIQUE key over the same two columns is untouched by the new spelling.
DROP TABLE IF EXISTS drv_wo3 CASCADE;
CREATE TABLE drv_wo3 (id int, valid_at daterange, UNIQUE (id, valid_at));
INSERT INTO drv_wo3 VALUES (1,'[2020-01-01,2021-01-01)');
INSERT INTO drv_wo3 VALUES (1,'[2020-06-01,2022-01-01)');

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n FROM drv_wo3;

-- ---------------------------------------------------------------------------------------------
-- Two PG 18 catalog functions. A large object that is not there, and an object with no ACL of its
-- own, are both unknown rather than errors.

-- begin-expected
-- columns: v
-- row: NULL
-- end-expected
SELECT has_largeobject_privilege(1, 'SELECT') AS v;

-- begin-expected
-- columns: v
-- row: NULL
-- end-expected
SELECT has_largeobject_privilege(1, 'UPDATE') AS v;

-- begin-expected
-- columns: v
-- row: NULL
-- end-expected
SELECT has_largeobject_privilege(current_user, 1, 'SELECT') AS v;

-- A privilege name that names no privilege is still a caller's mistake.
-- begin-expected-error
-- sqlstate: 22023
-- message-like: unrecognized privilege type: "BOGUS"
-- end-expected-error
SELECT has_largeobject_privilege(1, 'BOGUS');

-- begin-expected-error
-- sqlstate: 42704
-- message-like: role "drv_norole" does not exist
-- end-expected-error
SELECT has_largeobject_privilege('drv_norole', 1, 'SELECT');

-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT pg_get_acl('pg_class'::regclass, 1, 0) IS NULL AS v;

-- ---------------------------------------------------------------------------------------------
-- A partitioned parent holds no rows of its own, so there is nothing for UNLOGGED to mean.

DROP TABLE IF EXISTS drv_ul CASCADE;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: partitioned tables cannot be unlogged
-- end-expected-error
CREATE UNLOGGED TABLE drv_ul (i int) PARTITION BY RANGE (i);

-- An unlogged table that is not partitioned, and a partitioned table that is not unlogged, are
-- both ordinary; so is an unlogged partition of a logged parent.
DROP TABLE IF EXISTS drv_ul2 CASCADE;
CREATE UNLOGGED TABLE drv_ul2 (i int PRIMARY KEY);
DROP TABLE IF EXISTS drv_lp CASCADE;
CREATE TABLE drv_lp (i int) PARTITION BY RANGE (i);
CREATE UNLOGGED TABLE drv_lp1 PARTITION OF drv_lp FOR VALUES FROM (1) TO (5);
DROP TABLE drv_lp CASCADE;
DROP TABLE drv_ul2;

-- ---------------------------------------------------------------------------------------------
-- A foreign key pointing at a table is what blocks its TRUNCATE, whether or not the referencing
-- table currently holds a row. A migration that works against an empty database has to work here.

DROP TABLE IF EXISTS drv_fk_child CASCADE;
DROP TABLE IF EXISTS drv_fk_parent CASCADE;
CREATE TABLE drv_fk_parent (i int PRIMARY KEY);
CREATE TABLE drv_fk_child (j int PRIMARY KEY, i int REFERENCES drv_fk_parent(i));

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot truncate a table referenced in a foreign key constraint
-- end-expected-error
TRUNCATE drv_fk_parent;

INSERT INTO drv_fk_parent VALUES (1),(2);
INSERT INTO drv_fk_child VALUES (10,1),(20,2);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot truncate a table referenced in a foreign key constraint
-- end-expected-error
TRUNCATE drv_fk_parent;

-- Naming both together, or CASCADE, or the referencing table alone, all still work.
TRUNCATE drv_fk_parent, drv_fk_child;
TRUNCATE drv_fk_parent CASCADE;
TRUNCATE drv_fk_child;

-- A self-referencing table is the whole graph by itself.
DROP TABLE IF EXISTS drv_self CASCADE;
CREATE TABLE drv_self (i int PRIMARY KEY, p int REFERENCES drv_self(i));
TRUNCATE drv_self;

-- Dropping the constraint frees the parent again.
ALTER TABLE drv_fk_child DROP CONSTRAINT drv_fk_child_i_fkey;
TRUNCATE drv_fk_parent;

-- A partitioned parent truncates its whole tree.
DROP TABLE IF EXISTS drv_pp CASCADE;
CREATE TABLE drv_pp (i int PRIMARY KEY) PARTITION BY RANGE (i);
CREATE TABLE drv_pp1 PARTITION OF drv_pp FOR VALUES FROM (1) TO (10);
INSERT INTO drv_pp VALUES (1),(2);
TRUNCATE drv_pp;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM drv_pp;

-- ---------------------------------------------------------------------------------------------
-- A composite type owns a relation of its own name, so a rename onto a name another relation holds
-- is reported as the relation collision it is. Names held by an enum or a domain, which own no
-- relation, are still a type collision.

DROP TYPE IF EXISTS drv_ct1 CASCADE;
DROP TYPE IF EXISTS drv_ct2 CASCADE;
CREATE TYPE drv_ct1 AS (a int);
CREATE TYPE drv_ct2 AS (a int);

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "drv_ct2" already exists
-- end-expected-error
ALTER TYPE drv_ct1 RENAME TO drv_ct2;

DROP TABLE IF EXISTS drv_tbl CASCADE;
CREATE TABLE drv_tbl (x int PRIMARY KEY);

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "drv_tbl" already exists
-- end-expected-error
ALTER TYPE drv_ct1 RENAME TO drv_tbl;

DROP VIEW IF EXISTS drv_vw CASCADE;
CREATE VIEW drv_vw AS SELECT x FROM drv_tbl;

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "drv_vw" already exists
-- end-expected-error
ALTER TYPE drv_ct1 RENAME TO drv_vw;

DROP SEQUENCE IF EXISTS drv_sq CASCADE;
CREATE SEQUENCE drv_sq;

-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "drv_sq" already exists
-- end-expected-error
ALTER TYPE drv_ct1 RENAME TO drv_sq;

DROP TYPE IF EXISTS drv_en CASCADE;
CREATE TYPE drv_en AS ENUM ('x');

-- begin-expected-error
-- sqlstate: 42710
-- message-like: type "drv_en" already exists
-- end-expected-error
ALTER TYPE drv_ct1 RENAME TO drv_en;

DROP DOMAIN IF EXISTS drv_dm CASCADE;
CREATE DOMAIN drv_dm AS int;

-- begin-expected-error
-- sqlstate: 42710
-- message-like: type "drv_dm" already exists
-- end-expected-error
ALTER TYPE drv_ct1 RENAME TO drv_dm;

-- An enum's own rename onto a taken name is a type collision either way.
-- begin-expected-error
-- sqlstate: 42710
-- message-like: type "drv_dm" already exists
-- end-expected-error
ALTER TYPE drv_en RENAME TO drv_dm;

-- And a rename onto a free name works.
ALTER TYPE drv_ct1 RENAME TO drv_ct3;

-- begin-expected
-- columns: typname
-- row: drv_ct2
-- row: drv_ct3
-- end-expected
SELECT typname FROM pg_type WHERE typname IN ('drv_ct1','drv_ct2','drv_ct3') ORDER BY typname;

-- ---------------------------------------------------------------------------------------------
-- ALTER VIEW and ALTER SEQUENCE naming a relation of another kind: the name resolves and the kind
-- is wrong, which is a different complaint from the object not being there at all.

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "drv_tbl" is not a view
-- end-expected-error
ALTER VIEW drv_tbl RENAME TO drv_tbl2;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "drv_tbl" is not a sequence
-- end-expected-error
ALTER SEQUENCE drv_tbl RENAME TO drv_tbl2;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "drv_vw" is not a sequence
-- end-expected-error
ALTER SEQUENCE drv_vw RENAME TO drv_vw2;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "drv_sq" is not a view
-- end-expected-error
ALTER VIEW drv_sq RENAME TO drv_sq2;

-- A name that is nothing at all is still reported as missing.
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "drv_nosuchrel" does not exist
-- end-expected-error
ALTER VIEW drv_nosuchrel RENAME TO drv_x;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "drv_nosuchrel" does not exist
-- end-expected-error
ALTER SEQUENCE drv_nosuchrel RENAME TO drv_x;

-- Each kind still renames under its own word, and ALTER TABLE renames a view as it always did.
ALTER VIEW drv_vw RENAME TO drv_vw2;
ALTER SEQUENCE drv_sq RENAME TO drv_sq2;
ALTER TABLE drv_vw2 RENAME TO drv_vw3;
ALTER SEQUENCE drv_sq2 RESTART WITH 5;

-- begin-expected
-- columns: v
-- row: 5
-- end-expected
SELECT nextval('drv_sq2') AS v;

-- ALTER INDEX's rename is the generic relation rename and never looks at the kind, so a table
-- named here is renamed rather than reported missing.
DROP TABLE IF EXISTS drv_ai CASCADE;
DROP TABLE IF EXISTS drv_ai2 CASCADE;
CREATE TABLE drv_ai (a int PRIMARY KEY);
ALTER INDEX drv_ai RENAME TO drv_ai2;

-- begin-expected
-- columns: relname
-- row: drv_ai2
-- end-expected
SELECT relname FROM pg_class WHERE relname IN ('drv_ai','drv_ai2') AND relkind = 'r' ORDER BY relname;

-- An index of its own is renamed the same way, and a name that is nothing is still missing.
DROP TABLE IF EXISTS drv_ait CASCADE;
CREATE TABLE drv_ait (a int PRIMARY KEY);
CREATE INDEX drv_aix ON drv_ait (a);
ALTER INDEX drv_aix RENAME TO drv_aix2;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "drv_nosuchix" does not exist
-- end-expected-error
ALTER INDEX drv_nosuchix RENAME TO drv_z;

-- ---------------------------------------------------------------------------------------------
-- An ALTER that names a publication or a statistics object nobody created is a mistake worth
-- reporting, and a rename onto a taken name must not silently destroy what holds it.

DROP PUBLICATION IF EXISTS drv_pub1;
DROP PUBLICATION IF EXISTS drv_pub2;
CREATE PUBLICATION drv_pub1 FOR TABLE drv_tbl;
ALTER PUBLICATION drv_pub1 RENAME TO drv_pub2;

-- begin-expected
-- columns: pubname
-- row: drv_pub2
-- end-expected
SELECT pubname FROM pg_publication WHERE pubname LIKE 'drv_pub%' ORDER BY pubname;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: publication "drv_nosuchpub" does not exist
-- end-expected-error
ALTER PUBLICATION drv_nosuchpub RENAME TO drv_x;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: publication "drv_nosuchpub" does not exist
-- end-expected-error
ALTER PUBLICATION drv_nosuchpub ADD TABLE drv_tbl;

CREATE PUBLICATION drv_pub3 FOR TABLE drv_tbl;

-- begin-expected-error
-- sqlstate: 42710
-- message-like: publication "drv_pub2" already exists
-- end-expected-error
ALTER PUBLICATION drv_pub3 RENAME TO drv_pub2;

-- Both survive the refused rename.
-- begin-expected
-- columns: pubname
-- row: drv_pub2
-- row: drv_pub3
-- end-expected
SELECT pubname FROM pg_publication WHERE pubname LIKE 'drv_pub%' ORDER BY pubname;

-- The ordinary alterations are unaffected.
ALTER PUBLICATION drv_pub3 ADD TABLE drv_ait;
ALTER PUBLICATION drv_pub3 DROP TABLE drv_ait;
ALTER PUBLICATION drv_pub3 OWNER TO CURRENT_USER;
DROP PUBLICATION drv_pub3;
DROP PUBLICATION drv_pub2;

DROP TABLE IF EXISTS drv_st CASCADE;
CREATE TABLE drv_st (a int PRIMARY KEY, b int);
CREATE STATISTICS drv_s1 ON a, b FROM drv_st;
CREATE STATISTICS drv_s2 ON a, b FROM drv_st;

-- begin-expected-error
-- sqlstate: 42710
-- message-like: statistics object "drv_s2" already exists in schema "public"
-- end-expected-error
ALTER STATISTICS drv_s1 RENAME TO drv_s2;

-- begin-expected
-- columns: stxname
-- row: drv_s1
-- row: drv_s2
-- end-expected
SELECT stxname FROM pg_statistic_ext WHERE stxname LIKE 'drv_s%' ORDER BY stxname;

ALTER STATISTICS drv_s1 RENAME TO drv_s3;
ALTER STATISTICS drv_s3 SET STATISTICS 100;

-- begin-expected
-- columns: stxname
-- row: drv_s2
-- row: drv_s3
-- end-expected
SELECT stxname FROM pg_statistic_ext WHERE stxname LIKE 'drv_s%' ORDER BY stxname;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: statistics object "drv_nosuchstat" does not exist
-- end-expected-error
ALTER STATISTICS drv_nosuchstat RENAME TO drv_y;

-- ---------------------------------------------------------------------------------------------
-- ALTER OPERATOR names the operator the way a caller would have to write it, operand types and
-- all, because the same symbol over other types may well exist.

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: integer === integer
-- end-expected-error
ALTER OPERATOR ===(int, int) OWNER TO CURRENT_USER;

-- A prefix operator has no left operand to name.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: === integer
-- end-expected-error
ALTER OPERATOR ===(NONE, int) OWNER TO CURRENT_USER;

-- ---------------------------------------------------------------------------------------------
-- date and timestamp have ends. Arithmetic that lands past the end of date is out of range rather
-- than a value no PostgreSQL could store, and the widest legal timestamps are legal input.

-- begin-expected
-- columns: v
-- row: 5874897-12-31
-- end-expected
SELECT DATE '5874897-12-31' AS v;

-- begin-expected-error
-- sqlstate: 22008
-- message-like: date out of range
-- end-expected-error
SELECT DATE '5874897-12-31' + 1;

-- begin-expected-error
-- sqlstate: 22008
-- message-like: date out of range
-- end-expected-error
SELECT DATE '2000-01-01' + 2147483647;

-- begin-expected-error
-- sqlstate: 22008
-- message-like: date out of range
-- end-expected-error
SELECT DATE '2000-01-01' - 2147483647;

-- Ordinary date arithmetic is untouched.
-- begin-expected
-- columns: v
-- row: 2000-01-02
-- end-expected
SELECT DATE '2000-01-01' + 1 AS v;

-- begin-expected
-- columns: v
-- row: 1999-12-02
-- end-expected
SELECT DATE '2000-01-01' - 30 AS v;

-- begin-expected
-- columns: v
-- row: 5874897-12-30
-- end-expected
SELECT DATE '5874897-12-31' - 1 AS v;

-- begin-expected
-- columns: v
-- row: 31
-- end-expected
SELECT DATE '2000-02-01' - DATE '2000-01-01' AS v;

-- The last timestamp PostgreSQL can hold is valid input; past it, it is the range that fails.
-- begin-expected
-- columns: v
-- row: 294276-12-31 23:59:59
-- end-expected
SELECT TIMESTAMP '294276-12-31 23:59:59' AS v;

-- begin-expected
-- columns: v
-- row: 294276-12-31 23:59:59.999999
-- end-expected
SELECT TIMESTAMP '294276-12-31 23:59:59.999999' AS v;

-- begin-expected
-- columns: v
-- row: 294276-12-31 00:00:00
-- end-expected
SELECT TIMESTAMP '294276-12-31' AS v;

-- begin-expected-error
-- sqlstate: 22008
-- message-like: timestamp out of range
-- end-expected-error
SELECT TIMESTAMP '294277-01-01 00:00:00';

-- begin-expected
-- columns: v
-- row: 2024-03-04 05:06:07
-- end-expected
SELECT TIMESTAMP '2024-03-04 05:06:07' AS v;

-- begin-expected
-- columns: v
-- row: 2024-03-04 05:06:07.125
-- end-expected
SELECT TIMESTAMP '2024-03-04 05:06:07.125' AS v;

-- ---------------------------------------------------------------------------------------------
-- The text-search ranking and highlighting functions are strict. A rank of zero is a real answer
-- meaning "matched nothing", so a null document must not be reported as one.

-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT ts_rank(to_tsvector('cat dog'), NULL::tsquery) IS NULL AS v;

-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT ts_rank(NULL::tsvector, to_tsquery('cat')) IS NULL AS v;

-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT ts_rank_cd(to_tsvector('cat dog'), NULL::tsquery) IS NULL AS v;

-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT ts_rank_cd(NULL::tsvector, to_tsquery('cat')) IS NULL AS v;

-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT ts_headline('cat dog', NULL::tsquery) IS NULL AS v;

-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT ts_headline(NULL::text, to_tsquery('cat')) IS NULL AS v;

-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT ts_headline('cat dog', to_tsquery('cat'), NULL) IS NULL AS v;

-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT ts_rewrite(to_tsquery('cat'), NULL::tsquery, to_tsquery('dog')) IS NULL AS v;

-- With everything present they answer as they always did.
-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT ts_rank(to_tsvector('cat dog'), to_tsquery('cat')) > 0 AS v;

-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT ts_rank_cd(to_tsvector('cat dog'), to_tsquery('cat')) > 0 AS v;

-- begin-expected
-- columns: v
-- row: <b>cat</b> dog
-- end-expected
SELECT ts_headline('cat dog', to_tsquery('cat')) AS v;

-- begin-expected
-- columns: v
-- row: 'dog'
-- end-expected
SELECT ts_rewrite(to_tsquery('cat'), to_tsquery('cat'), to_tsquery('dog'))::text AS v;

-- ---------------------------------------------------------------------------------------------
-- Casting bytea to an integer reads the bytes as the integer's own big-endian representation.

-- begin-expected
-- columns: v
-- row: 256
-- end-expected
SELECT '\x00000100'::bytea::int AS v;

-- begin-expected
-- columns: v
-- row: 0
-- end-expected
SELECT '\x00000000'::bytea::int AS v;

-- begin-expected
-- columns: v
-- row: -1
-- end-expected
SELECT '\xffffffff'::bytea::int AS v;

-- ---------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS drv_pk CASCADE;
DROP TABLE IF EXISTS drv_pk_ok CASCADE;
DROP TABLE IF EXISTS drv_uq CASCADE;
DROP TABLE IF EXISTS drv_ck CASCADE;
DROP TABLE IF EXISTS drv_ck_ok CASCADE;
DROP TABLE IF EXISTS drv_nv CASCADE;
DROP TABLE IF EXISTS drv_df CASCADE;
DROP TABLE IF EXISTS drv_df2 CASCADE;
DROP TABLE IF EXISTS drv_df_ok CASCADE;
DROP TABLE IF EXISTS drv_ser CASCADE;
DROP TABLE IF EXISTS drv_nodf CASCADE;
DROP TABLE IF EXISTS drv_va CASCADE;
DROP TABLE IF EXISTS drv_wo CASCADE;
DROP TABLE IF EXISTS drv_wo2 CASCADE;
DROP TABLE IF EXISTS drv_wo3 CASCADE;
DROP TABLE IF EXISTS drv_fk_child CASCADE;
DROP TABLE IF EXISTS drv_fk_parent CASCADE;
DROP TABLE IF EXISTS drv_self CASCADE;
DROP TABLE IF EXISTS drv_pp CASCADE;
DROP TABLE IF EXISTS drv_ai2 CASCADE;
DROP TABLE IF EXISTS drv_ait CASCADE;
DROP TABLE IF EXISTS drv_st CASCADE;
DROP VIEW IF EXISTS drv_vw3 CASCADE;
DROP TABLE IF EXISTS drv_tbl CASCADE;
DROP SEQUENCE IF EXISTS drv_sq2 CASCADE;
DROP TYPE IF EXISTS drv_ct2 CASCADE;
DROP TYPE IF EXISTS drv_ct3 CASCADE;
DROP TYPE IF EXISTS drv_en CASCADE;
DROP DOMAIN IF EXISTS drv_dm CASCADE;
