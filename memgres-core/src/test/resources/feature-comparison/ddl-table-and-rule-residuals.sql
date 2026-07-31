-- ============================================================================
-- Feature Comparison: CREATE TABLE ... LIKE, index definition options, and
-- rule application
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Three groups of residual differences:
--   * LIKE copied a column whole, so the source's defaults, identity and
--     generation expression travelled with it, and a copied NOT NULL was
--     renamed after the new table instead of keeping the source's name.
--   * An index definition's collation, operator class and storage parameters
--     were never checked, and neither were a table's.
--   * A DO ALSO rule on UPDATE or DELETE never fired, and a rule with several
--     actions ran none of them and was written back wrongly by pg_rules.
-- Every table here has a primary key: the corpus leaves a FOR ALL TABLES
-- publication behind, under which PostgreSQL refuses UPDATE and DELETE on a
-- table with no replica identity.
-- ============================================================================

DROP TABLE IF EXISTS btr_lk2 CASCADE;
DROP TABLE IF EXISTS btr_lk CASCADE;
DROP TABLE IF EXISTS btr_gi2 CASCADE;
DROP TABLE IF EXISTS btr_gi CASCADE;
DROP TABLE IF EXISTS btr_gg2 CASCADE;
DROP TABLE IF EXISTS btr_gg CASCADE;
DROP TABLE IF EXISTS btr_sr2 CASCADE;
DROP TABLE IF EXISTS btr_sr CASCADE;
DROP TABLE IF EXISTS btr_cm2 CASCADE;
DROP TABLE IF EXISTS btr_cm CASCADE;
DROP TABLE IF EXISTS btr_up CASCADE;
DROP TABLE IF EXISTS btr_ix CASCADE;
DROP TABLE IF EXISTS btr_rt CASCADE;
DROP TABLE IF EXISTS btr_rlog CASCADE;
DROP TABLE IF EXISTS btr_r2 CASCADE;
DROP TABLE IF EXISTS btr_rlog2 CASCADE;
DROP TABLE IF EXISTS btr_rc CASCADE;
DROP TABLE IF EXISTS btr_rclog CASCADE;

-- ============================================================================
-- 1. LIKE copies the shape of a column and nothing else unless asked
-- ============================================================================

CREATE TABLE btr_lk (
    i int PRIMARY KEY,
    j text DEFAULT 'q',
    k int DEFAULT 4,
    d date DEFAULT DATE '2020-01-01',
    n numeric(5,2) DEFAULT 1.5,
    b boolean DEFAULT true
);
CREATE TABLE btr_lk2 (LIKE btr_lk);

-- Not one of the defaults travelled, whatever its type.
-- begin-expected
-- columns: column_name | column_default
-- row: i, NULL
-- row: j, NULL
-- row: k, NULL
-- row: d, NULL
-- row: n, NULL
-- row: b, NULL
-- end-expected
SELECT column_name, column_default FROM information_schema.columns
 WHERE table_name = 'btr_lk2' ORDER BY ordinal_position;

-- ...and nothing is recorded as a default either.
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM pg_attrdef WHERE adrelid = 'btr_lk2'::regclass;

-- So a row written without values is all nulls.
INSERT INTO btr_lk2 (i) VALUES (1);

-- begin-expected
-- columns: j | k | b
-- row: NULL, NULL, NULL
-- end-expected
SELECT j, k::text, b::text FROM btr_lk2;

-- The types themselves are copied, precision and all.
-- begin-expected
-- columns: column_name | data_type | numeric_precision | numeric_scale
-- row: n, numeric, 5, 2
-- end-expected
SELECT column_name, data_type, numeric_precision::text, numeric_scale::text
  FROM information_schema.columns
 WHERE table_name = 'btr_lk2' AND column_name = 'n';

-- The copied NOT NULL constraint keeps the name it has on the source table.
-- begin-expected
-- columns: conname | contype
-- row: btr_lk_i_not_null, n
-- end-expected
SELECT conname, contype::text FROM pg_constraint
 WHERE conrelid = 'btr_lk2'::regclass ORDER BY conname;

DROP TABLE btr_lk2;

-- INCLUDING DEFAULTS brings them, under the new table's own name for the key.
CREATE TABLE btr_lk2 (LIKE btr_lk INCLUDING DEFAULTS);

-- begin-expected
-- columns: column_name | column_default
-- row: j, 'q'::text
-- row: k, 4
-- end-expected
SELECT column_name, column_default FROM information_schema.columns
 WHERE table_name = 'btr_lk2' AND column_name IN ('j','k') ORDER BY ordinal_position;

DROP TABLE btr_lk2;

-- INCLUDING ALL brings the key too, and the NOT NULL still keeps its old name.
CREATE TABLE btr_lk2 (LIKE btr_lk INCLUDING ALL);

-- begin-expected
-- columns: conname | contype
-- row: btr_lk2_pkey, p
-- row: btr_lk_i_not_null, n
-- end-expected
SELECT conname, contype::text FROM pg_constraint
 WHERE conrelid = 'btr_lk2'::regclass ORDER BY conname;

DROP TABLE btr_lk2;

-- A later EXCLUDING takes back what INCLUDING ALL brought in.
CREATE TABLE btr_lk2 (LIKE btr_lk INCLUDING ALL EXCLUDING DEFAULTS);

-- begin-expected
-- columns: column_name | column_default
-- row: j, NULL
-- row: k, NULL
-- end-expected
SELECT column_name, column_default FROM information_schema.columns
 WHERE table_name = 'btr_lk2' AND column_name IN ('j','k') ORDER BY ordinal_position;

DROP TABLE btr_lk2;

-- ============================================================================
-- 2. LIKE and an identity, a generated column, a serial and a comment
-- ============================================================================

CREATE TABLE btr_gi (i int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, j int);
CREATE TABLE btr_gi2 (LIKE btr_gi);

-- begin-expected
-- columns: column_name | is_identity | identity_generation | column_default
-- row: i, NO, NULL, NULL
-- row: j, NO, NULL, NULL
-- end-expected
SELECT column_name, is_identity, identity_generation, column_default
  FROM information_schema.columns
 WHERE table_name = 'btr_gi2' ORDER BY ordinal_position;

DROP TABLE btr_gi2;
CREATE TABLE btr_gi2 (LIKE btr_gi INCLUDING IDENTITY);

-- begin-expected
-- columns: column_name | is_identity | identity_generation
-- row: i, YES, BY DEFAULT
-- end-expected
SELECT column_name, is_identity, identity_generation
  FROM information_schema.columns
 WHERE table_name = 'btr_gi2' AND column_name = 'i';

CREATE TABLE btr_gg (i int PRIMARY KEY, j int GENERATED ALWAYS AS (i * 2) STORED);
CREATE TABLE btr_gg2 (LIKE btr_gg);

-- Without INCLUDING GENERATED the copy is a plain column.
-- begin-expected
-- columns: column_name | is_generated
-- row: i, NEVER
-- row: j, NEVER
-- end-expected
SELECT column_name, is_generated FROM information_schema.columns
 WHERE table_name = 'btr_gg2' ORDER BY ordinal_position;

DROP TABLE btr_gg2;
CREATE TABLE btr_gg2 (LIKE btr_gg INCLUDING GENERATED);

-- begin-expected
-- columns: column_name | is_generated
-- row: i, NEVER
-- row: j, ALWAYS
-- end-expected
SELECT column_name, is_generated FROM information_schema.columns
 WHERE table_name = 'btr_gg2' ORDER BY ordinal_position;

CREATE TABLE btr_sr (i serial PRIMARY KEY, j int);
CREATE TABLE btr_sr2 (LIKE btr_sr);

-- The serial's sequence default does not travel, and the column is an integer.
-- begin-expected
-- columns: column_name | data_type | column_default
-- row: i, integer, NULL
-- row: j, integer, NULL
-- end-expected
SELECT column_name, data_type, column_default FROM information_schema.columns
 WHERE table_name = 'btr_sr2' ORDER BY ordinal_position;

CREATE TABLE btr_cm (i int PRIMARY KEY, j text);
COMMENT ON COLUMN btr_cm.i IS 'hello';
CREATE TABLE btr_cm2 (LIKE btr_cm);

-- begin-expected
-- columns: c
-- row: NULL
-- end-expected
SELECT col_description('btr_cm2'::regclass, 1) AS c;

DROP TABLE btr_cm2;
CREATE TABLE btr_cm2 (LIKE btr_cm INCLUDING COMMENTS);

-- begin-expected
-- columns: c
-- row: hello
-- end-expected
SELECT col_description('btr_cm2'::regclass, 1) AS c;

-- ============================================================================
-- 3. A statement's target columns are resolved before any row is looked at
-- ============================================================================

CREATE TABLE btr_up (i int PRIMARY KEY, j text);

-- The table is empty, and the assignment target is still refused.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" of relation "btr_up" does not exist
-- end-expected-error
UPDATE btr_up SET nosuch = 1;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" of relation "btr_up" does not exist
-- end-expected-error
INSERT INTO btr_up (nosuch) VALUES (1);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" of relation "btr_up" does not exist
-- end-expected-error
INSERT INTO btr_up (i, nosuch) VALUES (5, 1);

-- A qualified assignment target names a column that is not there either.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "btr_up" of relation "btr_up" does not exist
-- end-expected-error
UPDATE btr_up SET btr_up.i = 1;

-- The WHERE and the right of an assignment are resolved just as early.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" does not exist
-- end-expected-error
UPDATE btr_up SET i = 1 WHERE nosuch = 2;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" does not exist
-- end-expected-error
UPDATE btr_up SET i = nosuch;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" does not exist
-- end-expected-error
DELETE FROM btr_up WHERE nosuch = 1;

-- The ordinary shapes over the same empty table still run.
UPDATE btr_up SET j = 'z' WHERE i = 1;
UPDATE btr_up SET j = 'z' WHERE btr_up.i = 1;
UPDATE btr_up SET i = i + 1 WHERE j LIKE 'a%';
UPDATE btr_up SET j = upper(j) WHERE length(j) > 0;
UPDATE btr_up SET j = CASE WHEN i > 0 THEN 'p' ELSE 'n' END;
UPDATE btr_up SET i = (SELECT max(i) FROM btr_up) WHERE i IS NOT NULL;
UPDATE btr_up SET i = 1 WHERE EXISTS (SELECT 1 FROM btr_up x WHERE x.i = btr_up.i);
UPDATE btr_up AS t SET j = 'k' WHERE t.i = 1;
DELETE FROM btr_up WHERE i = 1;
INSERT INTO btr_up (i, j) SELECT 4, 'v';

-- begin-expected
-- columns: i | j
-- row: 4, v
-- end-expected
SELECT i::text, j FROM btr_up ORDER BY i NULLS LAST;

-- A FROM clause brings other columns into scope, and is left to run.
UPDATE btr_up SET j = t.z FROM (SELECT 'q' AS z) t WHERE btr_up.i = 4;

-- begin-expected
-- columns: j
-- row: q
-- end-expected
SELECT j FROM btr_up WHERE i = 4;

-- ============================================================================
-- 4. An index definition's collation, operator class and parameters
-- ============================================================================

CREATE TABLE btr_ix (a int PRIMARY KEY, txt text);

-- begin-expected-error
-- sqlstate: 42704
-- message-like: collation "nosuchcollation" for encoding "UTF8" does not exist
-- end-expected-error
CREATE INDEX btr_i1 ON btr_ix (txt COLLATE "nosuchcollation");

-- begin-expected-error
-- sqlstate: 42704
-- message-like: operator class "nosuchopclass" does not exist for access method "btree"
-- end-expected-error
CREATE INDEX btr_i2 ON btr_ix (a nosuchopclass);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: value 0 out of bounds for option "fillfactor"
-- end-expected-error
CREATE INDEX btr_i3 ON btr_ix USING btree (a) WITH (fillfactor = 0);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: value 200 out of bounds for option "fillfactor"
-- end-expected-error
CREATE INDEX btr_i4 ON btr_ix USING btree (a) WITH (fillfactor = 200);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: unrecognized parameter "nosuchoption"
-- end-expected-error
CREATE INDEX btr_i5 ON btr_ix USING btree (a) WITH (nosuchoption = 1);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for boolean option "deduplicate_items": 7
-- end-expected-error
CREATE INDEX btr_i6 ON btr_ix USING btree (a) WITH (deduplicate_items = 7);

-- A parameter belongs to the access method that has it, and to no other.
-- begin-expected-error
-- sqlstate: 22023
-- message-like: unrecognized parameter "deduplicate_items"
-- end-expected-error
CREATE INDEX btr_i7 ON btr_ix USING hash (a) WITH (deduplicate_items = on);

-- The same on the table side.
-- begin-expected-error
-- sqlstate: 22023
-- message-like: value 200 out of bounds for option "fillfactor"
-- end-expected-error
CREATE TABLE btr_tf (a int) WITH (fillfactor = 200);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: unrecognized parameter "nosuchoption"
-- end-expected-error
CREATE TABLE btr_tf (a int) WITH (nosuchoption = 1);

-- ...and when the parameters are set afterwards.
-- begin-expected-error
-- sqlstate: 22023
-- message-like: value 200 out of bounds for option "fillfactor"
-- end-expected-error
ALTER TABLE btr_ix SET (fillfactor = 200);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: unrecognized parameter "nosuchoption"
-- end-expected-error
ALTER TABLE btr_ix SET (nosuchoption = 1);

-- Every ordinary form still builds.
CREATE INDEX btr_ok1 ON btr_ix (txt COLLATE "C");
CREATE INDEX btr_ok2 ON btr_ix (txt text_pattern_ops);
CREATE INDEX btr_ok3 ON btr_ix (a DESC NULLS LAST, txt ASC NULLS FIRST);
CREATE INDEX btr_ok4 ON btr_ix (a) INCLUDE (txt);
CREATE INDEX btr_ok5 ON btr_ix (a) WHERE a > 0;
CREATE INDEX btr_ok6 ON btr_ix ((a + 1));
CREATE INDEX btr_ok7 ON btr_ix (lower(txt));
CREATE INDEX btr_ok8 ON btr_ix USING btree (a) WITH (fillfactor = 70);
CREATE INDEX btr_ok9 ON btr_ix USING btree (a) WITH (deduplicate_items = on);
CREATE INDEX btr_ok10 ON btr_ix USING hash (a) WITH (fillfactor = 50);
CREATE INDEX btr_ok11 ON btr_ix USING btree (txt COLLATE "C" text_pattern_ops DESC NULLS FIRST)
       INCLUDE (a) WITH (fillfactor = 80) WHERE txt IS NOT NULL;
CREATE TABLE btr_tf (a int PRIMARY KEY) WITH (fillfactor = 70, autovacuum_enabled = false);
ALTER TABLE btr_ix SET (fillfactor = 80);
ALTER TABLE btr_ix SET (autovacuum_vacuum_scale_factor = 0.1);
ALTER TABLE btr_ix RESET (fillfactor);
ALTER INDEX btr_ok1 SET (fillfactor = 90);
ALTER INDEX btr_ok1 RESET (fillfactor);

-- begin-expected
-- columns: indexdef
-- row: CREATE INDEX btr_ok1 ON public.btr_ix USING btree (txt COLLATE "C")
-- end-expected
SELECT indexdef FROM pg_indexes WHERE indexname = 'btr_ok1';

-- begin-expected
-- columns: n
-- row: 12
-- end-expected
SELECT count(*)::text AS n FROM pg_indexes WHERE tablename = 'btr_ix';

DROP TABLE btr_tf;

-- ============================================================================
-- 5. A DO ALSO rule on UPDATE and on DELETE fires
-- ============================================================================

CREATE TABLE btr_rt (a int PRIMARY KEY, b int);
CREATE TABLE btr_rlog (m text);
CREATE RULE btr_ri AS ON INSERT TO btr_rt DO ALSO INSERT INTO btr_rlog VALUES ('ins');
CREATE RULE btr_ru AS ON UPDATE TO btr_rt DO ALSO INSERT INTO btr_rlog VALUES ('upd');
CREATE RULE btr_rd AS ON DELETE TO btr_rt DO ALSO INSERT INTO btr_rlog VALUES ('del');

INSERT INTO btr_rt VALUES (1,1);
UPDATE btr_rt SET b = 2 WHERE a = 1;
DELETE FROM btr_rt WHERE a = 1;

-- begin-expected
-- columns: m | n
-- row: del, 1
-- row: ins, 1
-- row: upd, 1
-- end-expected
SELECT m, count(*)::text AS n FROM btr_rlog GROUP BY m ORDER BY m;

-- An action that never says OLD or NEW is the same command for every row, so it
-- runs once for the statement — and not at all when no row was touched.
DELETE FROM btr_rlog;
INSERT INTO btr_rt VALUES (1,1),(2,2);
DELETE FROM btr_rlog;
UPDATE btr_rt SET b = b + 1;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::text AS n FROM btr_rlog WHERE m = 'upd';

DELETE FROM btr_rlog;
UPDATE btr_rt SET b = 5 WHERE a = 99;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM btr_rlog;

-- The statement still writes what it said it would.
DELETE FROM btr_rlog;
UPDATE btr_rt SET b = 100;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*)::text AS n FROM btr_rt WHERE b = 100;

-- An action that reads OLD or NEW runs once per row and sees both values.
DROP TABLE btr_rt CASCADE;
DROP TABLE btr_rlog;
CREATE TABLE btr_rt (a int PRIMARY KEY, b int);
CREATE TABLE btr_rlog (o int, n int);
CREATE RULE btr_ru AS ON UPDATE TO btr_rt
    DO ALSO INSERT INTO btr_rlog VALUES (OLD.b, NEW.b);
INSERT INTO btr_rt VALUES (1,10),(2,20);
UPDATE btr_rt SET b = b + 1;

-- begin-expected
-- columns: o | n
-- row: 10, 11
-- row: 20, 21
-- end-expected
SELECT o::text, n::text FROM btr_rlog ORDER BY o;

-- A qualified rule fires only for the rows its WHERE holds for.
CREATE TABLE btr_rc (a int PRIMARY KEY, b int);
CREATE TABLE btr_rclog (m int);
CREATE RULE btr_rcr AS ON UPDATE TO btr_rc WHERE NEW.b > 5
    DO ALSO INSERT INTO btr_rclog VALUES (NEW.b);
INSERT INTO btr_rc VALUES (1,1),(2,2);
UPDATE btr_rc SET b = 9 WHERE a = 1;
UPDATE btr_rc SET b = 2 WHERE a = 2;

-- begin-expected
-- columns: m
-- row: 9
-- end-expected
SELECT m::text FROM btr_rclog ORDER BY m;

-- ============================================================================
-- 6. pg_rules writes a rule's action back the way PostgreSQL deparses it
-- ============================================================================
-- The parenthesised several-action form is not written here: this harness ends
-- a statement at a semicolon, and DO ( a; b; ) has two of them inside it. That
-- shape is covered by DdlTableAndRuleResidualsTest instead.

-- A single action takes the extra space where ALSO would have stood.
-- begin-expected
-- columns: definition
-- row: CREATE RULE btr_rcr AS~    ON UPDATE TO public.btr_rc~   WHERE (new.b > 5) DO  INSERT INTO btr_rclog (m)~  VALUES (new.b);
-- end-expected
SELECT replace(definition, chr(10), '~') AS definition
  FROM pg_rules WHERE tablename = 'btr_rc';

-- DO INSTEAD keeps its word, and DO INSTEAD NOTHING keeps its shape.
CREATE TABLE btr_ris (a int PRIMARY KEY, b int);
CREATE TABLE btr_rislog (m text);
CREATE RULE btr_risr AS ON UPDATE TO btr_ris DO INSTEAD INSERT INTO btr_rislog VALUES ('x');

-- begin-expected
-- columns: definition
-- row: CREATE RULE btr_risr AS~    ON UPDATE TO public.btr_ris DO INSTEAD  INSERT INTO btr_rislog (m)~  VALUES ('x'::text);
-- end-expected
SELECT replace(definition, chr(10), '~') AS definition
  FROM pg_rules WHERE tablename = 'btr_ris';

CREATE TABLE btr_rin (a int PRIMARY KEY, b int);
INSERT INTO btr_rin VALUES (1,1);
CREATE RULE btr_rinr AS ON UPDATE TO btr_rin DO INSTEAD NOTHING;
UPDATE btr_rin SET b = 5;

-- begin-expected
-- columns: a | b
-- row: 1, 1
-- end-expected
SELECT a::text, b::text FROM btr_rin;

-- begin-expected
-- columns: definition
-- row: CREATE RULE btr_rinr AS~    ON UPDATE TO public.btr_rin DO INSTEAD NOTHING;
-- end-expected
SELECT replace(definition, chr(10), '~') AS definition
  FROM pg_rules WHERE tablename = 'btr_rin';

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP TABLE IF EXISTS btr_rin CASCADE;
DROP TABLE IF EXISTS btr_rislog CASCADE;
DROP TABLE IF EXISTS btr_ris CASCADE;
DROP TABLE IF EXISTS btr_rclog CASCADE;
DROP TABLE IF EXISTS btr_rc CASCADE;
DROP TABLE IF EXISTS btr_rlog2 CASCADE;
DROP TABLE IF EXISTS btr_r2 CASCADE;
DROP TABLE IF EXISTS btr_rlog CASCADE;
DROP TABLE IF EXISTS btr_rt CASCADE;
DROP TABLE IF EXISTS btr_ix CASCADE;
DROP TABLE IF EXISTS btr_up CASCADE;
DROP TABLE IF EXISTS btr_cm2 CASCADE;
DROP TABLE IF EXISTS btr_cm CASCADE;
DROP TABLE IF EXISTS btr_sr2 CASCADE;
DROP TABLE IF EXISTS btr_sr CASCADE;
DROP TABLE IF EXISTS btr_gg2 CASCADE;
DROP TABLE IF EXISTS btr_gg CASCADE;
DROP TABLE IF EXISTS btr_gi2 CASCADE;
DROP TABLE IF EXISTS btr_gi CASCADE;
DROP TABLE IF EXISTS btr_lk2 CASCADE;
DROP TABLE IF EXISTS btr_lk CASCADE;
