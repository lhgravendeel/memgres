-- ============================================================================
-- Feature Comparison: the catalogs agree with each other and with the relation
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- A catalog is read to find out what a relation is. Two catalogs describing the
-- same column differently, a view that reports a column it does not have, or a
-- setting that reports a bound nothing enforces all leave a tool believing
-- something the database will not do.
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP TABLE IF EXISTS ca_dt CASCADE;
DROP TABLE IF EXISTS ca_dc CASCADE;
DROP TABLE IF EXISTS ca_nd CASCADE;
DROP TABLE IF EXISTS ca_ck CASCADE;
DROP TABLE IF EXISTS ca_ivl CASCADE;
DROP TABLE IF EXISTS ca_trg CASCADE;
DROP VIEW IF EXISTS ca_vv CASCADE;
DROP TABLE IF EXISTS ca_v1 CASCADE;
DROP FUNCTION IF EXISTS ca_tf() CASCADE;
DROP DOMAIN IF EXISTS ca_dv CASCADE;
DROP DOMAIN IF EXISTS ca_dn CASCADE;
DROP DOMAIN IF EXISTS ca_dt2 CASCADE;
DROP DOMAIN IF EXISTS ca_darr CASCADE;
DROP DOMAIN IF EXISTS ca_divl CASCADE;
DROP DOMAIN IF EXISTS ca_divl2 CASCADE;
DROP SCHEMA IF EXISTS ca_s1 CASCADE;
DROP SCHEMA IF EXISTS ca_s2 CASCADE;

CREATE DOMAIN ca_dv AS varchar(9);
CREATE DOMAIN ca_dn AS numeric(7,3);
CREATE DOMAIN ca_dt2 AS timestamp(2);
CREATE DOMAIN ca_darr AS integer[];
CREATE DOMAIN ca_divl AS interval day to second(3);
CREATE DOMAIN ca_divl2 AS interval year to month;
CREATE TABLE ca_dt (a timestamp, b timestamp(3), c time(2), d timestamptz(1),
                    e date, f timetz(4), g interval, h interval(2), i timestamp(0),
                    j bit(4), k bit varying(8), l varchar(9), m numeric(7,3),
                    n interval day to second(3));
CREATE TABLE ca_dc (a ca_dv, b ca_dn, c ca_dt2);
CREATE TABLE ca_ivl (x ca_divl, y ca_divl2);
CREATE TABLE ca_nd (a int, b int);
ALTER TABLE ca_nd ADD CONSTRAINT ca_nd_a UNIQUE NULLS NOT DISTINCT (a);
CREATE TABLE ca_ck (a int, b int, CONSTRAINT ca_ck_ck CHECK (a > 0));
CREATE TABLE ca_v1 (id int primary key, a int);
CREATE VIEW ca_vv AS SELECT id, a FROM ca_v1;
CREATE TABLE ca_trg (id int);
CREATE FUNCTION ca_tf() RETURNS trigger AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql;
CREATE TRIGGER ca_trigger BEFORE INSERT ON ca_trg FOR EACH ROW EXECUTE FUNCTION ca_tf();
CREATE SCHEMA ca_s1;
CREATE SCHEMA ca_s2;
CREATE DOMAIN ca_s1.d1 AS varchar(6);
CREATE SEQUENCE ca_s1.sq1;
CREATE TABLE ca_s2.t (a int);

-- ============================================================================
-- SECTION A: an interval qualifier takes a precision only on SECOND
-- ============================================================================

-- Only a qualifier ending in SECOND carries a precision, so the parenthesis
-- after YEAR is a syntax error rather than a type nobody has heard of.
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error
-- end-expected-error
CREATE TABLE ca_bad (a interval year(2));

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error
-- end-expected-error
CREATE TABLE ca_bad (a interval minute(4));

-- A range runs from the larger field down to a smaller one and never back up.
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error
-- end-expected-error
CREATE TABLE ca_bad (a interval second to day);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error
-- end-expected-error
CREATE TABLE ca_bad (a interval hour to year);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error
-- end-expected-error
CREATE TABLE ca_bad (a interval month to day);

-- ============================================================================
-- SECTION B: pg_attribute and information_schema describe one column alike
-- ============================================================================

-- The declared precision of a temporal column is part of its type modifier, and
-- format_type reads it back out of the same number a client sizes the column by.
-- begin-expected
-- columns: attname | format_type | atttypmod
-- row: b, timestamp(3) without time zone, 3
-- end-expected
SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.atttypmod
FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
WHERE c.relname = 'ca_dt' AND a.attname = 'b';

-- information_schema reports the same precision for the same column.
-- begin-expected
-- columns: column_name | datetime_precision
-- row: b, 3
-- end-expected
SELECT column_name, datetime_precision FROM information_schema.columns
WHERE table_name = 'ca_dt' AND column_name = 'b';

-- An interval qualifier packs the fields it keeps and the precision into one
-- modifier, which is how a client reads both back.
-- begin-expected
-- columns: format_type | atttypmod
-- row: interval day to second(3), 470286339
-- end-expected
SELECT format_type(a.atttypid, a.atttypmod), a.atttypmod
FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
WHERE c.relname = 'ca_dt' AND a.attname = 'n';

-- A plain interval with a precision keeps every field.
-- begin-expected
-- columns: format_type | atttypmod
-- row: interval(2), 2147418114
-- end-expected
SELECT format_type(a.atttypid, a.atttypmod), a.atttypmod
FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
WHERE c.relname = 'ca_dt' AND a.attname = 'h';

-- A bit string's modifier is its length, with nothing added to it.
-- begin-expected
-- columns: attname | format_type | atttypmod
-- row: j, bit(4), 4
-- row: k, bit varying(8), 8
-- end-expected
SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.atttypmod
FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
WHERE c.relname = 'ca_dt' AND a.attname IN ('j','k') ORDER BY a.attname;

-- attbyval says whether a value is passed by value, and it is read off the same
-- record pg_type answers from.
-- begin-expected
-- columns: attname | attbyval | attlen | attalign | attstorage
-- row: l, f, -1, i, x
-- row: m, f, -1, i, m
-- end-expected
SELECT a.attname, a.attbyval, a.attlen, a.attalign, a.attstorage
FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
WHERE c.relname = 'ca_dt' AND a.attname IN ('l','m') ORDER BY a.attname;

-- ============================================================================
-- SECTION C: format_type names an array by its element and the modifier
-- ============================================================================

-- begin-expected
-- columns: format_type
-- row: character varying(10)[]
-- end-expected
SELECT format_type('varchar[]'::regtype, 14);

-- begin-expected
-- columns: format_type
-- row: numeric(10,2)[]
-- end-expected
SELECT format_type('numeric[]'::regtype, 655366);

-- The OID may be written as a number; the answer is the same either way.
-- begin-expected
-- columns: format_type
-- row: character varying(10)[]
-- end-expected
SELECT format_type(1015, 14);

-- begin-expected
-- columns: format_type
-- row: timestamp(3) with time zone[]
-- end-expected
SELECT format_type(1185, 3);

-- A regtype-cast argument is a type name, not a number to be parsed.
-- begin-expected
-- columns: format_type
-- row: integer
-- end-expected
SELECT format_type('int4'::regtype, -1);

-- ============================================================================
-- SECTION D: a domain carries its base type's declaration into a column
-- ============================================================================

-- A column of a varchar(9) domain is nine characters wide, and refuses a tenth.
-- begin-expected-error
-- sqlstate: 22001
-- message-like: too long
-- end-expected-error
INSERT INTO ca_dc (a) VALUES ('0123456789012');

-- begin-expected
-- columns: column_name | character_maximum_length | numeric_precision | numeric_scale | datetime_precision
-- row: a, 9, NULL, NULL, NULL
-- row: b, NULL, 7, 3, NULL
-- row: c, NULL, NULL, NULL, 2
-- end-expected
SELECT column_name, character_maximum_length, numeric_precision, numeric_scale,
       datetime_precision
FROM information_schema.columns WHERE table_name = 'ca_dc' ORDER BY column_name;

-- An interval domain reports the qualifier it was declared with.
-- begin-expected
-- columns: domain_name | interval_type | interval_precision
-- row: ca_divl, DAY TO SECOND(3), NULL
-- row: ca_divl2, YEAR TO MONTH, NULL
-- end-expected
SELECT domain_name, interval_type, interval_precision
FROM information_schema.domains WHERE domain_name LIKE 'ca_divl%' ORDER BY 1;

-- And a column of that domain keeps it.
-- begin-expected
-- columns: column_name | interval_type
-- row: x, DAY TO SECOND(3)
-- row: y, YEAR TO MONTH
-- end-expected
SELECT column_name, interval_type FROM information_schema.columns
WHERE table_name = 'ca_ivl' ORDER BY 1;

-- A domain over an array is described as the array, not as its element.
-- begin-expected
-- columns: domain_name | data_type | udt_schema | udt_name | numeric_precision
-- row: ca_darr, integer[], pg_catalog, _int4, NULL
-- end-expected
SELECT domain_name, data_type, udt_schema, udt_name, numeric_precision
FROM information_schema.domains WHERE domain_name = 'ca_darr';

-- pg_type says the same about it.
-- begin-expected
-- columns: typbasetype
-- row: integer[]
-- end-expected
SELECT typbasetype::regtype::text FROM pg_type WHERE typname = 'ca_darr';

-- A domain and a sequence live in the schema they were created in.
-- begin-expected
-- columns: domain_schema | domain_name
-- row: ca_s1, d1
-- end-expected
SELECT domain_schema, domain_name FROM information_schema.domains
WHERE domain_name = 'd1';

-- begin-expected
-- columns: sequence_schema | sequence_name
-- row: ca_s1, sq1
-- end-expected
SELECT sequence_schema, sequence_name FROM information_schema.sequences
WHERE sequence_name = 'sq1';

-- ============================================================================
-- SECTION E: a constraint names the columns it is used by
-- ============================================================================

-- begin-expected
-- columns: conname | conkey
-- row: ca_ck_ck, {1}
-- end-expected
SELECT conname, conkey::text FROM pg_constraint WHERE conname = 'ca_ck_ck';

-- begin-expected
-- columns: constraint_name | table_name | column_name
-- row: ca_ck_ck, ca_ck, a
-- end-expected
SELECT constraint_name, table_name, column_name
FROM information_schema.constraint_column_usage WHERE constraint_name = 'ca_ck_ck';

-- NULLS NOT DISTINCT is part of the constraint, so the index it creates, the
-- definition it reports and the view that describes it all say so.
-- begin-expected
-- columns: relname | indnullsnotdistinct
-- row: ca_nd_a, t
-- end-expected
SELECT c.relname, i.indnullsnotdistinct FROM pg_index i
JOIN pg_class c ON c.oid = i.indexrelid
JOIN pg_class t ON t.oid = i.indrelid WHERE t.relname = 'ca_nd';

-- begin-expected
-- columns: conname | pg_get_constraintdef
-- row: ca_nd_a, UNIQUE NULLS NOT DISTINCT (a)
-- end-expected
SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'ca_nd_a';

-- begin-expected
-- columns: indexname | indexdef
-- row: ca_nd_a, CREATE UNIQUE INDEX ca_nd_a ON public.ca_nd USING btree (a) NULLS NOT DISTINCT
-- end-expected
SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'ca_nd' ORDER BY 1;

-- begin-expected
-- columns: constraint_name | nulls_distinct
-- row: ca_nd_a, NO
-- end-expected
SELECT constraint_name, nulls_distinct FROM information_schema.table_constraints
WHERE constraint_name = 'ca_nd_a';

-- ============================================================================
-- SECTION F: pg_trigger holds what a trigger was declared with
-- ============================================================================

-- begin-expected
-- columns: tgname | tgnargs | tgtype | tgenabled | tgconstrindid | tgqual
-- row: ca_trigger, 0, 7, O, 0, NULL
-- end-expected
SELECT tgname, tgnargs, tgtype, tgenabled, tgconstrindid, tgqual
FROM pg_trigger WHERE tgname = 'ca_trigger';

-- begin-expected
-- columns: pg_get_triggerdef
-- row: CREATE TRIGGER ca_trigger BEFORE INSERT ON public.ca_trg FOR EACH ROW EXECUTE FUNCTION ca_tf()
-- end-expected
SELECT pg_get_triggerdef(oid) FROM pg_trigger WHERE tgname = 'ca_trigger';

-- ============================================================================
-- SECTION G: the catalogs describe themselves
-- ============================================================================

-- The "char" type is a type of its own, and _char's element points at it.
-- begin-expected
-- columns: typname | typcategory | typtype | typlen
-- row: _char, A, b, -1
-- row: char, Z, b, 1
-- end-expected
SELECT typname, typcategory, typtype, typlen FROM pg_type
WHERE typname IN ('char','_char') ORDER BY 1;

-- No array type points at an element the catalog does not hold.
-- begin-expected
-- columns: typname
-- end-expected
SELECT t.typname FROM pg_type t WHERE t.typelem <> 0
AND NOT EXISTS (SELECT 1 FROM pg_type e WHERE e.oid = t.typelem) ORDER BY 1;

-- information_schema.columns describes a catalog relation with the columns it
-- has and no others: attbyval is there, and no system column is.
-- begin-expected
-- columns: column_name
-- row: attbyval
-- end-expected
SELECT column_name FROM information_schema.columns
WHERE table_schema = 'pg_catalog' AND table_name = 'pg_attribute'
AND column_name IN ('attbyval','xmin','ctid') ORDER BY column_name;

-- begin-expected
-- columns: count
-- row: 19
-- end-expected
SELECT count(*) FROM information_schema.columns
WHERE table_schema = 'pg_catalog' AND table_name = 'pg_trigger';

-- pg_attribute holds a row per column of a catalog relation too.
-- begin-expected
-- columns: count
-- row: 32
-- end-expected
SELECT count(*) FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'pg_catalog' AND c.relname = 'pg_type' AND a.attnum > 0;

-- And pg_class holds a row for an information_schema view.
-- begin-expected
-- columns: count
-- row: 1
-- end-expected
SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'information_schema' AND c.relname = 'columns';

-- ============================================================================
-- SECTION H: information_schema.tables and .views agree about a view
-- ============================================================================

-- begin-expected
-- columns: table_name | is_insertable_into
-- row: ca_vv, YES
-- end-expected
SELECT table_name, is_insertable_into FROM information_schema.tables
WHERE table_name = 'ca_vv';

-- begin-expected
-- columns: table_name | is_insertable_into
-- row: ca_vv, YES
-- end-expected
SELECT table_name, is_insertable_into FROM information_schema.views
WHERE table_name = 'ca_vv';

-- A catalog relation is listed there as well.
-- begin-expected
-- columns: table_type | is_insertable_into
-- row: BASE TABLE, YES
-- end-expected
SELECT table_type, is_insertable_into FROM information_schema.tables
WHERE table_schema = 'pg_catalog' AND table_name = 'pg_class';

-- ============================================================================
-- SECTION I: a view belongs to a schema
-- ============================================================================

CREATE VIEW public.ca_shared AS SELECT id, a FROM ca_v1;

-- The name is free in another schema, and creating it there says nothing about
-- the view in public.
CREATE VIEW ca_s2.ca_shared AS SELECT a FROM ca_s2.t;

-- begin-expected
-- columns: table_schema | table_name
-- row: ca_s2, ca_shared
-- row: public, ca_shared
-- end-expected
SELECT table_schema, table_name FROM information_schema.views
WHERE table_name = 'ca_shared' ORDER BY table_schema;

-- A qualified read reaches the view in the schema it names.
-- begin-expected
-- columns: a
-- end-expected
SELECT * FROM ca_s2.ca_shared;

-- ============================================================================
-- SECTION J: SET is judged by what pg_settings reports
-- ============================================================================

-- A preset the server computed at startup cannot be assigned at all.
-- begin-expected-error
-- sqlstate: 55P02
-- message-like: cannot be changed
-- end-expected-error
SET block_size = 4096;

-- begin-expected-error
-- sqlstate: 55P02
-- message-like: cannot be changed
-- end-expected-error
SET shared_buffers = '64MB';

-- begin-expected-error
-- sqlstate: 55P02
-- message-like: cannot be changed
-- end-expected-error
SET wal_level = 'logical';

-- An enum takes one of the values pg_settings lists for it.
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter
-- end-expected-error
SET client_min_messages = 'bogus';

-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter
-- end-expected-error
SET default_toast_compression = 'gzip';

-- A boolean parameter takes a boolean.
-- begin-expected-error
-- sqlstate: 22023
-- message-like: requires a Boolean value
-- end-expected-error
SET array_nulls = 'maybe';

-- A number has to fall inside the range pg_settings reports.
-- begin-expected-error
-- sqlstate: 22023
-- message-like: outside the valid range
-- end-expected-error
SET work_mem = '-1';

-- begin-expected-error
-- sqlstate: 22023
-- message-like: outside the valid range
-- end-expected-error
SET extra_float_digits = 99;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: outside the valid range
-- end-expected-error
SET deadlock_timeout = 0;

-- synchronous_commit is an enum, so remote_apply is one of its values.
SET synchronous_commit = 'remote_apply';

-- An enum value reads back in the spelling its own list uses.
SET client_min_messages = 'WARNING';

-- begin-expected
-- columns: client_min_messages
-- row: warning
-- end-expected
SHOW client_min_messages;

RESET client_min_messages;
RESET synchronous_commit;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP VIEW IF EXISTS ca_s2.ca_shared;
DROP VIEW IF EXISTS public.ca_shared;
DROP VIEW IF EXISTS ca_vv;
DROP TABLE IF EXISTS ca_v1 CASCADE;
DROP TRIGGER IF EXISTS ca_trigger ON ca_trg;
DROP TABLE IF EXISTS ca_trg CASCADE;
DROP FUNCTION IF EXISTS ca_tf() CASCADE;
DROP TABLE IF EXISTS ca_dt CASCADE;
DROP TABLE IF EXISTS ca_dc CASCADE;
DROP TABLE IF EXISTS ca_ivl CASCADE;
DROP TABLE IF EXISTS ca_nd CASCADE;
DROP TABLE IF EXISTS ca_ck CASCADE;
DROP DOMAIN IF EXISTS ca_dv CASCADE;
DROP DOMAIN IF EXISTS ca_dn CASCADE;
DROP DOMAIN IF EXISTS ca_dt2 CASCADE;
DROP DOMAIN IF EXISTS ca_darr CASCADE;
DROP DOMAIN IF EXISTS ca_divl CASCADE;
DROP DOMAIN IF EXISTS ca_divl2 CASCADE;
DROP SCHEMA IF EXISTS ca_s1 CASCADE;
DROP SCHEMA IF EXISTS ca_s2 CASCADE;
