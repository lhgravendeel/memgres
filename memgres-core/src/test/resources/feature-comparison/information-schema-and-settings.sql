-- ============================================================================
-- Feature Comparison: information_schema, pg_settings and pg_type attributes
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- information_schema is the portable view of a database, so a column that is
-- absent there is a tool that cannot introspect. pg_settings carries the
-- metadata a client reads to decide how to render and whether it may change a
-- value. pg_type records how a value is physically laid out.
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP TABLE IF EXISTS isas_dt CASCADE;
DROP TABLE IF EXISTS isas_ivl CASCADE;
DROP TABLE IF EXISTS isas_tc CASCADE;
DROP TABLE IF EXISTS isas_cn CASCADE;
DROP TABLE IF EXISTS isas_trg CASCADE;
DROP FUNCTION IF EXISTS isas_tf() CASCADE;
DROP SEQUENCE IF EXISTS isas_seq CASCADE;
DROP DOMAIN IF EXISTS isas_vc CASCADE;
DROP DOMAIN IF EXISTS isas_num CASCADE;
DROP DOMAIN IF EXISTS isas_txt CASCADE;
DROP DOMAIN IF EXISTS isas_ts CASCADE;
DROP DOMAIN IF EXISTS isas_int CASCADE;

CREATE DOMAIN isas_vc AS varchar(12) NOT NULL DEFAULT 'x';
CREATE DOMAIN isas_num AS numeric(10,2);
CREATE DOMAIN isas_txt AS text;
CREATE DOMAIN isas_ts AS timestamp(3);
CREATE DOMAIN isas_int AS integer DEFAULT 7;
CREATE SEQUENCE isas_seq;
CREATE TABLE isas_dt (a timestamp, b timestamp(3), c time(2), d timestamptz(1),
                      e date, f timetz(4), g interval, h interval(2));
CREATE TABLE isas_ivl (a interval year to month, b interval day to second(3),
                       c interval hour, d interval minute to second);
CREATE TABLE isas_tc (id int PRIMARY KEY, u int UNIQUE,
                      w int UNIQUE NULLS NOT DISTINCT, v int NOT NULL);
CREATE TABLE isas_cn (a int, b int, CHECK (a > 0), CHECK (a < b), CHECK (b > 0));
CREATE TABLE isas_trg (id int);
CREATE FUNCTION isas_tf() RETURNS trigger AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql;
CREATE TRIGGER isas_trigger BEFORE INSERT ON isas_trg
    FOR EACH ROW EXECUTE FUNCTION isas_tf();

-- ============================================================================
-- SECTION A: information_schema.domains describes a domain like a column
-- ============================================================================

-- A varchar domain reports both its declared width and the octets it can hold.
-- begin-expected
-- columns: data_type | character_maximum_length | character_octet_length | udt_schema | udt_name | dtd_identifier
-- row: character varying, 12, 48, pg_catalog, varchar, 1
-- end-expected
SELECT data_type, character_maximum_length, character_octet_length,
       udt_schema, udt_name, dtd_identifier
FROM information_schema.domains WHERE domain_name = 'isas_vc';

-- A numeric domain reports precision, radix and scale.
-- begin-expected
-- columns: data_type | numeric_precision | numeric_precision_radix | numeric_scale
-- row: numeric, 10, 10, 2
-- end-expected
SELECT data_type, numeric_precision, numeric_precision_radix, numeric_scale
FROM information_schema.domains WHERE domain_name = 'isas_num';

-- A text domain has no maximum length but still a maximum octet length.
-- begin-expected
-- columns: character_maximum_length | character_octet_length | udt_name
-- row: null, 1073741824, text
-- end-expected
SELECT character_maximum_length, character_octet_length, udt_name
FROM information_schema.domains WHERE domain_name = 'isas_txt';

-- A timestamp(3) domain reports the typmod as its datetime precision.
-- begin-expected
-- columns: data_type | datetime_precision | numeric_precision | udt_name
-- row: timestamp without time zone, 3, null, timestamp
-- end-expected
SELECT data_type, datetime_precision, numeric_precision, udt_name
FROM information_schema.domains WHERE domain_name = 'isas_ts';

-- An integer domain: binary radix, zero scale, and its default.
-- begin-expected
-- columns: numeric_precision | numeric_precision_radix | numeric_scale | domain_default
-- row: 32, 2, 0, 7
-- end-expected
SELECT numeric_precision, numeric_precision_radix, numeric_scale, domain_default
FROM information_schema.domains WHERE domain_name = 'isas_int';

-- The scope and cardinality columns exist and are null for every domain here.
-- begin-expected
-- columns: n
-- row: 5
-- end-expected
SELECT count(*)::int AS n FROM information_schema.domains
WHERE domain_name LIKE 'isas!_%' ESCAPE '!'
  AND scope_catalog IS NULL AND scope_schema IS NULL AND scope_name IS NULL
  AND maximum_cardinality IS NULL AND interval_type IS NULL
  AND character_set_name IS NULL AND collation_name IS NULL;

-- ============================================================================
-- SECTION B: information_schema.columns.datetime_precision follows the typmod
-- ============================================================================

-- begin-expected
-- columns: column_name | data_type | datetime_precision
-- row: a, timestamp without time zone, 6
-- row: b, timestamp without time zone, 3
-- row: c, time without time zone, 2
-- row: d, timestamp with time zone, 1
-- row: e, date, 0
-- row: f, time with time zone, 4
-- row: g, interval, 6
-- row: h, interval, 2
-- end-expected
SELECT column_name, data_type, datetime_precision FROM information_schema.columns
WHERE table_name = 'isas_dt' ORDER BY ordinal_position;

-- ============================================================================
-- SECTION C: interval field qualifiers parse and are reported
-- ============================================================================

-- begin-expected
-- columns: column_name | data_type | interval_type | interval_precision | datetime_precision
-- row: a, interval, YEAR TO MONTH, null, 6
-- row: b, interval, DAY TO SECOND(3), null, 3
-- row: c, interval, HOUR, null, 6
-- row: d, interval, MINUTE TO SECOND, null, 6
-- end-expected
SELECT column_name, data_type, interval_type, interval_precision, datetime_precision
FROM information_schema.columns WHERE table_name = 'isas_ivl' ORDER BY ordinal_position;

-- The qualifier restricts which fields a stored value keeps.
INSERT INTO isas_ivl VALUES ('1 year 2 months 3 days 4:05:06.789',
                             '1 year 2 months 3 days 4:05:06.789',
                             '1 year 2 months 3 days 4:05:06.789',
                             '1 year 2 months 3 days 4:05:06.789');

-- begin-expected
-- columns: a | b | c | d
-- row: 1 year 2 mons, 1 year 2 mons 3 days 04:05:06.789, 1 year 2 mons 3 days 04:00:00, 1 year 2 mons 3 days 04:05:06.789
-- end-expected
SELECT a::text AS a, b::text AS b, c::text AS c, d::text AS d FROM isas_ivl;

-- ============================================================================
-- SECTION D: information_schema.tables.commit_action
-- ============================================================================

-- begin-expected
-- columns: table_type | is_insertable_into | commit_action
-- row: BASE TABLE, YES, null
-- end-expected
SELECT table_type, is_insertable_into, commit_action FROM information_schema.tables
WHERE table_name = 'isas_dt';

-- ============================================================================
-- SECTION E: information_schema.table_constraints.nulls_distinct
-- ============================================================================

-- Only a uniqueness constraint answers; the rest report nothing.
-- begin-expected
-- columns: constraint_name | constraint_type | nulls_distinct
-- row: isas_tc_pkey, PRIMARY KEY, null
-- row: isas_tc_u_key, UNIQUE, YES
-- row: isas_tc_w_key, UNIQUE, NO
-- end-expected
SELECT constraint_name, constraint_type, nulls_distinct
FROM information_schema.table_constraints
WHERE table_name = 'isas_tc' AND constraint_type IN ('PRIMARY KEY', 'UNIQUE')
ORDER BY constraint_name;

-- An unnamed CHECK is named after the one column it mentions, and after the
-- table alone when it mentions several; a repeat gets a numeric suffix.
-- begin-expected
-- columns: constraint_name
-- row: isas_cn_a_check
-- row: isas_cn_b_check
-- row: isas_cn_check
-- end-expected
SELECT constraint_name FROM information_schema.table_constraints
WHERE table_name = 'isas_cn' AND constraint_type = 'CHECK'
  AND constraint_name NOT LIKE '%not_null'
ORDER BY constraint_name;

-- ============================================================================
-- SECTION F: information_schema.sequences reports radix and scale
-- ============================================================================

-- begin-expected
-- columns: data_type | numeric_precision | numeric_precision_radix | numeric_scale | cycle_option
-- row: bigint, 64, 2, 0, NO
-- end-expected
SELECT data_type, numeric_precision, numeric_precision_radix, numeric_scale, cycle_option
FROM information_schema.sequences WHERE sequence_name = 'isas_seq';

-- ============================================================================
-- SECTION G: information_schema.schemata.sql_path
-- ============================================================================

-- begin-expected
-- columns: sql_path
-- row: null
-- end-expected
SELECT sql_path FROM information_schema.schemata WHERE schema_name = 'public';

-- ============================================================================
-- SECTION H: information_schema.triggers row-variable columns
-- ============================================================================

-- begin-expected
-- columns: event_manipulation | action_timing | action_orientation | action_reference_old_row | action_reference_new_row
-- row: INSERT, BEFORE, ROW, null, null
-- end-expected
SELECT event_manipulation, action_timing, action_orientation,
       action_reference_old_row, action_reference_new_row
FROM information_schema.triggers WHERE trigger_name = 'isas_trigger';

-- ============================================================================
-- SECTION I: pg_settings metadata belongs to the setting
-- ============================================================================

-- A memory setting: integer, counted in kB, with real bounds.
-- begin-expected
-- columns: vartype | context | unit | min_val | max_val | boot_val
-- row: integer, user, kB, 64, 2147483647, 4096
-- end-expected
SELECT vartype, context, unit, min_val, max_val, boot_val
FROM pg_settings WHERE name = 'work_mem';

-- A boolean setting is not a string, and its category is its own.
-- begin-expected
-- columns: vartype | context | category | boot_val
-- row: bool, user, Version and Platform Compatibility / Previous PostgreSQL Versions, on
-- end-expected
SELECT vartype, context, category, boot_val FROM pg_settings WHERE name = 'array_nulls';

-- A setting only the postmaster can change says so.
-- begin-expected
-- columns: vartype | context | enumvals
-- row: enum, postmaster, {always,on,off}
-- end-expected
SELECT vartype, context, enumvals::text AS enumvals
FROM pg_settings WHERE name = 'archive_mode';

-- An internal, unchangeable setting reports its own value as both bounds.
-- begin-expected
-- columns: vartype | context | min_val | max_val | boot_val
-- row: integer, internal, 8192, 8192, 8192
-- end-expected
SELECT vartype, context, min_val, max_val, boot_val
FROM pg_settings WHERE name = 'block_size';

-- A background setting reloaded on SIGHUP, counted in seconds.
-- begin-expected
-- columns: vartype | context | unit | boot_val
-- row: integer, sighup, s, 60
-- end-expected
SELECT vartype, context, unit, boot_val FROM pg_settings WHERE name = 'autovacuum_naptime';

-- A floating-point setting.
-- begin-expected
-- columns: vartype | context | min_val | max_val
-- row: real, sighup, 0, 10
-- end-expected
SELECT vartype, context, min_val, max_val FROM pg_settings WHERE name = 'bgwriter_lru_multiplier';

-- An enum's permitted values are quoted where a value contains a space.
-- begin-expected
-- columns: vartype | enumvals
-- row: enum, {serializable,"repeatable read","read committed","read uncommitted"}
-- end-expected
SELECT vartype, enumvals::text AS enumvals
FROM pg_settings WHERE name = 'default_transaction_isolation';

-- Nothing is left in the catch-all category, and nothing is typed by default.
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_settings
WHERE name IN ('autovacuum', 'archive_timeout', 'checkpoint_timeout', 'deadlock_timeout',
               'max_wal_size', 'wal_level', 'bytea_output', 'shared_buffers')
  AND (vartype = 'string' OR category = 'Ungrouped');

-- PostgreSQL keeps role, session_authorization and is_superuser out of the listing.
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_settings
WHERE name IN ('role', 'session_authorization', 'is_superuser');

-- ============================================================================
-- SECTION J: a setting with a unit stores its base value and displays a unit
-- ============================================================================

-- begin-expected
-- columns: setting | shown
-- row: 4096, 4MB
-- end-expected
SELECT setting, current_setting('work_mem') AS shown
FROM pg_settings WHERE name = 'work_mem';

-- begin-expected
-- columns: v
-- row: 1min
-- end-expected
SELECT current_setting('autovacuum_naptime') AS v;

-- begin-expected
-- columns: v
-- row: 1GB
-- end-expected
SELECT current_setting('segment_size') AS v;

-- A written unit is converted into the unit the setting counts in.
SET work_mem = '8MB';

-- begin-expected
-- columns: setting | boot_val | shown
-- row: 8192, 4096, 8MB
-- end-expected
SELECT setting, boot_val, current_setting('work_mem') AS shown
FROM pg_settings WHERE name = 'work_mem';

-- begin-expected
-- columns: source
-- row: session
-- end-expected
SELECT source FROM pg_settings WHERE name = 'work_mem';

RESET work_mem;

-- begin-expected
-- columns: setting | source
-- row: 4096, default
-- end-expected
SELECT setting, source FROM pg_settings WHERE name = 'work_mem';

-- A timeout is stored in milliseconds and shown in the largest unit that fits.
SET statement_timeout = '5s';

-- begin-expected
-- columns: setting | shown
-- row: 5000, 5s
-- end-expected
SELECT setting, current_setting('statement_timeout') AS shown
FROM pg_settings WHERE name = 'statement_timeout';

RESET statement_timeout;

-- A non-positive value keeps no unit, the way PostgreSQL prints it.
-- begin-expected
-- columns: t | l
-- row: 0, -1
-- end-expected
SELECT current_setting('statement_timeout') AS t, current_setting('temp_file_limit') AS l;

-- ============================================================================
-- SECTION K: pg_type records how a value is laid out
-- ============================================================================

-- An array of a double-aligned type is double-aligned; _text really is int-aligned.
-- begin-expected
-- columns: typname | typlen | typbyval | typtype | typcategory | typalign | typstorage | typdelim
-- row: _float8, -1, f, b, A, d, x, ,
-- row: _int8, -1, f, b, A, d, x, ,
-- row: _interval, -1, f, b, A, d, x, ,
-- row: _text, -1, f, b, A, i, x, ,
-- row: _timestamptz, -1, f, b, A, d, x, ,
-- end-expected
SELECT typname, typlen, typbyval, typtype, typcategory, typalign, typstorage, typdelim
FROM pg_type WHERE typname IN ('_float8', '_int8', '_interval', '_text', '_timestamptz')
ORDER BY typname;

-- A box array is delimited by semicolons, because a box is.
-- begin-expected
-- columns: typname | typdelim | typalign
-- row: _box, ;, d
-- end-expected
SELECT typname, typdelim, typalign FROM pg_type WHERE typname = '_box';

-- name sorts under the C collation, and so does an array of it.
-- begin-expected
-- columns: typname | typcollation
-- row: _name, 950
-- end-expected
SELECT typname, typcollation FROM pg_type WHERE typname = '_name';

-- aclitem is a 16-byte, double-aligned struct.
-- begin-expected
-- columns: typlen | typbyval | typalign | typstorage
-- row: 16, f, d, p
-- end-expected
SELECT typlen, typbyval, typalign, typstorage FROM pg_type WHERE typname = 'aclitem';

-- A pseudo-type is never passed by value when it is varlena.
-- begin-expected
-- columns: typname | typlen | typbyval | typtype
-- row: _record, -1, f, p
-- row: anyarray, -1, f, p
-- row: record, -1, f, p
-- end-expected
SELECT typname, typlen, typbyval, typtype FROM pg_type
WHERE typname IN ('anyarray', 'record', '_record') ORDER BY typname;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP TABLE IF EXISTS isas_dt CASCADE;
DROP TABLE IF EXISTS isas_ivl CASCADE;
DROP TABLE IF EXISTS isas_tc CASCADE;
DROP TABLE IF EXISTS isas_cn CASCADE;
DROP TABLE IF EXISTS isas_trg CASCADE;
DROP FUNCTION IF EXISTS isas_tf() CASCADE;
DROP SEQUENCE IF EXISTS isas_seq CASCADE;
DROP DOMAIN IF EXISTS isas_vc CASCADE;
DROP DOMAIN IF EXISTS isas_num CASCADE;
DROP DOMAIN IF EXISTS isas_txt CASCADE;
DROP DOMAIN IF EXISTS isas_ts CASCADE;
DROP DOMAIN IF EXISTS isas_int CASCADE;
