-- ============================================================================
-- Feature Comparison: information_schema's own _pg_* helper functions
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- information_schema's views are written in terms of a handful of helper
-- functions declared in that schema: _pg_char_max_length, _pg_char_octet_length,
-- _pg_numeric_precision, _pg_numeric_precision_radix, _pg_numeric_scale,
-- _pg_datetime_precision, _pg_interval_type, _pg_truetypid, _pg_truetypmod,
-- _pg_index_position and _pg_expandarray. Tools call them directly, so each has
-- to answer what the reference server answers for the same typmod arithmetic.
-- They live in information_schema and not in pg_catalog, so an unqualified call
-- resolves only where the session has put information_schema on the search path.
-- pg_logical_emit_message is here too: it answers in pg_lsn in both its
-- overloads.
-- ============================================================================

-- ============================================================================
-- 1. _pg_char_max_length: a varchar's typmod is its length plus four
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 10
-- end-expected
SELECT information_schema._pg_char_max_length('varchar'::regtype::oid, 14) AS a;

-- begin-expected
-- columns: a
-- row: 5
-- end-expected
SELECT information_schema._pg_char_max_length('bpchar'::regtype::oid, 9) AS a;

-- A bit string's typmod is the length itself, with no four to subtract
-- begin-expected
-- columns: a
-- row: 3
-- end-expected
SELECT information_schema._pg_char_max_length('bit'::regtype::oid, 3) AS a;

-- begin-expected
-- columns: a
-- row: 8
-- end-expected
SELECT information_schema._pg_char_max_length('varbit'::regtype::oid, 8) AS a;

-- text has no declared length, and neither has a type that is not a string
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT information_schema._pg_char_max_length('text'::regtype::oid, -1) AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT information_schema._pg_char_max_length('int4'::regtype::oid, -1) AS a;

-- Declared RETURNS NULL ON NULL INPUT
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT information_schema._pg_char_max_length(NULL::oid, 14) AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT information_schema._pg_char_max_length('varchar'::regtype::oid, NULL) AS a;

-- ============================================================================
-- 2. _pg_char_octet_length: four bytes a character under UTF8
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 40
-- end-expected
SELECT information_schema._pg_char_octet_length('varchar'::regtype::oid, 14) AS a;

-- begin-expected
-- columns: a
-- row: 20
-- end-expected
SELECT information_schema._pg_char_octet_length('bpchar'::regtype::oid, 9) AS a;

-- An unbounded string is reported as 2^30 rather than as no answer at all
-- begin-expected
-- columns: a
-- row: 1073741824
-- end-expected
SELECT information_schema._pg_char_octet_length('text'::regtype::oid, -1) AS a;

-- A bit string has a length but no octet length
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT information_schema._pg_char_octet_length('bit'::regtype::oid, 3) AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT information_schema._pg_char_octet_length('int4'::regtype::oid, -1) AS a;

-- ============================================================================
-- 3. _pg_numeric_precision: the integer and float widths are constants
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 16
-- end-expected
SELECT information_schema._pg_numeric_precision('int2'::regtype::oid, -1) AS a;

-- begin-expected
-- columns: a
-- row: 32
-- end-expected
SELECT information_schema._pg_numeric_precision('int4'::regtype::oid, -1) AS a;

-- begin-expected
-- columns: a
-- row: 64
-- end-expected
SELECT information_schema._pg_numeric_precision('int8'::regtype::oid, -1) AS a;

-- begin-expected
-- columns: a
-- row: 24
-- end-expected
SELECT information_schema._pg_numeric_precision('float4'::regtype::oid, -1) AS a;

-- begin-expected
-- columns: a
-- row: 53
-- end-expected
SELECT information_schema._pg_numeric_precision('float8'::regtype::oid, -1) AS a;

-- numeric's precision is packed into the high half of its typmod, less four
-- begin-expected
-- columns: a
-- row: 10
-- end-expected
SELECT information_schema._pg_numeric_precision('numeric'::regtype::oid, 655366) AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT information_schema._pg_numeric_precision('numeric'::regtype::oid, -1) AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT information_schema._pg_numeric_precision('text'::regtype::oid, -1) AS a;

-- ============================================================================
-- 4. _pg_numeric_precision_radix: binary for the machine types, ten for numeric
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT information_schema._pg_numeric_precision_radix('int4'::regtype::oid, -1) AS a;

-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT information_schema._pg_numeric_precision_radix('float8'::regtype::oid, -1) AS a;

-- begin-expected
-- columns: a
-- row: 10
-- end-expected
SELECT information_schema._pg_numeric_precision_radix('numeric'::regtype::oid, -1) AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT information_schema._pg_numeric_precision_radix('text'::regtype::oid, -1) AS a;

-- ============================================================================
-- 5. _pg_numeric_scale: an integer's scale is zero, a float's is nothing at all
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 0
-- end-expected
SELECT information_schema._pg_numeric_scale('int4'::regtype::oid, -1) AS a;

-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT information_schema._pg_numeric_scale('numeric'::regtype::oid, 655366) AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT information_schema._pg_numeric_scale('numeric'::regtype::oid, -1) AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT information_schema._pg_numeric_scale('float8'::regtype::oid, -1) AS a;

-- ============================================================================
-- 6. _pg_datetime_precision: six where none was written, zero for a date
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 0
-- end-expected
SELECT information_schema._pg_datetime_precision('date'::regtype::oid, -1) AS a;

-- begin-expected
-- columns: a
-- row: 6
-- end-expected
SELECT information_schema._pg_datetime_precision('timestamp'::regtype::oid, -1) AS a;

-- begin-expected
-- columns: a
-- row: 3
-- end-expected
SELECT information_schema._pg_datetime_precision('timestamp'::regtype::oid, 3) AS a;

-- begin-expected
-- columns: a
-- row: 4
-- end-expected
SELECT information_schema._pg_datetime_precision('timestamptz'::regtype::oid, 4) AS a;

-- begin-expected
-- columns: a
-- row: 6
-- end-expected
SELECT information_schema._pg_datetime_precision('time'::regtype::oid, -1) AS a;

-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT information_schema._pg_datetime_precision('timetz'::regtype::oid, 2) AS a;

-- An interval's typmod carries a field mask above its precision, and a
-- precision of 0xFFFF means none was written
-- begin-expected
-- columns: a
-- row: 6
-- end-expected
SELECT information_schema._pg_datetime_precision('interval'::regtype::oid, 327679) AS a;

-- begin-expected
-- columns: a
-- row: 3
-- end-expected
SELECT information_schema._pg_datetime_precision('interval'::regtype::oid, 470286339) AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT information_schema._pg_datetime_precision('int4'::regtype::oid, -1) AS a;

-- ============================================================================
-- 7. _pg_interval_type: the qualifier list, upper-cased
-- ============================================================================

-- begin-expected
-- columns: a
-- row: YEAR
-- end-expected
SELECT information_schema._pg_interval_type('interval'::regtype::oid, 327679) AS a;

-- begin-expected
-- columns: a
-- row: DAY TO SECOND(3)
-- end-expected
SELECT information_schema._pg_interval_type('interval'::regtype::oid, 470286339) AS a;

-- A plain interval has no qualifier at all, which is NULL and not the empty
-- string: PostgreSQL reads the qualifier out of format_type's rendering, and
-- an unqualified interval has nothing after the type name to read
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT information_schema._pg_interval_type('interval'::regtype::oid, -1) AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT information_schema._pg_interval_type('int4'::regtype::oid, -1) AS a;

-- ============================================================================
-- 8. _pg_truetypid / _pg_truetypmod over whole catalog rows
-- ============================================================================

DROP TABLE IF EXISTS ish_cols CASCADE;
CREATE TABLE ish_cols (v varchar(10), n numeric(10,2), t text, ts timestamp(3), iv interval year, b int NOT NULL, c int NOT NULL);

-- begin-expected
-- columns: attname,tid,tmod
-- row: v|1043|14
-- row: n|1700|655366
-- row: t|25|-1
-- row: ts|1114|3
-- row: iv|1186|327679
-- row: b|23|-1
-- row: c|23|-1
-- end-expected
SELECT a.attname, information_schema._pg_truetypid(a.*, t.*) AS tid,
       information_schema._pg_truetypmod(a.*, t.*) AS tmod
  FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid
 WHERE a.attrelid = 'ish_cols'::regclass AND a.attnum > 0
 ORDER BY a.attnum;

-- The helpers compose the way information_schema's own views compose them
-- begin-expected
-- columns: attname,len,prec,scale,dtp,ivt
-- row: v|10|NULL|NULL|NULL|NULL
-- row: n|NULL|10|2|NULL|NULL
-- row: t|NULL|NULL|NULL|NULL|NULL
-- row: ts|NULL|NULL|NULL|3|NULL
-- row: iv|NULL|NULL|NULL|6|YEAR
-- row: b|NULL|32|0|NULL|NULL
-- row: c|NULL|32|0|NULL|NULL
-- end-expected
SELECT a.attname,
       information_schema._pg_char_max_length(information_schema._pg_truetypid(a.*, t.*), information_schema._pg_truetypmod(a.*, t.*)) AS len,
       information_schema._pg_numeric_precision(information_schema._pg_truetypid(a.*, t.*), information_schema._pg_truetypmod(a.*, t.*)) AS prec,
       information_schema._pg_numeric_scale(information_schema._pg_truetypid(a.*, t.*), information_schema._pg_truetypmod(a.*, t.*)) AS scale,
       information_schema._pg_datetime_precision(information_schema._pg_truetypid(a.*, t.*), information_schema._pg_truetypmod(a.*, t.*)) AS dtp,
       information_schema._pg_interval_type(information_schema._pg_truetypid(a.*, t.*), information_schema._pg_truetypmod(a.*, t.*)) AS ivt
  FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid
 WHERE a.attrelid = 'ish_cols'::regclass AND a.attnum > 0
 ORDER BY a.attnum;

-- ...and the views they exist for still say the same thing
-- begin-expected
-- columns: column_name,character_maximum_length,character_octet_length,numeric_precision,numeric_precision_radix,numeric_scale,datetime_precision,interval_type
-- row: v|10|40|NULL|NULL|NULL|NULL|NULL
-- row: n|NULL|NULL|10|10|2|NULL|NULL
-- row: t|NULL|1073741824|NULL|NULL|NULL|NULL|NULL
-- row: ts|NULL|NULL|NULL|NULL|NULL|3|NULL
-- row: iv|NULL|NULL|NULL|NULL|NULL|6|YEAR
-- row: b|NULL|NULL|32|2|0|NULL|NULL
-- row: c|NULL|NULL|32|2|0|NULL|NULL
-- end-expected
SELECT column_name, character_maximum_length, character_octet_length, numeric_precision,
       numeric_precision_radix, numeric_scale, datetime_precision, interval_type
  FROM information_schema.columns
 WHERE table_name = 'ish_cols' AND table_schema = 'public'
 ORDER BY ordinal_position;

-- ============================================================================
-- 9. _pg_index_position: where an attribute sits in an index's key list
-- ============================================================================

CREATE INDEX ish_cols_i ON ish_cols (t, n);

-- attnum 3 is t, the first key; attnum 2 is n, the second
-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT information_schema._pg_index_position('ish_cols_i'::regclass::oid, 3::smallint) AS a;

-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT information_schema._pg_index_position('ish_cols_i'::regclass::oid, 2::smallint) AS a;

-- An attribute the index does not cover is not in it at any position
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT information_schema._pg_index_position('ish_cols_i'::regclass::oid, 1::smallint) AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT information_schema._pg_index_position(0::oid, 1::smallint) AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT information_schema._pg_index_position(NULL::oid, 1::smallint) AS a;

-- ============================================================================
-- 10. The helpers work wherever an expression may stand
-- ============================================================================

-- begin-expected
-- columns: x
-- row: 10
-- end-expected
WITH c AS (SELECT information_schema._pg_char_max_length('varchar'::regtype::oid, 14) AS x)
SELECT x FROM c;

-- begin-expected
-- columns: s
-- row: 2
-- end-expected
SELECT * FROM (SELECT information_schema._pg_numeric_scale('numeric'::regtype::oid, 655366) AS s) q
 WHERE q.s = 2;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT 1 AS a WHERE information_schema._pg_numeric_precision('int4'::regtype::oid, -1) = 32;

DROP VIEW IF EXISTS ish_v CASCADE;
CREATE VIEW ish_v AS
SELECT information_schema._pg_numeric_precision('int4'::regtype::oid, -1) AS p,
       information_schema._pg_char_max_length('varchar'::regtype::oid, 14) AS l;

-- begin-expected
-- columns: p,l
-- row: 32|10
-- end-expected
SELECT p, l FROM ish_v;

-- ============================================================================
-- 11. They live in information_schema, and pg_catalog does not hold them
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_catalog._pg_char_max_length
-- end-expected-error
SELECT pg_catalog._pg_char_max_length('varchar'::regtype::oid, 14) AS a;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_catalog._pg_numeric_precision
-- end-expected-error
SELECT pg_catalog._pg_numeric_precision('int4'::regtype::oid, -1) AS a;

-- With information_schema on the search path the unqualified spelling resolves.
-- (memgres answers the unqualified spelling whatever the path says; see the
-- note on requireInformationSchemaVisible for the view that made refusing it
-- worse than the divergence.)
SET search_path = public, information_schema;

-- begin-expected
-- columns: a
-- row: 10
-- end-expected
SELECT _pg_char_max_length('varchar'::regtype::oid, 14) AS a;

-- begin-expected
-- columns: a
-- row: 32
-- end-expected
SELECT _pg_numeric_precision('int4'::regtype::oid, -1) AS a;

-- begin-expected
-- columns: a
-- row: YEAR
-- end-expected
SELECT _pg_interval_type('interval'::regtype::oid, 327679) AS a;

RESET search_path;

-- ============================================================================
-- 12. A call that resolves to no signature of the name
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function information_schema._pg_char_max_length(integer) does not exist
-- end-expected-error
SELECT information_schema._pg_char_max_length(1043) AS a;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function information_schema._pg_char_max_length(integer, integer, integer) does not exist
-- end-expected-error
SELECT information_schema._pg_char_max_length(1043, 14, 1) AS a;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function information_schema._pg_truetypid(integer) does not exist
-- end-expected-error
SELECT information_schema._pg_truetypid(1) AS a;

-- ============================================================================
-- 13. pg_logical_emit_message answers in pg_lsn, in both overloads
-- ============================================================================

-- begin-expected
-- columns: a
-- row: pg_lsn
-- end-expected
SELECT pg_typeof(pg_logical_emit_message(true, 'ish', 'hello'::text)) AS a;

-- begin-expected
-- columns: a
-- row: pg_lsn
-- end-expected
SELECT pg_typeof(pg_logical_emit_message(true, 'ish', '\x0102'::bytea)) AS a;

-- flush carries a default, so the three-argument call resolves too
-- begin-expected
-- columns: a
-- row: pg_lsn
-- end-expected
SELECT pg_typeof(pg_logical_emit_message(true, 'ish', 'hello'::text, false)) AS a;

-- begin-expected
-- columns: a
-- row: pg_lsn
-- end-expected
SELECT pg_typeof(pg_logical_emit_message(true, 'ish', '\x0102'::bytea, true)) AS a;

-- An unknown-typed payload resolves to the text overload without ambiguity
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT pg_logical_emit_message(false, 'ish', 'hello') IS NOT NULL AS a;

-- Declared strict: a NULL anywhere in the call answers NULL
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT pg_logical_emit_message(true, 'ish', NULL::text) IS NULL AS a;

-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT pg_logical_emit_message(NULL, 'ish', 'x') IS NULL AS a;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_logical_emit_message(boolean, unknown) does not exist
-- end-expected-error
SELECT pg_logical_emit_message(true, 'ish') AS a;

DROP VIEW IF EXISTS ish_v CASCADE;
DROP TABLE IF EXISTS ish_cols CASCADE;
