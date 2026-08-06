-- ============================================================================
-- Feature Comparison: what PostgreSQL 18 added
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Some of these were missing outright. The ones worth more attention were the
-- ones that answered: crc32 and reverse are declared over bytea, and reading a
-- bytea through its Java toString hashed and reversed the identity of an array
-- rather than the value, while 256::bytea gave back the characters of "256".
--
-- gamma and lgamma are read here at real precision. The two engines differ in
-- the last bit of a double, and it is PostgreSQL that is out: its answers come
-- from the C library, whose gamma(10) is 362880.00000000006 where the answer
-- is 362880 and a double holds it exactly.
-- ============================================================================

SET search_path = public;

SET TimeZone = 'UTC';

-- ============================================================================
-- gamma and lgamma
-- ============================================================================

-- begin-expected
-- columns: r
-- row: 24
-- end-expected
SELECT gamma(5)::real::text AS r;

-- begin-expected
-- columns: r
-- row: 1
-- end-expected
SELECT gamma(1)::real::text AS r;

-- begin-expected
-- columns: r
-- row: 362880
-- end-expected
SELECT gamma(10)::real::text AS r;

-- begin-expected
-- columns: r
-- row: 1.7724539
-- end-expected
SELECT gamma(0.5)::real::text AS r;

-- begin-expected
-- columns: r
-- row: -3.5449078
-- end-expected
SELECT gamma(-0.5)::real::text AS r;

-- begin-expected
-- columns: r
-- row: 1.3293403
-- end-expected
SELECT gamma(2.5)::real::text AS r;

-- begin-expected
-- columns: r
-- row: 0
-- end-expected
SELECT lgamma(1)::real::text AS r;

-- begin-expected
-- columns: r
-- row: 3.1780539
-- end-expected
SELECT lgamma(5)::real::text AS r;

-- begin-expected
-- columns: r
-- row: 0.5723649
-- end-expected
SELECT lgamma(0.5)::real::text AS r;

-- begin-expected
-- columns: r
-- row: 1.2655121
-- end-expected
SELECT lgamma(-0.5)::real::text AS r;

-- begin-expected
-- columns: r
-- row: 359.13422
-- end-expected
SELECT lgamma(100)::real::text AS r;

-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT gamma(NULL::float8)::text AS r;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: value out of range: overflow
-- end-expected-error
SELECT gamma(0);

-- begin-expected-error
-- sqlstate: 22003
-- message-like: value out of range: overflow
-- end-expected-error
SELECT gamma(-1);

-- begin-expected-error
-- sqlstate: 22003
-- message-like: value out of range: overflow
-- end-expected-error
SELECT gamma(200);

-- begin-expected-error
-- sqlstate: 22003
-- message-like: value out of range: overflow
-- end-expected-error
SELECT lgamma(0);

-- begin-expected
-- columns: r
-- row: Infinity
-- end-expected
SELECT gamma('inf'::float8)::text AS r;

-- begin-expected
-- columns: r
-- row: NaN
-- end-expected
SELECT gamma('nan'::float8)::text AS r;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: value out of range: overflow
-- end-expected-error
SELECT gamma('-inf'::float8);

-- begin-expected
-- columns: r
-- row: double precision
-- end-expected
SELECT pg_typeof(gamma(2))::text AS r;

-- ============================================================================
-- bytea: hashed, reversed, and converted
-- ============================================================================

-- begin-expected
-- columns: r
-- row: 891568578
-- end-expected
SELECT crc32('abc'::bytea)::text AS r;

-- begin-expected
-- columns: r
-- row: 910901175
-- end-expected
SELECT crc32c('abc'::bytea)::text AS r;

-- begin-expected
-- columns: r
-- row: 0
-- end-expected
SELECT crc32(''::bytea)::text AS r;

-- begin-expected
-- columns: r
-- row: 0
-- end-expected
SELECT crc32c(''::bytea)::text AS r;

-- begin-expected
-- columns: r
-- row: 3523407757
-- end-expected
SELECT crc32('\x00'::bytea)::text AS r;

-- begin-expected
-- columns: r
-- row: bigint
-- end-expected
SELECT pg_typeof(crc32('a'::bytea))::text AS r;

-- begin-expected
-- columns: r
-- row: \x030201
-- end-expected
SELECT reverse('\x010203'::bytea)::text AS r;

-- begin-expected
-- columns: r
-- row: 3
-- end-expected
SELECT length(reverse('\x010203'::bytea))::text AS r;

-- begin-expected
-- columns: r
-- row: cba
-- end-expected
SELECT reverse('abc') AS r;

-- begin-expected
-- columns: r
-- row: 256
-- end-expected
SELECT '\x00000100'::bytea::int::text AS r;

-- begin-expected
-- columns: r
-- row: 255
-- end-expected
SELECT '\xff'::bytea::int::text AS r;

-- begin-expected
-- columns: r
-- row: -1
-- end-expected
SELECT '\xffffffff'::bytea::int::text AS r;

-- begin-expected
-- columns: r
-- row: 0
-- end-expected
SELECT '\x'::bytea::int::text AS r;

-- begin-expected
-- columns: r
-- row: 1
-- end-expected
SELECT '\x000001'::bytea::int::text AS r;

-- begin-expected
-- columns: r
-- row: 255
-- end-expected
SELECT '\xff'::bytea::smallint::text AS r;

-- begin-expected
-- columns: r
-- row: -1
-- end-expected
SELECT '\xffff'::bytea::smallint::text AS r;

-- begin-expected
-- columns: r
-- row: -1
-- end-expected
SELECT '\xffffffffffffffff'::bytea::bigint::text AS r;

-- begin-expected
-- columns: r
-- row: \x00000100
-- end-expected
SELECT (256::int)::bytea::text AS r;

-- begin-expected
-- columns: r
-- row: \x0100
-- end-expected
SELECT (256::smallint)::bytea::text AS r;

-- begin-expected
-- columns: r
-- row: \x0000000000000100
-- end-expected
SELECT (256::bigint)::bytea::text AS r;

-- begin-expected
-- columns: r
-- row: \xffffffff
-- end-expected
SELECT ((-1)::int)::bytea::text AS r;

-- begin-expected
-- columns: r
-- row: \x00000000
-- end-expected
SELECT (0::int)::bytea::text AS r;

-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT (NULL::int)::bytea::text AS r;

-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT (NULL::bytea)::int::text AS r;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: integer out of range
-- end-expected-error
SELECT '\x0000000001'::bytea::int;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: smallint out of range
-- end-expected-error
SELECT '\xffffff'::bytea::smallint;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: bigint out of range
-- end-expected-error
SELECT '\xffffffffffffffffff'::bytea::bigint;

-- ============================================================================
-- Roman numerals, and the direction an array is sorted
-- ============================================================================

-- begin-expected
-- columns: r
-- row: 2025
-- end-expected
SELECT to_number('MMXXV', 'RN')::text AS r;

-- begin-expected
-- columns: r
-- row: 4
-- end-expected
SELECT to_number('IV', 'RN')::text AS r;

-- begin-expected
-- columns: r
-- row: 1994
-- end-expected
SELECT to_number('MCMXCIV', 'RN')::text AS r;

-- begin-expected
-- columns: r
-- row: 2025
-- end-expected
SELECT to_number('mmxxv', 'RN')::text AS r;

-- begin-expected
-- columns: r
-- row: 3000
-- end-expected
SELECT to_number('MMM', 'RN')::text AS r;

-- begin-expected
-- columns: r
-- row: 10
-- end-expected
SELECT to_number('XYZ', 'RN')::text AS r;

-- begin-expected
-- columns: r
-- row: 9
-- end-expected
SELECT to_number('IXY', 'RN')::text AS r;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid Roman numeral
-- end-expected-error
SELECT to_number('IIII', 'RN');

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid Roman numeral
-- end-expected-error
SELECT to_number('VV', 'RN');

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid Roman numeral
-- end-expected-error
SELECT to_number('IL', 'RN');

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid Roman numeral
-- end-expected-error
SELECT to_number('Q', 'RN');

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid Roman numeral
-- end-expected-error
SELECT to_number('123', 'RN');

-- begin-expected
-- columns: r
-- row:           MMXXV
-- end-expected
SELECT to_char(2025, 'RN') AS r;

-- begin-expected
-- columns: r
-- row: {1,2,3}
-- end-expected
SELECT array_sort(ARRAY[3,1,2])::text AS r;

-- begin-expected
-- columns: r
-- row: {3,2,1}
-- end-expected
SELECT array_sort(ARRAY[3,1,2], true)::text AS r;

-- begin-expected
-- columns: r
-- row: {1,2,3}
-- end-expected
SELECT array_sort(ARRAY[3,1,2], false)::text AS r;

-- begin-expected
-- columns: r
-- row: {NULL,1,3}
-- end-expected
SELECT array_sort(ARRAY[3,NULL,1], false, true)::text AS r;

-- begin-expected
-- columns: r
-- row: {1,3,NULL}
-- end-expected
SELECT array_sort(ARRAY[3,NULL,1], false, false)::text AS r;

-- begin-expected
-- columns: r
-- row: {NULL,3,1}
-- end-expected
SELECT array_sort(ARRAY[3,NULL,1], true, true)::text AS r;

-- begin-expected
-- columns: r
-- row: {NULL,3,1}
-- end-expected
SELECT array_sort(ARRAY[3,NULL,1], true)::text AS r;

-- begin-expected
-- columns: r
-- row: {3,2,1}
-- end-expected
SELECT array_reverse(ARRAY[1,2,3])::text AS r;

-- ============================================================================
-- What a server says about itself
-- ============================================================================

-- begin-expected
-- columns: r
-- row: 79
-- end-expected
SELECT count(*)::text AS r FROM pg_stat_io;

-- begin-expected
-- columns: r
-- row: 14
-- end-expected
SELECT count(DISTINCT backend_type)::text AS r FROM pg_stat_io;

-- begin-expected
-- columns: r
-- row: 3
-- end-expected
SELECT count(DISTINCT object)::text AS r FROM pg_stat_io;

-- begin-expected
-- columns: r
-- row: 5
-- end-expected
SELECT count(DISTINCT context)::text AS r FROM pg_stat_io;

-- begin-expected
-- columns: r
-- row: 8
-- end-expected
SELECT count(*)::text AS r FROM pg_stat_io WHERE backend_type = 'client backend';

-- begin-expected
-- columns: r
-- row: 0
-- end-expected
SELECT count(*)::text AS r FROM pg_aios;

-- begin-expected
-- columns: r
-- row: 8
-- end-expected
SELECT count(*)::text AS r FROM pg_stat_get_backend_io(pg_backend_pid());

-- begin-expected
-- columns: r
-- row: 0
-- end-expected
SELECT count(*)::text AS r FROM pg_stat_get_backend_io(999999);

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT (count(*) >= 0)::text AS r FROM pg_get_loaded_modules();

-- begin-expected
-- columns: r
-- row: NULL
-- end-expected
SELECT pg_column_toast_chunk_id('x'::text)::text AS r;

-- begin-expected
-- columns: r
-- row: oid
-- end-expected
SELECT pg_typeof(pg_column_toast_chunk_id(NULL::text))::text AS r;

-- begin-expected
-- columns: r
-- row: worker
-- end-expected
SELECT current_setting('io_method') AS r;

-- begin-expected
-- columns: r
-- row: copy
-- end-expected
SELECT current_setting('file_copy_method') AS r;

-- begin-expected
-- columns: r
-- row: 100000000
-- end-expected
SELECT current_setting('autovacuum_vacuum_max_threshold') AS r;

-- begin-expected
-- columns: r
-- row: enum
-- end-expected
SELECT vartype AS r FROM pg_settings WHERE name = 'io_method';

-- begin-expected
-- columns: r
-- row: {sync,worker}
-- end-expected
SELECT enumvals::text AS r FROM pg_settings WHERE name = 'io_method';

-- begin-expected
-- columns: r
-- row: Resource Usage / Disk
-- end-expected
SELECT category AS r FROM pg_settings WHERE name = 'file_copy_method';

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT (setting IS NOT NULL)::text AS r FROM pg_settings WHERE name = 'num_os_semaphores';

-- ============================================================================
-- A foreign key over a period
-- ============================================================================

CREATE TABLE b12_par (id int, v daterange, PRIMARY KEY (id, v WITHOUT OVERLAPS));

INSERT INTO b12_par VALUES (1, daterange('2020-01-01','2020-06-01'));

INSERT INTO b12_par VALUES (1, daterange('2020-06-01','2021-01-01'));

INSERT INTO b12_par VALUES (2, daterange('2020-01-01','2020-02-01'));

CREATE TABLE b12_chi (id int, v daterange, FOREIGN KEY (id, PERIOD v) REFERENCES b12_par (id, PERIOD v));

-- inside one referenced row
INSERT INTO b12_chi VALUES (1, daterange('2020-02-01','2020-03-01'));

-- across two that meet exactly
INSERT INTO b12_chi VALUES (1, daterange('2020-05-01','2020-08-01'));

-- the whole of what is covered
INSERT INTO b12_chi VALUES (1, daterange('2020-01-01','2021-01-01'));

-- past the end of it
-- begin-expected-error
-- sqlstate: 23503
-- message-like: insert or update on table "b12_chi" violates foreign key constraint "b12_chi_id_v_fkey"
-- end-expected-error
INSERT INTO b12_chi VALUES (1, daterange('2020-05-01','2021-06-01'));

-- a key nothing references
-- begin-expected-error
-- sqlstate: 23503
-- message-like: insert or update on table "b12_chi" violates foreign key constraint "b12_chi_id_v_fkey"
-- end-expected-error
INSERT INTO b12_chi VALUES (9, daterange('2020-05-01','2020-06-01'));

-- covered in part
-- begin-expected-error
-- sqlstate: 23503
-- message-like: insert or update on table "b12_chi" violates foreign key constraint "b12_chi_id_v_fkey"
-- end-expected-error
INSERT INTO b12_chi VALUES (2, daterange('2020-01-01','2020-03-01'));

-- an empty period is covered by nothing
-- begin-expected-error
-- sqlstate: 23503
-- message-like: insert or update on table "b12_chi" violates foreign key constraint "b12_chi_id_v_fkey"
-- end-expected-error
INSERT INTO b12_chi VALUES (3, daterange('2020-01-01','2020-01-01'));

-- a null references nothing and is asked nothing
INSERT INTO b12_chi VALUES (NULL, daterange('2020-01-01','2020-02-01'));

INSERT INTO b12_chi VALUES (1, NULL);

-- begin-expected
-- columns: r
-- row: 5
-- end-expected
SELECT count(*)::text AS r FROM b12_chi;

-- taking away half of what covers a child
-- begin-expected-error
-- sqlstate: 23503
-- message-like: update or delete on table "b12_par" violates foreign key constraint "b12_chi_id_v_fkey" on table "b12_chi"
-- end-expected-error
DELETE FROM b12_par WHERE v = daterange('2020-01-01','2020-06-01');

-- moving one out from under it
-- begin-expected-error
-- sqlstate: 23503
-- message-like: update or delete on table "b12_par" violates foreign key constraint "b12_chi_id_v_fkey" on table "b12_chi"
-- end-expected-error
UPDATE b12_par SET id = 5 WHERE id = 1 AND v = daterange('2020-06-01','2021-01-01');

-- one no child needs
DELETE FROM b12_par WHERE id = 2;

-- begin-expected
-- columns: r
-- row: 2
-- end-expected
SELECT count(*)::text AS r FROM b12_par;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT conperiod::text AS r FROM pg_constraint WHERE conrelid = 'b12_chi'::regclass AND contype = 'f';

-- begin-expected
-- columns: r
-- row: FOREIGN KEY (id, PERIOD v) REFERENCES b12_par(id, PERIOD v)
-- end-expected
SELECT pg_get_constraintdef(oid) AS r FROM pg_constraint WHERE conrelid = 'b12_chi'::regclass AND contype = 'f';

CREATE TABLE b12_plain (id int primary key, v daterange);

-- begin-expected-error
-- sqlstate: 42830
-- message-like: there is no unique constraint matching given keys for referenced table "b12_plain"
-- end-expected-error
CREATE TABLE b12_bad (id int, v daterange, FOREIGN KEY (id, PERIOD v) REFERENCES b12_plain (id, PERIOD v));

-- begin-expected-error
-- sqlstate: 42830
-- message-like: foreign key uses PERIOD on the referencing table but not the referenced table
-- end-expected-error
CREATE TABLE b12_bad2 (id int, v daterange, FOREIGN KEY (id, PERIOD v) REFERENCES b12_par (id, v));

-- begin-expected-error
-- sqlstate: 42804
-- message-like: foreign key constraint "b12_bad3_id_v_fkey" cannot be implemented
-- end-expected-error
CREATE TABLE b12_bad3 (id int, v int, FOREIGN KEY (id, PERIOD v) REFERENCES b12_par (id, PERIOD v));

-- PERIOD is not a reserved word, so a column may be called it
CREATE TABLE b12_named (period int PRIMARY KEY);

CREATE TABLE b12_nref (period int, FOREIGN KEY (period) REFERENCES b12_named (period));

INSERT INTO b12_named VALUES (1);

INSERT INTO b12_nref VALUES (1);

-- begin-expected-error
-- sqlstate: 23503
-- message-like: insert or update on table "b12_nref" violates foreign key constraint "b12_nref_period_fkey"
-- end-expected-error
INSERT INTO b12_nref VALUES (2);

DROP TABLE b12_nref;

DROP TABLE b12_named;

DROP TABLE b12_plain;

DROP TABLE b12_chi;

DROP TABLE b12_par;

-- ============================================================================
-- The rest of the 18 surface, which already answered as it should
-- ============================================================================

-- begin-expected
-- columns: r
-- row: 4
-- end-expected
SELECT extract(week from interval '30 days')::text AS r;

-- begin-expected
-- columns: r
-- row: 2
-- end-expected
SELECT extract(week from interval '3 months 20 days')::text AS r;

-- begin-expected
-- columns: r
-- row: abc
-- end-expected
SELECT casefold('ABC') AS r;

-- begin-expected
-- columns: r
-- row: 7
-- end-expected
SELECT uuid_extract_version(uuidv7())::text AS r;

-- begin-expected
-- columns: r
-- row: {}
-- end-expected
SELECT jsonb_strip_nulls('{"a":null}'::jsonb, true)::text AS r;

-- begin-expected
-- columns: r
-- row: 4
-- end-expected
SELECT bit_count('\x0f'::bytea)::text AS r;

-- begin-expected
-- columns: r
-- row: {1,2}
-- end-expected
SELECT min(ARRAY[1,2])::text AS r;

-- begin-expected
-- columns: r
-- row: axc
-- end-expected
SELECT regexp_replace('abc', 'b', 'x', start => 1)::text AS r;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT (relallfrozen IS NOT NULL)::text AS r FROM pg_class WHERE relname = 'pg_class';

-- begin-expected
-- columns: r
-- row: 0
-- end-expected
SELECT count(*)::text AS r FROM pg_attribute WHERE attname = 'attcacheoff';

CREATE TABLE b12_wo (i int, d daterange, UNIQUE (i, d WITHOUT OVERLAPS));

DROP TABLE b12_wo;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: partitioned tables cannot be unlogged
-- end-expected-error
CREATE UNLOGGED TABLE b12_ul (i int) PARTITION BY RANGE (i);

