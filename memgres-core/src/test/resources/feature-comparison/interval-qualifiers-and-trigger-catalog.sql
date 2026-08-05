-- ============================================================================
-- Feature Comparison: interval field qualifiers, format_type typmods,
--                     and the pg_trigger columns a trigger really fills
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Three subjects that all turn on a modifier being read the way PostgreSQL
-- reads it:
--   * a two-part time field in an interval literal is MINUTE:SECOND, not
--     HOUR:MINUTE, as soon as its second part carries a fraction;
--   * an interval typmod is a field mask, and format_type refuses one that
--     names no qualifier rather than printing 'interval(3)';
--   * a trigger's arguments and UPDATE OF column list are catalog content,
--     and a foreign key installs four internal triggers of its own.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / sqlstate: / message-like: / end-expected-error
-- ============================================================================

DROP SCHEMA IF EXISTS ivqual CASCADE;
CREATE SCHEMA ivqual;
SET search_path = ivqual, public;

-- ============================================================================
-- SECTION A: a two-part time field with a fraction is MINUTE:SECOND
-- ============================================================================

-- 1. The plain form: '3:04' is hours and minutes ...

-- begin-expected
-- columns: v
-- row: 03:04:00
-- end-expected
SELECT CAST('3:04' AS interval)::text AS v;

-- 2. ... but '3:04.5678' is minutes and seconds, because only the seconds
--    field can carry a fraction.

-- begin-expected
-- columns: v
-- row: 00:03:04.5678
-- end-expected
SELECT CAST('3:04.5678' AS interval)::text AS v;

-- 3. The sign on the field covers the whole field, fraction included.

-- begin-expected
-- columns: v
-- row: -00:03:04.5678
-- end-expected
SELECT CAST('-3:04.5678' AS interval)::text AS v;

-- 4. A trailing dot is an empty fraction, and still switches the reading.

-- begin-expected
-- columns: v
-- row: 00:03:04
-- end-expected
SELECT CAST('3:04.' AS interval)::text AS v;

-- 5. Sixty seconds is in range and carries into the minutes.

-- begin-expected
-- columns: v
-- row: 00:04:00.5
-- end-expected
SELECT CAST('3:60.5' AS interval)::text AS v;

-- 6. The fraction is rounded to microseconds, and may round up a whole second.

-- begin-expected
-- columns: v
-- row: 00:12:35
-- end-expected
SELECT CAST('12:34.9999995' AS interval)::text AS v;

-- 7. A day count written before the time field stays a day count.

-- begin-expected
-- columns: v
-- row: 1 day 00:03:04.5678
-- end-expected
SELECT CAST('1 day 3:04.5678' AS interval)::text AS v;

-- 8. 'ago' negates everything, including a time field.

-- begin-expected
-- columns: v
-- row: -00:03:04.5678
-- end-expected
SELECT CAST('3:04.5678 ago' AS interval)::text AS v;

-- 9. An unlabelled number standing between a year-month field and a time of
--    day is a day count.

-- begin-expected
-- columns: v
-- row: 1 year 2 mons 3 days 00:04:05.678
-- end-expected
SELECT CAST('1-2 3 4:05.678' AS interval)::text AS v;

-- begin-expected
-- columns: v
-- row: 3 days 00:04:05.678
-- end-expected
SELECT CAST('3 4:05.678' AS interval)::text AS v;

-- 10. With no time of day after it, the same number is a count of seconds.

-- begin-expected
-- columns: v
-- row: 1 year 2 mons 00:00:03
-- end-expected
SELECT CAST('1-2 3' AS interval)::text AS v;

-- 11. A unit word standing to the right of a time field names nothing, so it
--     is ignored rather than applied to the field.

-- begin-expected
-- columns: v
-- row: 00:03:04.5678
-- end-expected
SELECT CAST('3:04.5678 hour' AS interval)::text AS v;

-- 12. The minutes of a time field may not exceed 59 ...

-- begin-expected-error
-- sqlstate: 22015
-- message-like: interval field value out of range
-- end-expected-error
SELECT CAST('100:04.5' AS interval)::text AS v;

-- begin-expected-error
-- sqlstate: 22015
-- message-like: interval field value out of range
-- end-expected-error
SELECT CAST('3:60' AS interval)::text AS v;

-- begin-expected-error
-- sqlstate: 22015
-- message-like: interval field value out of range
-- end-expected-error
SELECT CAST('3:70:05' AS interval)::text AS v;

-- 13. ... and no field may be filled twice.

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval
-- end-expected-error
SELECT CAST('1 hour 2 hour' AS interval)::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval
-- end-expected-error
SELECT CAST('1 day 1 day' AS interval)::text AS v;

-- 14. Two different unit words for the same interval field are not the same
--     field, so these are accepted.

-- begin-expected
-- columns: v
-- row: 9 days
-- end-expected
SELECT CAST('1 week 2 days' AS interval)::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:01.5
-- end-expected
SELECT CAST('1 second 500 milliseconds' AS interval)::text AS v;

-- ============================================================================
-- SECTION B: the same literal read against a field qualifier
-- ============================================================================

-- begin-expected
-- columns: v
-- row: 00:03:04.5678
-- end-expected
SELECT CAST('3:04.5678' AS interval minute to second)::text AS v;

-- begin-expected
-- columns: v
-- row: 00:03:04.57
-- end-expected
SELECT CAST('3:04.5678' AS interval minute to second(2))::text AS v;

-- begin-expected
-- columns: v
-- row: 00:03:00
-- end-expected
SELECT CAST('3:04.5678' AS interval hour to minute)::text AS v;

-- begin-expected
-- columns: v
-- row: 00:03:04.568
-- end-expected
SELECT CAST('3:04.5678' AS interval hour to second(3))::text AS v;

-- begin-expected
-- columns: v
-- row: 00:03:04.57
-- end-expected
SELECT CAST('3:04.5678' AS interval day to second(2))::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00
-- end-expected
SELECT CAST('3:04.5678' AS interval day to hour)::text AS v;

-- begin-expected
-- columns: v
-- row: 1 day 00:03:04.5678
-- end-expected
SELECT CAST('1 3:04.5678' AS interval day to second)::text AS v;

-- Without a fraction, MINUTE TO SECOND is the only qualifier that reads the
-- two parts as minutes and seconds.

-- begin-expected
-- columns: v
-- row: 00:03:04
-- end-expected
SELECT CAST('3:04' AS interval minute to second)::text AS v;

-- begin-expected
-- columns: v
-- row: 03:04:00
-- end-expected
SELECT CAST('3:04' AS interval hour to second)::text AS v;

-- The written-literal spelling agrees with the cast.

-- begin-expected
-- columns: v
-- row: 00:03:04.57
-- end-expected
SELECT (INTERVAL '3:04.5678' MINUTE TO SECOND(2))::text AS v;

-- ============================================================================
-- SECTION C: interval field qualifiers in a declaration
-- ============================================================================

CREATE TABLE iv_cols (
    a interval day to second(2),
    b interval year to month,
    c interval day to hour,
    d interval day to minute,
    e interval hour to second(3),
    f interval minute to second(6),
    g interval second(2),
    h interval(4)
);

-- The typmod is what format_type reads back, so the declaration survives a
-- round trip through the catalog.

-- begin-expected
-- columns: attname, t
-- row: a, interval day to second(2)
-- row: b, interval year to month
-- row: c, interval day to hour
-- row: d, interval day to minute
-- row: e, interval hour to second(3)
-- row: f, interval minute to second(6)
-- row: g, interval second(2)
-- row: h, interval(4)
-- end-expected
SELECT attname::text AS attname, format_type(atttypid, atttypmod) AS t
FROM pg_attribute WHERE attrelid = 'iv_cols'::regclass AND attnum > 0 ORDER BY attnum;

-- begin-expected
-- columns: attname, m
-- row: a, 470286338
-- row: b, 458751
-- row: c, 67698687
-- row: d, 201916415
-- row: e, 469762051
-- row: f, 402653190
-- row: g, 268435458
-- row: h, 2147418116
-- end-expected
SELECT attname::text AS attname, atttypmod AS m
FROM pg_attribute WHERE attrelid = 'iv_cols'::regclass AND attnum > 0 ORDER BY attnum;

-- information_schema reports the qualifier in its own column, and
-- interval_precision is null for every one of them.

-- begin-expected
-- columns: column_name, data_type, interval_type, interval_precision, datetime_precision
-- row: a, interval, DAY TO SECOND(2), NULL, 2
-- row: b, interval, YEAR TO MONTH, NULL, 6
-- row: c, interval, DAY TO HOUR, NULL, 6
-- row: d, interval, DAY TO MINUTE, NULL, 6
-- row: e, interval, HOUR TO SECOND(3), NULL, 3
-- row: f, interval, MINUTE TO SECOND(6), NULL, 6
-- row: g, interval, SECOND(2), NULL, 2
-- row: h, interval, NULL, NULL, 4
-- end-expected
SELECT column_name::text AS column_name, data_type::text AS data_type,
       interval_type::text AS interval_type, interval_precision AS interval_precision,
       datetime_precision AS datetime_precision
FROM information_schema.columns
WHERE table_schema = 'ivqual' AND table_name = 'iv_cols' ORDER BY ordinal_position;

-- A qualifier truncates the fields below it when the value is stored.

INSERT INTO iv_cols VALUES ('1 day 2:03:04.5678', '1 year 2 mons 3 days',
                            '1 day 2:03:04', '1 day 2:03:04',
                            '2:03:04.5678', '2:03:04.5678', '4.5678 seconds',
                            '1 day 2:03:04.5678');

-- begin-expected
-- columns: a, b, c
-- row: 1 day 02:03:04.57, 1 year 2 mons, 1 day 02:00:00
-- end-expected
SELECT a::text AS a, b::text AS b, c::text AS c FROM iv_cols;

-- begin-expected
-- columns: d, e, f
-- row: 1 day 02:03:00, 02:03:04.568, 02:03:04.5678
-- end-expected
SELECT d::text AS d, e::text AS e, f::text AS f FROM iv_cols;

-- A domain over a qualified interval keeps the qualifier.

CREATE DOMAIN iv_dom AS interval day to second(2);

-- begin-expected
-- columns: data_type, interval_type, datetime_precision
-- row: interval, DAY TO SECOND(2), 2
-- end-expected
SELECT data_type::text AS data_type, interval_type::text AS interval_type,
       datetime_precision AS datetime_precision
FROM information_schema.domains WHERE domain_name = 'iv_dom';

-- ============================================================================
-- SECTION D: format_type on interval typmods
-- ============================================================================

-- begin-expected
-- columns: t
-- row: interval day to second(2)
-- end-expected
SELECT format_type('interval'::regtype, 470286338) AS t;

-- begin-expected
-- columns: t
-- row: interval day to second(2)[]
-- end-expected
SELECT format_type('interval[]'::regtype, 470286338) AS t;

-- begin-expected
-- columns: t
-- row: interval year to month
-- end-expected
SELECT format_type('interval'::regtype, 458751) AS t;

-- begin-expected
-- columns: t
-- row: interval hour to minute
-- end-expected
SELECT format_type('interval'::regtype, 201392127) AS t;

-- begin-expected
-- columns: t
-- row: interval second(2)
-- end-expected
SELECT format_type('interval'::regtype, 268435458) AS t;

-- begin-expected
-- columns: t
-- row: interval(4)
-- end-expected
SELECT format_type('interval'::regtype, 2147418116) AS t;

-- begin-expected
-- columns: t
-- row: interval
-- end-expected
SELECT format_type('interval'::regtype, -1) AS t;

-- The high half of an interval typmod is a field mask. A number that names no
-- qualifier is not a typmod at all, and printing it as 'interval(3)' would be
-- a type nothing could be declared as.

-- begin-expected-error
-- sqlstate: XX000
-- message-like: invalid INTERVAL typmod: 0x3
-- end-expected-error
SELECT format_type('interval'::regtype, 3) AS t;

-- begin-expected-error
-- sqlstate: XX000
-- message-like: invalid INTERVAL typmod: 0x3
-- end-expected-error
SELECT format_type('interval[]'::regtype, 3) AS t;

-- begin-expected-error
-- sqlstate: XX000
-- message-like: invalid INTERVAL typmod: 0x0
-- end-expected-error
SELECT format_type('interval'::regtype, 0) AS t;

-- begin-expected-error
-- sqlstate: XX000
-- message-like: invalid INTERVAL typmod: 0x7fff
-- end-expected-error
SELECT format_type('interval'::regtype, 32767) AS t;

-- begin-expected-error
-- sqlstate: XX000
-- message-like: invalid INTERVAL typmod: 0xffff
-- end-expected-error
SELECT format_type('interval'::regtype, 65535) AS t;

-- ============================================================================
-- SECTION E: format_type and regtype on array types
-- ============================================================================

-- A typmod applies to the element of an array type.

-- begin-expected
-- columns: t
-- row: character varying(10)[]
-- end-expected
SELECT format_type('varchar[]'::regtype, 14) AS t;

-- begin-expected
-- columns: t
-- row: numeric(10,2)[]
-- end-expected
SELECT format_type('numeric[]'::regtype, 655366) AS t;

-- begin-expected
-- columns: t
-- row: character varying(10)
-- end-expected
SELECT format_type('varchar'::regtype, 14) AS t;

-- begin-expected
-- columns: t
-- row: numeric(10,2)
-- end-expected
SELECT format_type('numeric'::regtype, 655366) AS t;

-- The bit string array types are types like any other.

-- begin-expected
-- columns: t
-- row: bit(8)[]
-- end-expected
SELECT format_type('bit[]'::regtype, 8) AS t;

-- begin-expected
-- columns: t
-- row: bit varying(8)[]
-- end-expected
SELECT format_type('varbit[]'::regtype, 8) AS t;

-- begin-expected
-- columns: a, b, c
-- row: 1561, 1563, 1017
-- end-expected
SELECT 'bit[]'::regtype::oid AS a, 'varbit[]'::regtype::oid AS b,
       'point[]'::regtype::oid AS c;

-- begin-expected
-- columns: a, b
-- row: 1561, 651
-- end-expected
SELECT to_regtype('bit[]')::oid AS a, to_regtype('cidr[]')::oid AS b;

-- to_regtype answers a regtype, so it can be asked for its OID as well as its
-- name, and it answers nothing at all for a name that is no type.

-- begin-expected
-- columns: a
-- row: character varying[]
-- end-expected
SELECT to_regtype('varchar[]')::text AS a;

-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT to_regtype('nosuchtype[]') AS a;

-- ============================================================================
-- SECTION F: what a trigger records in pg_trigger
-- ============================================================================

CREATE TABLE trg_t (id int, v text, w text);
CREATE FUNCTION trg_f() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql;
CREATE TRIGGER trg_1 BEFORE INSERT ON trg_t FOR EACH ROW EXECUTE FUNCTION trg_f();
CREATE TRIGGER trg_2 BEFORE UPDATE OF v, w ON trg_t FOR EACH ROW EXECUTE FUNCTION trg_f('x');
CREATE TRIGGER trg_3 BEFORE UPDATE OF v ON trg_t FOR EACH ROW WHEN (OLD.v IS DISTINCT FROM NEW.v) EXECUTE FUNCTION trg_f();
CREATE TRIGGER trg_4 BEFORE INSERT ON trg_t FOR EACH ROW EXECUTE FUNCTION trg_f('abc','d');

-- The arguments are stored as one bytea of NUL-terminated strings, and
-- tgnargs counts them.

-- begin-expected
-- columns: tgname, tgnargs, args, len
-- row: trg_1, 0, , 0
-- row: trg_2, 1, x\000, 2
-- row: trg_3, 0, , 0
-- row: trg_4, 2, abc\000d\000, 6
-- end-expected
SELECT tgname::text AS tgname, tgnargs AS tgnargs, encode(tgargs, 'escape') AS args,
       length(tgargs) AS len
FROM pg_trigger WHERE tgrelid = 'trg_t'::regclass ORDER BY tgname;

-- tgattr holds the attnums of an UPDATE OF list. A trigger without one has an
-- empty vector, not a null.

-- begin-expected
-- columns: tgname, attr, n
-- row: trg_1, , 0
-- row: trg_2, 2 3, 2
-- row: trg_3, 2, 1
-- row: trg_4, , 0
-- end-expected
SELECT tgname::text AS tgname, tgattr::text AS attr, array_length(tgattr, 1) AS n
FROM pg_trigger WHERE tgrelid = 'trg_t'::regclass ORDER BY tgname;

-- begin-expected
-- columns: a0, a1
-- row: 2, 3
-- end-expected
SELECT tgattr[0] AS a0, tgattr[1] AS a1 FROM pg_trigger WHERE tgname = 'trg_2';

-- The reconstructed definition carries the arguments, so a dump and restore
-- gives the trigger function the same TG_ARGV it had.

-- begin-expected
-- columns: d
-- row: CREATE TRIGGER trg_2 BEFORE UPDATE OF v, w ON ivqual.trg_t FOR EACH ROW EXECUTE FUNCTION trg_f('x')
-- end-expected
SELECT pg_get_triggerdef(oid) AS d FROM pg_trigger WHERE tgname = 'trg_2';

-- begin-expected
-- columns: d
-- row: CREATE TRIGGER trg_4 BEFORE INSERT ON ivqual.trg_t FOR EACH ROW EXECUTE FUNCTION trg_f('abc', 'd')
-- end-expected
SELECT pg_get_triggerdef(oid) AS d FROM pg_trigger WHERE tgname = 'trg_4';

-- A WHEN condition is printed back from the catalog, in its own spelling:
-- lower-case row references and a second pair of parentheses.

-- begin-expected
-- columns: d
-- row: CREATE TRIGGER trg_3 BEFORE UPDATE OF v ON ivqual.trg_t FOR EACH ROW WHEN ((old.v IS DISTINCT FROM new.v)) EXECUTE FUNCTION trg_f()
-- end-expected
SELECT pg_get_triggerdef(oid) AS d FROM pg_trigger WHERE tgname = 'trg_3';

-- ============================================================================
-- SECTION G: the internal triggers a foreign key installs
-- ============================================================================

CREATE TABLE fk_p (id int primary key);
CREATE TABLE fk_c (id int references fk_p(id));

-- begin-expected
-- columns: n
-- row: 4
-- end-expected
SELECT count(*)::int AS n FROM pg_trigger
WHERE tgrelid IN ('fk_p'::regclass, 'fk_c'::regclass) AND tgisinternal;

-- An INSERT and an UPDATE check on the referencing table; a DELETE and an
-- UPDATE action on the referenced one.

-- begin-expected
-- columns: rel, tgtype, isint, nargs, hascon, enabled
-- row: fk_c, 5, true, 0, true, O
-- row: fk_c, 17, true, 0, true, O
-- row: fk_p, 9, true, 0, true, O
-- row: fk_p, 17, true, 0, true, O
-- end-expected
SELECT tgrelid::regclass::text AS rel, tgtype AS tgtype, tgisinternal AS isint,
       tgnargs AS nargs, tgconstraint <> 0 AS hascon, tgenabled::text AS enabled
FROM pg_trigger WHERE tgrelid IN ('fk_p'::regclass, 'fk_c'::regclass) ORDER BY 1, 2;

-- They are named after themselves, point at the other table, and hang off the
-- constraint's index.

-- begin-expected
-- columns: rel, namefmt, other, hasidx, qualnull
-- row: fk_c, true, fk_p, true, true
-- row: fk_c, true, fk_p, true, true
-- row: fk_p, true, fk_c, true, true
-- row: fk_p, true, fk_c, true, true
-- end-expected
SELECT tgrelid::regclass::text AS rel,
       tgname ~ '^RI_ConstraintTrigger_[ac]_[0-9]+$' AS namefmt,
       tgconstrrelid::regclass::text AS other, tgconstrindid <> 0 AS hasidx,
       tgqual IS NULL AS qualnull
FROM pg_trigger WHERE tgrelid IN ('fk_p'::regclass, 'fk_c'::regclass) ORDER BY 1, 2;

-- Every one of them belongs to the foreign key constraint.

-- begin-expected
-- columns: matched, n
-- row: true, 4
-- end-expected
SELECT c.conname = (SELECT conname FROM pg_constraint
                    WHERE conrelid = 'fk_c'::regclass AND contype = 'f') AS matched,
       count(*)::int AS n
FROM pg_trigger t JOIN pg_constraint c ON c.oid = t.tgconstraint
WHERE t.tgrelid IN ('fk_p'::regclass, 'fk_c'::regclass) GROUP BY 1;

-- The check triggers carry the constraint's deferrability, and so do the
-- action triggers as long as the action is NO ACTION.

CREATE TABLE fk_p2 (id int primary key);
CREATE TABLE fk_c2 (id int references fk_p2(id) DEFERRABLE INITIALLY DEFERRED);

-- begin-expected
-- columns: rel, tgtype, def, initdef
-- row: fk_c2, 5, true, true
-- row: fk_c2, 17, true, true
-- row: fk_p2, 9, true, true
-- row: fk_p2, 17, true, true
-- end-expected
SELECT tgrelid::regclass::text AS rel, tgtype AS tgtype, tgdeferrable AS def,
       tginitdeferred AS initdef
FROM pg_trigger WHERE tgrelid IN ('fk_p2'::regclass, 'fk_c2'::regclass) ORDER BY 1, 2;

-- A cascade or a set-null has work to do as the row changes, so its trigger
-- cannot be deferred whatever the constraint said.

CREATE TABLE fk_p3 (id int primary key);
CREATE TABLE fk_c3 (id int references fk_p3(id)
                    ON DELETE CASCADE ON UPDATE SET NULL DEFERRABLE INITIALLY DEFERRED);

-- begin-expected
-- columns: rel, tgtype, def, initdef
-- row: fk_c3, 5, true, true
-- row: fk_c3, 17, true, true
-- row: fk_p3, 9, false, false
-- row: fk_p3, 17, false, false
-- end-expected
SELECT tgrelid::regclass::text AS rel, tgtype AS tgtype, tgdeferrable AS def,
       tginitdeferred AS initdef
FROM pg_trigger WHERE tgrelid IN ('fk_p3'::regclass, 'fk_c3'::regclass) ORDER BY 1, 2;

-- ============================================================================
-- SECTION H: pg_proc keeps no typmod for a function's arguments
-- ============================================================================

CREATE FUNCTION fn_iv(p interval day to second(2)) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;
CREATE FUNCTION fn_iv2(p interval hour to minute) RETURNS int AS $$ SELECT 2 $$ LANGUAGE sql;
CREATE FUNCTION fn_mod(p varchar(10), q numeric(5,2)) RETURNS int AS $$ SELECT 3 $$ LANGUAGE sql;

-- begin-expected
-- columns: a
-- row: p interval
-- end-expected
SELECT pg_get_function_arguments(oid) AS a FROM pg_proc WHERE proname = 'fn_iv';

-- begin-expected
-- columns: a
-- row: p interval
-- end-expected
SELECT pg_get_function_arguments(oid) AS a FROM pg_proc WHERE proname = 'fn_iv2';

-- begin-expected
-- columns: a
-- row: p character varying, q numeric
-- end-expected
SELECT pg_get_function_arguments(oid) AS a FROM pg_proc WHERE proname = 'fn_mod';

-- begin-expected
-- columns: a
-- row: p character varying, q numeric
-- end-expected
SELECT pg_get_function_identity_arguments(oid) AS a FROM pg_proc WHERE proname = 'fn_mod';

-- The argument still accepts the value the qualifier would have narrowed,
-- because the qualifier was never part of the argument's type.

-- begin-expected
-- columns: v
-- row: 1
-- end-expected
SELECT fn_iv('1 day 2:03:04.5678') AS v;

-- ============================================================================
-- SECTION I: an interval literal has to name a quantity
-- ============================================================================
-- A unit word with no number in front of it is not an interval of zero. Each
-- of these used to decode to 00:00:00 and a column would store it.

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval
-- end-expected-error
SELECT CAST('day' AS interval) AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval
-- end-expected-error
SELECT CAST('years' AS interval) AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval
-- end-expected-error
SELECT CAST('millennium' AS interval) AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval
-- end-expected-error
SELECT CAST('ago' AS interval) AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval
-- end-expected-error
SELECT CAST('@' AS interval) AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval
-- end-expected-error
SELECT CAST('@ ago' AS interval) AS v;

-- Reached through a column, where it used to store a quantity nobody wrote.

CREATE TABLE ivz (id int, v interval);

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval
-- end-expected-error
INSERT INTO ivz VALUES (1, 'day');

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM ivz;

DROP TABLE ivz;

-- Reached through date arithmetic.

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval
-- end-expected-error
SELECT DATE '2020-01-01' + CAST('day' AS interval) AS v;

-- ============================================================================
-- SECTION J: 'ago' is the last word of a literal, and names no unit
-- ============================================================================
-- 'ago' stands where the unit for the number to its left would have been, so
-- that number has no unit at all.

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval
-- end-expected-error
SELECT CAST('1 ago' AS interval) AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval
-- end-expected-error
SELECT CAST('1 day 2 ago' AS interval) AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval
-- end-expected-error
SELECT CAST('1 ago' AS interval day to second) AS v;

-- Nothing may follow it, and it may not be written twice.

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval
-- end-expected-error
SELECT CAST('ago 1 day' AS interval) AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval
-- end-expected-error
SELECT CAST('1 day ago 2 hours' AS interval) AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval
-- end-expected-error
SELECT CAST('1 day ago ago' AS interval) AS v;

-- What it does turn around still turns around.

-- begin-expected
-- columns: v
-- row: -1 days
-- end-expected
SELECT CAST('1 day ago' AS interval)::text AS v;

-- begin-expected
-- columns: v
-- row: -1 days
-- end-expected
SELECT CAST('@ 1 day ago' AS interval)::text AS v;

-- begin-expected
-- columns: v
-- row: -1 years -2 mons
-- end-expected
SELECT CAST('1-2 ago' AS interval)::text AS v;

-- begin-expected
-- columns: v
-- row: -04:05:00
-- end-expected
SELECT CAST('4:05 ago' AS interval)::text AS v;

-- begin-expected
-- columns: v
-- row: -1 days -02:00:00
-- end-expected
SELECT CAST('1 day 2 hours ago' AS interval)::text AS v;

-- ============================================================================
-- SECTION K: a unit keeps naming the numbers to its left
-- ============================================================================
-- '1 2 days' fills DAY twice, which is a duplicate-field error and not a day
-- and a stray second.

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval
-- end-expected-error
SELECT CAST('1 2 days' AS interval) AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval
-- end-expected-error
SELECT CAST('1 2 minutes' AS interval) AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval
-- end-expected-error
SELECT CAST('5 1 day' AS interval) AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval
-- end-expected-error
SELECT CAST('1 2 3 seconds' AS interval) AS v;

-- An hour is the one unit that hands DAY leftwards, because 'D H' is the SQL
-- standard spelling of DAY TO HOUR.

-- begin-expected
-- columns: v
-- row: 1 day 02:00:00
-- end-expected
SELECT CAST('1 2 hours' AS interval)::text AS v;

-- begin-expected
-- columns: v
-- row: 1 day 02:00:00
-- end-expected
SELECT CAST('1 2' AS interval day to hour)::text AS v;

-- A time of day does the same.

-- begin-expected
-- columns: v
-- row: 3 days 04:05:00
-- end-expected
SELECT CAST('3 4:05' AS interval)::text AS v;

-- begin-expected
-- columns: v
-- row: 1 year 2 mons 00:00:03
-- end-expected
SELECT CAST('1-2 3' AS interval)::text AS v;

-- begin-expected
-- columns: v
-- row: 1 year 2 mons 3 days 04:05:00
-- end-expected
SELECT CAST('1-2 3 4:05' AS interval)::text AS v;

-- ============================================================================
-- SECTION L: format_type shows a modifier a type has no spelling of its own for
-- ============================================================================
-- PostgreSQL's printTypmod prints the number as it stands for any type with no
-- typmod output function. Dropping it made format_type answer one name for two
-- different typmods.

-- begin-expected
-- columns: a|b|c|d
-- row: date(3)|date(3)[]|text(5)|text(5)[]
-- end-expected
SELECT format_type('date'::regtype, 3) AS a, format_type('date[]'::regtype, 3) AS b,
       format_type('text'::regtype, 5) AS c, format_type('text[]'::regtype, 5) AS d;

-- begin-expected
-- columns: a|b|c|d
-- row: uuid(5)|bytea(5)|inet(5)|jsonb(5)
-- end-expected
SELECT format_type('uuid'::regtype, 5) AS a, format_type('bytea'::regtype, 5) AS b,
       format_type('inet'::regtype, 5) AS c, format_type('jsonb'::regtype, 5) AS d;

-- The handful of names PostgreSQL writes out by hand never carry one.

-- begin-expected
-- columns: a|b|c|d|e|f|g
-- row: integer|smallint|bigint|boolean|real|double precision|json
-- end-expected
SELECT format_type('int4'::regtype, 5) AS a, format_type('int2'::regtype, 5) AS b,
       format_type('int8'::regtype, 5) AS c, format_type('bool'::regtype, 5) AS d,
       format_type('float4'::regtype, 5) AS e, format_type('float8'::regtype, 5) AS f,
       format_type('json'::regtype, 5) AS g;

-- begin-expected
-- columns: a|b|c
-- row: integer[]|date|text[]
-- end-expected
SELECT format_type('int4[]'::regtype, 5) AS a, format_type('date'::regtype, -1) AS b,
       format_type('text[]'::regtype, NULL) AS c;

-- ============================================================================
-- SECTION M: a field qualifier with a precision, in every position
-- ============================================================================

CREATE TABLE ivq (a interval day to second(2), b interval second(3),
                  c interval hour to minute, d interval(4),
                  e interval minute to second(0), f interval year to month,
                  g interval hour to second(5));

-- begin-expected
-- columns: attname|t
-- row: a|interval day to second(2)
-- row: b|interval second(3)
-- row: c|interval hour to minute
-- row: d|interval(4)
-- row: e|interval minute to second(0)
-- row: f|interval year to month
-- row: g|interval hour to second(5)
-- end-expected
SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS t
FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
WHERE c.relname = 'ivq' AND a.attnum > 0 ORDER BY a.attnum;

-- information_schema names the qualifier and its precision separately, and
-- interval_precision is the leading field's precision, which PostgreSQL never
-- records for these.

-- begin-expected
-- columns: column_name|data_type|interval_type|interval_precision|datetime_precision
-- row: a|interval|DAY TO SECOND(2)|NULL|2
-- row: b|interval|SECOND(3)|NULL|3
-- row: c|interval|HOUR TO MINUTE|NULL|6
-- row: d|interval|NULL|NULL|4
-- row: e|interval|MINUTE TO SECOND(0)|NULL|0
-- row: f|interval|YEAR TO MONTH|NULL|6
-- row: g|interval|HOUR TO SECOND(5)|NULL|5
-- end-expected
SELECT column_name, data_type, interval_type, interval_precision, datetime_precision
FROM information_schema.columns WHERE table_name = 'ivq' ORDER BY ordinal_position;

-- The same qualifier in a cast, written before and after the literal.

-- begin-expected
-- columns: a|b|c|d
-- row: 1 day 02:03:04.57|1 day 02:03:04.6|1 day 02:03:04.568|1 day 02:03:05
-- end-expected
SELECT CAST('1 day 02:03:04.5678' AS interval day to second(2))::text AS a,
       CAST('1 day 02:03:04.5678' AS interval second(1))::text AS b,
       CAST('1 day 02:03:04.5678' AS interval hour to second(3))::text AS c,
       CAST('1 day 02:03:04.5678' AS interval minute to second(0))::text AS d;

-- begin-expected
-- columns: a|b
-- row: 1 day 02:03:04.57|1 day 02:03:04.6
-- end-expected
SELECT ('1 day 02:03:04.5678'::interval day to second(2))::text AS a,
       ('1 day 02:03:04.5678'::interval second(1))::text AS b;

-- In a domain, where information_schema.domains reports it the same way.

CREATE DOMAIN ivq_dom AS interval day to second(2);

-- begin-expected
-- columns: domain_name|interval_type|interval_precision|datetime_precision
-- row: ivq_dom|DAY TO SECOND(2)|NULL|2
-- end-expected
SELECT domain_name, interval_type, interval_precision, datetime_precision
FROM information_schema.domains WHERE domain_name = 'ivq_dom';

-- And in a PL/pgSQL declaration, which used to be the one place a qualifier
-- with a precision would not parse at all.

CREATE FUNCTION ivq_pl() RETURNS text AS $$
DECLARE v interval day to second(2);
BEGIN v := '1 day 02:03:04'; RETURN v::text; END $$ LANGUAGE plpgsql;

-- begin-expected
-- columns: v
-- row: 1 day 02:03:04
-- end-expected
SELECT ivq_pl() AS v;

DROP FUNCTION ivq_pl();
DROP DOMAIN ivq_dom;
DROP TABLE ivq;

-- ============================================================================
-- SECTION N: a foreign key's rows belong to the tables the key really names
-- ============================================================================
-- An unqualified REFERENCES resolves through the search path. The action
-- triggers belong to the table it found, in the schema that table is in — not
-- to a relation named after the child's schema, which need not exist at all.

DROP SCHEMA IF EXISTS ivqfk_a CASCADE;
DROP SCHEMA IF EXISTS ivqfk_b CASCADE;
CREATE SCHEMA ivqfk_a;
CREATE SCHEMA ivqfk_b;
CREATE TABLE ivqfk_a.ivqfk_ref (id int CONSTRAINT ivqfk_ref_pk PRIMARY KEY);
SET search_path = ivqfk_a, ivqual, public;
CREATE TABLE ivqfk_b.ivqfk_kid (id int, p int REFERENCES ivqfk_ref(id));

-- begin-expected
-- columns: nspname|relname|n
-- row: ivqfk_a|ivqfk_ref|2
-- row: ivqfk_b|ivqfk_kid|2
-- end-expected
SELECT n.nspname, c.relname, count(*) AS n
FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname IN ('ivqfk_a', 'ivqfk_b') GROUP BY n.nspname, c.relname
ORDER BY n.nspname, c.relname;

-- Nothing in pg_trigger points at a relation that is not there.

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_trigger t LEFT JOIN pg_class c ON c.oid = t.tgrelid
WHERE c.oid IS NULL;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_trigger t LEFT JOIN pg_class c ON c.oid = t.tgconstrindid
WHERE t.tgconstrindid <> 0 AND c.oid IS NULL;

SET search_path = ivqual, public;
DROP SCHEMA ivqfk_b CASCADE;
DROP SCHEMA ivqfk_a CASCADE;

-- ============================================================================
-- SECTION O: two schemas may each hold a constraint of the same name
-- ============================================================================

DROP SCHEMA IF EXISTS ivqcn_a CASCADE;
DROP SCHEMA IF EXISTS ivqcn_b CASCADE;
CREATE SCHEMA ivqcn_a;
CREATE SCHEMA ivqcn_b;
CREATE TABLE ivqcn_a.ivqcn_par (id int CONSTRAINT ivqcn_pk_x PRIMARY KEY);
CREATE TABLE ivqcn_b.ivqcn_par (id int CONSTRAINT ivqcn_pk_y PRIMARY KEY);
CREATE TABLE ivqcn_a.ivqcn_chi (id int, pid int CONSTRAINT ivqcn_fk_z REFERENCES ivqcn_a.ivqcn_par(id));
CREATE TABLE ivqcn_b.ivqcn_chi (id int, pid int CONSTRAINT ivqcn_fk_z REFERENCES ivqcn_b.ivqcn_par(id));

-- Two constraints, so two OIDs. One OID between them doubled every row of a
-- join from pg_trigger to pg_constraint.

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(DISTINCT oid) AS n FROM pg_constraint WHERE conname = 'ivqcn_fk_z';

-- begin-expected
-- columns: nspname|conname|n
-- row: ivqcn_a|ivqcn_fk_z|4
-- row: ivqcn_b|ivqcn_fk_z|4
-- end-expected
SELECT n.nspname, cn.conname, count(*) AS n
FROM pg_trigger t JOIN pg_constraint cn ON cn.oid = t.tgconstraint
JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname IN ('ivqcn_a', 'ivqcn_b') GROUP BY n.nspname, cn.conname
ORDER BY n.nspname, cn.conname;

-- Each key hangs off the index of the primary key it actually references.

-- begin-expected
-- columns: nspname|relname|iname
-- row: ivqcn_a|ivqcn_chi|ivqcn_pk_x
-- row: ivqcn_a|ivqcn_chi|ivqcn_pk_x
-- row: ivqcn_b|ivqcn_chi|ivqcn_pk_y
-- row: ivqcn_b|ivqcn_chi|ivqcn_pk_y
-- end-expected
SELECT n.nspname, c.relname, i.relname AS iname
FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_class i ON i.oid = t.tgconstrindid
WHERE c.relname = 'ivqcn_chi' ORDER BY n.nspname, i.relname;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_constraint cn LEFT JOIN pg_class c ON c.oid = cn.confrelid
WHERE cn.confrelid <> 0 AND c.oid IS NULL;

-- A schema the reader's search path does not reach is written out in full,
-- even when the constraint and the table it names sit in the same schema.

-- begin-expected
-- columns: d
-- row: FOREIGN KEY (pid) REFERENCES ivqcn_a.ivqcn_par(id)
-- end-expected
SELECT pg_get_constraintdef(cn.oid) AS d FROM pg_constraint cn
JOIN pg_class c ON c.oid = cn.conrelid JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE cn.conname = 'ivqcn_fk_z' AND n.nspname = 'ivqcn_a';

DROP SCHEMA ivqcn_b CASCADE;
DROP SCHEMA ivqcn_a CASCADE;

-- ============================================================================
-- SECTION P: what pg_get_triggerdef prints back
-- ============================================================================

CREATE TABLE tgd (id int, v text, a int);
CREATE FUNCTION tgd_f() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql;
CREATE TRIGGER tgd_1 BEFORE UPDATE ON tgd FOR EACH ROW
  WHEN (NEW.v = 'Old. Faithful') EXECUTE FUNCTION tgd_f();
CREATE TRIGGER tgd_2 BEFORE UPDATE ON tgd FOR EACH ROW
  WHEN (NEW.v <> OLD.v) EXECUTE FUNCTION tgd_f();
CREATE TRIGGER tgd_3 AFTER UPDATE ON tgd
  REFERENCING OLD TABLE AS oldt NEW TABLE AS newt
  FOR EACH STATEMENT EXECUTE FUNCTION tgd_f();
CREATE CONSTRAINT TRIGGER tgd_4 AFTER INSERT ON tgd
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION tgd_f();

-- The row references are lower-cased. The value one is compared with is not,
-- and the space after its dot survives: rewriting inside the string constant
-- produced a definition that restores a trigger firing on another value.

-- begin-expected
-- columns: v
-- row: yes
-- end-expected
SELECT CASE WHEN pg_get_triggerdef(oid) LIKE '%''Old. Faithful''%' THEN 'yes' ELSE 'no' END AS v
FROM pg_trigger WHERE tgname = 'tgd_1';

-- begin-expected
-- columns: v
-- row: yes
-- end-expected
SELECT CASE WHEN pg_get_triggerdef(oid) LIKE '%WHEN ((new.v %' THEN 'yes' ELSE 'no' END AS v
FROM pg_trigger WHERE tgname = 'tgd_1';

-- begin-expected
-- columns: d
-- row: CREATE TRIGGER tgd_2 BEFORE UPDATE ON ivqual.tgd FOR EACH ROW WHEN ((new.v <> old.v)) EXECUTE FUNCTION tgd_f()
-- end-expected
SELECT pg_get_triggerdef(oid) AS d FROM pg_trigger WHERE tgname = 'tgd_2';

-- The transition tables are what the trigger function reads its rows from, so
-- the definition has to name them.

-- begin-expected
-- columns: oldt|newt
-- row: oldt|newt
-- end-expected
SELECT tgoldtable AS oldt, tgnewtable AS newt FROM pg_trigger WHERE tgname = 'tgd_3';

-- begin-expected
-- columns: d
-- row: CREATE TRIGGER tgd_3 AFTER UPDATE ON ivqual.tgd REFERENCING OLD TABLE AS oldt NEW TABLE AS newt FOR EACH STATEMENT EXECUTE FUNCTION tgd_f()
-- end-expected
SELECT pg_get_triggerdef(oid) AS d FROM pg_trigger WHERE tgname = 'tgd_3';

-- A deferrable trigger says so, which is what tells a client SET CONSTRAINTS
-- can reach it.

-- begin-expected
-- columns: tgname|d|i
-- row: tgd_1|false|false
-- row: tgd_2|false|false
-- row: tgd_3|false|false
-- row: tgd_4|true|true
-- end-expected
SELECT tgname, tgdeferrable::text AS d, tginitdeferred::text AS i
FROM pg_trigger WHERE tgname LIKE 'tgd\_%' ORDER BY tgname;

DROP TABLE tgd CASCADE;
DROP FUNCTION tgd_f();

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA IF EXISTS ivqual CASCADE;
