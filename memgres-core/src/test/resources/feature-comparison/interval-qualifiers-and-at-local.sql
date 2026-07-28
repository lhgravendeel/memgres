-- ============================================================================
-- Feature Comparison: interval field qualifiers, interval precision, AT LOCAL
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- An interval type is not just "interval". The field qualifier decides what an
-- unlabelled number in the literal counts -- INTERVAL '3' DAY is three days
-- where a bare INTERVAL '3' is three seconds -- and it decides which fields
-- survive afterwards. An unlabelled number takes the qualifier's *last* field,
-- so INTERVAL '5' DAY TO HOUR is five hours; two of them spell the SQL standard
-- 'D H'. A SECOND(n) keeps n fractional digits and rounds the rest away from
-- zero. AT LOCAL (PG 17) is AT TIME ZONE against the session's own TimeZone.
-- Finally a date literal may name an offset with no time of day at all.
-- ============================================================================

-- setup
SET TIME ZONE 'UTC';
DROP VIEW IF EXISTS ivq_v;
DROP TABLE IF EXISTS ivq_t;
CREATE TABLE ivq_t (id int, d interval, ts timestamptz, tsn timestamp, tz timetz, iv3 interval(3));
INSERT INTO ivq_t VALUES (1, interval '3 days', timestamptz '2001-02-16 20:38:40-05', timestamp '2001-02-16 20:38:40', timetz '10:00:00+02', interval '1.234567 seconds');
INSERT INTO ivq_t VALUES (2, NULL, NULL, NULL, NULL, NULL);
CREATE VIEW ivq_v AS SELECT id, d, ts AT LOCAL AS tsl, iv3 FROM ivq_t;

-- ============================================================================
-- 1. A single-field qualifier names the unit of an unlabelled number
-- ============================================================================
SELECT INTERVAL '3' AS a;
SELECT INTERVAL '3' YEAR AS a;
SELECT INTERVAL '3' MONTH AS a;
SELECT INTERVAL '3' DAY AS a;
SELECT INTERVAL '3' HOUR AS a;
SELECT INTERVAL '3' MINUTE AS a;
SELECT INTERVAL '3' SECOND AS a;
SELECT INTERVAL '10' YEAR AS a;
SELECT INTERVAL '10' DAY AS a;
SELECT INTERVAL '-3' DAY AS a;
SELECT INTERVAL '-3' HOUR AS a;
SELECT INTERVAL '-3' YEAR AS a;

-- ============================================================================
-- 2. A fraction spills into the next field, then the qualifier drops it
-- ============================================================================
SELECT INTERVAL '1.5' YEAR AS a;
SELECT INTERVAL '1.5' MONTH AS a;
SELECT INTERVAL '1.5' DAY AS a;
SELECT INTERVAL '1.5' HOUR AS a;
SELECT INTERVAL '1.5' MINUTE AS a;
SELECT INTERVAL '1.5' SECOND AS a;
SELECT INTERVAL '0.5' DAY AS a;
SELECT INTERVAL '0.5' SECOND AS a;
SELECT INTERVAL '1.234567' HOUR AS a;
SELECT INTERVAL '1.234567' SECOND AS a;

-- ============================================================================
-- 3. A ranged qualifier gives a bare number its least significant field
-- ============================================================================
SELECT INTERVAL '3' YEAR TO MONTH AS a;
SELECT INTERVAL '20' YEAR TO MONTH AS a;
SELECT INTERVAL '5' DAY TO HOUR AS a;
SELECT INTERVAL '10' DAY TO HOUR AS a;
SELECT INTERVAL '10' DAY TO MINUTE AS a;
SELECT INTERVAL '10' DAY TO SECOND AS a;
SELECT INTERVAL '10' HOUR TO MINUTE AS a;
SELECT INTERVAL '10' HOUR TO SECOND AS a;
SELECT INTERVAL '10' MINUTE TO SECOND AS a;
SELECT INTERVAL '1.5' YEAR TO MONTH AS a;
SELECT INTERVAL '1.5' DAY TO HOUR AS a;
SELECT INTERVAL '1.5' MINUTE TO SECOND AS a;

-- ============================================================================
-- 4. Two numbers spell the SQL standard day and hour
-- ============================================================================
SELECT INTERVAL '1 2' DAY TO HOUR AS a;
SELECT INTERVAL '1 2' HOUR AS a;
SELECT INTERVAL '-1 -2' DAY TO HOUR AS a;
SELECT INTERVAL '-1 -2' HOUR AS a;

-- ============================================================================
-- 5. A literal that fills the same field twice is rejected
-- ============================================================================
-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval
-- end-expected-error
SELECT INTERVAL '1 2' DAY AS a;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval
-- end-expected-error
SELECT INTERVAL '1 2 3' DAY TO HOUR AS a;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval
-- end-expected-error
SELECT INTERVAL '1 2' MINUTE AS a;

-- ============================================================================
-- 6. MINUTE TO SECOND rereads a two-part time field as minutes and seconds
-- ============================================================================
SELECT INTERVAL '2:03' MINUTE TO SECOND AS a;
SELECT INTERVAL '1 2:03' MINUTE TO SECOND AS a;
SELECT INTERVAL '2:03:04' MINUTE TO SECOND AS a;
SELECT INTERVAL '2:03' SECOND AS a;
SELECT INTERVAL '2:03' MINUTE AS a;
SELECT INTERVAL '2:03' HOUR TO MINUTE AS a;
SELECT INTERVAL '2:03' DAY TO HOUR AS a;
SELECT INTERVAL '2:03' YEAR AS a;
SELECT INTERVAL '1 2:03' DAY TO MINUTE AS a;
SELECT INTERVAL '1 2:03:04' DAY TO SECOND AS a;
SELECT INTERVAL '1 2:03:04' HOUR TO MINUTE AS a;
SELECT INTERVAL '1:02:03.456789' MINUTE AS a;

-- ============================================================================
-- 7. The qualifier drops everything below its last field
-- ============================================================================
SELECT INTERVAL '1 year 2 months 3 days 4:05:06' YEAR AS a;
SELECT INTERVAL '1 year 2 months 3 days 4:05:06' MONTH AS a;
SELECT INTERVAL '1 year 2 months 3 days 4:05:06' DAY AS a;
SELECT INTERVAL '1 year 2 months 3 days 4:05:06' HOUR AS a;
SELECT INTERVAL '1 year 2 months 3 days 4:05:06' MINUTE AS a;
SELECT INTERVAL '1 year 2 months 3 days 4:05:06' SECOND AS a;
SELECT INTERVAL '1 year 2 months 3 days 4:05:06' YEAR TO MONTH AS a;
SELECT INTERVAL '1 year 2 months 3 days 4:05:06' DAY TO HOUR AS a;
SELECT INTERVAL '1 year 2 months 3 days 4:05:06' HOUR TO MINUTE AS a;
SELECT INTERVAL '1 day 2 hours' DAY AS a;
SELECT INTERVAL '1 day 2 hours' HOUR AS a;
SELECT INTERVAL '1 day 2 hours' YEAR AS a;
SELECT INTERVAL '3 days 4 hours 5 minutes' DAY AS a;
SELECT INTERVAL '3 days 4 hours 5 minutes' MINUTE AS a;
SELECT INTERVAL '1 day' MINUTE TO SECOND AS a;
SELECT INTERVAL '1-2' YEAR AS a;
SELECT INTERVAL '1-2' SECOND AS a;

-- ============================================================================
-- 8. Fractional-seconds precision rounds away from zero
-- ============================================================================
SELECT INTERVAL '1.234567 seconds' SECOND(3) AS a;
SELECT INTERVAL '1.234567' SECOND(3) AS a;
SELECT INTERVAL '1.5' SECOND(0) AS a;
SELECT INTERVAL '1.5' SECOND(3) AS a;
SELECT INTERVAL '1.4999' SECOND(2) AS a;
SELECT INTERVAL '-1.4999' SECOND(2) AS a;
SELECT INTERVAL '-1.5' SECOND(0) AS a;
SELECT INTERVAL '1' SECOND(7) AS a;
SELECT INTERVAL '1 2:03:04.56789' DAY TO SECOND(2) AS a;
SELECT INTERVAL '1 2:03:04.5678' HOUR TO SECOND(1) AS a;
SELECT INTERVAL '1 2:03:04' DAY TO SECOND(2) AS a;

-- ============================================================================
-- 9. Every spelling of a precised interval type rounds the same way
-- ============================================================================
SELECT CAST('1.234567 seconds' AS interval(3)) AS a;
SELECT '1.234567 seconds'::interval(3) AS a;
SELECT CAST('1.234567 seconds' AS interval second(3)) AS a;
SELECT interval(3) '1.234567 seconds' AS a;
SELECT interval(0) '1.5 seconds' AS a;
SELECT (INTERVAL '1.234567 seconds')::interval(3) AS a;
SELECT CAST(interval '1.234567' AS interval(0)) AS a;
SELECT CAST(interval '-1.5' AS interval(0)) AS a;
SELECT CAST(INTERVAL '1 2:03:04.56789' AS interval day to second(2)) AS a;

-- ============================================================================
-- 10. Shapes that carry their own units are only trimmed
-- ============================================================================
SELECT INTERVAL 'infinity' DAY AS a;
SELECT INTERVAL '-infinity' SECOND(2) AS a;
SELECT INTERVAL '1 day ago' DAY AS a;
SELECT INTERVAL '@ 1 day 2 hours' HOUR AS a;
SELECT INTERVAL 'P1Y2M3DT4H5M6.789S' SECOND(2) AS a;
SELECT INTERVAL 'P0001-02-03T04:05:06' DAY AS a;
SELECT INTERVAL '1000000000 days' DAY AS a;

-- ============================================================================
-- 11. Words that are not interval fields stay column aliases
-- ============================================================================
SELECT INTERVAL '1.5' WEEK;
SELECT INTERVAL '5' MILLENNIUM;
SELECT 1 AS second;
SELECT 1 AS local;
SELECT interval '1 day' AS interval;

-- ============================================================================
-- 12. Unqualified intervals are unchanged
-- ============================================================================
SELECT INTERVAL '1 day' AS a;
SELECT INTERVAL '1 day 2 hours 3 minutes' AS a;
SELECT INTERVAL '04:05:06' AS a;
SELECT INTERVAL '1.234567' AS a;
SELECT INTERVAL '1-2' AS a;
SELECT INTERVAL '2 04:05:06' AS a;
SELECT INTERVAL 'P1Y2M3DT4H5M6S' AS a;
SELECT INTERVAL '-1 mons +3 days' AS a;
SELECT '1 day'::interval AS a;
SELECT CAST('1 day' AS interval) AS a;
SELECT INTERVAL '1 day' + INTERVAL '2 hours' AS a;
SELECT NULL::interval AS a;
SELECT (NULL::text)::interval AS a;
SELECT (NULL::text)::interval(3) AS a;

-- ============================================================================
-- 13. A qualified interval is an ordinary interval afterwards
-- ============================================================================
SELECT date '2001-01-01' + INTERVAL '3' DAY AS a;
SELECT timestamp '2001-01-01 00:00:00' + INTERVAL '3' HOUR AS a;
SELECT INTERVAL '3' DAY + INTERVAL '3' HOUR AS a;
SELECT justify_hours(INTERVAL '30' HOUR) AS a;
SELECT INTERVAL '3' DAY = INTERVAL '3 days' AS a;
SELECT INTERVAL '3' DAY > INTERVAL '2 days' AS a;
SELECT extract(day from INTERVAL '3' DAY) AS a;
SELECT extract(hour from INTERVAL '3' HOUR) AS a;
SELECT pg_typeof(INTERVAL '3' DAY)::text AS a;
SELECT pg_typeof(INTERVAL '3' DAY TO SECOND(2))::text AS a;
SELECT pg_typeof(CAST('1' AS interval(3)))::text AS a;

-- ============================================================================
-- 14. AT LOCAL converts against the session TimeZone
-- ============================================================================
SELECT timestamptz '2001-02-16 20:38:40-05' AT LOCAL AS a;
SELECT timestamp '2001-02-16 20:38:40' AT LOCAL AS a;
SELECT (timetz '10:00:00+02') AT LOCAL AS a;
SELECT time '10:00:00' AT LOCAL AS a;
SELECT date '2001-01-01' AT LOCAL AS a;
SELECT timestamptz '2001-02-16 20:38:40-05' AT LOCAL AT TIME ZONE 'UTC' AS a;
SELECT (NULL::timestamptz) AT LOCAL AS a;
SELECT (NULL::timestamp) AT LOCAL AS a;
SELECT (NULL::timetz) AT LOCAL AS a;
SELECT pg_typeof(timestamptz '2001-02-16 20:38:40-05' AT LOCAL)::text AS a;
SELECT pg_typeof(timestamp '2001-02-16 20:38:40' AT LOCAL)::text AS a;
SELECT pg_typeof((timetz '10:00:00+02') AT LOCAL)::text AS a;

-- ============================================================================
-- 15. AT TIME ZONE still moves what it always moved, and now moves a time too
-- ============================================================================
SELECT timestamptz '2001-02-16 20:38:40-05' AT TIME ZONE 'UTC' AS a;
SELECT timestamp '2001-02-16 20:38:40' AT TIME ZONE 'UTC' AS a;
SELECT timestamptz '2001-02-16 20:38:40-05' AT TIME ZONE 'Europe/Amsterdam' AS a;
SELECT (timetz '10:00:00+02') AT TIME ZONE 'UTC' AS a;
SELECT time '10:00:00' AT TIME ZONE 'UTC' AS a;

-- ============================================================================
-- 16. A date literal may name an offset with no time of day
-- ============================================================================
SELECT timestamptz '2001-01-01+00' AS a;
SELECT timestamptz '2001-01-01+02' AS a;
SELECT timestamptz '2001-01-01+05:30' AS a;
SELECT timestamptz '2001-01-01+2' AS a;
SELECT timestamptz '2001-01-01 +00' AS a;
SELECT timestamptz '2001-01-01 +2' AS a;
SELECT timestamptz '2001-01-01 -05' AS a;
SELECT timestamp '2001-01-01+00' AS a;
SELECT timestamp '2001-01-01+02' AS a;
SELECT timestamp '2001-01-01 +02' AS a;
SELECT date '2001-01-01+02' AS a;
SELECT date '2001-01-01' AS a;
SELECT timestamp '2001-01-01 12:00:00+02' AS a;

-- Without a space the '-' is another date field, not an offset
-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type timestamp with time zone
-- end-expected-error
SELECT timestamptz '2001-01-01-05' AS a;

-- begin-expected-error
-- sqlstate: 22008
-- message-like: date/time field value out of range
-- end-expected-error
SELECT timestamptz '2024-02-30' AS a;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type timestamp with time zone
-- end-expected-error
SELECT timestamptz 'garbage' AS a;

-- ============================================================================
-- 17. The same operators against stored rows, a view and a subquery
-- ============================================================================
SELECT id, d::text AS d, iv3::text AS iv3 FROM ivq_t ORDER BY id;
SELECT id, (ts AT LOCAL)::text AS a FROM ivq_t ORDER BY id;
SELECT id, (tsn AT LOCAL)::text AS a FROM ivq_t ORDER BY id;
SELECT id, (tz AT LOCAL)::text AS a FROM ivq_t ORDER BY id;
SELECT id, tsl::text AS tsl, iv3::text AS iv3 FROM ivq_v ORDER BY id;
SELECT sub.x::text AS a FROM (SELECT d AS x FROM ivq_t WHERE id = 1) sub;
SELECT id FROM ivq_t WHERE d >= INTERVAL '1' DAY ORDER BY id;
SELECT id FROM ivq_t WHERE ts AT LOCAL > timestamp '2000-01-01' ORDER BY id;
SELECT d::text AS d, count(*) AS n FROM ivq_t GROUP BY d ORDER BY 1;
SELECT (ts AT LOCAL)::text AS a FROM ivq_t GROUP BY ts AT LOCAL ORDER BY 1;
SELECT id FROM ivq_t ORDER BY d NULLS LAST;
SELECT id FROM ivq_t ORDER BY ts AT LOCAL NULLS LAST;

-- cleanup
DROP VIEW ivq_v;
DROP TABLE ivq_t;
RESET TimeZone;
