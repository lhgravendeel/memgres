-- ============================================================================
-- Feature Comparison: the session TimeZone decides what "now" means
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- CURRENT_DATE, LOCALTIMESTAMP, the 'today'/'now' literals, date_trunc and
-- EXTRACT all answer in the session's own zone, not the server's. A server in
-- Amsterdam is on the next calendar day for two hours before a session running
-- with TimeZone UTC is, and every one of these has to agree about which day it
-- is. Each case is written as a comparison so the answer is stable whenever it
-- runs; the zones span +14 to -11 so the calendar dates genuinely disagree.
-- ============================================================================

SET TimeZone = 'UTC';
SELECT (TIMESTAMP 'today' = date_trunc('day', now())::timestamp)::text AS a;
SELECT (TIMESTAMP 'yesterday' = date_trunc('day', now())::timestamp - interval '1 day')::text AS a;
SELECT (TIMESTAMP 'tomorrow' = date_trunc('day', now())::timestamp + interval '1 day')::text AS a;
SELECT (DATE 'today' = current_date)::text AS a;
SELECT (DATE 'tomorrow' - DATE 'today')::text AS a;
SELECT (current_date = (now() AT TIME ZONE current_setting('TimeZone'))::date)::text AS a;
SELECT (localtimestamp = (now() AT TIME ZONE current_setting('TimeZone')))::text AS a;
SELECT (localtime = (now() AT TIME ZONE current_setting('TimeZone'))::time)::text AS a;
SELECT (current_date = localtimestamp::date)::text AS a;
SELECT (TIMESTAMP 'now' = localtimestamp)::text AS a;
SELECT (TIME 'now' = localtime)::text AS a;
SELECT (extract(day from now()) = extract(day from current_date))::text AS a;
SELECT (extract(hour from now()) = extract(hour from localtimestamp))::text AS a;
SELECT (date_trunc('day', now()) = current_date::timestamptz)::text AS a;

-- ============================================================================
-- +14: the far side of the date line is already on tomorrow
-- ============================================================================
SET TimeZone = 'Pacific/Kiritimati';
SELECT (TIMESTAMP 'today' = date_trunc('day', now())::timestamp)::text AS a;
SELECT (DATE 'today' = current_date)::text AS a;
SELECT (current_date = (now() AT TIME ZONE current_setting('TimeZone'))::date)::text AS a;
SELECT (localtimestamp = (now() AT TIME ZONE current_setting('TimeZone')))::text AS a;
SELECT (current_date = localtimestamp::date)::text AS a;
SELECT (extract(day from now()) = extract(day from current_date))::text AS a;
SELECT (date_trunc('day', now()) = current_date::timestamptz)::text AS a;

-- ============================================================================
-- -11: and the near side is still on yesterday
-- ============================================================================
SET TimeZone = 'Pacific/Midway';
SELECT (TIMESTAMP 'today' = date_trunc('day', now())::timestamp)::text AS a;
SELECT (DATE 'today' = current_date)::text AS a;
SELECT (current_date = (now() AT TIME ZONE current_setting('TimeZone'))::date)::text AS a;
SELECT (localtimestamp = (now() AT TIME ZONE current_setting('TimeZone')))::text AS a;
SELECT (current_date = localtimestamp::date)::text AS a;
SELECT (extract(day from now()) = extract(day from current_date))::text AS a;
SELECT (date_trunc('day', now()) = current_date::timestamptz)::text AS a;

-- ============================================================================
-- A zone with a DST offset, and one behind UTC
-- ============================================================================
SET TimeZone = 'Europe/Amsterdam';
SELECT (TIMESTAMP 'today' = date_trunc('day', now())::timestamp)::text AS a;
SELECT (current_date = (now() AT TIME ZONE current_setting('TimeZone'))::date)::text AS a;
SELECT (localtimestamp = (now() AT TIME ZONE current_setting('TimeZone')))::text AS a;
SELECT (date_trunc('day', now()) = current_date::timestamptz)::text AS a;
SET TimeZone = 'America/Los_Angeles';
SELECT (TIMESTAMP 'today' = date_trunc('day', now())::timestamp)::text AS a;
SELECT (current_date = (now() AT TIME ZONE current_setting('TimeZone'))::date)::text AS a;
SELECT (localtimestamp = (now() AT TIME ZONE current_setting('TimeZone')))::text AS a;
SELECT (date_trunc('day', now()) = current_date::timestamptz)::text AS a;

-- ============================================================================
-- date_trunc's third argument names the zone to truncate in
-- ============================================================================
SET TimeZone = 'UTC';
SELECT (date_trunc('day', TIMESTAMPTZ '2024-06-15 23:30:00+00', 'Pacific/Kiritimati')
        = TIMESTAMPTZ '2024-06-15 10:00:00+00')::text AS a;
SELECT (date_trunc('day', TIMESTAMPTZ '2024-06-15 00:30:00+00', 'Pacific/Midway')
        = TIMESTAMPTZ '2024-06-14 11:00:00+00')::text AS a;
SELECT (date_trunc('hour', TIMESTAMPTZ '2024-06-15 23:45:00+00', 'Asia/Kolkata')
        = TIMESTAMPTZ '2024-06-15 23:30:00+00')::text AS a;
SELECT date_trunc('day', NULL::timestamptz, 'UTC') AS a;
SELECT date_trunc('day', TIMESTAMPTZ '2024-06-15 12:00:00+00', NULL) AS a;

-- ============================================================================
-- A fixed timestamptz reads its fields in whatever zone the session is in
-- ============================================================================
SET TimeZone = 'Pacific/Kiritimati';
SELECT (TIMESTAMPTZ '2024-06-15 23:00:00+00')::timestamp::text AS a;
SELECT (TIMESTAMPTZ '2024-06-15 23:00:00+00')::date::text AS a;
SELECT extract(day from TIMESTAMPTZ '2024-06-15 23:00:00+00')::text AS a;
SELECT extract(hour from TIMESTAMPTZ '2024-06-15 23:00:00+00')::text AS a;
SELECT date_trunc('day', TIMESTAMPTZ '2024-06-15 23:00:00+00')::text AS a;
SET TimeZone = 'Pacific/Midway';
SELECT (TIMESTAMPTZ '2024-06-15 23:00:00+00')::timestamp::text AS a;
SELECT (TIMESTAMPTZ '2024-06-15 23:00:00+00')::date::text AS a;
SELECT extract(day from TIMESTAMPTZ '2024-06-15 23:00:00+00')::text AS a;
SELECT extract(hour from TIMESTAMPTZ '2024-06-15 23:00:00+00')::text AS a;
SELECT date_trunc('day', TIMESTAMPTZ '2024-06-15 23:00:00+00')::text AS a;

-- ============================================================================
-- A column default reads the same clock the session does
-- ============================================================================
SET TimeZone = 'Pacific/Kiritimati';
DROP TABLE IF EXISTS stz_t CASCADE;
CREATE TABLE stz_t (i int, d date DEFAULT current_date, ts timestamp DEFAULT localtimestamp);
INSERT INTO stz_t (i) VALUES (1);
SELECT (d = current_date)::text AS a FROM stz_t WHERE i = 1;
SELECT (ts::date = current_date)::text AS a FROM stz_t WHERE i = 1;
DROP TABLE stz_t CASCADE;

-- ============================================================================
-- Every current reading agrees inside one statement, whatever the zone
-- ============================================================================
SET TimeZone = 'Pacific/Midway';
SELECT (current_date = localtimestamp::date
        AND localtimestamp::date = (TIMESTAMP 'today')::date
        AND (TIMESTAMP 'today')::date = (DATE 'today'))::text AS a;

RESET TimeZone;
