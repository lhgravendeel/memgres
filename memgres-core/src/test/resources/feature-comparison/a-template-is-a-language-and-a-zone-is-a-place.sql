-- ============================================================================
-- -- A template is a language, and a zone is a place.
-- --
-- -- A date/time template is not a java.time pattern: its keywords are looked up in a table of
-- -- their own, a field's width counts the sign or does not depending on which field it is, and
-- -- an interval -- which is a length and not a point -- is refused the keywords that ask about
-- -- the calendar. Reading is the same language backwards, and a template that names a
-- -- displacement reads the value against it rather than against the session. A zone is named
-- -- out of PostgreSQL's own abbreviation table before the zone database is asked, a bare number
-- -- is read the POSIX way round, and the day a zone puts its clocks back is twenty-five hours
-- -- long for everything that counts days.
--
-- ============================================================================

-- ============================================================================
-- 1. A template is a language of its own
-- ============================================================================
SET TimeZone = 'UTC';
-- begin-expected
-- columns: a
-- row: 2020-06-15
-- end-expected
SELECT to_char(date '2020-06-15', 'YYYY-MM-DD') AS a;
-- begin-expected
-- columns: a
-- row: 2 3 24 167
-- end-expected
SELECT to_char(date '2020-06-15', 'Q W WW DDD') AS a;
-- begin-expected
-- columns: a
-- row: MONDAY    Monday    monday   
-- end-expected
SELECT to_char(date '2020-06-15', 'DAY Day day') AS a;
-- begin-expected
-- columns: a
-- row: MON Mon mon
-- end-expected
SELECT to_char(date '2020-06-15', 'DY Dy dy') AS a;
-- begin-expected
-- columns: a
-- row: JUNE      June      june     
-- end-expected
SELECT to_char(date '2020-06-15', 'MONTH Month month') AS a;
-- begin-expected
-- columns: a
-- row: JUN Jun jun
-- end-expected
SELECT to_char(date '2020-06-15', 'MON Mon mon') AS a;
-- begin-expected
-- columns: a
-- row: VI   vi  
-- end-expected
SELECT to_char(date '2020-06-15', 'RM rm') AS a;
-- begin-expected
-- columns: a
-- row: Monday June
-- end-expected
SELECT to_char(date '2020-06-15', 'TMDay TMMonth') AS a;
-- begin-expected
-- columns: a
-- row: June the 15th
-- end-expected
SELECT to_char(date '2020-06-15', 'FMMonth "the" DDth') AS a;
-- begin-expected
-- columns: a
-- row: 15thth
-- end-expected
SELECT to_char(date '2020-06-15', 'DDthth') AS a;
-- begin-expected
-- columns: a
-- row: 2,020 020 20 0
-- end-expected
SELECT to_char(date '2020-06-15', 'Y,YYY YYY YY Y') AS a;
-- begin-expected
-- columns: a
-- row: 2020 020 20 0
-- end-expected
SELECT to_char(date '2020-06-15', 'IYYY IYY IY I') AS a;
-- begin-expected
-- columns: a
-- row: 21 2459016
-- end-expected
SELECT to_char(date '2020-06-15', 'CC J') AS a;
-- begin-expected
-- columns: a
-- row: 0001 BC
-- end-expected
SELECT to_char(date '0001-01-01 BC', 'YYYY BC') AS a;
-- begin-expected
-- columns: a
-- row: 01:45 PM
-- end-expected
SELECT to_char(timestamp '2020-06-15 13:45:00', 'HH12:MI AM') AS a;
-- begin-expected
-- columns: a
-- row: 01:45 P.M.
-- end-expected
SELECT to_char(timestamp '2020-06-15 13:45:00', 'HH12:MI P.M.') AS a;
-- begin-expected
-- columns: a
-- row: 49500 00 123 123456 1 123 123456
-- end-expected
SELECT to_char(timestamp '2020-06-15 13:45:00.123456', 'SSSS SS MS US FF1 FF3 FF6') AS a;
-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT to_char(date '2020-06-15', '') IS NULL AS a;
-- ============================================================================
-- 2. An interval is a length, and a template asking about the calendar is refused
-- ============================================================================
-- begin-expected
-- columns: a
-- row: 003
-- end-expected
SELECT to_char(interval '3 days', 'DDD') AS a;
-- begin-expected
-- columns: a
-- row: 400 400
-- end-expected
SELECT to_char(interval '400 days', 'DDD DD') AS a;
-- begin-expected
-- columns: a
-- row: 0001 02 03
-- end-expected
SELECT to_char(interval '1 year 2 mons 3 days', 'YYYY MM DD') AS a;
-- begin-expected
-- columns: a
-- row: -1
-- end-expected
SELECT to_char(interval '-1 day', 'DD') AS a;
-- begin-expected
-- columns: a
-- row: -01
-- end-expected
SELECT to_char(interval '-1 day', 'DDD') AS a;
-- begin-expected
-- columns: a
-- row: -01
-- end-expected
SELECT to_char(interval '-1 mon', 'MM') AS a;
-- begin-expected
-- columns: a
-- row: -0001 -001 -01 -1
-- end-expected
SELECT to_char(interval '-1 year', 'YYYY YYY YY Y') AS a;
-- begin-expected
-- columns: a
-- row: -01
-- end-expected
SELECT to_char(interval '-1 hour', 'HH24') AS a;
-- begin-expected
-- columns: a
-- row: -01 -500 -500000
-- end-expected
SELECT to_char(interval '-1.5 sec', 'SS MS US') AS a;
-- begin-expected
-- columns: a
-- row: -1
-- end-expected
SELECT to_char(interval '-1 sec', 'SSSS') AS a;
-- begin-expected
-- columns: a
-- row: 0,000
-- end-expected
SELECT to_char(interval '1 day', 'Y,YYY') AS a;
-- begin-expected
-- columns: a
-- row: 01st 01ST
-- end-expected
SELECT to_char(interval '1 day', 'DDth DDTH') AS a;
-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid format specification for an interval value
-- end-expected-error
SELECT to_char(interval '1 day', 'D') AS a;
-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid format specification for an interval value
-- end-expected-error
SELECT to_char(interval '1 day', 'DAY') AS a;
-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid format specification for an interval value
-- end-expected-error
SELECT to_char(interval '1 day', 'MONTH') AS a;
-- begin-expected
-- columns: a
-- row: 
-- end-expected
SELECT to_char(interval '1 day', 'RM') AS a;
-- begin-expected
-- columns: a
-- row: 
-- end-expected
SELECT to_char(interval '1 day', 'Q') AS a;
-- begin-expected
-- columns: a
-- row: 00
-- end-expected
SELECT to_char(interval '1 day', 'CC') AS a;
-- begin-expected
-- columns: a
-- row: -001
-- end-expected
SELECT to_char(interval '1 day', 'IYYY') AS a;
-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid format specification for an interval value
-- end-expected-error
SELECT to_char(interval '1 day', 'BC') AS a;
-- begin-expected
-- columns: a
-- row: AM
-- end-expected
SELECT to_char(interval '1 day', 'AM') AS a;
-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid format specification for an interval value
-- end-expected-error
SELECT to_char(interval '1 day', 'TZ') AS a;
-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid format specification for an interval value
-- end-expected-error
SELECT to_char(interval '1 day', 'OF') AS a;
-- begin-expected
-- columns: a
-- row: 1721029
-- end-expected
SELECT to_char(interval '1 day', 'J') AS a;
-- ============================================================================
-- 3. A time reaches to_char as the length it stands for
-- ============================================================================
-- begin-expected
-- columns: a
-- row: 12:34:56
-- end-expected
SELECT to_char(time '12:34:56', 'HH24:MI:SS') AS a;
-- begin-expected
-- columns: a
-- row: 500 500000 500 45296
-- end-expected
SELECT to_char(time '12:34:56.5', 'MS US FF3 SSSS') AS a;
-- begin-expected
-- columns: a
-- row: 0000-00-00 12:34:56
-- end-expected
SELECT to_char(time '12:34:56', 'YYYY-MM-DD HH24:MI:SS') AS a;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function to_char(time with time zone, unknown) does not exist
-- end-expected-error
SELECT to_char(timetz '12:34:56+05:30', 'HH24:MI:SS') AS a;
-- ============================================================================
-- 4. A displacement is written the way the zone writes one
-- ============================================================================
-- begin-expected
-- columns: a
-- row: +00 +0
-- end-expected
SELECT to_char(timestamptz '2020-01-01 12:00:00+00', 'OF FMOF') AS a;
-- begin-expected
-- columns: a
-- row: UTC +00 00
-- end-expected
SELECT to_char(timestamptz '2020-01-01 12:00:00+00', 'TZ TZH TZM') AS a;
SET TimeZone = 'Asia/Kolkata';
-- begin-expected
-- columns: a
-- row: +05:30 +5:30
-- end-expected
SELECT to_char(timestamptz '2020-01-01 12:00:00+00', 'OF FMOF') AS a;
-- begin-expected
-- columns: a
-- row: IST +05:30
-- end-expected
SELECT to_char(timestamptz '2020-01-01 12:00:00+00', 'TZ TZH:TZM') AS a;
SET TimeZone = 'America/New_York';
-- begin-expected
-- columns: a
-- row: EST
-- end-expected
SELECT to_char(timestamptz '2020-01-01 12:00:00+00', 'TZ') AS a;
-- begin-expected
-- columns: a
-- row: EDT
-- end-expected
SELECT to_char(timestamptz '2020-07-01 12:00:00+00', 'TZ') AS a;
SET TimeZone = 'UTC';
-- ============================================================================
-- 5. The reading side takes the same language
-- ============================================================================
-- begin-expected
-- columns: a
-- row: 2020-06-15 13:45:00+00
-- end-expected
SELECT to_timestamp('2020-06-15 13:45', 'YYYY-MM-DD HH24:MI')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-06-15 00:00:00+00
-- end-expected
SELECT to_timestamp('15-Jun-2020', 'DD-Mon-YYYY')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-04-09 00:00:00+00
-- end-expected
SELECT to_timestamp('2020 100', 'YYYY DDD')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-06-15 00:00:00+00
-- end-expected
SELECT to_timestamp('20 6 15', 'YY MM DD')::text AS a;
-- begin-expected
-- columns: a
-- row: 2000-01-01
-- end-expected
SELECT to_date('2451545', 'J')::text AS a;
-- begin-expected
-- columns: a
-- row: 0001-06-15 BC
-- end-expected
SELECT to_date('06 15', 'MM DD')::text AS a;
-- begin-expected
-- columns: a
-- row: 0001-01-01 00:00:00+00 BC
-- end-expected
SELECT to_timestamp('01 01', 'MM DD')::text AS a;
-- begin-expected-error
-- sqlstate: 22007
-- message-like: hour "13" is invalid for the 12-hour clock
-- end-expected-error
SELECT to_timestamp('13:45 PM', 'HH12:MI PM')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-06-15 13:45:00.123+00
-- end-expected
SELECT to_timestamp('2020-06-15 13:45:00.123', 'YYYY-MM-DD HH24:MI:SS.MS')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-06-15
-- end-expected
SELECT to_date('  2020-06-15', 'YYYY-MM-DD')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-06-15
-- end-expected
SELECT to_date('2020/06/15', 'YYYY-MM-DD')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-06-15
-- end-expected
SELECT to_date('20200615', 'YYYYMMDD')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-06-15 00:00:00+00
-- end-expected
SELECT to_timestamp('2020-06-15', 'FXYYYY-MM-DD')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-06-15 00:00:00+00
-- end-expected
SELECT to_timestamp('2020-6-15', 'FXYYYY-MM-DD')::text AS a;
-- begin-expected
-- columns: a
-- row: timestamp with time zone
-- end-expected
SELECT pg_typeof(to_timestamp(1))::text AS a;
-- begin-expected
-- columns: a
-- row: timestamp with time zone
-- end-expected
SELECT pg_typeof(to_timestamp('2020','YYYY'))::text AS a;
-- begin-expected
-- columns: a
-- row: date
-- end-expected
SELECT pg_typeof(to_date('2020','YYYY'))::text AS a;
-- begin-expected
-- columns: a
-- row: numeric
-- end-expected
SELECT pg_typeof(to_number('1','9'))::text AS a;
-- ============================================================================
-- 6. A template that names a displacement reads the value against it
-- ============================================================================
-- begin-expected
-- columns: a
-- row: 2020-06-14 18:30:00+00
-- end-expected
SELECT to_timestamp('2020-06-15 +05:30', 'YYYY-MM-DD OF')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-06-15 07:00:00+00
-- end-expected
SELECT to_timestamp('2020-06-15 -07', 'YYYY-MM-DD OF')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-06-14 18:30:00+00
-- end-expected
SELECT to_timestamp('2020-06-15 05 30', 'YYYY-MM-DD TZH TZM')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-06-15 05:30:00+00
-- end-expected
SELECT to_timestamp('2020-06-15 -05 30', 'YYYY-MM-DD TZH TZM')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-06-15 00:00:00+00
-- end-expected
SELECT to_timestamp('2020-06-15 UTC', 'YYYY-MM-DD TZ')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-06-15 05:00:00+00
-- end-expected
SELECT to_timestamp('2020-06-15 EST', 'YYYY-MM-DD TZ')::text AS a;
-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid value "America/New_York" for "TZ"
-- end-expected-error
SELECT to_timestamp('2020-06-15 America/New_York', 'YYYY-MM-DD TZ')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-06-15
-- end-expected
SELECT to_date('2020-06-15 +05:30', 'YYYY-MM-DD OF')::text AS a;
-- ============================================================================
-- 7. The seconds of the epoch carry their fraction, and their limits
-- ============================================================================
-- begin-expected
-- columns: a
-- row: 2009-02-13 23:31:30.123456+00
-- end-expected
SELECT to_timestamp(1234567890.123456)::text AS a;
-- begin-expected
-- columns: a
-- row: 1970-01-01 00:00:00.5+00
-- end-expected
SELECT to_timestamp(0.5)::text AS a;
-- begin-expected
-- columns: a
-- row: 1969-12-31 23:59:59.5+00
-- end-expected
SELECT to_timestamp(-0.5)::text AS a;
-- begin-expected
-- columns: a
-- row: 1970-01-01 00:00:01+00
-- end-expected
SELECT to_timestamp(1.0000005)::text AS a;
-- begin-expected
-- columns: a
-- row: 1970-01-01 00:00:01.000002+00
-- end-expected
SELECT to_timestamp(1.0000015)::text AS a;
-- begin-expected
-- columns: a
-- row: 1970-01-01 00:00:00+00
-- end-expected
SELECT to_timestamp(0.0000005)::text AS a;
-- begin-expected-error
-- sqlstate: 22008
-- message-like: timestamp out of range: "1e+20"
-- end-expected-error
SELECT to_timestamp(1e20) AS a;
-- begin-expected-error
-- sqlstate: 22008
-- message-like: timestamp out of range: "1.23457e+14"
-- end-expected-error
SELECT to_timestamp(123456789012345.0) AS a;
-- begin-expected-error
-- sqlstate: 22008
-- message-like: timestamp out of range: "9.22337e+18"
-- end-expected-error
SELECT to_timestamp(9223372036854775807) AS a;
-- ============================================================================
-- 8. AT TIME ZONE names a zone out of the abbreviation table first
-- ============================================================================
-- begin-expected
-- columns: a
-- row: 2020-01-01 17:00:00+00
-- end-expected
SELECT (timestamp '2020-01-01 12:00:00' AT TIME ZONE 'EST')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-01-01 17:00:00+00
-- end-expected
SELECT (timestamp '2020-01-01 12:00:00' AT TIME ZONE 'est')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-07-01 11:00:00+00
-- end-expected
SELECT (timestamp '2020-07-01 12:00' AT TIME ZONE 'CET')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-07-01 11:00:00+00
-- end-expected
SELECT (timestamp '2020-07-01 12:00' AT TIME ZONE 'MET')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-07-01 10:00:00+00
-- end-expected
SELECT (timestamp '2020-07-01 12:00' AT TIME ZONE 'IST')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-07-01 16:00:00+00
-- end-expected
SELECT (timestamp '2020-07-01 12:00' AT TIME ZONE 'EST5EDT')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-01-01 17:00:00+00
-- end-expected
SELECT (timestamp '2020-01-01 12:00:00' AT TIME ZONE '+05')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-01-01 06:30:00+00
-- end-expected
SELECT (timestamp '2020-01-01 12:00:00' AT TIME ZONE '-05:30')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-01-01 17:30:00+00
-- end-expected
SELECT (timestamp '2020-01-01 12:00:00' AT TIME ZONE '05:30')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-01-01 17:00:00+00
-- end-expected
SELECT (timestamp '2020-01-01 12:00:00' AT TIME ZONE 'UTC+5')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-01-01 06:30:00+00
-- end-expected
SELECT (timestamp '2020-01-01 12:00:00' AT TIME ZONE INTERVAL '5:30')::text AS a;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: interval time zone "1 day" must not include months or days
-- end-expected-error
SELECT (timestamp '2020-01-01 12:00:00' AT TIME ZONE INTERVAL '1 day')::text AS a;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_catalog.timezone(integer, timestamp without time zone) does not exist
-- end-expected-error
SELECT (timestamp '2020-01-01 12:00:00' AT TIME ZONE 5)::text AS a;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: time zone "" not recognized
-- end-expected-error
SELECT (timestamp '2020-01-01 12:00:00' AT TIME ZONE '')::text AS a;
-- begin-expected
-- columns: a
-- row: t
-- end-expected
SELECT timezone(NULL, timestamp '2020-01-01 12:00:00') IS NULL AS a;
-- begin-expected
-- columns: a
-- row: 2020-01-01 07:00:00+00
-- end-expected
SELECT (timezone(interval '5 hours', timestamp '2020-01-01 12:00:00'))::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-01-01 17:00:00+00
-- end-expected
SELECT (timezone('EST', timestamp '2020-01-01 12:00:00'))::text AS a;
-- begin-expected
-- columns: a
-- row: 07:00:00-05
-- end-expected
SELECT (time '12:00:00' AT TIME ZONE 'EST')::text AS a;
-- begin-expected
-- columns: a
-- row: 07:00:00-05
-- end-expected
SELECT (timetz '12:00:00+00' AT TIME ZONE 'EST')::text AS a;
-- begin-expected
-- columns: a
-- row: time with time zone
-- end-expected
SELECT pg_typeof(time '12:00:00' AT TIME ZONE 'EST')::text AS a;
-- begin-expected
-- columns: a
-- row: 2019-12-31 19:00:00
-- end-expected
SELECT (date '2020-01-01' AT TIME ZONE 'EST')::text AS a;
-- ============================================================================
-- 9. The hour a zone puts back comes round twice, and standard time is the second
-- ============================================================================
SET TimeZone = 'America/New_York';
-- begin-expected
-- columns: a
-- row: 2020-11-01 01:30:00-05
-- end-expected
SELECT (timestamp '2020-11-01 01:30:00' AT TIME ZONE 'America/New_York')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-03-08 03:30:00-04
-- end-expected
SELECT (timestamp '2020-03-08 02:30:00' AT TIME ZONE 'America/New_York')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-03-08 12:00:00-04
-- end-expected
SELECT (timestamptz '2020-03-07 12:00:00-05' + interval '1 day')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-03-08 13:00:00-04
-- end-expected
SELECT (timestamptz '2020-03-07 12:00:00-05' + interval '24 hours')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-11-02 00:00:00-05
-- end-expected
SELECT (timestamptz '2020-11-01 00:00:00-04' + interval '1 day')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-11-01 01:30:00-05
-- end-expected
SELECT (timestamptz '2020-10-31 01:30:00-04' + interval '1 day')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-03-08 03:30:00-04
-- end-expected
SELECT (timestamptz '2020-03-07 02:30:00-05' + interval '1 day')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-11-01 00:00:00-04
-- end-expected
SELECT (timestamptz '2020-11-02 00:00:00-05' - interval '1 day')::text AS a;
-- begin-expected
-- columns: a
-- row: 3
-- end-expected
SELECT count(*) AS a FROM generate_series(timestamptz '2020-03-07 00:00:00-05', timestamptz '2020-03-09 00:00:00-04', interval '1 day');
-- begin-expected
-- columns: a
-- row: 2020-03-07 00:00:00-05
-- row: 2020-03-08 00:00:00-05
-- row: 2020-03-09 00:00:00-04
-- end-expected
SELECT g::text AS a FROM generate_series(timestamptz '2020-03-07 00:00:00-05', timestamptz '2020-03-09 00:00:00-04', interval '1 day') g;
-- begin-expected
-- columns: a
-- row: 2020-11-01 01:30:00-05
-- end-expected
SELECT date_add(timestamptz '2020-10-31 01:30:00-04', interval '1 day')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-11-01 01:30:00-04
-- end-expected
SELECT date_add(timestamptz '2020-10-31 01:30:00-04', interval '1 day', 'UTC')::text AS a;
-- begin-expected
-- columns: a
-- row: 2020-10-31 01:30:00-04
-- end-expected
SELECT date_subtract(timestamptz '2020-11-01 01:30:00-05', interval '1 day')::text AS a;
-- begin-expected
-- columns: a
-- row: timestamp with time zone
-- end-expected
SELECT pg_typeof(date_add(now(), interval '1 day'))::text AS a;
SET TimeZone = 'UTC';
-- ============================================================================
-- 10. The zone catalogue reports the names the zone database writes
-- ============================================================================
-- begin-expected
-- columns: a
-- row: 195
-- end-expected
SELECT count(*) AS a FROM pg_timezone_abbrevs;
-- begin-expected
-- columns: abbrev | o | is_dst
-- row: ACDT | 10:30:00 | t
-- row: CST | -06:00:00 | f
-- row: EST | -05:00:00 | f
-- row: IST | 02:00:00 | f
-- row: UTC | 00:00:00 | f
-- row: Z | 00:00:00 | f
-- row: ZULU | 00:00:00 | f
-- end-expected
SELECT abbrev, utc_offset::text AS o, is_dst FROM pg_timezone_abbrevs WHERE abbrev IN ('UTC','EST','IST','CST','ACDT','Z','ZULU') ORDER BY abbrev;
-- begin-expected
-- columns: name | abbrev
-- row: Africa/Windhoek | CAT
-- row: America/New_York | EDT
-- row: America/Sao_Paulo | -03
-- row: Asia/Kathmandu | +0545
-- row: Asia/Kolkata | IST
-- row: Europe/London | BST
-- row: UTC | UTC
-- end-expected
SELECT name, abbrev FROM pg_timezone_names WHERE name IN ('UTC','America/New_York','Asia/Kolkata','Asia/Kathmandu','America/Sao_Paulo','Europe/London','Africa/Windhoek') ORDER BY name;
-- expected-divergence: a zone named for a region and a place is in the file that region is
-- described in, and every build of the zone database has it. A zone named by one word is not:
-- MET, EST, Factory and ROC are written in the legacy, backward and factory files, which a
-- reduced build leaves out. How many of them come back is a property of the server's zone data.
-- begin-expected
-- columns: name | abbrev
-- row: EST | EST
-- row: Factory | -00
-- row: MET | CEST
-- row: ROC | CST
-- end-expected
SELECT name, abbrev FROM pg_timezone_names WHERE name IN ('MET','EST','Factory','ROC') ORDER BY name;
-- begin-expected
-- columns: a
-- row: 05:45:00
-- end-expected
SELECT utc_offset::text AS a FROM pg_timezone_names WHERE name = 'Asia/Kathmandu';
