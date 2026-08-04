-- How PostgreSQL reads an interval literal, and which spellings it refuses.
--
-- An interval literal is not one grammar but four, tried in turn, and the differences between
-- them are the whole subject. Everything below was measured against PostgreSQL 18.
--
--  1. ISO 8601 durations are read from the text exactly as given. The duration must begin at the
--     first character with an upper-case 'P' and end at the last, so ' P1Y', 'P1Y ' and 'p1Y'
--     are not durations at all -- and neither is a bare 'P', which names no field. Inside, PG
--     simply loops over number-and-designator pairs: a designator may repeat and its total
--     accumulates ('P1D1D' is two days), a quantity may be fractional or written with an
--     exponent, and every letter is upper case ('P1Y2M3Dt4H' is refused).
--
--  2. Two alternative ISO forms write the fields positionally instead: extended 'P0001-02-03',
--     and basic 'P00010203' with the separators run out. Which one is meant is decided by the
--     width of the very first number -- eight digits is a date, six a time -- and neither may
--     follow a designator field.
--
--  3. A unit word is looked up in a keyword table that holds each unit under at most ten
--     characters, after truncating the word it is given. That one rule decides the whole
--     accepted set: 'microseconds' and 'microsecon' name the same field because both shorten to
--     the same key, 'millenniums' works because it shortens to 'millennium' -- while 'cents',
--     'millenium' and 'milleniums' name nothing, and neither does 'quarter'.
--
--  4. A bare number is a count of seconds, but only when it is nothing but digits: a trailing
--     letter is a unit word, so '1d' is a day and not one second.
--
-- Where a fraction goes is decided per unit, and it never cascades twice. A fraction of a year
-- rounds to a whole month. A fraction of a month or a week becomes whole days first and only the
-- remainder becomes part of a day. A fraction of a day or smaller becomes microseconds. And an
-- interval with nothing in it -- '' or blank -- is a syntax error, not a zero.

-- 1: ISO 8601 durations: a designator per field, and the field may repeat

-- begin-expected
-- columns: v
-- row: 1 year 2 mons 3 days 04:05:06
-- end-expected
SELECT (interval 'P1Y2M3DT4H5M6S')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 year
-- end-expected
SELECT (interval 'P1Y')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 mon
-- end-expected
SELECT (interval 'P1M')::text AS v;

-- begin-expected
-- columns: v
-- row: 7 days
-- end-expected
SELECT (interval 'P1W')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 day
-- end-expected
SELECT (interval 'P1D')::text AS v;

-- begin-expected
-- columns: v
-- row: 01:00:00
-- end-expected
SELECT (interval 'PT1H')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:01:00
-- end-expected
SELECT (interval 'PT1M')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:01
-- end-expected
SELECT (interval 'PT1S')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00
-- end-expected
SELECT (interval 'PT0S')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00
-- end-expected
SELECT (interval 'PT')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 day
-- end-expected
SELECT (interval 'P1DT')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 year
-- end-expected
SELECT (interval 'P1YT')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 year 2 mons 25 days
-- end-expected
SELECT (interval 'P1Y2M3W4D')::text AS v;

-- begin-expected
-- columns: v
-- row: -1 years -2 mons -3 days -04:05:06
-- end-expected
SELECT (interval 'P-1Y-2M-3DT-4H-5M-6S')::text AS v;

-- begin-expected
-- columns: v
-- row: 2 days
-- end-expected
SELECT (interval 'P1D1D')::text AS v;

-- begin-expected
-- columns: v
-- row: 02:00:00
-- end-expected
SELECT (interval 'PT1H1H')::text AS v;

-- begin-expected
-- columns: v
-- row: 2 years
-- end-expected
SELECT (interval 'P1Y1Y')::text AS v;

-- begin-expected
-- columns: v
-- row: 2 mons
-- end-expected
SELECT (interval 'P1M1M')::text AS v;

-- begin-expected
-- columns: v
-- row: 21 days
-- end-expected
SELECT (interval 'P1W2W')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:02:00
-- end-expected
SELECT (interval 'PT1M1M')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:02
-- end-expected
SELECT (interval 'PT1S1S')::text AS v;

-- begin-expected
-- columns: v
-- row: 10 mons
-- end-expected
SELECT (interval 'P1Y-2M')::text AS v;

-- 2: ISO 8601 durations: a quantity may be fractional, and spills as PG's own units do

-- begin-expected
-- columns: v
-- row: 1 year 6 mons
-- end-expected
SELECT (interval 'P1.5Y')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 year
-- end-expected
SELECT (interval 'P0.99Y')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 year 1 mon
-- end-expected
SELECT (interval 'P1.08333Y')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 mon 15 days
-- end-expected
SELECT (interval 'P1.5M')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 mon 16 days 12:00:00
-- end-expected
SELECT (interval 'P1.55M')::text AS v;

-- begin-expected
-- columns: v
-- row: 29 days 16:48:00
-- end-expected
SELECT (interval 'P0.99M')::text AS v;

-- begin-expected
-- columns: v
-- row: 10 days 12:00:00
-- end-expected
SELECT (interval 'P1.5W')::text AS v;

-- begin-expected
-- columns: v
-- row: 13 days 22:19:12
-- end-expected
SELECT (interval 'P1.99W')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 day 12:00:00
-- end-expected
SELECT (interval 'P1.5D')::text AS v;

-- begin-expected
-- columns: v
-- row: 23:45:36
-- end-expected
SELECT (interval 'P0.99D')::text AS v;

-- begin-expected
-- columns: v
-- row: 12:00:00
-- end-expected
SELECT (interval 'P.5D')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 day
-- end-expected
SELECT (interval 'P1.D')::text AS v;

-- begin-expected
-- columns: v
-- row: -1 days -12:00:00
-- end-expected
SELECT (interval 'P-1.5D')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 day 13:30:00
-- end-expected
SELECT (interval 'P1.5DT1.5H')::text AS v;

-- begin-expected
-- columns: v
-- row: 01:30:00
-- end-expected
SELECT (interval 'PT1.5H')::text AS v;

-- begin-expected
-- columns: v
-- row: -01:30:00
-- end-expected
SELECT (interval 'PT-1.5H')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:01:30
-- end-expected
SELECT (interval 'PT1.5M')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:01.5
-- end-expected
SELECT (interval 'PT1.5S')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00
-- end-expected
SELECT (interval 'PT0.0000005S')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.000001
-- end-expected
SELECT (interval 'PT0.0000015S')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:02
-- end-expected
SELECT (interval 'PT1.9999999S')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 year 7 mons 15 days
-- end-expected
SELECT (interval 'P1.5Y1.5M')::text AS v;

-- begin-expected
-- columns: v
-- row: 100 days
-- end-expected
SELECT (interval 'P1e2D')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:10
-- end-expected
SELECT (interval 'PT1e1S')::text AS v;

-- begin-expected
-- columns: v
-- row: 15 days
-- end-expected
SELECT (interval 'P1.5e1D')::text AS v;

-- 3: ISO 8601 durations: the two alternative forms, positional and run together

-- begin-expected
-- columns: v
-- row: 1 year 2 mons 3 days
-- end-expected
SELECT (interval 'P0001-02-03')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 year 2 mons 3 days 04:05:06
-- end-expected
SELECT (interval 'P0001-02-03T04:05:06')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 year 2 mons 3 days 04:05:06.789
-- end-expected
SELECT (interval 'P0001-02-03T04:05:06.789')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 year 2 mons 3 days 04:05:06
-- end-expected
SELECT (interval 'P1-2-3T4:5:6')::text AS v;

-- begin-expected
-- columns: v
-- row: -10 mons +3 days
-- end-expected
SELECT (interval 'P-0001-02-03')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 year 2 mons 3 days
-- end-expected
SELECT (interval 'P00010203')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 year 2 mons 3 days 04:05:06
-- end-expected
SELECT (interval 'P00010203T040506')::text AS v;

-- begin-expected
-- columns: v
-- row: 102 years
-- end-expected
SELECT (interval 'P000102')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00
-- end-expected
SELECT (interval 'P00000000')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 year 2 mons 3 days 12:00:00
-- end-expected
SELECT (interval 'P00010203.5')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 year 2 mons 3 days 16:05:06
-- end-expected
SELECT (interval 'P00010203.5T040506')::text AS v;

-- begin-expected
-- columns: v
-- row: 405:30:00
-- end-expected
SELECT (interval 'PT0405.5')::text AS v;

-- begin-expected
-- columns: v
-- row: 04:30:00
-- end-expected
SELECT (interval 'PT04.5')::text AS v;

-- begin-expected
-- columns: v
-- row: 04:05:06.000001
-- end-expected
SELECT (interval 'PT040506.789')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:01
-- end-expected
SELECT (interval 'PT000001.5')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 year 2 mons 3 days 405:00:00
-- end-expected
SELECT (interval 'P00010203T0405')::text AS v;

-- 4: ISO 8601 durations: what is not one

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "P"
-- end-expected-error
SELECT (interval 'P')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "p1Y"
-- end-expected-error
SELECT (interval 'p1Y')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "P1y2m3dT4h5m6s"
-- end-expected-error
SELECT (interval 'P1y2m3dT4h5m6s')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "P1Y2M3Dt4H"
-- end-expected-error
SELECT (interval 'P1Y2M3Dt4H')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: " P1Y"
-- end-expected-error
SELECT (interval ' P1Y')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "P1Y "
-- end-expected-error
SELECT (interval 'P1Y ')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: " P1Y "
-- end-expected-error
SELECT (interval ' P1Y ')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "P 1Y"
-- end-expected-error
SELECT (interval 'P 1Y')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "P1Y2M3D4H"
-- end-expected-error
SELECT (interval 'P1Y2M3D4H')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "PY"
-- end-expected-error
SELECT (interval 'PY')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "PT1D"
-- end-expected-error
SELECT (interval 'PT1D')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "P1S"
-- end-expected-error
SELECT (interval 'P1S')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "PTS"
-- end-expected-error
SELECT (interval 'PTS')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "P+1D"
-- end-expected-error
SELECT (interval 'P+1D')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "PT+1S"
-- end-expected-error
SELECT (interval 'PT+1S')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "P--1D"
-- end-expected-error
SELECT (interval 'P--1D')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "P1D2"
-- end-expected-error
SELECT (interval 'P1D2')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "P1DX"
-- end-expected-error
SELECT (interval 'P1DX')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "P1,5D"
-- end-expected-error
SELECT (interval 'P1,5D')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "PT1,5S"
-- end-expected-error
SELECT (interval 'PT1,5S')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "P0001-02-03 04:05:06"
-- end-expected-error
SELECT (interval 'P0001-02-03 04:05:06')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "P0001-02-03t04:05:06"
-- end-expected-error
SELECT (interval 'P0001-02-03t04:05:06')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "P1e400D"
-- end-expected-error
SELECT (interval 'P1e400D')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "PT1e400S"
-- end-expected-error
SELECT (interval 'PT1e400S')::text AS v;

-- 5: ISO 8601 durations: a field that will not fit

-- begin-expected
-- columns: v
-- row: 2147483647 days
-- end-expected
SELECT (interval 'P2147483647D')::text AS v;

-- begin-expected-error
-- sqlstate: 22015
-- message-like: interval field value out of range: "P2147483648D"
-- end-expected-error
SELECT (interval 'P2147483648D')::text AS v;

-- begin-expected-error
-- sqlstate: 22008
-- message-like: interval out of range
-- end-expected-error
SELECT (interval 'P1000000000Y')::text AS v;

-- begin-expected
-- columns: v
-- row: 99999999 years
-- end-expected
SELECT (interval 'P99999999Y')::text AS v;

-- begin-expected
-- columns: v
-- row: 32767 years
-- end-expected
SELECT (interval 'P32767Y')::text AS v;

-- 6: Unit words: every spelling PostgreSQL keeps, and the near misses it does not

-- begin-expected
-- columns: v
-- row: 200 years
-- end-expected
SELECT (interval '2 c')::text AS v;

-- begin-expected
-- columns: v
-- row: 200 years
-- end-expected
SELECT (interval '2 cent')::text AS v;

-- begin-expected
-- columns: v
-- row: 200 years
-- end-expected
SELECT (interval '2 century')::text AS v;

-- begin-expected
-- columns: v
-- row: 200 years
-- end-expected
SELECT (interval '2 centuries')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "2 cents"
-- end-expected-error
SELECT (interval '2 cents')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "2 centurie"
-- end-expected-error
SELECT (interval '2 centurie')::text AS v;

-- begin-expected
-- columns: v
-- row: 2000 years
-- end-expected
SELECT (interval '2 mil')::text AS v;

-- begin-expected
-- columns: v
-- row: 2000 years
-- end-expected
SELECT (interval '2 mils')::text AS v;

-- begin-expected
-- columns: v
-- row: 2000 years
-- end-expected
SELECT (interval '2 millennia')::text AS v;

-- begin-expected
-- columns: v
-- row: 2000 years
-- end-expected
SELECT (interval '2 millennium')::text AS v;

-- begin-expected
-- columns: v
-- row: 2000 years
-- end-expected
SELECT (interval '2 millenniums')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "2 millennias"
-- end-expected-error
SELECT (interval '2 millennias')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "2 millenium"
-- end-expected-error
SELECT (interval '2 millenium')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "2 milleniums"
-- end-expected-error
SELECT (interval '2 milleniums')::text AS v;

-- begin-expected
-- columns: v
-- row: 20 years
-- end-expected
SELECT (interval '2 dec')::text AS v;

-- begin-expected
-- columns: v
-- row: 20 years
-- end-expected
SELECT (interval '2 decs')::text AS v;

-- begin-expected
-- columns: v
-- row: 20 years
-- end-expected
SELECT (interval '2 decade')::text AS v;

-- begin-expected
-- columns: v
-- row: 20 years
-- end-expected
SELECT (interval '2 decades')::text AS v;

-- begin-expected
-- columns: v
-- row: 2 years
-- end-expected
SELECT (interval '2 y')::text AS v;

-- begin-expected
-- columns: v
-- row: 2 years
-- end-expected
SELECT (interval '2 yr')::text AS v;

-- begin-expected
-- columns: v
-- row: 2 years
-- end-expected
SELECT (interval '2 yrs')::text AS v;

-- begin-expected
-- columns: v
-- row: 2 years
-- end-expected
SELECT (interval '2 year')::text AS v;

-- begin-expected
-- columns: v
-- row: 2 years
-- end-expected
SELECT (interval '2 years')::text AS v;

-- begin-expected
-- columns: v
-- row: 2 mons
-- end-expected
SELECT (interval '2 mon')::text AS v;

-- begin-expected
-- columns: v
-- row: 2 mons
-- end-expected
SELECT (interval '2 mons')::text AS v;

-- begin-expected
-- columns: v
-- row: 2 mons
-- end-expected
SELECT (interval '2 month')::text AS v;

-- begin-expected
-- columns: v
-- row: 2 mons
-- end-expected
SELECT (interval '2 months')::text AS v;

-- begin-expected
-- columns: v
-- row: 14 days
-- end-expected
SELECT (interval '2 w')::text AS v;

-- begin-expected
-- columns: v
-- row: 14 days
-- end-expected
SELECT (interval '2 week')::text AS v;

-- begin-expected
-- columns: v
-- row: 14 days
-- end-expected
SELECT (interval '2 weeks')::text AS v;

-- begin-expected
-- columns: v
-- row: 2 days
-- end-expected
SELECT (interval '2 d')::text AS v;

-- begin-expected
-- columns: v
-- row: 2 days
-- end-expected
SELECT (interval '2 day')::text AS v;

-- begin-expected
-- columns: v
-- row: 2 days
-- end-expected
SELECT (interval '2 days')::text AS v;

-- begin-expected
-- columns: v
-- row: 02:00:00
-- end-expected
SELECT (interval '2 h')::text AS v;

-- begin-expected
-- columns: v
-- row: 02:00:00
-- end-expected
SELECT (interval '2 hr')::text AS v;

-- begin-expected
-- columns: v
-- row: 02:00:00
-- end-expected
SELECT (interval '2 hrs')::text AS v;

-- begin-expected
-- columns: v
-- row: 02:00:00
-- end-expected
SELECT (interval '2 hour')::text AS v;

-- begin-expected
-- columns: v
-- row: 02:00:00
-- end-expected
SELECT (interval '2 hours')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:02:00
-- end-expected
SELECT (interval '2 m')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:02:00
-- end-expected
SELECT (interval '2 min')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:02:00
-- end-expected
SELECT (interval '2 mins')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:02:00
-- end-expected
SELECT (interval '2 minute')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:02:00
-- end-expected
SELECT (interval '2 minutes')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:02
-- end-expected
SELECT (interval '2 s')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:02
-- end-expected
SELECT (interval '2 sec')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:02
-- end-expected
SELECT (interval '2 secs')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:02
-- end-expected
SELECT (interval '2 second')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:02
-- end-expected
SELECT (interval '2 seconds')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.002
-- end-expected
SELECT (interval '2 ms')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.002
-- end-expected
SELECT (interval '2 msec')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.002
-- end-expected
SELECT (interval '2 msecs')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.002
-- end-expected
SELECT (interval '2 msecond')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.002
-- end-expected
SELECT (interval '2 mseconds')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.002
-- end-expected
SELECT (interval '2 millisecon')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.002
-- end-expected
SELECT (interval '2 millisecond')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.002
-- end-expected
SELECT (interval '2 milliseconds')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.000002
-- end-expected
SELECT (interval '2 us')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.000002
-- end-expected
SELECT (interval '2 usec')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.000002
-- end-expected
SELECT (interval '2 usecs')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.000002
-- end-expected
SELECT (interval '2 usecond')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.000002
-- end-expected
SELECT (interval '2 useconds')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.000002
-- end-expected
SELECT (interval '2 microsecon')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.000002
-- end-expected
SELECT (interval '2 microsecond')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.000002
-- end-expected
SELECT (interval '2 microseconds')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "2 quarter"
-- end-expected-error
SELECT (interval '2 quarter')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "2 qtr"
-- end-expected-error
SELECT (interval '2 qtr')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "2 timezone"
-- end-expected-error
SELECT (interval '2 timezone')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "2 epoch"
-- end-expected-error
SELECT (interval '2 epoch')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "2 julian"
-- end-expected-error
SELECT (interval '2 julian')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "2 j"
-- end-expected-error
SELECT (interval '2 j')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.002
-- end-expected
SELECT (interval '2 MS')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.000002
-- end-expected
SELECT (interval '2 US')::text AS v;

-- begin-expected
-- columns: v
-- row: 2000 years
-- end-expected
SELECT (interval '2 MIL')::text AS v;

-- begin-expected
-- columns: v
-- row: 200 years
-- end-expected
SELECT (interval '2 C')::text AS v;

-- 7: A fractional quantity spills into the next smaller field, never further

-- begin-expected
-- columns: v
-- row: 10 days 12:00:00
-- end-expected
SELECT (interval '1.5 weeks')::text AS v;

-- begin-expected
-- columns: v
-- row: 3 days 12:00:00
-- end-expected
SELECT (interval '0.5 weeks')::text AS v;

-- begin-expected
-- columns: v
-- row: 8 days 18:00:00
-- end-expected
SELECT (interval '1.25 weeks')::text AS v;

-- begin-expected
-- columns: v
-- row: 10 days 20:24:00
-- end-expected
SELECT (interval '1.55 weeks')::text AS v;

-- begin-expected
-- columns: v
-- row: 13 days 22:19:12
-- end-expected
SELECT (interval '1.99 weeks')::text AS v;

-- begin-expected
-- columns: v
-- row: -10 days -12:00:00
-- end-expected
SELECT (interval '-1.5 weeks')::text AS v;

-- begin-expected
-- columns: v
-- row: 17 days 12:00:00
-- end-expected
SELECT (interval '2.5 weeks')::text AS v;

-- begin-expected
-- columns: v
-- row: 10 days 12:00:00
-- end-expected
SELECT (interval '1.5 w')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 mon 16 days 12:00:00
-- end-expected
SELECT (interval '1.55 months')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 mon 27 days
-- end-expected
SELECT (interval '1.9 months')::text AS v;

-- begin-expected
-- columns: v
-- row: 29 days 16:48:00
-- end-expected
SELECT (interval '0.99 months')::text AS v;

-- begin-expected
-- columns: v
-- row: -1 mons -16 days -12:00:00
-- end-expected
SELECT (interval '-1.55 months')::text AS v;

-- begin-expected
-- columns: v
-- row: 2 years
-- end-expected
SELECT (interval '1.99 years')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 year
-- end-expected
SELECT (interval '0.99 years')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 year 1 mon
-- end-expected
SELECT (interval '1.08333 years')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 year 6 mons
-- end-expected
SELECT (interval '1.5 years')::text AS v;

-- begin-expected
-- columns: v
-- row: 15 years
-- end-expected
SELECT (interval '1.5 decades')::text AS v;

-- begin-expected
-- columns: v
-- row: 15 years 6 mons
-- end-expected
SELECT (interval '1.55 dec')::text AS v;

-- begin-expected
-- columns: v
-- row: 150 years
-- end-expected
SELECT (interval '1.5 centuries')::text AS v;

-- begin-expected
-- columns: v
-- row: 155 years
-- end-expected
SELECT (interval '1.55 centuries')::text AS v;

-- begin-expected
-- columns: v
-- row: 1500 years
-- end-expected
SELECT (interval '1.5 millennia')::text AS v;

-- begin-expected
-- columns: v
-- row: 1550 years
-- end-expected
SELECT (interval '1.55 mil')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 day 12:00:00
-- end-expected
SELECT (interval '1.5 days')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:30:00
-- end-expected
SELECT (interval '0.5 hours')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:01:30
-- end-expected
SELECT (interval '1.5 minutes')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:01.5
-- end-expected
SELECT (interval '1.5 seconds')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.0015
-- end-expected
SELECT (interval '1.5 ms')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.0014
-- end-expected
SELECT (interval '1.4 ms')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.000001
-- end-expected
SELECT (interval '0.0015 ms')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.000001
-- end-expected
SELECT (interval '1.5 us')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.000002
-- end-expected
SELECT (interval '2.5 us')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.000002
-- end-expected
SELECT (interval '1.6 us')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00
-- end-expected
SELECT (interval '0.5 us')::text AS v;

-- begin-expected
-- columns: v
-- row: -00:00:00.000001
-- end-expected
SELECT (interval '-1.5 us')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00
-- end-expected
SELECT (interval '0.0000005 seconds')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.000001
-- end-expected
SELECT (interval '0.0000015 seconds')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:02
-- end-expected
SELECT (interval '1.9999999 seconds')::text AS v;

-- 8: A fraction that lands exactly half way keeps the smaller magnitude, except in months

-- begin-expected
-- columns: v
-- row: 1 year 2 mons
-- end-expected
SELECT (interval '1.125 years')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 year 4 mons
-- end-expected
SELECT (interval '1.375 years')::text AS v;

-- begin-expected
-- columns: v
-- row: 2 mons
-- end-expected
SELECT (interval '0.125 years')::text AS v;

-- begin-expected
-- columns: v
-- row: -1 years -2 mons
-- end-expected
SELECT (interval '-1.125 years')::text AS v;

-- begin-expected
-- columns: v
-- row: 10 years 2 mons
-- end-expected
SELECT (interval '1.0125 decades')::text AS v;

-- begin-expected
-- columns: v
-- row: 100 years 2 mons
-- end-expected
SELECT (interval '1.00125 centuries')::text AS v;

-- begin-expected
-- columns: v
-- row: 1000 years 2 mons
-- end-expected
SELECT (interval '1.000125 millennia')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.000001
-- end-expected
SELECT (interval '0.0015 ms')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.000003
-- end-expected
SELECT (interval '0.0035 ms')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.000004
-- end-expected
SELECT (interval '0.0045 ms')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00
-- end-expected
SELECT (interval '0.0005 ms')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.001
-- end-expected
SELECT (interval '1.0005 ms')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.000001
-- end-expected
SELECT (interval '1.5 us')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.000002
-- end-expected
SELECT (interval '2.5 us')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.000003
-- end-expected
SELECT (interval '3.5 us')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.000004
-- end-expected
SELECT (interval '4.5 us')::text AS v;

-- begin-expected
-- columns: v
-- row: -00:00:00.000002
-- end-expected
SELECT (interval '-2.5 us')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.000001
-- end-expected
SELECT (interval '0.0000015 seconds')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.000003
-- end-expected
SELECT (interval '0.0000035 seconds')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.000004
-- end-expected
SELECT (interval '0.0000045 seconds')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:01.000001
-- end-expected
SELECT (interval '1.0000015 seconds')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:01.000002
-- end-expected
SELECT (interval '1.0000025 seconds')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.000001
-- end-expected
SELECT (interval '0.000000025 minutes')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 mon 12:00:00
-- end-expected
SELECT (interval '1.0166666666666667 months')::text AS v;

-- begin-expected
-- columns: v
-- row: 7 days 12:00:00
-- end-expected
SELECT (interval '1.0714285714285714 weeks')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 year 2 mons
-- end-expected
SELECT (interval 'P1.125Y')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.000001
-- end-expected
SELECT (interval 'PT0.0000015S')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.000003
-- end-expected
SELECT (interval 'PT0.0000035S')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 mon 12:00:00
-- end-expected
SELECT (interval 'P1.0166666666666667M')::text AS v;

-- begin-expected
-- columns: v
-- row: 7 days 12:00:00
-- end-expected
SELECT (interval 'P1.0714285714285714W')::text AS v;

-- 9: A bare number is seconds; a trailing letter is a unit, not a numeric suffix

-- begin-expected
-- columns: v
-- row: 00:00:01
-- end-expected
SELECT (interval '1')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:12
-- end-expected
SELECT (interval '12')::text AS v;

-- begin-expected
-- columns: v
-- row: -00:00:01
-- end-expected
SELECT (interval '-1')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:01.5
-- end-expected
SELECT (interval '1.5')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:01
-- end-expected
SELECT (interval '1.')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.5
-- end-expected
SELECT (interval '.5')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:01
-- end-expected
SELECT (interval '1.0000005')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00
-- end-expected
SELECT (interval '0')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 day
-- end-expected
SELECT (interval '1d')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 day
-- end-expected
SELECT (interval '1D')::text AS v;

-- begin-expected
-- columns: v
-- row: 01:00:00
-- end-expected
SELECT (interval '1h')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:01:00
-- end-expected
SELECT (interval '1m')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:01
-- end-expected
SELECT (interval '1s')::text AS v;

-- begin-expected
-- columns: v
-- row: 7 days
-- end-expected
SELECT (interval '1w')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 year
-- end-expected
SELECT (interval '1y')::text AS v;

-- begin-expected
-- columns: v
-- row: 100 years
-- end-expected
SELECT (interval '1c')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.001
-- end-expected
SELECT (interval '1ms')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.000001
-- end-expected
SELECT (interval '1us')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 mon
-- end-expected
SELECT (interval '1mon')::text AS v;

-- begin-expected
-- columns: v
-- row: 1000 years
-- end-expected
SELECT (interval '1mil')::text AS v;

-- begin-expected
-- columns: v
-- row: 2 days
-- end-expected
SELECT (interval '2days')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 day 12:00:00
-- end-expected
SELECT (interval '1.5d')::text AS v;

-- begin-expected
-- columns: v
-- row: 10 days 12:00:00
-- end-expected
SELECT (interval '1.5w')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "1e3"
-- end-expected-error
SELECT (interval '1e3')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "1f"
-- end-expected-error
SELECT (interval '1f')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "1e400"
-- end-expected-error
SELECT (interval '1e400')::text AS v;

-- 10: An interval with nothing in it is not an interval

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: ""
-- end-expected-error
SELECT (interval '')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: ""
-- end-expected-error
SELECT (''::interval)::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "   "
-- end-expected-error
SELECT (interval '   ')::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: "   "
-- end-expected-error
SELECT ('   '::interval)::text AS v;

-- begin-expected
-- columns: v
-- row: 
-- end-expected
SELECT (NULL::interval)::text AS v;

-- begin-expected
-- columns: v
-- row: 1 day
-- end-expected
SELECT ('1 day'::interval)::text AS v;

-- 11: The shapes PostgreSQL itself writes still read back

-- begin-expected
-- columns: v
-- row: 1 year 2 mons 3 days 04:05:06
-- end-expected
SELECT (interval '1 year 2 mons 3 days 04:05:06')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 year 2 mons -3 days -04:05:06
-- end-expected
SELECT (interval '1 year 2 mons -3 days -04:05:06')::text AS v;

-- begin-expected
-- columns: v
-- row: 04:05:06
-- end-expected
SELECT (interval '04:05:06')::text AS v;

-- begin-expected
-- columns: v
-- row: -04:05:06
-- end-expected
SELECT (interval '-04:05:06')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 year 2 mons
-- end-expected
SELECT (interval '1-2')::text AS v;

-- begin-expected
-- columns: v
-- row: 2 days 04:05:06
-- end-expected
SELECT (interval '2 04:05:06')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 day
-- end-expected
SELECT (interval '@ 1 day')::text AS v;

-- begin-expected
-- columns: v
-- row: -1 days
-- end-expected
SELECT (interval '@ 1 day ago')::text AS v;

-- begin-expected
-- columns: v
-- row: -1 days
-- end-expected
SELECT (interval '1 day ago')::text AS v;

-- begin-expected
-- columns: v
-- row: -1 days
-- end-expected
SELECT (interval '-1 day')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 mon 2 days
-- end-expected
SELECT (interval '1 mon 2 days')::text AS v;

-- begin-expected
-- columns: v
-- row: infinity
-- end-expected
SELECT (interval 'infinity')::text AS v;

-- begin-expected
-- columns: v
-- row: -infinity
-- end-expected
SELECT (interval '-infinity')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 day 01:00:00
-- end-expected
SELECT (interval '1 day 01:00:00')::text AS v;

-- begin-expected
-- columns: v
-- row: 1 year 1 mon 8 days 01:01:01
-- end-expected
SELECT (interval '1 year 1 month 1 week 1 day 1 hour 1 minute 1 second')::text AS v;

-- 12: Field qualifiers, and the units they read from a literal

-- begin-expected
-- columns: v
-- row: 00:00:01.235
-- end-expected
SELECT (INTERVAL '1.234567 seconds' SECOND(3))::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:01
-- end-expected
SELECT (INTERVAL '1.234567 seconds' SECOND(0))::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:01.234567
-- end-expected
SELECT (INTERVAL '1.234567 seconds' SECOND)::text AS v;

-- begin-expected
-- columns: v
-- row: 5 days
-- end-expected
SELECT (INTERVAL '5' DAY)::text AS v;

-- begin-expected
-- columns: v
-- row: 5 years
-- end-expected
SELECT (INTERVAL '5' YEAR)::text AS v;

-- begin-expected
-- columns: v
-- row: 1 day 02:00:00
-- end-expected
SELECT (INTERVAL '1 day 2 hours' DAY TO HOUR)::text AS v;

-- begin-expected
-- columns: v
-- row: 1 year 2 mons
-- end-expected
SELECT (INTERVAL '1-2' YEAR TO MONTH)::text AS v;

-- begin-expected
-- columns: v
-- row: 1 day 02:03:04
-- end-expected
SELECT (INTERVAL '1 2:03:04' DAY TO SECOND(2))::text AS v;

-- begin-expected
-- columns: v
-- row: 10 days
-- end-expected
SELECT (INTERVAL '1.5 weeks' DAY)::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.0015
-- end-expected
SELECT (INTERVAL '1.5 ms' SECOND)::text AS v;

-- begin-expected
-- columns: v
-- row: 200 years
-- end-expected
SELECT (INTERVAL '2 c' YEAR)::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.000002
-- end-expected
SELECT (INTERVAL '2 us' SECOND(6))::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:01.2
-- end-expected
SELECT (INTERVAL '1.234567 seconds' MINUTE TO SECOND(1))::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:01.23
-- end-expected
SELECT ('1.23456789 sec'::interval second(2))::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:01.23
-- end-expected
SELECT (CAST('1.23456789 sec' AS interval second(2)))::text AS v;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type interval: ""
-- end-expected-error
SELECT (''::interval day)::text AS v;

-- 13: extract from an interval, over every unit PostgreSQL documents

-- begin-expected
-- columns: v
-- row: 2
-- end-expected
SELECT extract(millennium from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v;

-- begin-expected
-- columns: v
-- row: 20
-- end-expected
SELECT extract(century from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v;

-- begin-expected
-- columns: v
-- row: 200
-- end-expected
SELECT extract(decade from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v;

-- begin-expected
-- columns: v
-- row: 2001
-- end-expected
SELECT extract(year from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v;

-- begin-expected
-- columns: v
-- row: 2
-- end-expected
SELECT extract(quarter from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v;

-- begin-expected
-- columns: v
-- row: 3
-- end-expected
SELECT extract(month from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v;

-- begin-expected
-- columns: v
-- row: 1
-- end-expected
SELECT extract(week from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v;

-- begin-expected
-- columns: v
-- row: 13
-- end-expected
SELECT extract(day from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v;

-- begin-expected
-- columns: v
-- row: 24
-- end-expected
SELECT extract(hour from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v;

-- begin-expected
-- columns: v
-- row: 5
-- end-expected
SELECT extract(minute from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v;

-- begin-expected
-- columns: v
-- row: 6.789000
-- end-expected
SELECT extract(second from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v;

-- begin-expected
-- columns: v
-- row: 6789.000
-- end-expected
SELECT extract(millisecond from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v;

-- begin-expected
-- columns: v
-- row: 6789000
-- end-expected
SELECT extract(microsecond from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v;

-- begin-expected
-- columns: v
-- row: 63155743506.789000
-- end-expected
SELECT extract(epoch from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: unit "dow" not supported for type interval
-- end-expected-error
SELECT extract(dow from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: unit "doy" not supported for type interval
-- end-expected-error
SELECT extract(doy from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: unit "isodow" not supported for type interval
-- end-expected-error
SELECT extract(isodow from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: unit "isoyear" not supported for type interval
-- end-expected-error
SELECT extract(isoyear from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: unit "timezone" not supported for type interval
-- end-expected-error
SELECT extract(timezone from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: unit "timezone_hour" not supported for type interval
-- end-expected-error
SELECT extract(timezone_hour from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: unit "timezone_minute" not supported for type interval
-- end-expected-error
SELECT extract(timezone_minute from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: unit "julian" not supported for type interval
-- end-expected-error
SELECT extract(julian from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v;

-- begin-expected
-- columns: v
-- row: 1
-- end-expected
SELECT extract(week from interval '13 days 24 hours') AS v;

-- begin-expected
-- columns: v
-- row: -20
-- end-expected
SELECT extract(century from interval '-2001 years') AS v;

-- begin-expected
-- columns: v
-- row: 
-- end-expected
SELECT extract(century from NULL::interval) AS v;

-- begin-expected
-- columns: v
-- row: 20
-- end-expected
SELECT date_part('century', interval '2001 years') AS v;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: unit "nosuchunit" not recognized for type interval
-- end-expected-error
SELECT extract(nosuchunit from interval '1 day') AS v;

-- 14: date_bin refuses a stride it cannot step with

-- begin-expected
-- columns: v
-- row: 2020-02-11 15:30:00
-- end-expected
SELECT date_bin(interval '15 minutes', timestamp '2020-02-11 15:44:17', timestamp '2001-01-01')::text AS v;

-- begin-expected
-- columns: v
-- row: 2020-02-11 00:00:00
-- end-expected
SELECT date_bin(interval '1 day', timestamp '2020-02-11 15:44:17', timestamp '2001-01-01')::text AS v;

-- begin-expected-error
-- sqlstate: 22008
-- message-like: stride must be greater than zero
-- end-expected-error
SELECT date_bin(interval '-2 hours', timestamp '2020-02-11 15:44:17', timestamp '2001-01-01')::text AS v;

-- begin-expected-error
-- sqlstate: 22008
-- message-like: stride must be greater than zero
-- end-expected-error
SELECT date_bin(interval '0 sec', timestamp '2020-02-11 15:44:17', timestamp '2001-01-01')::text AS v;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: timestamps cannot be binned into intervals containing months or years
-- end-expected-error
SELECT date_bin(interval '1 mon 1 day', timestamp '2020-02-11 15:44:17', timestamp '2001-01-01')::text AS v;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: timestamps cannot be binned into intervals containing months or years
-- end-expected-error
SELECT date_bin(interval '1 year', timestamp '2020-02-11 15:44:17', timestamp '2001-01-01')::text AS v;

-- begin-expected
-- columns: v
-- row: 
-- end-expected
SELECT date_bin(NULL::interval, timestamp '2020-02-11 15:44:17', timestamp '2001-01-01')::text AS v;

-- 15: An interval literal reads the same wherever it is written

-- begin-expected
-- columns: v
-- row: 10 days 12:00:00
-- end-expected
WITH c AS (SELECT interval '1.5 weeks' AS x) SELECT x::text AS v FROM c;

-- begin-expected
-- columns: v
-- row: 1 day 12:00:00
-- end-expected
SELECT (SELECT interval 'P1.5D')::text AS v;

-- begin-expected
-- columns: v
-- row: 00:00:00.002002
-- end-expected
SELECT (interval '2 ms' + interval '2 us')::text AS v;

-- begin-expected
-- columns: v
-- row: t
-- end-expected
SELECT (interval '1.5 weeks' = interval '10 days 12 hours') AS v;

-- begin-expected
-- columns: v
-- row: 907200.000000
-- end-expected
SELECT extract(epoch from interval '1.5 weeks') AS v;
