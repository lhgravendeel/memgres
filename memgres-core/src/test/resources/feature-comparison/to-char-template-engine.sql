-- ============================================================================
-- Feature Comparison: the to_char / to_timestamp / to_date template engine
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- A formatting template is a language of its own, not a java.time pattern.
-- Handing it to DateTimeFormatter made to_timestamp and to_date ignore the
-- template altogether -- separators had to match exactly, MS meant absolute
-- milliseconds instead of a decimal fraction, a two-digit year was rejected
-- rather than pulled toward the present, and CC or a name pattern leaked
-- "Unknown pattern letter" as an internal error. On the writing side the
-- keyword table was a handful of prefixes, so FM landed inside its own field,
-- the numeric patterns lost V, RN, TH and the sign forms, and a value too wide
-- for its field silently widened instead of filling with '#'.
--
-- The locale-sensitive patterns L, D and G are left out on purpose: their
-- output follows the server's lc_numeric and lc_monetary, so they say nothing
-- about the template engine.
-- ============================================================================

SET TIME ZONE 'UTC';

-- ============================================================================
-- 1. to_char of a timestamp: the time-of-day patterns
-- ============================================================================
SELECT to_char(timestamp '2020-06-06 15:04:05.123456', 'HH HH12 HH24 MI SS SSSS SSSSS') AS a;
SELECT to_char(timestamp '2020-06-06 15:04:05.123456', 'MS US FF1 FF2 FF3 FF4 FF5 FF6') AS a;
SELECT to_char(timestamp '2020-06-06 05:04:05', 'FMHH FMHH24 FMMI FMSS FMSSSS') AS a;
SELECT to_char(timestamp '2020-06-06 15:04:05', 'AM PM A.M. P.M. am pm a.m. p.m.') AS a;
SELECT to_char(timestamp '2020-06-06 05:04:05', 'AM PM A.M. P.M. am pm a.m. p.m.') AS a;
SELECT to_char(timestamp '2020-01-01 00:00:00', 'HH12 AM') AS a;
SELECT to_char(timestamp '2020-01-01 12:00:00', 'HH12 AM') AS a;

-- ============================================================================
-- 2. to_char of a timestamp: the year, month and day patterns
-- ============================================================================
SELECT to_char(timestamp '2020-06-06 15:04:05', 'Y,YYY YYYY YYY YY Y CC') AS a;
SELECT to_char(timestamp '2020-06-06 15:04:05', 'FMY,YYY FMYYYY FMYYY FMYY FMY FMCC') AS a;
SELECT to_char(timestamp '2020-06-06 15:04:05', 'IYYY IYY IY I IW IDDD ID') AS a;
SELECT to_char(timestamp '2020-06-06 15:04:05', 'FMIYYY FMIW FMIDDD') AS a;
SELECT to_char(timestamp '2020-06-06 15:04:05', 'MONTH Month month MON Mon mon MM') AS a;
SELECT to_char(timestamp '2020-06-06 15:04:05', 'FMMONTH FMMonth FMmonth FMMON FMMon FMmon FMMM') AS a;
SELECT to_char(timestamp '2020-06-06 15:04:05', 'DAY Day day DY Dy dy DDD DD D') AS a;
SELECT to_char(timestamp '2020-06-06 15:04:05', 'FMDAY FMDay FMday FMDY FMDy FMdy FMDDD FMDD FMD') AS a;
SELECT to_char(timestamp '2020-06-06 15:04:05', 'W WW Q RM rm J') AS a;
SELECT to_char(timestamp '2020-06-06 15:04:05', 'FMW FMWW FMQ FMRM FMrm FMJ') AS a;
SELECT to_char(timestamp '2020-08-06 15:04:05', 'RM rm FMRM') AS a;
SELECT to_char(timestamp '2020-12-06 15:04:05', 'RM') AS a;
SELECT to_char(timestamp '2020-06-06 15:04:05', 'AD BC A.D. B.C. ad bc a.d. b.c.') AS a;
SELECT to_char(timestamp '2020-01-01 00:00:00', 'W WW DDD IW IDDD ID D') AS a;
SELECT to_char(timestamp '2020-12-31 00:00:00', 'W WW DDD IW IYYY ID D') AS a;
SELECT to_char(timestamp '2021-01-01 00:00:00', 'IW IYYY IDDD') AS a;

-- ============================================================================
-- 3. Suffixes, quoted runs and text that is not a keyword
-- ============================================================================
SELECT to_char(timestamp '2020-06-06 15:04:05', 'DDth ddth DDTH Ddth') AS a;
SELECT to_char(timestamp '2020-06-01 15:04:05', 'DDth DDTH FMDDth') AS a;
SELECT to_char(timestamp '2020-06-02 15:04:05', 'DDth') AS a;
SELECT to_char(timestamp '2020-06-03 15:04:05', 'DDth') AS a;
SELECT to_char(timestamp '2020-06-11 15:04:05', 'DDth') AS a;
SELECT to_char(timestamp '2020-06-12 15:04:05', 'DDth') AS a;
SELECT to_char(timestamp '2020-06-13 15:04:05', 'DDth') AS a;
SELECT to_char(timestamp '2020-06-21 15:04:05', 'DDth') AS a;
SELECT to_char(timestamp '2020-06-06 15:04:05', 'YYYYth MMth HH24th') AS a;
SELECT to_char(timestamp '2020-06-06 15:04:05', 'MMSP MMSPTH MMTHSP') AS a;
SELECT to_char(timestamp '2020-06-06 15:04:05', 'TMMonth TMDay TMMON') AS a;
SELECT to_char(timestamp '2020-06-06 15:04:05', '"Hello" YYYY') AS a;
SELECT to_char(timestamp '2020-06-06 15:04:05', 'HH24:MI:SS "o''clock"') AS a;
SELECT to_char(timestamp '2020-06-06 15:04:05', 'ZZZ') AS a;
SELECT to_char(timestamp '2020-06-06 15:04:05', 'MOn Mont MONt') AS a;
SELECT to_char(timestamp '2020-06-06 15:04:05', '') AS a;
SELECT to_char(timestamp '2020-06-06 15:04:05', NULL) AS a;
SELECT to_char(NULL::timestamp, 'YYYY') AS a;

-- ============================================================================
-- 4. to_char of the neighbouring temporal types
-- ============================================================================
SELECT to_char(date '2020-06-06', 'YYYY-MM-DD HH24:MI:SS') AS a;
SELECT to_char(time '15:04:05', 'HH24:MI:SS') AS a;
SELECT to_char(timestamptz '2020-06-06 15:04:05+02', 'YYYY-MM-DD HH24:MI:SS') AS a;
SELECT to_char(timestamptz '2020-06-06 15:04:05+02', 'TZH TZM OF') AS a;
SELECT to_char(interval '3 days 4 hours', 'HH24:MI:SS DD MM YYYY') AS a;
SELECT to_char(interval '1 year 2 months', 'YYYY MM DD HH24') AS a;

-- ============================================================================
-- 5. to_timestamp separators: without FX a separator matches loosely
-- ============================================================================
SELECT to_timestamp(' 2000    JUN', 'YYYY MON') AS a;
SELECT to_timestamp('2000 - JUN', 'YYYY-MON') AS a;
SELECT to_timestamp('2000JUN', 'YYYY///MON') AS a;
SELECT to_timestamp('2000/JUN', 'YYYY MON') AS a;
SELECT to_timestamp('2000/JUN', 'FXYYYY MON') AS a;
SELECT to_timestamp('2000 JUN', 'FXYYYY MON') AS a;
SELECT to_timestamp('2000-JUN', 'FXYYYY MON') AS a;
SELECT to_timestamp('2000  JUN', 'FXYYYY MON') AS a;
SELECT to_date('2020-06-06', 'FXYYYY-MM-DD') AS a;
SELECT to_date('2020-6-6', 'FXYYYY-MM-DD') AS a;

-- ============================================================================
-- 6. MS, US and FFn read a decimal fraction, not an absolute count
-- ============================================================================
SELECT to_timestamp('12.3', 'SS.MS') AS a;
SELECT to_timestamp('12.03', 'SS.MS') AS a;
SELECT to_timestamp('12.003', 'SS.MS') AS a;
SELECT to_timestamp('12.0003', 'SS.MS') AS a;
SELECT to_timestamp('15:12:02.020.001230', 'HH24:MI:SS.MS.US') AS a;
SELECT to_timestamp('1.2', 'SS.US') AS a;
SELECT to_timestamp('1.2', 'SS.FF3') AS a;
SELECT to_timestamp('2020-06-06 15:04:05.123', 'YYYY-MM-DD HH24:MI:SS.FF3') AS a;

-- ============================================================================
-- 7. A year shorter than four digits is pulled toward the present
-- ============================================================================
SELECT to_date('95', 'YY') AS a;
SELECT to_date('095', 'YYY') AS a;
SELECT to_date('5', 'Y') AS a;
SELECT to_date('20', 'YY') AS a;
SELECT to_date('69', 'YY') AS a;
SELECT to_date('70', 'YY') AS a;
SELECT to_date('00', 'YY') AS a;
SELECT to_date('1', 'YYY') AS a;
SELECT to_date('0', 'Y') AS a;
SELECT to_date('995', 'YYY') AS a;
SELECT to_date('519', 'YYY') AS a;
SELECT to_date('520', 'YYY') AS a;
-- four digits are taken at face value, in either direction
SELECT to_date('1995', 'YYYY') AS a;
SELECT to_date('95', 'YYYY') AS a;
SELECT to_date('1995', 'YYY') AS a;

-- ============================================================================
-- 8. CC, and years wider than four digits
-- ============================================================================
SELECT to_date('20 2020 06 06', 'CC YYYY MM DD') AS a;
SELECT to_date('21 95 06 06', 'CC YY MM DD') AS a;
SELECT to_date('21 00 06 06', 'CC YY MM DD') AS a;
SELECT to_date('19 95 06 06', 'CC YY MM DD') AS a;
SELECT to_date('20 06 06', 'CC MM DD') AS a;
SELECT to_date('21 06 06', 'CC MM DD') AS a;
SELECT to_date('1 06 06', 'CC MM DD') AS a;
SELECT to_date('20000-1130', 'YYYY-MMDD') AS a;
SELECT to_date('20000Nov30', 'YYYYMonDD') AS a;

-- ============================================================================
-- 9. Quoted runs skip input, and a letter in the template skips one character
-- ============================================================================
SELECT to_date('2020XX06XX06', 'YYYY"XX"MM"XX"DD') AS a;
SELECT to_date('2020ab06cd06', 'YYYY"XX"MM"XX"DD') AS a;
SELECT to_date('2020ab0cd06', 'YYYY"XX"MM"XX"DD') AS a;
SELECT to_timestamp('2000y6m1d', 'yyyytMMtDDt') AS a;
SELECT to_timestamp('2000y6m1d', 'yyyy"y"MM"m"DD"d"') AS a;

-- ============================================================================
-- 10. The ordinary shapes an application actually writes
-- ============================================================================
SELECT to_date('2020-06-06', 'YYYY-MM-DD') AS a;
SELECT to_date('06/06/2020', 'MM/DD/YYYY') AS a;
SELECT to_date('20200606', 'YYYYMMDD') AS a;
SELECT to_date('2020-6-6', 'YYYY-MM-DD') AS a;
SELECT to_date('  2020-6-6', 'YYYY-MM-DD') AS a;
SELECT to_date('2020-06-06extra', 'YYYY-MM-DD') AS a;
SELECT to_date('2020-06', 'YYYY-MM-DD') AS a;
SELECT to_date('2020 Jun 06', 'YYYY Mon DD') AS a;
SELECT to_date('2020 JUN 06', 'YYYY Mon DD') AS a;
SELECT to_date('2020 jun 06', 'YYYY MON DD') AS a;
SELECT to_date('2020 June 06', 'YYYY Month DD') AS a;
SELECT to_date('2020 JUNE 06', 'YYYY MONTH DD') AS a;
SELECT to_date('2020 june 06', 'YYYY month DD') AS a;
SELECT to_date('2020 Saturday 06 06', 'YYYY Day MM DD') AS a;
SELECT to_date('2020 Sat 06 06', 'YYYY Dy MM DD') AS a;
SELECT to_date('2020-160', 'YYYY-DDD') AS a;
SELECT to_date('2020-366', 'YYYY-DDD') AS a;
SELECT to_date('2020-000', 'YYYY-DDD') AS a;
SELECT to_date('2451187', 'J') AS a;
SELECT to_date('2020-00-01', 'YYYY-MM-DD') AS a;
SELECT to_date('0000-06-06', 'YYYY-MM-DD') AS a;
SELECT to_date('2020-06-06 BC', 'YYYY-MM-DD BC') AS a;
SELECT to_date('2020-06-06 AD', 'YYYY-MM-DD BC') AS a;
SELECT to_date('', 'YYYY') AS a;
SELECT to_date('2020', 'YYYY') AS a;
SELECT to_timestamp('2020', 'YYYY') AS a;
SELECT to_timestamp('2020-06-06 15:04:05', 'YYYY-MM-DD HH24:MI:SS') AS a;
SELECT to_timestamp('2020-06-06 03:04:05 PM', 'YYYY-MM-DD HH12:MI:SS AM') AS a;
SELECT to_timestamp('2020-06-06 03:04:05 pm', 'YYYY-MM-DD HH:MI:SS pm') AS a;
SELECT to_timestamp('2020-06-06 03:04:05 AM', 'YYYY-MM-DD HH12:MI:SS AM') AS a;
SELECT to_timestamp('2020-06-06 12:04:05 AM', 'YYYY-MM-DD HH12:MI:SS AM') AS a;
SELECT to_timestamp('2020-06-06 12:04:05 PM', 'YYYY-MM-DD HH12:MI:SS AM') AS a;
SELECT to_date('2020-06-06', 'YYYY-MM-DD') = date '2020-06-06' AS a;
SELECT to_date('2020-06-06', NULL) AS a;
SELECT to_date(NULL, 'YYYY-MM-DD') AS a;
SELECT to_timestamp(0) AS a;
SELECT to_timestamp(1000000000) AS a;

-- ============================================================================
-- 11. What the reader refuses
-- ============================================================================
SELECT to_date('abc', 'YYYY') AS a;
SELECT to_date('2020 Xyz 06', 'YYYY Mon DD') AS a;
SELECT to_date('2020 Jun 06', 'YYYY Month DD') AS a;
SELECT to_date('2020606', 'YYYYMMDD') AS a;
SELECT to_date('2020-13-01', 'YYYY-MM-DD') AS a;
SELECT to_date('2020-02-30', 'YYYY-MM-DD') AS a;
SELECT to_date('2020-06-31', 'YYYY-MM-DD') AS a;
SELECT to_date('Feb 30 2020', 'Mon DD YYYY') AS a;
SELECT to_timestamp('2020-06-06 13:04:05 PM', 'YYYY-MM-DD HH12:MI:SS AM') AS a;

-- ============================================================================
-- 12. Numeric templates: digits, padding and overflow
-- ============================================================================
SELECT to_char(485, '999') AS a;
SELECT to_char(-485, '999') AS a;
SELECT to_char(485, '9 9 9') AS a;
SELECT to_char(1485, '9,999') AS a;
SELECT to_char(148, '9,999') AS a;
SELECT to_char(148.5, '999.999') AS a;
SELECT to_char(148.5, 'FM999.999') AS a;
SELECT to_char(148.5, 'FM999.990') AS a;
SELECT to_char(12345, '999') AS a;
SELECT to_char(-12345, '999') AS a;
SELECT to_char(12345, 'FM999') AS a;
SELECT to_char(1234, '9999') AS a;
SELECT to_char(-1234, '9999') AS a;
SELECT to_char(485, '99.99') AS a;
SELECT to_char(4.85, '99.99') AS a;
SELECT to_char(0, '9') AS a;
SELECT to_char(0, '99') AS a;
SELECT to_char(0, '0') AS a;
SELECT to_char(0, 'FM9') AS a;
SELECT to_char(0, 'B9') AS a;
SELECT to_char(0, 'B99.99') AS a;
SELECT to_char(0.0, '9.9') AS a;
SELECT to_char(0.0, '0.9') AS a;
SELECT to_char(0.1, '9.9') AS a;
SELECT to_char(-0.1, '9.9') AS a;
SELECT to_char(-0.1, 'FM9.9') AS a;
SELECT to_char(0.1, 'FM9.9') AS a;
SELECT to_char(0, '999.999') AS a;
SELECT to_char(0, 'FM999.999') AS a;
SELECT to_char(12, '0000') AS a;
SELECT to_char(12, 'FM0000') AS a;
SELECT to_char(12, '99099') AS a;
SELECT to_char(12, '90009') AS a;
SELECT to_char(-12, '0000') AS a;
SELECT to_char(0.5, '.9') AS a;
SELECT to_char(0.5, 'FM.9') AS a;
SELECT to_char(485, '999.') AS a;
SELECT to_char(485, 'FM999.') AS a;
SELECT to_char(-485, 'FM999') AS a;

-- ============================================================================
-- 13. Numeric templates: V, EEEE, RN and TH
-- ============================================================================
SELECT to_char(148.5, '999V99') AS a;
SELECT to_char(485, '9V999') AS a;
SELECT to_char(0.485, '9V999') AS a;
SELECT to_char(0.0485, 'FM9V999') AS a;
SELECT to_char(12, '99V999') AS a;
SELECT to_char(12.4, '99V999') AS a;
SELECT to_char(12.45, '99V9') AS a;
SELECT to_char(-12.45, '99V9') AS a;
SELECT to_char(0.0004859, '9.99EEEE') AS a;
SELECT to_char(1234.5, '9.99EEEE') AS a;
SELECT to_char(0, '9.99EEEE') AS a;
SELECT to_char(-1234.5, '9.9EEEE') AS a;
SELECT to_char(1234.5, '99EEEE') AS a;
SELECT to_char(485, 'RN') AS a;
SELECT to_char(485, 'rn') AS a;
SELECT to_char(485, 'FMRN') AS a;
SELECT to_char(485, 'FMrn') AS a;
SELECT to_char(1, 'RN') AS a;
SELECT to_char(3999, 'RN') AS a;
SELECT to_char(0, 'RN') AS a;
SELECT to_char(4000, 'RN') AS a;
SELECT to_char(-5, 'RN') AS a;
SELECT to_char(5.2, 'FMRN') AS a;
SELECT to_char(5.6, 'FMRN') AS a;
SELECT to_char(481, '999th') AS a;
SELECT to_char(482, '999th') AS a;
SELECT to_char(483, '999th') AS a;
SELECT to_char(411, '999TH') AS a;
SELECT to_char(485, '999TH') AS a;
SELECT to_char(-481, '999th') AS a;

-- ============================================================================
-- 14. Numeric templates: where the sign goes
-- ============================================================================
SELECT to_char(485, 'S999') AS a;
SELECT to_char(-485, 'S999') AS a;
SELECT to_char(485, '999S') AS a;
SELECT to_char(-485, '999S') AS a;
SELECT to_char(-485, '99S9') AS a;
SELECT to_char(485, 'FMS999') AS a;
SELECT to_char(-485, 'FM999S') AS a;
SELECT to_char(485, 'MI999') AS a;
SELECT to_char(-485, 'MI999') AS a;
SELECT to_char(485, '999MI') AS a;
SELECT to_char(-485, '999MI') AS a;
SELECT to_char(485, 'FM999MI') AS a;
SELECT to_char(485, 'PL999') AS a;
SELECT to_char(-485, 'PL999') AS a;
SELECT to_char(485, '999PL') AS a;
SELECT to_char(-485, '999PL') AS a;
SELECT to_char(485, 'SG999') AS a;
SELECT to_char(-485, 'SG999') AS a;
SELECT to_char(485, '999SG') AS a;
SELECT to_char(-485, '999SG') AS a;
SELECT to_char(-485, '9SG99') AS a;
SELECT to_char(485, '999PR') AS a;
SELECT to_char(-485, '999PR') AS a;
SELECT to_char(-485, 'FM999PR') AS a;
SELECT to_char(485, 'FM999PR') AS a;
SELECT to_char(1.5, 'MI9PL') AS a;
SELECT to_char(-1.5, 'MI9PL') AS a;
SELECT to_char(1.5, 'PL9MI') AS a;
SELECT to_char(-1.5, 'PL9MI') AS a;
SELECT to_char(1.5, 'SG9MI') AS a;
SELECT to_char(-1.5, 'SG9MI') AS a;

-- ============================================================================
-- 15. Numeric templates: quoted runs, rounding and the templates it refuses
-- ============================================================================
SELECT to_char(485, '"Good number:"999') AS a;
SELECT to_char(485.8, '"Pre:"999" Post:" .999') AS a;
SELECT to_char(485, '999xyz') AS a;
SELECT to_char(485, 'xyz999') AS a;
SELECT to_char(1.5, 'abc') AS a;
SELECT to_char(1.5, '') AS a;
SELECT to_char(1.5::float8, '') AS a;
SELECT to_char(1.5, '9SP') AS a;
SELECT to_char(0.5, '9') AS a;
SELECT to_char(1.5, '9') AS a;
SELECT to_char(2.5, '9') AS a;
SELECT to_char(-2.5, '9') AS a;
SELECT to_char(0.5::float8, '9') AS a;
SELECT to_char(2.5::float8, '9') AS a;
SELECT to_char(485::int, '999') AS a;
SELECT to_char(485::bigint, '999') AS a;
SELECT to_char(485.5::float8, '999.9') AS a;
SELECT to_char(1.5::float4, '99.99') AS a;
SELECT to_char(1234567, '9999999') AS a;
SELECT to_char(123.456, '9999999.99') AS a;
SELECT to_char(1e-5::numeric, '9.99999') AS a;
SELECT to_char(1234.5678, 'FM9999.999') AS a;
SELECT to_char(NULL::numeric, '999') AS a;
SELECT to_char(485, NULL) AS a;
SELECT to_char(1.5, '99.9V99') AS a;
SELECT to_char(1.5, '9.9.9') AS a;
SELECT to_char(1.5, 'S9S9') AS a;
SELECT to_char(1.5, 'S9PR') AS a;
SELECT to_char(1.5, '9PR9') AS a;
SELECT to_char(1.5, '9MI9PR') AS a;
SELECT to_char(1.5, 'PR9') AS a;
SELECT to_char(1.5, 'S9MI') AS a;
SELECT to_char(1.5, 'MI9S') AS a;
SELECT to_char(1234.5, 'FM9.99EEEE') AS a;
SELECT to_char(1234.5, 'S9.99EEEE') AS a;
SELECT to_char(1234.5, '9.99EEEEV9') AS a;
