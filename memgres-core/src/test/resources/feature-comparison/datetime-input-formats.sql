-- ============================================================================
-- Feature Comparison: Date/time input format coverage
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- ============================================================================
-- SECTION A: DATE — standard formats (working)
-- ============================================================================

-- 1. ISO 8601
-- begin-expected
-- columns: d
-- row: 1999-01-08
-- end-expected
SELECT DATE '1999-01-08' AS d;

-- 2. Slash separator yyyy/mm/dd
-- begin-expected
-- columns: d
-- row: 1999-01-08
-- end-expected
SELECT DATE '1999/01/08' AS d;

-- 3. US format mm/dd/yyyy
-- begin-expected
-- columns: d
-- row: 1999-01-08
-- end-expected
SELECT DATE '01/08/1999' AS d;

-- ============================================================================
-- SECTION B: DATE — special keywords
-- ============================================================================

-- 4. epoch
-- begin-expected
-- columns: d
-- row: 1970-01-01
-- end-expected
SELECT DATE 'epoch' AS d;

-- 5. infinity
-- begin-expected
-- columns: d
-- row: infinity
-- end-expected
SELECT DATE 'infinity' AS d;

-- 6. -infinity
-- begin-expected
-- columns: d
-- row: -infinity
-- end-expected
SELECT DATE '-infinity' AS d;

-- 7. today (just check it parses)
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT DATE 'today' IS NOT NULL AS ok;

-- 8. yesterday
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT DATE 'yesterday' = DATE 'today' - INTERVAL '1 day' AS ok;

-- 9. tomorrow
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT DATE 'tomorrow' = DATE 'today' + INTERVAL '1 day' AS ok;

-- ============================================================================
-- SECTION C: DATE — alternative formats
-- ============================================================================

-- 10. Compact YYYYMMDD
-- begin-expected
-- columns: d
-- row: 1999-01-08
-- end-expected
SELECT DATE '19990108' AS d;

-- 11. Named month long
-- begin-expected
-- columns: d
-- row: 1999-01-08
-- end-expected
SELECT DATE 'January 8, 1999' AS d;

-- 12. Named month short
-- begin-expected
-- columns: d
-- row: 1999-01-08
-- end-expected
SELECT DATE 'Jan 8, 1999' AS d;

-- 13. Named month ISO style
-- begin-expected
-- columns: d
-- row: 1999-01-08
-- end-expected
SELECT DATE '1999-Jan-08' AS d;

-- 14. Julian date (J2451545 = 2000-01-01)
-- begin-expected
-- columns: d
-- row: 2000-01-01
-- end-expected
SELECT DATE 'J2451545' AS d;

-- ============================================================================
-- SECTION D: TIME formats
-- ============================================================================

-- 15. Standard HH:MM:SS
-- begin-expected
-- columns: t
-- row: 04:05:06
-- end-expected
SELECT TIME '04:05:06' AS t;

-- 16. With microseconds
-- begin-expected
-- columns: t
-- row: 04:05:06.789
-- end-expected
SELECT TIME '04:05:06.789' AS t;

-- 17. HH:MM without seconds
-- begin-expected
-- columns: t
-- row: 04:05:00
-- end-expected
SELECT TIME '04:05' AS t;

-- 18. allballs = 00:00:00
-- begin-expected
-- columns: t
-- row: 00:00:00
-- end-expected
SELECT TIME 'allballs' AS t;

-- ============================================================================
-- SECTION E: TIMESTAMP special keywords
-- ============================================================================

-- 19. epoch
-- begin-expected
-- columns: t
-- row: 1970-01-01 00:00:00
-- end-expected
SELECT TIMESTAMP 'epoch' AS t;

-- 20. now (just verify it parses)
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT TIMESTAMP 'now' IS NOT NULL AS ok;

-- 21. today = midnight
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT TIMESTAMP 'today' = DATE_TRUNC('day', NOW())::timestamp AS ok;

-- 22. yesterday
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT TIMESTAMP 'yesterday' IS NOT NULL AS ok;

-- 23. tomorrow
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT TIMESTAMP 'tomorrow' IS NOT NULL AS ok;

-- ============================================================================
-- SECTION F: TIMESTAMPTZ offset formats
-- ============================================================================

SET TimeZone = 'UTC';

-- 24. +HH:MM (standard)
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT '2024-01-08 04:05:06+00:00'::timestamptz = '2024-01-08 04:05:06+00'::timestamptz AS ok;

-- 25. Space before offset
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT '2024-01-08 04:05:06 +00:00'::timestamptz = '2024-01-08 04:05:06+00'::timestamptz AS ok;

-- 26. +HHMM (no colon)
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT '2024-01-08 04:05:06+0000'::timestamptz = '2024-01-08 04:05:06+00'::timestamptz AS ok;

-- 27. Z suffix = UTC
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT '2024-01-08T04:05:06Z'::timestamptz = '2024-01-08 04:05:06+00'::timestamptz AS ok;

-- 28. TZ abbreviation PST
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT '2024-01-08 04:05:06 PST'::timestamptz = '2024-01-08 12:05:06+00'::timestamptz AS ok;

-- 29. epoch for timestamptz
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT TIMESTAMPTZ 'epoch' = '1970-01-01 00:00:00+00'::timestamptz AS ok;

-- 30. now for timestamptz
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT TIMESTAMPTZ 'now' IS NOT NULL AS ok;

-- ============================================================================
-- SECTION G: INTERVAL ISO 8601 duration
-- ============================================================================

-- 31. ISO 8601 duration format
-- begin-expected
-- columns: i
-- row: 1 year 2 mons 3 days 04:05:06
-- end-expected
SELECT INTERVAL 'P1Y2M3DT4H5M6S' AS i;

-- ============================================================================
-- SECTION H: Negative tests — formats PG rejects
-- ============================================================================

-- 32. Invalid month
-- begin-expected-error
-- sqlstate: 22008
-- end-expected
SELECT DATE '2024-13-01';

-- 33. Invalid day
-- begin-expected-error
-- sqlstate: 22008
-- end-expected
SELECT DATE '2023-02-29';

-- 34. Nonsense string
-- begin-expected-error
-- end-expected
SELECT DATE 'not-a-date';
