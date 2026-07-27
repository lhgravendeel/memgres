-- ============================================================================
-- Feature Comparison: OVERLAPS with unknown endpoints
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- A NULL endpoint makes that end of the period unknown rather than absent, so
-- the result is often NULL rather than true or false. Covers every NULL
-- placement, both the (start, end) and (start, length) spellings, and the
-- fully-known cases that must be unaffected.
-- ============================================================================

-- ============================================================================
-- 1. Fully-known periods
-- ============================================================================
SELECT ((DATE '2001-02-16', DATE '2001-12-21') OVERLAPS (DATE '2001-10-30', DATE '2002-10-30'))::text AS a;
SELECT ((DATE '2001-02-16', DATE '2001-12-21') OVERLAPS (DATE '2002-10-30', DATE '2003-10-30'))::text AS a;
SELECT ((DATE '2001-02-16', DATE '2001-02-20') OVERLAPS (DATE '2001-02-16', DATE '2001-02-20'))::text AS a;
-- adjacent periods do not overlap
SELECT ((DATE '2001-02-16', DATE '2001-02-20') OVERLAPS (DATE '2001-02-20', DATE '2001-02-25'))::text AS a;
-- a reversed pair is normalised, not an error
SELECT ((DATE '2001-12-21', DATE '2001-02-16') OVERLAPS (DATE '2001-10-30', DATE '2002-10-30'))::text AS a;
-- the (start, length) spelling
SELECT ((DATE '2001-02-16', INTERVAL '100 days') OVERLAPS (DATE '2001-02-16', INTERVAL '100 days'))::text AS a;
SELECT ((DATE '2001-02-16', INTERVAL '1 day') OVERLAPS (DATE '2001-06-01', INTERVAL '1 day'))::text AS a;
-- zero-length periods
SELECT ((DATE '2001-02-16', DATE '2001-02-16') OVERLAPS (DATE '2001-02-16', DATE '2001-02-20'))::text AS a;
SELECT ((DATE '2001-02-16', DATE '2001-02-16') OVERLAPS (DATE '2001-02-17', DATE '2001-02-20'))::text AS a;

-- ============================================================================
-- 2. One endpoint unknown
-- ============================================================================
SELECT coalesce(((DATE '2001-11-30', NULL::date) OVERLAPS (DATE '2001-10-30', DATE '2001-11-01'))::text, 'NULL') AS a;
SELECT coalesce(((NULL::date, DATE '2001-11-30') OVERLAPS (DATE '2001-10-30', DATE '2001-11-01'))::text, 'NULL') AS a;
SELECT coalesce(((DATE '2001-10-30', DATE '2001-11-01') OVERLAPS (DATE '2001-11-30', NULL::date))::text, 'NULL') AS a;
SELECT coalesce(((DATE '2001-10-30', DATE '2001-11-01') OVERLAPS (NULL::date, DATE '2001-11-30'))::text, 'NULL') AS a;
-- an unknown end that cannot change the answer still yields a definite result
SELECT coalesce(((DATE '2001-02-16', DATE '2001-02-20') OVERLAPS (DATE '2001-02-17', NULL::date))::text, 'NULL') AS a;
SELECT coalesce(((DATE '2001-02-17', NULL::date) OVERLAPS (DATE '2001-02-16', DATE '2001-02-20'))::text, 'NULL') AS a;
-- an unknown end that leaves the answer undetermined
SELECT coalesce(((DATE '2001-02-16', NULL::date) OVERLAPS (DATE '2001-06-01', DATE '2001-06-05'))::text, 'NULL') AS a;

-- ============================================================================
-- 3. Both endpoints of one period unknown
-- ============================================================================
SELECT coalesce(((NULL::date, NULL::date) OVERLAPS (DATE '2001-10-30', DATE '2001-11-01'))::text, 'NULL') AS a;
SELECT coalesce(((DATE '2001-10-30', DATE '2001-11-01') OVERLAPS (NULL::date, NULL::date))::text, 'NULL') AS a;
SELECT coalesce(((NULL::date, NULL::date) OVERLAPS (NULL::date, NULL::date))::text, 'NULL') AS a;

-- ============================================================================
-- 4. Unknown length in the (start, length) spelling
-- ============================================================================
SELECT coalesce(((DATE '2001-02-16', NULL::interval) OVERLAPS (DATE '2001-02-16', DATE '2001-02-20'))::text, 'NULL') AS a;
SELECT coalesce(((DATE '2001-02-16', INTERVAL '1 day') OVERLAPS (DATE '2001-02-16', NULL::date))::text, 'NULL') AS a;
SELECT coalesce(((NULL::date, INTERVAL '1 day') OVERLAPS (DATE '2001-02-16', DATE '2001-02-20'))::text, 'NULL') AS a;
SELECT coalesce(((DATE '2001-02-16', DATE '2001-02-20') OVERLAPS (NULL::date, INTERVAL '1 day'))::text, 'NULL') AS a;

-- ============================================================================
-- 5. Timestamp and timestamptz forms
-- ============================================================================
SELECT coalesce(((NULL::timestamp, NULL::timestamp) OVERLAPS (NULL::timestamp, NULL::timestamp))::text, 'NULL') AS a;
SELECT coalesce(((TIMESTAMP '2001-02-16 10:00', NULL::timestamp)
       OVERLAPS (TIMESTAMP '2001-02-16 09:00', TIMESTAMP '2001-02-16 11:00'))::text, 'NULL') AS a;
SELECT ((TIMESTAMP '2001-02-16 10:00', TIMESTAMP '2001-02-16 12:00')
       OVERLAPS (TIMESTAMP '2001-02-16 11:00', TIMESTAMP '2001-02-16 13:00'))::text AS a;
SELECT ((TIMESTAMPTZ '2001-02-16 10:00+00', TIMESTAMPTZ '2001-02-16 12:00+00')
       OVERLAPS (TIMESTAMPTZ '2001-02-16 11:00+00', TIMESTAMPTZ '2001-02-16 13:00+00'))::text AS a;
SELECT coalesce(((TIMESTAMPTZ '2001-02-16 10:00+00', NULL::timestamptz)
       OVERLAPS (TIMESTAMPTZ '2001-02-16 11:00+00', TIMESTAMPTZ '2001-02-16 13:00+00'))::text, 'NULL') AS a;

-- ============================================================================
-- 6. Column-driven, so the NULLs arrive as data rather than literals
-- ============================================================================
DROP TABLE IF EXISTS ovl_p CASCADE;
CREATE TABLE ovl_p (s1 date, e1 date, s2 date, e2 date);
INSERT INTO ovl_p VALUES
  ('2001-02-16', '2001-02-20', '2001-02-18', '2001-02-25'),
  ('2001-02-16', NULL,         '2001-02-18', '2001-02-25'),
  (NULL,         NULL,         '2001-02-18', '2001-02-25'),
  ('2001-02-16', '2001-02-20', '2001-06-01', NULL);
SELECT string_agg(coalesce(r::text, 'NULL'), ',' ORDER BY o) AS a FROM (
  SELECT row_number() OVER () AS o, ((s1, e1) OVERLAPS (s2, e2)) AS r FROM ovl_p) t;
SELECT count(*)::text AS a FROM ovl_p WHERE (s1, e1) OVERLAPS (s2, e2);
SELECT count(*)::text AS a FROM ovl_p WHERE NOT ((s1, e1) OVERLAPS (s2, e2));
SELECT count(*)::text AS a FROM ovl_p WHERE ((s1, e1) OVERLAPS (s2, e2)) IS NULL;

DROP TABLE IF EXISTS ovl_p CASCADE;
