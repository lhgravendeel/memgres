-- ============================================================================
-- Feature Comparison: Range Bound Typing and Ordered-Set Aggregates
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Two clusters:
--   (a) A range's bounds are values of its element type. They are read with that
--       type's input function (so "infinity" and "NaN" are ordinary bounds),
--       written with its output function (so a timestamp bound is normalised and
--       quoted), returned by lower()/upper() as values of it, and compared
--       against probes on the same scale.
--   (b) The hypothetical-set aggregates rank a whole row against the group, so
--       their direct arguments must match the WITHIN GROUP sort columns one for
--       one and every sort column takes part in the comparison.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
-- ============================================================================

SET TIME ZONE 'UTC';

DROP SCHEMA IF EXISTS rbo_test CASCADE;
CREATE SCHEMA rbo_test;
SET search_path = rbo_test, public;

-- ============================================================================
-- SECTION A: infinite bounds are read, not refused
-- ============================================================================

-- begin-expected
-- columns: r
-- row: ["2020-01-01 00:00:00",infinity)
-- end-expected
SELECT '[2020-01-01,infinity)'::tsrange AS r;

-- begin-expected
-- columns: r
-- row: ["2020-01-01 00:00:00",infinity)
-- end-expected
SELECT '["2020-01-01",infinity)'::tsrange AS r;

-- begin-expected
-- columns: r
-- row: ["2020-01-01 00:00:00+00",infinity)
-- end-expected
SELECT '[2020-01-01,infinity)'::tstzrange AS r;

-- begin-expected
-- columns: r
-- row: [2020-01-01,infinity)
-- end-expected
SELECT '[2020-01-01,infinity)'::daterange AS r;

-- begin-expected
-- columns: r
-- row: [2020-01-01,infinity]
-- end-expected
SELECT '[2020-01-01,infinity]'::daterange AS r;

-- begin-expected
-- columns: r
-- row: [infinity,infinity]
-- end-expected
SELECT '[infinity,infinity]'::tsrange AS r;

-- begin-expected
-- columns: r
-- row: (,infinity)
-- end-expected
SELECT '(,infinity)'::tsrange AS r;

-- begin-expected
-- columns: r
-- row: [-infinity,infinity]
-- end-expected
SELECT '[-infinity,infinity]'::tsrange AS r;

-- begin-expected
-- columns: upper_inf|lower_inf|isempty
-- row: false|false|false
-- end-expected
SELECT upper_inf('[2020-01-01,infinity)'::tsrange) AS upper_inf,
       lower_inf('[-infinity,2020-01-01)'::tsrange) AS lower_inf,
       isempty('[2020-01-01,infinity)'::tsrange) AS isempty;

-- begin-expected
-- columns: u
-- row: infinity
-- end-expected
SELECT upper('[2020-01-01,infinity)'::tsrange) AS u;

-- begin-expected
-- columns: l|u
-- row: 2020-01-01|infinity
-- end-expected
SELECT lower('[2020-01-01,infinity)'::daterange) AS l,
       upper('[2020-01-01,infinity)'::daterange) AS u;

-- begin-expected
-- columns: upper_inf
-- row: false
-- end-expected
SELECT upper_inf('[2020-01-01,infinity)'::daterange) AS upper_inf;

-- begin-expected
-- columns: held
-- row: true
-- end-expected
SELECT '[2020-01-01,infinity)'::daterange @> '2021-01-01'::date AS held;

-- ============================================================================
-- SECTION B: a numeric range takes numeric's own specials
-- ============================================================================

-- begin-expected
-- columns: r
-- row: [1,Infinity)
-- end-expected
SELECT '[1,Infinity)'::numrange AS r;

-- begin-expected
-- columns: r
-- row: [1,Infinity)
-- end-expected
SELECT '[1,infinity)'::numrange AS r;

-- begin-expected
-- columns: r
-- row: (-Infinity,1]
-- end-expected
SELECT '(-Infinity,1]'::numrange AS r;

-- begin-expected
-- columns: r
-- row: [1,NaN)
-- end-expected
SELECT '[1,NaN)'::numrange AS r;

-- begin-expected
-- columns: l|u
-- row: 1|Infinity
-- end-expected
SELECT lower('[1,Infinity)'::numrange) AS l, upper('[1,Infinity)'::numrange) AS u;

-- begin-expected
-- columns: held
-- row: true
-- end-expected
SELECT '[1,Infinity)'::numrange @> 1e10 AS held;

-- An integer has no infinity, so its range type refuses the word outright.
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "infinity"
-- end-expected-error
SELECT '[1,infinity)'::int4range;

-- ============================================================================
-- SECTION C: lower()/upper() answer in the element type
-- ============================================================================

-- begin-expected
-- columns: u|t
-- row: 2020-02-01 00:00:00|timestamp without time zone
-- end-expected
SELECT upper('[2020-01-01,2020-02-01)'::tsrange) AS u,
       pg_typeof(upper('[2020-01-01,2020-02-01)'::tsrange))::text AS t;

-- begin-expected
-- columns: l|t
-- row: 2020-01-01|date
-- end-expected
SELECT lower('[2020-01-01,2020-02-01)'::daterange) AS l,
       pg_typeof(lower('[2020-01-01,2020-02-01)'::daterange))::text AS t;

-- begin-expected
-- columns: u
-- row: 2020-01-01 13:00:00
-- end-expected
SELECT upper('["2020-01-01 12:00:00","2020-01-01 13:00:00")'::tsrange) AS u;

-- begin-expected
-- columns: u
-- row: 2020-02-01 00:00:00
-- end-expected
SELECT upper(tsrange('2020-01-01'::timestamp,'2020-02-01'::timestamp)) AS u;

-- A bound with a time of day is a bound like any other, not an unreadable one.
-- begin-expected
-- columns: l
-- row: 2020-01-01 12:34:56
-- end-expected
SELECT lower('[2020-01-01 12:34:56,2020-02-01)'::tsrange) AS l;

-- begin-expected
-- columns: held
-- row: true
-- end-expected
SELECT '[2020-01-01,2020-02-01)'::tsrange @> '2020-01-15'::timestamp AS held;

-- ============================================================================
-- SECTION D: the text form is the element type's text form
-- ============================================================================

-- begin-expected
-- columns: r
-- row: ["2020-01-01 00:00:00","2020-02-01 00:00:00")
-- end-expected
SELECT '[2020-01-01,2020-02-01)'::tsrange AS r;

-- begin-expected
-- columns: r
-- row: ["2020-01-01 00:00:00+00","2020-02-01 00:00:00+00")
-- end-expected
SELECT '[2020-01-01,2020-02-01)'::tstzrange AS r;

-- begin-expected
-- columns: r
-- row: ["2020-01-01 00:00:00.5","2020-02-01 00:00:00")
-- end-expected
SELECT '[2020-01-01 00:00:00.5,2020-02-01)'::tsrange AS r;

-- begin-expected
-- columns: r
-- row: ["2020-02-01 00:00:00","2020-03-01 00:00:00")
-- end-expected
SELECT '[2020-01-01,2020-03-01)'::tsrange * '[2020-02-01,2020-04-01)'::tsrange AS r;

-- begin-expected
-- columns: r
-- row: ["2020-01-01 00:00:00","2020-04-01 00:00:00")
-- end-expected
SELECT range_merge('[2020-01-01,2020-02-01)'::tsrange,
                   '[2020-03-01,2020-04-01)'::tsrange) AS r;

-- begin-expected
-- columns: r
-- row: {["2020-01-01 00:00:00","2020-02-01 00:00:00")}
-- end-expected
SELECT '{[2020-01-01,2020-02-01)}'::tsmultirange AS r;

-- The two spellings name the same value, so they compare equal.
-- begin-expected
-- columns: same
-- row: true
-- end-expected
SELECT '[2020-01-01,2020-02-01)'::tsrange
     = '["2020-01-01 00:00:00","2020-02-01 00:00:00")'::tsrange AS same;

-- A date range is discrete, so an inclusive upper bound is rewritten.
-- begin-expected
-- columns: a|b
-- row: [2020-01-01,2020-02-02)|[2020-01-02,2020-02-01)
-- end-expected
SELECT '[2020-01-01,2020-02-01]'::daterange AS a,
       '(2020-01-01,2020-02-01)'::daterange AS b;

-- A timestamp range is continuous, so its bounds stay as written.
-- begin-expected
-- columns: r
-- row: ["2020-01-01 00:00:00","2020-02-01 00:00:00"]
-- end-expected
SELECT '[2020-01-01,2020-02-01]'::tsrange AS r;

-- A range literal is not a multirange literal, whatever the braces would make of it.
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed multirange literal
-- end-expected-error
SELECT '[2020-01-01,2020-02-01)'::tsmultirange;

-- A range value, on the other hand, casts to its multirange type.
-- begin-expected
-- columns: r
-- row: {[1,4)}
-- end-expected
SELECT int4range(1,4)::int4multirange AS r;

-- ============================================================================
-- SECTION E: a range column stores the element type's canonical form
-- ============================================================================

CREATE TABLE rbo_r (id int, ts tsrange, dr daterange, nr numrange, ir int4range);
INSERT INTO rbo_r VALUES
  (1, '[2020-01-01,2020-02-01)', '[2020-01-01,2020-02-01)', '[1.5,9.5)', '[1,10)');
INSERT INTO rbo_r VALUES (2, NULL, NULL, NULL, NULL);
CREATE VIEW rbo_v AS SELECT id, ts FROM rbo_r;

-- begin-expected
-- columns: ts|dr|nr|ir
-- row: ["2020-01-01 00:00:00","2020-02-01 00:00:00")|[2020-01-01,2020-02-01)|[1.5,9.5)|[1,10)
-- row: NULL|NULL|NULL|NULL
-- end-expected
SELECT ts, dr, nr, ir FROM rbo_r ORDER BY id;

-- begin-expected
-- columns: ts
-- row: ["2020-01-01 00:00:00","2020-02-01 00:00:00")
-- row: NULL
-- end-expected
SELECT ts FROM rbo_v ORDER BY id;

-- begin-expected
-- columns: lts|uts|ldr|udr|lnr
-- row: 2020-01-01 00:00:00|2020-02-01 00:00:00|2020-01-01|2020-02-01|1.5
-- end-expected
SELECT lower(ts) AS lts, upper(ts) AS uts, lower(dr) AS ldr,
       upper(dr) AS udr, lower(nr) AS lnr
FROM rbo_r WHERE id = 1;

-- A NULL range has no bounds to report, and says so rather than failing.
-- begin-expected
-- columns: lts|uts
-- row: NULL|NULL
-- end-expected
SELECT lower(ts) AS lts, upper(ts) AS uts FROM rbo_r WHERE id = 2;

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM rbo_r WHERE ts @> '2020-01-15'::timestamp;

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM rbo_r WHERE dr @> '2020-01-15'::date;

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM rbo_r WHERE ir @> 5;

-- A derived column keeps the type its expression produced.
-- begin-expected
-- columns: lo
-- row: 2020-01-01 00:00:00
-- end-expected
SELECT sub.lo FROM (SELECT lower(ts) AS lo FROM rbo_r WHERE id = 1) sub
WHERE sub.lo >= '2019-01-01'::timestamp;

-- begin-expected
-- columns: u
-- row: 2020-02-01 00:00:00
-- end-expected
SELECT upper(ts) AS u FROM rbo_r WHERE id = 1 ORDER BY upper(ts);

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM rbo_r GROUP BY id, ts ORDER BY id;

-- begin-expected
-- columns: r
-- row: {[1,10)}
-- end-expected
SELECT range_agg(ir) AS r FROM rbo_r;

-- begin-expected
-- columns: r
-- row: {["2020-01-01 00:00:00","2020-02-01 00:00:00")}
-- end-expected
SELECT range_agg(ts) AS r FROM rbo_r;

-- ============================================================================
-- SECTION F: the integer and numeric ranges keep working exactly as before
-- ============================================================================

-- begin-expected
-- columns: a|b|c|d
-- row: [1,10)|1|10|true
-- end-expected
SELECT '[1,10)'::int4range AS a, lower('[1,10)'::int4range) AS b,
       upper('[1,10)'::int4range) AS c, '[1,10)'::int4range @> 5 AS d;

-- begin-expected
-- columns: a|b
-- row: false|true
-- end-expected
SELECT '[1,10)'::int4range @> 10 AS a, '[1.5,9.5)'::numrange @> 2.0 AS b;

-- begin-expected
-- columns: a|b|c
-- row: [1,10)|[5,10)|[1,10)
-- end-expected
SELECT '[1,10)'::int4range * '[0,20)'::int4range AS a,
       '[1,10)'::int4range - '[0,5)'::int4range AS b,
       '[1,5)'::int4range + '[5,10)'::int4range AS c;

-- begin-expected
-- columns: a|b
-- row: {[1,3)}|{[1,5),[10,20)}
-- end-expected
SELECT '{[1,5)}'::int4multirange - '{[3,7)}'::int4multirange AS a,
       '{[1,20)}'::int4multirange - '{[5,10)}'::int4multirange AS b;

-- begin-expected
-- columns: a|b|c
-- row: empty|(,5)|[5,)
-- end-expected
SELECT '[1,1)'::int4range AS a, '[,5)'::int4range AS b, '[5,)'::int4range AS c;

-- begin-expected
-- columns: a|b
-- row: NULL|true
-- end-expected
SELECT lower('empty'::int4range) AS a, isempty('empty'::int4range) AS b;

-- begin-expected
-- columns: a|b
-- row: NULL|NULL
-- end-expected
SELECT lower(NULL::tsrange) AS a, upper(NULL::daterange) AS b;

-- begin-expected
-- columns: a|b|c
-- row: true|true|true
-- end-expected
SELECT '[2020-01-01,2020-02-01)'::tsrange && '[2020-01-15,2020-03-01)'::tsrange AS a,
       '[2020-01-01,2020-02-01)'::tsrange @> '[2020-01-10,2020-01-20)'::tsrange AS b,
       '[2020-01-01,2020-02-01)'::daterange @> '[2020-01-10,2020-01-20)'::daterange AS c;

-- begin-expected
-- columns: a|b
-- row: true|true
-- end-expected
SELECT '[2020-01-01,2020-02-01)'::tsrange << '[2020-03-01,2020-04-01)'::tsrange AS a,
       '[2020-01-01,2020-02-01)'::tsrange -|- '[2020-02-01,2020-03-01)'::tsrange AS b;

-- lower()/upper() on text are still the case-folding functions.
-- begin-expected
-- columns: a|b
-- row: hello|HELLO
-- end-expected
SELECT lower('HELLO') AS a, upper('hello') AS b;

-- A literal with three bounds is no range at all.
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed range literal
-- end-expected-error
SELECT '[1,2,3)'::int4range;

-- ============================================================================
-- SECTION G: hypothetical-set aggregates compare every sort column
-- ============================================================================

CREATE TABLE rbo_t (v int);
INSERT INTO rbo_t VALUES (1), (2), (3);

CREATE TABLE rbo_h2 (v int, w int);
INSERT INTO rbo_h2 VALUES (1,1), (1,5), (1,9), (2,1);

-- begin-expected
-- columns: a|b|c
-- row: 2|3|4
-- end-expected
SELECT rank(1,5) WITHIN GROUP (ORDER BY v, w) AS a,
       rank(1,9) WITHIN GROUP (ORDER BY v, w) AS b,
       rank(1,10) WITHIN GROUP (ORDER BY v, w) AS c
FROM rbo_h2;

-- begin-expected
-- columns: a|b|c
-- row: 2|0.6|0.25
-- end-expected
SELECT dense_rank(1,5) WITHIN GROUP (ORDER BY v, w) AS a,
       cume_dist(1,5) WITHIN GROUP (ORDER BY v, w) AS b,
       percent_rank(1,5) WITHIN GROUP (ORDER BY v, w) AS c
FROM rbo_h2;

-- Each sort column's direction takes part in the comparison.
-- begin-expected
-- columns: a|b
-- row: 2|3
-- end-expected
SELECT rank(1,5) WITHIN GROUP (ORDER BY v, w DESC) AS a,
       rank(1,5) WITHIN GROUP (ORDER BY v DESC, w) AS b
FROM rbo_h2;

-- A NULL in the hypothetical row sorts where the ORDER BY clause puts it.
-- begin-expected
-- columns: a
-- row: 4
-- end-expected
SELECT rank(NULL) WITHIN GROUP (ORDER BY v) AS a FROM rbo_t;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT rank(NULL) WITHIN GROUP (ORDER BY v NULLS FIRST) AS a FROM rbo_t;

-- begin-expected
-- columns: a
-- row: 4
-- end-expected
SELECT rank(1,NULL) WITHIN GROUP (ORDER BY v, w) AS a FROM rbo_h2;

-- ============================================================================
-- SECTION H: the direct arguments must match the sort columns
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function rank(integer, integer, integer) does not exist
-- end-expected-error
SELECT rank(1,2) WITHIN GROUP (ORDER BY v) FROM rbo_t;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function rank(integer, integer, integer) does not exist
-- end-expected-error
SELECT rank(2) WITHIN GROUP (ORDER BY v, w) FROM rbo_h2;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function rank(integer, integer, integer, integer, integer) does not exist
-- end-expected-error
SELECT rank(2,20,3) WITHIN GROUP (ORDER BY v, w) FROM rbo_h2;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function rank(integer) does not exist
-- end-expected-error
SELECT rank() WITHIN GROUP (ORDER BY v) FROM rbo_t;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function dense_rank(integer, integer, integer) does not exist
-- end-expected-error
SELECT dense_rank(1,2) WITHIN GROUP (ORDER BY v) FROM rbo_t;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function percent_rank(integer, integer, integer) does not exist
-- end-expected-error
SELECT percent_rank(1,2) WITHIN GROUP (ORDER BY v) FROM rbo_t;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function cume_dist(integer, integer, integer) does not exist
-- end-expected-error
SELECT cume_dist(1,2) WITHIN GROUP (ORDER BY v) FROM rbo_t;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function percentile_cont(numeric, numeric, integer) does not exist
-- end-expected-error
SELECT percentile_cont(0.5,0.9) WITHIN GROUP (ORDER BY v) FROM rbo_t;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function percentile_disc(numeric, integer, integer) does not exist
-- end-expected-error
SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY v, w) FROM rbo_h2;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function mode(integer, integer) does not exist
-- end-expected-error
SELECT mode(1) WITHIN GROUP (ORDER BY v) FROM rbo_t;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function mode(integer, integer) does not exist
-- end-expected-error
SELECT mode() WITHIN GROUP (ORDER BY v, w) FROM rbo_h2;

-- ============================================================================
-- SECTION I: the well-formed calls keep working
-- ============================================================================

-- begin-expected
-- columns: a|b|c|d
-- row: 2|2|0.3333333333333333|0.75
-- end-expected
SELECT rank(2) WITHIN GROUP (ORDER BY v) AS a,
       dense_rank(2) WITHIN GROUP (ORDER BY v) AS b,
       percent_rank(2) WITHIN GROUP (ORDER BY v) AS c,
       cume_dist(2) WITHIN GROUP (ORDER BY v) AS d
FROM rbo_t;

-- begin-expected
-- columns: a|b|c
-- row: 2|2|1
-- end-expected
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v) AS a,
       percentile_disc(0.5) WITHIN GROUP (ORDER BY v) AS b,
       mode() WITHIN GROUP (ORDER BY v) AS c
FROM rbo_t;

-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT rank(2) WITHIN GROUP (ORDER BY v DESC) AS a FROM rbo_t;

-- begin-expected
-- columns: v|r
-- row: 1|2
-- row: 2|2
-- end-expected
SELECT v, rank(2) WITHIN GROUP (ORDER BY w) AS r FROM rbo_h2 GROUP BY v ORDER BY v;

-- An empty group still ranks the hypothetical row.
-- begin-expected
-- columns: a|b|c
-- row: 1|0|1
-- end-expected
SELECT rank(2) WITHIN GROUP (ORDER BY v) AS a,
       percent_rank(2) WITHIN GROUP (ORDER BY v) AS b,
       cume_dist(2) WITHIN GROUP (ORDER BY v) AS c
FROM rbo_t WHERE false;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP VIEW rbo_v;
DROP TABLE rbo_r;
DROP TABLE rbo_h2;
DROP TABLE rbo_t;
DROP SCHEMA IF EXISTS rbo_test CASCADE;
SET search_path = public;
