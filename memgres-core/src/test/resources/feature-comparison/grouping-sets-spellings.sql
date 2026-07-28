-- ============================================================================
-- Feature Comparison: the GROUP BY grouping-set spellings beyond the simplest one
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Only the fully parenthesised GROUPING SETS form used to parse. PostgreSQL also
-- accepts the empty grouping set GROUP BY (), a bare unparenthesised column inside
-- GROUPING SETS, a nested ROLLUP/CUBE (or GROUPING SETS) inside GROUPING SETS, and
-- an output alias or ordinal as a grouping expression. Memgres answered 42601 for
-- all of those. Row counts and GROUPING() bitmasks are compared, not just parsing;
-- the spellings that already worked are re-checked here so they cannot regress.
-- ============================================================================

-- setup
CREATE TABLE gss_v (a text, b text, c int);
INSERT INTO gss_v VALUES ('x','p',1),('x','q',2),('y','p',3),('y','q',4),('y','q',5);
CREATE TABLE gss_nulls (a text, b text, c int);
INSERT INTO gss_nulls VALUES ('x',NULL,1),(NULL,'q',2);

-- ----------------------------------------------------------------------------
-- The empty grouping set: one total row, and it cross-products with plain columns
-- ----------------------------------------------------------------------------
SELECT count(*) FROM gss_v GROUP BY ();
SELECT count(*) FROM gss_v GROUP BY (), ();
SELECT 1 FROM gss_v GROUP BY ();
SELECT count(*) FROM gss_v GROUP BY () HAVING count(*) > 2;
SELECT count(*) FROM gss_v GROUP BY () HAVING count(*) > 9;
SELECT count(*) AS n FROM gss_v GROUP BY () ORDER BY n;
SELECT a, count(*) FROM gss_v GROUP BY a, () ORDER BY 1;
SELECT a, count(*) FROM gss_v GROUP BY (), a ORDER BY 1;
SELECT a, count(*) FROM gss_v GROUP BY ROLLUP(a), () ORDER BY 1;
SELECT count(*) FROM gss_v GROUP BY GROUPING SETS (());
SELECT count(*) FROM gss_v GROUP BY GROUPING SETS ((), ());

-- ----------------------------------------------------------------------------
-- A bare, unparenthesised column inside GROUPING SETS is a one-column set
-- ----------------------------------------------------------------------------
SELECT a, count(*) FROM gss_v GROUP BY GROUPING SETS (a) ORDER BY 1;
SELECT a, b, count(*) FROM gss_v GROUP BY GROUPING SETS (a, (a,b)) ORDER BY 1,2;
SELECT a, b, count(*), grouping(a), grouping(b), grouping(a,b)
  FROM gss_v GROUP BY GROUPING SETS (a, b) ORDER BY 1,2;
SELECT a, b, count(*) FROM gss_v GROUP BY GROUPING SETS (a, (), b) ORDER BY 1,2;
SELECT a, count(*) FROM gss_v GROUP BY GROUPING SETS ((), a) ORDER BY 1;
SELECT a, b, count(*) FROM gss_v GROUP BY GROUPING SETS (a, b), () ORDER BY 1,2;
SELECT a, count(*) FROM gss_v GROUP BY GROUPING SETS (a, a, ()) ORDER BY 1;
SELECT a, count(*) FROM gss_v GROUP BY DISTINCT GROUPING SETS (a, a, ()) ORDER BY 1;
SELECT a, sum(c), count(*) FROM gss_v GROUP BY GROUPING SETS (a, ()) ORDER BY 1;
SELECT a, count(*) FROM gss_v WHERE c > 1 GROUP BY GROUPING SETS (a, ()) ORDER BY 1;
SELECT a, count(*) FROM gss_v GROUP BY GROUPING SETS (a) HAVING count(*) > 2 ORDER BY 1;

-- ----------------------------------------------------------------------------
-- A nested ROLLUP, CUBE or GROUPING SETS contributes its own sets
-- ----------------------------------------------------------------------------
SELECT a, b, count(*), grouping(a,b)
  FROM gss_v GROUP BY GROUPING SETS (ROLLUP (a, b)) ORDER BY 1,2;
SELECT a, b, count(*), grouping(a,b)
  FROM gss_v GROUP BY GROUPING SETS (CUBE (a, b)) ORDER BY 1,2;
SELECT a, b, count(*) FROM gss_v GROUP BY GROUPING SETS (ROLLUP(a), b) ORDER BY 1,2;
SELECT a, b, count(*) FROM gss_v GROUP BY GROUPING SETS ((a), ROLLUP(b)) ORDER BY 1,2;
SELECT a, b, count(*) FROM gss_v GROUP BY GROUPING SETS (GROUPING SETS (a, b)) ORDER BY 1,2;
SELECT a, b, count(*) FROM gss_v GROUP BY GROUPING SETS (a, ROLLUP(a,b), CUBE(b)) ORDER BY 1,2,3;
SELECT a, b, sum(c) FROM gss_v GROUP BY GROUPING SETS (ROLLUP(a,b)) HAVING sum(c) > 3 ORDER BY 1,2;

-- ----------------------------------------------------------------------------
-- An output alias (or ordinal) names the select-list expression to group on
-- ----------------------------------------------------------------------------
SELECT a AS gss_alias_a, count(*) FROM gss_v GROUP BY GROUPING SETS (gss_alias_a) ORDER BY 1;
SELECT a AS gss_alias_a, count(*) FROM gss_v GROUP BY GROUPING SETS ((gss_alias_a), ()) ORDER BY 1;
SELECT a AS gss_alias_a, count(*) FROM gss_v GROUP BY ROLLUP(gss_alias_a) ORDER BY 1;
SELECT a AS gss_alias_a, b AS gss_alias_b, count(*)
  FROM gss_v GROUP BY CUBE(gss_alias_a, gss_alias_b) ORDER BY 1,2;
SELECT upper(a) AS gss_ua, count(*) FROM gss_v GROUP BY GROUPING SETS (gss_ua, ()) ORDER BY 1;
SELECT upper(a) AS gss_ua, count(*) FROM gss_v GROUP BY GROUPING SETS (upper(a), ()) ORDER BY 1;
SELECT a, count(*) FROM gss_v GROUP BY GROUPING SETS (1, ()) ORDER BY 1;

-- ----------------------------------------------------------------------------
-- The spellings that already worked
-- ----------------------------------------------------------------------------
SELECT a, b, count(*), grouping(a), grouping(b)
  FROM gss_v GROUP BY GROUPING SETS ((a,b),(a),()) ORDER BY 1,2;
SELECT a, b, count(*), grouping(a,b) FROM gss_v GROUP BY ROLLUP(a,b) ORDER BY 1,2;
SELECT a, b, count(*), grouping(a,b) FROM gss_v GROUP BY CUBE(a,b) ORDER BY 1,2;
SELECT a, b, count(*) FROM gss_v GROUP BY a, GROUPING SETS ((b),()) ORDER BY 1,2;
SELECT a, b, count(*) FROM gss_v GROUP BY ROLLUP((a,b)) ORDER BY 1,2;
SELECT a, b, count(*) FROM gss_v GROUP BY ROLLUP(a, (b)) ORDER BY 1,2;
SELECT a, b, count(*) FROM gss_v GROUP BY CUBE((a,b)) ORDER BY 1,2;
SELECT a, b, count(*) FROM gss_v GROUP BY GROUPING SETS ((a,b)) ORDER BY 1,2;
SELECT a, count(*) FROM gss_v GROUP BY a ORDER BY 1;
SELECT a, count(*) FROM gss_v GROUP BY 1 ORDER BY 1;
SELECT (a), count(*) FROM gss_v GROUP BY (a) ORDER BY 1;
SELECT a, count(*) FROM gss_v GROUP BY DISTINCT ROLLUP(a), CUBE(a) ORDER BY 1;

-- ----------------------------------------------------------------------------
-- A NULL in the data is a group of its own, distinct from a rolled-up NULL
-- ----------------------------------------------------------------------------
SELECT a, b, count(*), grouping(a), grouping(b)
  FROM gss_nulls GROUP BY GROUPING SETS (a, b) ORDER BY 1,2,4,5;
SELECT a, b, count(*), grouping(a,b) FROM gss_nulls GROUP BY ROLLUP(a,b) ORDER BY 1,2,4;

-- ----------------------------------------------------------------------------
-- Still rejected
-- ----------------------------------------------------------------------------
SELECT count(*) FROM gss_v GROUP BY GROUPING SETS ();
SELECT count(*) FROM gss_v GROUP BY ROLLUP();
SELECT count(*) FROM gss_v GROUP BY CUBE();
SELECT a, grouping(b) FROM gss_v GROUP BY a;
SELECT grouping(a) FROM gss_v;

-- cleanup
DROP TABLE gss_v;
DROP TABLE gss_nulls;
