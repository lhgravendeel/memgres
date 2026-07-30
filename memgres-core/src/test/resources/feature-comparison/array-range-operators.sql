-- ============================================================================
-- Feature Comparison: Array and Range Operators (@>, <@, &&, -|-)
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Covers: array vs range operand dispatch (two-element arrays are arrays, not
-- ranges), numrange decimal bound precision, empty-range containment/overlap
-- semantics, NULL array elements in containment/overlap, unnest flattening of
-- multidimensional arrays, quoted array elements with commas, int4range
-- canonicalization of half-unbounded ranges, and empty-array dimension
-- functions.
-- ============================================================================

-- ============================================================================
-- SECTION A: Two-element arrays use array semantics, not range semantics
-- ============================================================================

-- stmt 1: range semantics would claim [1,5] contains [2,3]; array semantics: false
-- begin-expected
-- columns: result
-- row: f
-- end-expected
SELECT ARRAY[1,5] @> ARRAY[2,3] AS result;

-- stmt 2: two-element array containing an actual element
-- begin-expected
-- columns: result
-- row: t
-- end-expected
SELECT ARRAY[1,2] @> ARRAY[2] AS result;

-- stmt 3: two-element array contained by larger array
-- begin-expected
-- columns: result
-- row: t
-- end-expected
SELECT ARRAY[1,5] <@ ARRAY[1,2,3,4,5] AS result;

-- stmt 4: containment with duplicates
-- begin-expected
-- columns: result
-- row: t
-- end-expected
SELECT ARRAY[1] @> ARRAY[1,1] AS result;

-- stmt 5: empty array is contained in everything
-- begin-expected
-- columns: a | b | c
-- row: t | t | t
-- end-expected
SELECT ARRAY[1,2] @> ARRAY[]::int[] AS a,
       ARRAY[]::int[] <@ ARRAY[1,2] AS b,
       ARRAY[]::int[] @> ARRAY[]::int[] AS c;

-- stmt 6: two-element array overlap
-- begin-expected
-- columns: a | b
-- row: t | f
-- end-expected
SELECT ARRAY[1,5] && ARRAY[5,9] AS a, ARRAY[1,5] && ARRAY[2,4] AS b;

-- ============================================================================
-- SECTION B: Genuine range operators still work
-- ============================================================================

-- stmt 7: int4range @> value
-- begin-expected
-- columns: a | b
-- row: t | f
-- end-expected
SELECT '[1,5)'::int4range @> 3 AS a, '[1,5)'::int4range @> 5 AS b;

-- stmt 8: int4range @> int4range
-- begin-expected
-- columns: a | b
-- row: t | f
-- end-expected
SELECT '[1,10)'::int4range @> '[2,3)'::int4range AS a,
       '[2,3)'::int4range @> '[1,10)'::int4range AS b;

-- stmt 9: int4range overlap
-- begin-expected
-- columns: a | b
-- row: t | f
-- end-expected
SELECT '[1,5)'::int4range && '[4,8)'::int4range AS a,
       '[1,5)'::int4range && '[5,8)'::int4range AS b;

-- stmt 10: value <@ range
-- begin-expected
-- columns: result
-- row: t
-- end-expected
SELECT 3 <@ '[1,5)'::int4range AS result;

-- stmt 11: range adjacency
-- begin-expected
-- columns: a | b
-- row: t | f
-- end-expected
SELECT '[1,5)'::int4range -|- '[5,10)'::int4range AS a,
       '[1,5)'::int4range -|- '[6,10)'::int4range AS b;

-- ============================================================================
-- SECTION C: numrange decimal bounds are exact (no rounding)
-- ============================================================================

-- stmt 12: numrange text output preserves decimals
-- begin-expected
-- columns: result
-- row: [1.5,2.5)
-- end-expected
SELECT '[1.5,2.5)'::numrange AS result;

-- stmt 13: numrange @> numeric with decimal bounds
-- begin-expected
-- columns: a | b | c | d
-- row: t | f | t | f
-- end-expected
SELECT '[1.5,2.5)'::numrange @> 1.6 AS a,
       '[1.5,2.5)'::numrange @> 1.4 AS b,
       '[1.5,2.5)'::numrange @> 1.5 AS c,
       '[1.5,2.5)'::numrange @> 2.5 AS d;

-- stmt 14: lower/upper return exact decimal bounds
-- begin-expected
-- columns: lo | hi
-- row: 1.5 | 2.5
-- end-expected
SELECT lower('[1.5,2.5)'::numrange) AS lo, upper('[1.5,2.5)'::numrange) AS hi;

-- stmt 15: numrange bound inclusivity
-- begin-expected
-- columns: a | b | c
-- row: t | t | f
-- end-expected
SELECT lower_inc('[1.5,2.5]'::numrange) AS a,
       upper_inc('[1.5,2.5]'::numrange) AS b,
       upper_inc('[1.5,2.5)'::numrange) AS c;

-- stmt 16: numrange range containment with decimals
-- begin-expected
-- columns: a | b
-- row: t | f
-- end-expected
SELECT '[1.5,2.5)'::numrange @> '[1.6,2.0)'::numrange AS a,
       '[1.5,2.5)'::numrange @> '[1.4,2.0)'::numrange AS b;

-- stmt 17: numrange overlap with decimals (touching bounds)
-- begin-expected
-- columns: a | b | c
-- row: t | f | t
-- end-expected
SELECT '[1.1,1.9)'::numrange && '[1.8,3.0)'::numrange AS a,
       '[1.1,1.5)'::numrange && '[1.5,3.0)'::numrange AS b,
       '[1.1,1.5]'::numrange && '[1.5,3.0)'::numrange AS c;

-- ============================================================================
-- SECTION D: Empty range semantics
-- ============================================================================

-- stmt 18: every range contains the empty range
-- begin-expected
-- columns: a | b
-- row: t | t
-- end-expected
SELECT '[1,5)'::int4range @> 'empty'::int4range AS a,
       'empty'::int4range <@ '[1,5)'::int4range AS b;

-- stmt 19: empty contains only empty
-- begin-expected
-- columns: a | b
-- row: t | f
-- end-expected
SELECT 'empty'::int4range @> 'empty'::int4range AS a,
       'empty'::int4range @> '[1,2)'::int4range AS b;

-- stmt 20: empty ranges never overlap and are never adjacent
-- begin-expected
-- columns: a | b | c
-- row: f | f | f
-- end-expected
SELECT 'empty'::int4range && '[1,5)'::int4range AS a,
       'empty'::int4range && 'empty'::int4range AS b,
       'empty'::int4range -|- '[1,5)'::int4range AS c;

-- ============================================================================
-- SECTION E: NULL array elements never match in @> and &&
-- ============================================================================

-- stmt 21: NULL element containment.
-- Written over text[] and bigint[] rather than int[]: the intarray extension, which the corpus
-- installs and never drops, redeclares @>, <@ and && over integer[] and rejects a NULL element
-- there with 22004. That is intarray's rule and not PostgreSQL's -- a server without the
-- extension answers exactly what is asserted here -- so the operators are read over the element
-- types intarray does not touch, where both servers agree. See
-- array-null-and-filter-validation.sql for the whole boundary.
-- begin-expected
-- columns: a | b | c
-- row: f | f | t
-- end-expected
SELECT ARRAY['a',NULL]::text[] @> ARRAY[NULL]::text[] AS a,
       ARRAY[NULL]::text[] <@ ARRAY['a',NULL]::text[] AS b,
       ARRAY[1,NULL]::bigint[] @> ARRAY[1]::bigint[] AS c;

-- stmt 22: NULL element overlap
-- begin-expected
-- columns: a | b | c
-- row: f | t | f
-- end-expected
SELECT ARRAY[NULL]::text[] && ARRAY[NULL]::text[] AS a,
       ARRAY[1,NULL]::bigint[] && ARRAY[1]::bigint[] AS b,
       ARRAY[NULL,'b']::text[] && ARRAY['a',NULL]::text[] AS c;

-- stmt 23: the string 'NULL' is distinct from SQL NULL
-- begin-expected
-- columns: a | b
-- row: f | t
-- end-expected
SELECT ARRAY['NULL'] @> ARRAY[NULL]::text[] AS a,
       ARRAY['NULL'] @> ARRAY['NULL'] AS b;

-- ============================================================================
-- SECTION F: unnest flattens multidimensional arrays
-- ============================================================================

-- stmt 24: unnest of a 2-D array yields 4 rows
-- begin-expected
-- columns: u
-- row: 1
-- row: 2
-- row: 3
-- row: 4
-- end-expected
SELECT unnest(ARRAY[[1,2],[3,4]]) AS u;

-- stmt 25: unnest of a 3-D array yields 8 rows
-- begin-expected
-- columns: cnt | total
-- row: 8 | 36
-- end-expected
SELECT count(*) AS cnt, sum(u) AS total FROM unnest(ARRAY[[[1,2],[3,4]],[[5,6],[7,8]]]) AS t(u);

-- ============================================================================
-- SECTION G: Quoted array elements with commas and escapes
-- ============================================================================

-- stmt 26: array output quotes elements containing commas
-- begin-expected
-- columns: result
-- row: {"a,b",c}
-- end-expected
SELECT ARRAY['a,b','c'] AS result;

-- stmt 27: overlap on elements containing commas
-- begin-expected
-- columns: a | b
-- row: f | t
-- end-expected
SELECT ARRAY['a,b'] && ARRAY['c,b'] AS a,
       ARRAY['a,b'] && ARRAY['a,b','x'] AS b;

-- stmt 28: containment on elements containing commas
-- begin-expected
-- columns: a | b
-- row: t | f
-- end-expected
SELECT ARRAY['a,b','c'] @> ARRAY['a,b'] AS a,
       ARRAY['a,b','c'] @> ARRAY['a'] AS b;

-- stmt 29: cardinality is quote-aware for text array literals
-- begin-expected
-- columns: result
-- row: 1
-- end-expected
SELECT cardinality('{"a,b"}'::text[]) AS result;

-- ============================================================================
-- SECTION H: int4range canonicalization with a missing bound
-- ============================================================================

-- stmt 30: half-unbounded ranges are canonicalized to [ , ) form
-- begin-expected
-- columns: a | b
-- row: (,6) | [3,)
-- end-expected
SELECT '(,5]'::int4range AS a, '(2,)'::int4range AS b;

-- stmt 31: bounded canonicalization unchanged
-- begin-expected
-- columns: a | b
-- row: [1,4) | [2,4)
-- end-expected
SELECT '[1,3]'::int4range AS a, '(1,3]'::int4range AS b;

-- stmt 32: fully unbounded range
-- begin-expected
-- columns: result
-- row: (,)
-- end-expected
SELECT '(,)'::int4range AS result;

-- ============================================================================
-- SECTION I: Empty-array dimension functions
-- ============================================================================

-- stmt 33: array_upper/array_lower/array_length on empty arrays return NULL
-- begin-expected
-- columns: a | b | c | d
-- row: NULL | NULL | NULL | 0
-- end-expected
SELECT array_upper('{}'::int[], 1) AS a,
       array_lower('{}'::int[], 1) AS b,
       array_length('{}'::int[], 1) AS c,
       cardinality('{}'::int[]) AS d;

-- stmt 34: non-empty arrays unchanged
-- begin-expected
-- columns: a | b | c | d
-- row: 3 | 1 | 3 | 3
-- end-expected
SELECT array_upper(ARRAY[10,20,30], 1) AS a,
       array_lower(ARRAY[10,20,30], 1) AS b,
       array_length(ARRAY[10,20,30], 1) AS c,
       cardinality(ARRAY[10,20,30]) AS d;
