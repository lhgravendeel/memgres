-- ============================================================================
-- Feature Comparison: Round 13 — Type & Operator Gaps
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Focuses on types, operators, and built-in functions where Memgres' results
-- currently diverge from PG 18.
--
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected
--   -- begin-expected-error / message-like: / end-expected-error
-- ============================================================================

DROP SCHEMA IF EXISTS r13_types CASCADE;
CREATE SCHEMA r13_types;
SET search_path = r13_types, public;

-- ============================================================================
-- SECTION A: Numeric utility functions
-- ============================================================================

-- 1. trim_scale strips trailing zeros
-- begin-expected
-- columns: trim_scale
-- row: 123.45
-- end-expected
SELECT trim_scale(NUMERIC '123.4500');

-- 2. trim_scale on integer-like numeric
-- begin-expected
-- columns: trim_scale
-- row: 123
-- end-expected
SELECT trim_scale(NUMERIC '123.0000');

-- 3. scale() returns digits after decimal
-- begin-expected
-- columns: scale
-- row: 2
-- end-expected
SELECT scale(NUMERIC '10.25');

-- 4. min_scale preserves value with fewest digits
-- begin-expected
-- columns: min_scale
-- row: 2
-- end-expected
SELECT min_scale(NUMERIC '10.2500');

-- ============================================================================
-- SECTION B: Bit string / bytea bit helpers
-- ============================================================================

-- 5. bit_count on bytea
-- begin-expected
-- columns: c
-- row: 8
-- end-expected
SELECT bit_count('\xff'::bytea)::int AS c;

-- 6. bit_count on bit string
-- begin-expected
-- columns: c
-- row: 3
-- end-expected
SELECT bit_count(B'1010100')::int AS c;

-- 7. get_bit on bytea
-- begin-expected
-- columns: b
-- row: 1
-- end-expected
SELECT get_bit('\xff'::bytea, 0) AS b;

-- 8. set_bit on bytea
-- begin-expected
-- columns: r
-- row: \x80
-- end-expected
SELECT set_bit('\x00'::bytea, 0, 1) AS r;

-- ============================================================================
-- SECTION C: JSONB helpers
-- ============================================================================

-- 9. jsonb_path_query_array aggregates matches
-- begin-expected
-- columns: a
-- row: [1, 2, 3]
-- end-expected
SELECT jsonb_path_query_array('[1,2,3]'::jsonb, '$[*]') AS a;

-- 10. jsonb_path_query_array returns empty array when no match
-- begin-expected
-- columns: a
-- row: []
-- end-expected
SELECT jsonb_path_query_array('{}'::jsonb, '$.nothing') AS a;

-- 11. jsonb_path_match returns boolean
-- begin-expected
-- columns: m
-- row: t
-- end-expected
SELECT jsonb_path_match('{"a":1}'::jsonb, '$.a == 1')::text AS m;

-- 12. jsonb #- deletes key path
-- begin-expected
-- columns: result
-- row: {"a": 1}
-- end-expected
SELECT ('{"a":1,"b":{"c":2}}'::jsonb #- '{b,c}'::text[] #- '{b}'::text[])::text AS result;

-- 13. jsonb - array deletes multiple keys
-- begin-expected
-- columns: result
-- row: {"c": 3}
-- end-expected
SELECT ('{"a":1,"b":2,"c":3}'::jsonb - ARRAY['a','b'])::text AS result;

-- 14. jsonb ? key exists is top-level only
-- begin-expected
-- columns: m
-- row: f
-- end-expected
SELECT ('{"a":{"c":1}}'::jsonb ? 'c')::text AS m;

-- ============================================================================
-- SECTION D: Full-text search ranking precision
-- ============================================================================

-- 15. ts_rank matches PG precision
-- begin-expected
-- columns: r
-- row: 0.098500855
-- end-expected
SELECT ts_rank(
  to_tsvector('english', 'the quick brown fox jumps over the lazy dog'),
  plainto_tsquery('english', 'quick fox'))::text AS r;

-- ============================================================================
-- SECTION E: Array slicing
-- ============================================================================

-- 16. array slice lo:hi
-- begin-expected
-- columns: slice
-- row: {2,3,4}
-- end-expected
SELECT (ARRAY[1,2,3,4,5])[2:4] AS slice;

-- 17. array slice open lower
-- begin-expected
-- columns: slice
-- row: {1,2,3}
-- end-expected
SELECT (ARRAY[1,2,3,4,5])[:3] AS slice;

-- 18. array slice open upper
-- begin-expected
-- columns: slice
-- row: {3,4,5}
-- end-expected
SELECT (ARRAY[1,2,3,4,5])[3:] AS slice;

-- 19. multidim slice
-- begin-expected
-- columns: slice
-- row: {{1},{3}}
-- end-expected
SELECT (ARRAY[[1,2],[3,4]])[1:2][1:1] AS slice;

-- 20. out-of-range slice returns empty
-- begin-expected
-- columns: slice
-- row: {}
-- end-expected
SELECT (ARRAY[1,2,3])[10:20] AS slice;

-- 21. array_dims multidim
-- begin-expected
-- columns: dims
-- row: [1:2][1:3]
-- end-expected
SELECT array_dims(ARRAY[[1,2,3],[4,5,6]]) AS dims;

-- ============================================================================
-- SECTION F: reg* types
-- ============================================================================

-- 22. regconfig cast
-- begin-expected
-- columns: r
-- row: english
-- end-expected
SELECT 'english'::regconfig::text AS r;

-- 23. regnamespace cast
-- begin-expected
-- columns: r
-- row: public
-- end-expected
SELECT 'public'::regnamespace::text AS r;

-- 24. regdictionary cast
-- begin-expected
-- columns: r
-- row: simple
-- end-expected
SELECT 'simple'::regdictionary::text AS r;

-- ============================================================================
-- SECTION G: Network helpers
-- ============================================================================

-- 25. inet_merge
-- begin-expected
-- columns: m
-- row: 192.168.0.0/22
-- end-expected
SELECT inet_merge('192.168.1.5/24', '192.168.2.5/24')::text AS m;

-- 26. hostmask
-- begin-expected
-- columns: m
-- row: 0.0.0.255/32
-- end-expected
SELECT hostmask('192.168.1.0/24'::cidr)::text AS m;

-- ============================================================================
-- SECTION H: Text/pattern operators
-- ============================================================================

-- 27. !~~ is NOT LIKE
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT ('hello' !~~ 'world%')::text AS r;

-- 28. !~~* is NOT ILIKE
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT ('HELLO' !~~* 'world%')::text AS r;

-- 29. ^@ is starts-with
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT ('hello world' ^@ 'hello')::text AS r;

-- ============================================================================
-- SECTION I: Range operator edge cases
-- ============================================================================

-- 30. int4range intersection empty
-- begin-expected
-- columns: inter
-- row: empty
-- end-expected
SELECT (int4range(1,5) * int4range(10,20))::text AS inter;

-- 31. union of disjoint ranges fails
-- begin-expected-error
-- message-like: contiguous
-- end-expected-error
SELECT int4range(1,5) + int4range(10,20);

-- 32. range_agg builds multirange
-- begin-expected
-- columns: m
-- row: {[1,5),[10,20)}
-- end-expected
SELECT range_agg(r)::text AS m FROM (VALUES (int4range(1,5)), (int4range(10,20))) t(r);

-- 33. range_merge spans both
-- begin-expected
-- columns: m
-- row: [1,10)
-- end-expected
SELECT range_merge(int4range(1,3), int4range(7,10))::text AS m;

-- ============================================================================
-- SECTION J: Geometric operators
-- ============================================================================

-- 34. box intersects box
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT (box '((0,0),(3,3))' ?# box '((2,2),(5,5))')::text AS r;

-- 35. lseg horizontal
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT (?- lseg '((0,0),(3,0))')::text AS r;

-- 36. lseg vertical
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT (?| lseg '((0,0),(0,3))')::text AS r;

-- 37. point ## lseg (closest point)
-- begin-expected
-- columns: p
-- row: (2,0)
-- end-expected
SELECT (point '(2,2)' ## lseg '((0,0),(4,0))')::text AS p;
