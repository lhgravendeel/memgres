-- ============================================================================
-- Feature Comparison: Geometric Type Operations
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Tests geometric operations including distance, length, center, npoints,
-- area, bound_box, contains, overlap, intersects, closest_point, etc.
-- ============================================================================
-- Annotation format:
--   -- begin-expected / columns: / row: / end-expected   -> expected result set
--   -- begin-expected-error / message-like: / end-expected-error -> expected error
--   -- note: ...      -> informational comment
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP SCHEMA IF EXISTS geo_test CASCADE;
CREATE SCHEMA geo_test;
SET search_path = geo_test, public;

-- ============================================================================
-- SECTION A: Point Operations
-- ============================================================================

-- ============================================================================
-- 1. Point construction and extraction
-- ============================================================================

-- begin-expected
-- columns: p
-- row: (1,2)
-- end-expected
SELECT point(1, 2) AS p;

-- begin-expected
-- columns: result
-- row: (3,4)
-- end-expected
SELECT '(3,4)'::point AS result;

-- ============================================================================
-- 2. Point distance operator (<->)
-- ============================================================================

-- begin-expected
-- columns: dist
-- row: 5
-- end-expected
SELECT round((point(0,0) <-> point(3,4))::numeric, 0)::integer AS dist;

-- begin-expected
-- columns: dist
-- row: 0
-- end-expected
SELECT (point(1,1) <-> point(1,1))::integer AS dist;

-- ============================================================================
-- 3. Point equality
-- ============================================================================

-- begin-expected
-- columns: eq, neq
-- row: true, true
-- end-expected
SELECT
  point(1,2) ~= point(1,2) AS eq,
  NOT (point(1,2) ~= point(3,4)) AS neq;

-- ============================================================================
-- 4. Point addition and subtraction
-- ============================================================================

-- begin-expected
-- columns: result
-- row: (4,6)
-- end-expected
SELECT point(1,2) + point(3,4) AS result;

-- begin-expected
-- columns: result
-- row: (-2,-2)
-- end-expected
SELECT point(1,2) - point(3,4) AS result;

-- ============================================================================
-- 5. Point scaling (multiply/divide by point)
-- ============================================================================

-- begin-expected
-- columns: result
-- row: (2,4)
-- end-expected
SELECT point(1,2) * point(2,0) AS result;

-- ============================================================================
-- SECTION B: Box Operations
-- ============================================================================

-- ============================================================================
-- 6. Box construction
-- ============================================================================

-- begin-expected
-- columns: b
-- row: (3,4),(1,2)
-- end-expected
SELECT box(point(1,2), point(3,4)) AS b;

-- ============================================================================
-- 7. Box area
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 4
-- end-expected
SELECT area(box '(0,0),(2,2)')::integer AS a;

-- begin-expected
-- columns: a
-- row: 6
-- end-expected
SELECT area(box '(1,1),(4,3)')::integer AS a;

-- ============================================================================
-- 8. Box center
-- ============================================================================

-- begin-expected
-- columns: c
-- row: (1,1)
-- end-expected
SELECT center(box '(0,0),(2,2)') AS c;

-- ============================================================================
-- 9. Box overlap (&&)
-- ============================================================================

-- begin-expected
-- columns: overlaps, no_overlap
-- row: true, false
-- end-expected
SELECT
  box '(0,0),(2,2)' && box '(1,1),(3,3)' AS overlaps,
  box '(0,0),(1,1)' && box '(5,5),(6,6)' AS no_overlap;

-- ============================================================================
-- 10. Box contains (@> and <@)
-- ============================================================================

-- begin-expected
-- columns: contains, contained
-- row: true, true
-- end-expected
SELECT
  box '(0,0),(10,10)' @> box '(1,1),(5,5)' AS contains,
  box '(1,1),(5,5)' <@ box '(0,0),(10,10)' AS contained;

-- ============================================================================
-- 11. Box intersection (*)
-- ============================================================================

-- begin-expected
-- columns: inter
-- row: (2,2),(1,1)
-- end-expected
SELECT box '(0,0),(2,2)' # box '(1,1),(3,3)' AS inter;

-- ============================================================================
-- 12. Box distance
-- ============================================================================

-- begin-expected
-- columns: dist
-- row: 1
-- end-expected
SELECT (box '(0,0),(2,2)' <-> box '(1,1),(3,3)')::integer AS dist;

-- ============================================================================
-- SECTION C: Circle Operations
-- ============================================================================

-- ============================================================================
-- 13. Circle construction
-- ============================================================================

-- begin-expected
-- columns: c
-- row: <(1,2),3>
-- end-expected
SELECT circle(point(1,2), 3) AS c;

-- ============================================================================
-- 14. Circle area
-- ============================================================================

-- begin-expected
-- columns: a
-- row: 28.27
-- end-expected
SELECT round(area(circle '((0,0),3)')::numeric, 2) AS a;

-- ============================================================================
-- 15. Circle center and radius
-- ============================================================================

-- begin-expected
-- columns: c
-- row: (1,2)
-- end-expected
SELECT center(circle '((1,2),5)') AS c;

-- begin-expected
-- columns: r
-- row: 5
-- end-expected
SELECT radius(circle '((1,2),5)')::integer AS r;

-- ============================================================================
-- 16. Circle containment
-- ============================================================================

-- begin-expected
-- columns: contains
-- row: true
-- end-expected
SELECT circle '((0,0),10)' @> point(3,4) AS contains;

-- begin-expected
-- columns: outside
-- row: false
-- end-expected
SELECT circle '((0,0),1)' @> point(3,4) AS outside;

-- ============================================================================
-- 17. Circle overlap
-- ============================================================================

-- begin-expected
-- columns: overlaps, no_overlap
-- row: true, false
-- end-expected
SELECT
  circle '((0,0),5)' && circle '((3,0),5)' AS overlaps,
  circle '((0,0),1)' && circle '((10,0),1)' AS no_overlap;

-- ============================================================================
-- 18. Circle distance
-- ============================================================================

-- begin-expected
-- columns: dist
-- row: 0
-- end-expected
SELECT (circle '((0,0),5)' <-> circle '((3,0),5)')::integer AS dist;

-- ============================================================================
-- SECTION D: Line Segment Operations
-- ============================================================================

-- ============================================================================
-- 19. Line segment construction
-- ============================================================================

-- begin-expected
-- columns: ls
-- row: [(1,2),(3,4)]
-- end-expected
SELECT lseg '((1,2),(3,4))' AS ls;

-- ============================================================================
-- 20. Line segment length
-- ============================================================================

-- begin-expected
-- columns: len
-- row: 5.00
-- end-expected
SELECT round(length(lseg '((0,0),(3,4))')::numeric, 2) AS len;

-- ============================================================================
-- 21. Line segment center
-- ============================================================================

-- begin-expected-error
-- message-like: center
-- end-expected-error
SELECT center(lseg '((0,0),(3,4))') AS c;

-- ============================================================================
-- SECTION E: Polygon Operations
-- ============================================================================

-- ============================================================================
-- 22. Polygon construction
-- ============================================================================

-- begin-expected
-- columns: p
-- row: ((0,0),(4,0),(4,3),(0,3))
-- end-expected
SELECT polygon '((0,0),(4,0),(4,3),(0,3))' AS p;

-- ============================================================================
-- 23. Polygon area
-- ============================================================================

-- begin-expected-error
-- message-like: area
-- end-expected-error
SELECT area(polygon '((0,0),(4,0),(4,3),(0,3))')::integer AS a;

-- ============================================================================
-- 24. Polygon npoints
-- ============================================================================

-- begin-expected
-- columns: n
-- row: 4
-- end-expected
SELECT npoints(polygon '((0,0),(4,0),(4,3),(0,3))') AS n;

-- ============================================================================
-- 25. Polygon center
-- ============================================================================

-- begin-expected-error
-- message-like: center
-- end-expected-error
SELECT center(polygon '((0,0),(4,0),(4,3),(0,3))') AS c;

-- ============================================================================
-- 26. Polygon containment
-- ============================================================================

-- begin-expected
-- columns: inside
-- row: true
-- end-expected
SELECT polygon '((0,0),(10,0),(10,10),(0,10))' @> point(5,5) AS inside;

-- begin-expected
-- columns: outside
-- row: false
-- end-expected
SELECT polygon '((0,0),(10,0),(10,10),(0,10))' @> point(15,5) AS outside;

-- ============================================================================
-- 27. Polygon overlap
-- ============================================================================

-- begin-expected
-- columns: overlaps
-- row: true
-- end-expected
SELECT polygon '((0,0),(5,0),(5,5),(0,5))' && polygon '((3,3),(8,3),(8,8),(3,8))' AS overlaps;

-- ============================================================================
-- SECTION F: Path Operations
-- ============================================================================

-- ============================================================================
-- 28. Open path length
-- ============================================================================

-- begin-expected
-- columns: len
-- row: 10
-- end-expected
SELECT length(path '[(0,0),(3,0),(3,4),(0,4)]')::integer AS len;

-- ============================================================================
-- 29. Closed path length (perimeter)
-- ============================================================================

-- begin-expected
-- columns: len
-- row: 14
-- end-expected
SELECT length(path '((0,0),(4,0),(4,3),(0,3))')::integer AS len;

-- ============================================================================
-- 30. Path npoints
-- ============================================================================

-- begin-expected
-- columns: n
-- row: 4
-- end-expected
SELECT npoints(path '((0,0),(4,0),(4,3),(0,3))') AS n;

-- ============================================================================
-- SECTION G: Bounding box
-- ============================================================================

-- ============================================================================
-- 31. bound_box of circle
-- ============================================================================

-- begin-expected
-- columns: bb
-- row: (3.5355339059327373,3.5355339059327373),(-3.5355339059327373,-3.5355339059327373)
-- end-expected
SELECT box(circle '((0,0),5)') AS bb;

-- ============================================================================
-- 32. bound_box of polygon
-- ============================================================================

-- begin-expected
-- columns: bb
-- row: (4,3),(0,0)
-- end-expected
SELECT box(polygon '((0,0),(4,0),(4,3),(0,3))') AS bb;

-- ============================================================================
-- SECTION H: Type conversions
-- ============================================================================

-- ============================================================================
-- 33. Box to/from polygon
-- ============================================================================

-- begin-expected
-- columns: n
-- row: 4
-- end-expected
SELECT npoints(polygon(box '(0,0),(2,2)')) AS n;

-- ============================================================================
-- 34. Circle to polygon (approximation)
-- ============================================================================

-- note: circle to polygon with 8 points
-- begin-expected
-- columns: n
-- row: 8
-- end-expected
SELECT npoints(polygon(8, circle '((0,0),1)')) AS n;

-- ============================================================================
-- 35. Geometric operations with table data
-- ============================================================================

CREATE TABLE geo_shapes (
  id integer PRIMARY KEY,
  shape_type text,
  loc point,
  bounds box
);

INSERT INTO geo_shapes VALUES
  (1, 'building', point(10, 20), box '(5,15),(15,25)'),
  (2, 'park', point(30, 40), box '(25,35),(35,45)'),
  (3, 'school', point(12, 22), box '(8,18),(16,26)');

-- begin-expected
-- columns: id, shape_type
-- row: 1, building
-- row: 3, school
-- end-expected
SELECT id, shape_type
FROM geo_shapes
WHERE loc <-> point(10, 20) < 5
ORDER BY id;

-- begin-expected
-- columns: id, shape_type
-- row: 1, building
-- row: 3, school
-- end-expected
SELECT id, shape_type
FROM geo_shapes
WHERE bounds && box '(10,20),(14,24)'
ORDER BY id;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP SCHEMA geo_test CASCADE;
SET search_path = public;
