-- ============================================================================
-- Feature Comparison: FTS / Geometry / XML residual fixes
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Covers bugs H30 (tsquery parse errors + double negation), H31 (phantom /
-- boolean / unary geometry operators), H32 (float output formatting),
-- H33 (polygon(circle) winding), M18 (ts_stat SRF, ts_headline tag spacing,
-- position cap), H34 (xpath namespaces + array quoting), L15 (PG type names
-- in error messages) and N46 (EXECUTE USING/INTO order).
-- ============================================================================

DROP SCHEMA IF EXISTS fgx_test CASCADE;
CREATE SCHEMA fgx_test;
SET search_path = fgx_test, public;

-- ============================================================================
-- H30: tsquery construction
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error in tsquery
-- end-expected-error
SELECT 'fat rat'::tsquery;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: no operand in tsquery
-- end-expected-error
SELECT 'cat &'::tsquery;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error in tsquery
-- end-expected-error
SELECT to_tsquery('fat rat');

-- begin-expected
-- columns: q
-- row: !!'cat'
-- end-expected
SELECT '!!cat'::tsquery AS q;

-- begin-expected
-- columns: q
-- row: 'cat' & 'dog'
-- end-expected
SELECT to_tsquery('cat & dog') AS q;

-- ============================================================================
-- H31: geometry operator dispatch
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: line ## point
-- end-expected-error
SELECT line '{1,2,3}' ## point '(1,2)';

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: box ## point
-- end-expected-error
SELECT box '((0,0),(1,1))' ## point '(1,2)';

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: line ?# lseg
-- end-expected-error
SELECT line '{1,2,3}' ?# lseg '((0,0),(1,1))';

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: line @> point
-- end-expected-error
SELECT line '{1,-1,0}' @> point '(1,2)';

-- begin-expected
-- columns: a | b
-- row: t, f
-- end-expected
SELECT point '(1,2)' ?- point '(3,2)' AS a, point '(1,2)' ?- point '(3,9)' AS b;

-- begin-expected
-- columns: a | b
-- row: t, f
-- end-expected
SELECT point '(1,2)' ?| point '(1,5)' AS a, point '(1,2)' ?| point '(4,5)' AS b;

-- begin-expected
-- columns: c
-- row: (1,1)
-- end-expected
SELECT @@ box '((0,0),(2,2))' AS c;

-- begin-expected
-- columns: len
-- row: 5
-- end-expected
SELECT @-@ lseg '((0,0),(3,4))' AS len;

-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT # path '((0,0),(1,1),(2,2))' AS n;

-- ============================================================================
-- H32: geometry float output formatting
-- ============================================================================

-- begin-expected
-- columns: p
-- row: (1e+300,2e-300)
-- end-expected
SELECT point '(1e300,2e-300)' AS p;

-- begin-expected
-- columns: p
-- row: (12345678.5,0)
-- end-expected
SELECT point '(12345678.5,0)' AS p;

-- begin-expected
-- columns: p
-- row: (-0,3)
-- end-expected
SELECT point '(-0,3)' AS p;

-- begin-expected
-- columns: p
-- row: (1e-06,1)
-- end-expected
SELECT point '(1e-6,1)' AS p;

-- begin-expected
-- columns: p
-- row: (1e-05,1)
-- end-expected
SELECT point '(0.00001,1)' AS p;

-- begin-expected
-- columns: p
-- row: (1e+15,1)
-- end-expected
SELECT point '(1e15,1)' AS p;

-- ============================================================================
-- H33: polygon(circle) winding
-- ============================================================================

-- begin-expected
-- columns: poly
-- row: ((-2,0),(-1.2246467991473532e-16,2),(2,2.4492935982947064e-16),(3.6739403974420594e-16,-2))
-- end-expected
SELECT polygon(4, circle '<(0,0),2>') AS poly;

-- ============================================================================
-- M18: ts_stat SRF + ts_headline spacing
-- ============================================================================

CREATE TABLE fgx_tsstat(v tsvector);
INSERT INTO fgx_tsstat VALUES (to_tsvector('simple','cat cat dog')), (to_tsvector('simple','cat bird'));

-- begin-expected
-- columns: word | ndoc | nentry
-- row: bird, 1, 1
-- row: cat, 2, 3
-- row: dog, 1, 1
-- end-expected
SELECT word, ndoc, nentry FROM ts_stat('SELECT v FROM fgx_test.fgx_tsstat') ORDER BY word;

-- begin-expected
-- columns: h
-- row: the big <b>cat</b> sat
-- end-expected
SELECT ts_headline('english', 'the big<br>cat sat', to_tsquery('cat')) AS h;

-- ============================================================================
-- H34: xpath namespaces + array quoting
-- ============================================================================

-- begin-expected
-- columns: r
-- row: {<x>1</x>,<x>2</x>}
-- end-expected
SELECT xpath('//x', '<root><x>1</x><x>2</x></root>') AS r;

-- begin-expected
-- columns: r
-- row: {hi}
-- end-expected
SELECT xpath('//c:item/text()', '<root xmlns:c="http://ex.com"><c:item>hi</c:item></root>', ARRAY[ARRAY['c','http://ex.com']]) AS r;

-- ============================================================================
-- N46: EXECUTE with USING before INTO (reversed clause order)
-- ============================================================================

CREATE FUNCTION fgx_exec_rev() RETURNS int AS $$
DECLARE r int;
BEGIN
  EXECUTE 'SELECT $1 + $2' USING 2, 3 INTO r;
  RETURN r;
END; $$ LANGUAGE plpgsql;

-- begin-expected
-- columns: r
-- row: 5
-- end-expected
SELECT fgx_exec_rev() AS r;

-- ============================================================================
-- The closest-point operator over two segments, and over a segment and a box
--
-- Neither is "the nearest point of the two shapes" the way geometry would put
-- it. Two segments of the same slope have no answer at all, and a segment that
-- meets a box is answered with a point inside the box rather than on it: the
-- point of the segment nearest the box's centre. Which point is named when
-- several are equally near follows from the order the sides of a box are
-- walked, so the ties are here too.
-- ============================================================================

-- A segment beside a box: the point of the nearest side, the top winning the
-- tie with the right side.
-- begin-expected
-- columns: p
-- row: (2,4)
-- end-expected
SELECT lseg '[(4,0),(4,4)]' ## box '(-2,1),(2,4)' AS p;

-- ... and the left side winning the tie with the top.
-- begin-expected
-- columns: p
-- row: (0,0)
-- end-expected
SELECT lseg '[(-2,0),(-2,3)]' ## box '(0,0),(3,3)' AS p;

-- begin-expected
-- columns: p
-- row: (0,3)
-- end-expected
SELECT lseg '[(-10,10),(10,10)]' ## box '(0,0),(3,3)' AS p;

-- A segment lying inside the box is answered with its own point nearest the
-- centre of the box, which is not on the box at all.
-- begin-expected
-- columns: p
-- row: (0.24999999999999997,1.75)
-- end-expected
SELECT lseg '[(1,2),(-2,1)]' ## box '(-2,1),(2,4)' AS p;

-- ... and so is one that crosses it.
-- begin-expected
-- columns: p
-- row: (1.25,1.25)
-- end-expected
SELECT lseg '[(0,0),(6,6)]' ## box '(-2,1),(2,4)' AS p;

-- A segment whose endpoints coincide is still a point, and this one is inside
-- the box.
-- begin-expected
-- columns: p
-- row: (-3,-3)
-- end-expected
SELECT lseg '[(-3,-3),(-3,-3)]' ## box '(-5,-5),(-1,-1)' AS p;

-- Two segments: the answer lies on the second one.
-- begin-expected
-- columns: p
-- row: (-2,1)
-- end-expected
SELECT lseg '[(-3,-3),(-3,-3)]' ## lseg '[(1,2),(-2,1)]' AS p;

-- begin-expected
-- columns: p
-- row: (1,2)
-- end-expected
SELECT lseg '[(5,0),(0,5)]' ## lseg '[(1,2),(-2,1)]' AS p;

-- Where they cross, the crossing point.
-- begin-expected
-- columns: p
-- row: (1.4285714285714286,3.571428571428571)
-- end-expected
SELECT lseg '[(-2,1),(2,4)]' ## lseg '[(5,0),(0,5)]' AS p;

-- Two segments of the same slope have no closest point, and a segment whose
-- endpoints coincide counts as vertical -- so it is parallel to any vertical.
-- begin-expected
-- columns: p
-- row: NULL
-- end-expected
SELECT lseg '[(2,4),(6,8)]' ## lseg '[(0,0),(6,6)]' AS p;

-- begin-expected
-- columns: p
-- row: NULL
-- end-expected
SELECT lseg '[(1,1),(1,1)]' ## lseg '[(4,0),(4,4)]' AS p;

-- A line and a segment, which PostgreSQL has in that order only.
-- begin-expected
-- columns: p
-- row: (0,1.6666666666666667)
-- end-expected
SELECT line '{1,0,0}' ## lseg '[(1,2),(-2,1)]' AS p;

-- begin-expected
-- columns: p
-- row: NULL
-- end-expected
SELECT line '{1,-1,3}' ## lseg '[(0,0),(4,4)]' AS p;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: lseg ## line
-- end-expected-error
SELECT lseg '[(0,0),(4,4)]' ## line '{1,-1,3}';

-- There is no reversed spelling for a box either.
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: box ## lseg
-- end-expected-error
SELECT box '(-2,1),(2,4)' ## lseg '[(4,0),(4,4)]';

-- The functions the operator is recorded as, called by name.
-- begin-expected
-- columns: p
-- row: (1,2)
-- end-expected
SELECT close_lseg(lseg '[(4,0),(4,4)]', lseg '[(1,2),(-2,1)]')::text AS p;

-- begin-expected
-- columns: p
-- row: (2,4)
-- end-expected
SELECT close_sb(lseg '[(4,0),(4,4)]', box '(-2,1),(2,4)')::text AS p;

-- begin-expected
-- columns: p
-- row: (0,0)
-- end-expected
SELECT close_ls(line '{1,0,0}', lseg '[(0,0),(4,4)]')::text AS p;

-- Cleanup
DROP SCHEMA IF EXISTS fgx_test CASCADE;
