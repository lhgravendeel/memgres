-- Geometry correctness: H31, H32, H33, L14, L15

-- H32: formatting normal point
-- begin-expected
-- columns: p
-- row: (1,2)
-- end-expected
SELECT point '(1,2)' AS p;

-- H32: NaN coordinates
-- begin-expected
-- columns: p
-- row: (NaN,NaN)
-- end-expected
SELECT point '(NaN,NaN)' AS p;

-- H33: polygon contains boundary point
-- begin-expected
-- columns: c
-- row: t
-- end-expected
SELECT polygon '((0,0),(4,0),(4,4),(0,4))' @> point '(2,0)' AS c;

-- H33: polygon contains itself
-- begin-expected
-- columns: c
-- row: t
-- end-expected
SELECT polygon '((0,0),(1,0),(1,1),(0,1))' @> polygon '((0,0),(1,0),(1,1),(0,1))' AS c;

-- H33: lseg contained in box
-- begin-expected
-- columns: c
-- row: t
-- end-expected
SELECT lseg '[(1,1),(2,2)]' <@ box '((0,0),(3,3))' AS c;

-- H33: area of open path is NULL
-- begin-expected
-- columns: a
-- row: NULL
-- end-expected
SELECT area(path '[(0,0),(1,0),(1,1)]') AS a;

-- H33: area of polygon
-- begin-expected
-- columns: a
-- row: 12
-- end-expected
SELECT area(polygon '((0,0),(4,0),(4,3),(0,3))') AS a;

-- H31: box contains point (not misrouted)
-- begin-expected
-- columns: c
-- row: t
-- end-expected
SELECT box '((0,0),(2,2))' @> point '(1,1)' AS c;

-- H31: line parallel test
-- begin-expected
-- columns: p
-- row: t
-- end-expected
SELECT line '{0,1,0}' ?|| line '{0,1,-1}' AS p;

-- H31: point distance to path
-- begin-expected
-- columns: d
-- row: 3
-- end-expected
SELECT point '(0,0)' <-> path '[(3,0),(3,4)]' AS d;
