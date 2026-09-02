-- source: investigation-2026-08.md
-- finding: 181
-- title: the geometric module declares EPSILON as 1e-10 while PostgreSQL's geo_decls.h uses 1.0E-06, and the box, overlap and positional predicates compare exactly with 
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '(0,0),(1,1)'::box && '(1.0000005,0),(2,1)'::box;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '(0,0),(1,1)'::box <@ '(0,0),(0.9999995,1)'::box;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '(0,0),(1,1)'::box &< '(0,0),(0.9999995,1)'::box;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT '(0,0),(1,1)'::box << '(1.0000001,0),(2,1)'::box;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '((0,0),(1,0),(1,1),(0,1))'::polygon @> '(1.0000005,0.5)'::point;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '(0,0)'::point ?- '(1,0.0000005)'::point;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '(0,0)'::point ?| '(0.0000005,1)'::point;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT lseg '[(0,0),(1,1)]' ?|| lseg '[(0,0),(1000000,1000000.5)]';
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT lseg '[(0,0),(1,0)]' ?-| lseg '[(0,0),(0.0000005,1)]';
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT lseg '[(0,0),(1,1)]' ?-| lseg '[(0,0),(-1,1.0000005)]';
