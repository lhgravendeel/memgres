-- source: review-2026-08.md
-- finding: Root cause 18: the geometric module declares two epsilons and uses neither in the positional predicates
-- area: User-defined casts, operators, aggregates, collations and extensions
-- title: Root cause 18: the geometric module declares two epsilons and uses neither in the positional predicates
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '(0,0),(1,1)'::box && '(1.0000005,0),(2,1)'::box;
-- PG t, memgres f
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '(0,0),(1,1)'::box <@ '(0,0),(0.9999995,1)'::box;
-- PG t, memgres f
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '(0,0),(1,1)'::box &< '(0,0),(0.9999995,1)'::box;
-- PG t, memgres f
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT '(0,0),(1,1)'::box << '(1.0000001,0),(2,1)'::box;
-- PG f, memgres t
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '((0,0),(1,0),(1,1),(0,1))'::polygon @> '(1.0000005,0.5)'::point;
-- PG t, memgres f
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '(0,0)'::point ?- '(1,0.0000005)'::point;
-- PG t, memgres f
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '(0,0)'::point ?| '(0.0000005,1)'::point;
-- PG t, memgres f
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT lseg '[(0,0),(1,1)]' ?|| lseg '[(0,0),(1000000,1000000.5)]';
-- PG t, memgres f
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT lseg '[(0,0),(1,0)]' ?-| lseg '[(0,0),(0.0000005,1)]';
-- PG t, memgres f
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT lseg '[(0,0),(1,1)]' ?-| lseg '[(0,0),(-1,1.0000005)]';
-- PG t, memgres f;
