-- source: review-2026-08.md
-- finding: Root cause 4: the geometric type family loses its type before operator dispatch and under-implements PostgreSQL's semantics
-- area: Strings, pattern matching, text search, encodings and the exotic types
-- title: Root cause 4: the geometric type family loses its type before operator dispatch and under-implements PostgreSQL's semantics
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_box (id int, b box);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf_box VALUES (1, '(0,0),(1,1)'), (4, '(0,1),(1,0)');
-- begin-expected
-- columns: id:int4 | b:box | ?column?:bool
-- row: 1 | (1,1),(0,0) | t
-- row: 4 | (1,1),(0,0) | t
-- rowcount: 2
-- end-expected
SELECT id, b, b = '(1,1),(0,0)'::box FROM zz_vf_box ORDER BY id;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '<(0,0),1>'::circle = '<(9,9),1>'::circle;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '{1,1,0}'::line = '{2,2,0}'::line;
-- begin-expected
-- columns: line:line
-- row: {-1,0,0}
-- rowcount: 1
-- end-expected
SELECT '[(0,0),(0,1)]'::line;
-- begin-expected
-- columns: circle:circle
-- row: <(1.3333333333333333,0.6666666666666666),1.308077670527261>
-- rowcount: 1
-- end-expected
SELECT circle('((0,0),(2,0),(2,2))'::polygon);
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '(NaN,0)'::point ~= '(NaN,0)'::point;
-- begin-expected
-- columns: ?column?:point
-- row: (0,0)
-- rowcount: 1
-- end-expected
SELECT '{1,-1,0}'::line # '{1,1,0}'::line;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '(0,0),(1,1)'::box <^ '(0,5),(1,6)'::box;
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT ?- '{0,1,5}'::line;
-- begin-expected
-- columns: isperp:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT isperp('[(0,0),(1,0)]'::lseg, '[(0,0),(0,1)]'::lseg);
-- begin-expected
-- columns: ?column?:float8
-- row: 12
-- rowcount: 1
-- end-expected
SELECT @-@ '((0,0),(3,0),(3,4))'::path;
-- begin-expected
-- columns: area:float8
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT area('[(0,0),(1,1)]'::path);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: line + point
-- end-expected-error
SELECT '{1,2,3}'::line + '(1,1)'::point;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: lseg ~= lseg
-- end-expected-error
SELECT '[(0,0),(1,1)]'::lseg ~= '[(0,0),(1,1)]'::lseg;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid line specification: A and B cannot both be zero
-- end-expected-error
SELECT '{0,0,1}'::line;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type circle: "<(0,0),-1>"
-- end-expected-error
SELECT '<(0,0),-1>'::circle;
-- begin-expected-error
-- sqlstate: 22003
-- message-like: "1e400" is out of range for type double precision
-- end-expected-error
SELECT '(1e400,0)'::point;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: open path cannot be converted to polygon
-- end-expected-error
SELECT CAST(CAST('[(0,0),(1,0),(1,1)]' AS path) AS polygon);
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type path: "[]"
-- end-expected-error
SELECT '[]'::path;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: "2" is not a valid binary digit
-- end-expected-error
SELECT '102'::varbit;
