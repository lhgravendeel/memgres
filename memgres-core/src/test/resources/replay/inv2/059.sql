-- source: investigation-2026-08.md
-- finding: 59
-- title: The geometric type family loses its type before operator dispatch (error messages read `text <^ text`), normalises only on the literal-cast path, and implements
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_box (id int, b box);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf_box VALUES (1, '(0,0),(1,1)'), (4, '(0,1),(1,0)');
-- begin-expected
-- columns: id:int4 | b:text | text:text
-- row: 1 | (1,1),(0,0) | true
-- row: 4 | (1,1),(0,0) | true
-- rowcount: 2
-- end-expected
SELECT id, b::text, (b = '(1,1),(0,0)'::box)::text FROM zz_vf_box ORDER BY id;
-- begin-expected
-- ok: 0
-- end-expected
DROP TABLE zz_vf_box;
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
-- columns: circle:circle
-- row: <(1.3333333333333333,0.6666666666666666),1.308077670527261>
-- rowcount: 1
-- end-expected
SELECT circle('((0,0),(2,0),(2,2))'::polygon);
-- begin-expected
-- columns: line:line
-- row: {-1,0,0}
-- rowcount: 1
-- end-expected
SELECT '[(0,0),(0,1)]'::line;
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
