-- source: investigation-2026-08.md
-- finding: 66
-- title: compareValues ends in a universal `return TypeCoercion.compare(a, b)`, so every type gets an equality and an ordering whether PostgreSQL declares one or not — a
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_vf_mood AS ENUM ('sad','ok','happy');
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_e (m zz_vf_mood);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_e VALUES ('ok');
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input value for enum zz_vf_mood: "zzz"
-- end-expected-error
SELECT m < 'zzz' FROM zz_vf_e;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input value for enum zz_vf_mood: "nosuch"
-- end-expected-error
SELECT m = 'nosuch' FROM zz_vf_e;
-- begin-expected
-- ok: 0
-- end-expected
DROP TABLE zz_vf_e;
-- begin-expected
-- ok: 0
-- end-expected
DROP TYPE zz_vf_mood;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: point < point
-- end-expected-error
SELECT '(1,2)'::point < '(3,4)'::point;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: line < line
-- end-expected-error
SELECT '{1,1,0}'::line < '{2,2,0}'::line;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: polygon < polygon
-- end-expected-error
SELECT '((0,0),(1,0),(1,1))'::polygon < '((0,0),(2,0),(2,2))'::polygon;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: could not identify an equality operator for type point
-- end-expected-error
SELECT count(DISTINCT p) FROM (VALUES ('(1,2)'::point),('(1,2)'::point)) v(p);
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT '[(0,0),(1,1)]'::path < '[(0,0),(2,2)]'::path;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: point > point
-- end-expected-error
SELECT '(1,2)'::point > '(0,0)'::point;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: xml = xml
-- end-expected-error
SELECT '<a/>'::xml = '<a/>'::xml;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: operator does not exist: xml < xml
-- end-expected-error
SELECT '<a/>'::xml < '<b/>'::xml;
-- begin-expected-error
-- sqlstate: 42883
-- message-like: could not identify an equality operator for type xml
-- end-expected-error
SELECT count(DISTINCT x) FROM (VALUES ('<a/>'::xml)) v(x);
-- begin-expected-error
-- sqlstate: 42883
-- message-like: could not identify an equality operator for type xml
-- end-expected-error
SELECT x FROM (VALUES ('<a/>'::xml),('<b/>'::xml)) v(x) GROUP BY x;
