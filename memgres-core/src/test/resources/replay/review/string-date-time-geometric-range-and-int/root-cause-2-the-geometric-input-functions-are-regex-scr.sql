-- source: review-2026-08.md
-- finding: Root cause 2: the geometric input functions are regex scrapes that never require the whole literal, and a column write does not run them at all
-- area: String, date/time, geometric, range and interval implementations
-- title: Root cause 2: the geometric input functions are regex scrapes that never require the whole literal, and a column write does not run them at all
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type box: "(1,2),(3,4),(5,6)"
-- end-expected-error
SELECT '(1,2),(3,4),(5,6)'::box;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type line: "{1,2,3,4}"
-- end-expected-error
SELECT '{1,2,3,4}'::line;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type circle: "<(1,2),3,4>"
-- end-expected-error
SELECT '<(1,2),3,4>'::circle;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_g (c circle);
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type circle: "<(1e,2),3>"
-- end-expected-error
INSERT INTO zz_vf2_g VALUES ('<(1e,2),3>');
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type circle: "<(1,2),3,4>"
-- end-expected-error
INSERT INTO zz_vf2_g VALUES ('<(1,2),3,4>');
-- begin-expected
-- columns: c:circle
-- rowcount: 0
-- end-expected
SELECT c FROM zz_vf2_g;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type lseg: "[(1e,2),(3,4)]"
-- end-expected-error
SELECT '[(1e,2),(3,4)]'::lseg;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type polygon: "((1e,2),(3,4),(5,6))"
-- end-expected-error
SELECT '((1e,2),(3,4),(5,6))'::polygon;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type circle: "<(1e,2),3>"
-- end-expected-error
SELECT '<(1e,2),3>'::circle;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type lseg: "[(-,1),(2,3)]"
-- end-expected-error
SELECT '[(-,1),(2,3)]'::lseg;
