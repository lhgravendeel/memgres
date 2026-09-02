-- source: investigation.md
-- finding: 56
-- title: Untyped literals are classified by shape, not resolved against the operand ⚠️ high
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '192.168.1.1'::inet && '192.168.1.0/24';
--   PG: true | mg: 42883 overlap not supported between point and point   ← read as points
-- begin-expected
-- columns: ?column?:numrange
-- row: [2.0,3.0)
-- rowcount: 1
-- end-expected
SELECT '[1.0,3.0)'::numrange * '[2.0,5.0)';
--   PG: [2.0,3.0) | mg: 42883 operator does not exist: inet * inet       ← read as inet
-- begin-expected
-- columns: ?column?:_int4
-- row: {{1,2},{3,4},{5,6}}
-- rowcount: 1
-- end-expected
SELECT ARRAY[[1,2],[3,4]] || '{5,6}';
--   PG: {{1,2},{3,4},{5,6}} | mg: 42883 integer[] || point               ← read as point
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '192.168.1.1'::inet << '192.168.1.0/24';
--   PG: true | mg: XX000 Internal error: For input string: "192.168.1.1"
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT '10.0.0.0/8'::cidr >> '10.1.2.3';
--   PG: true | mg: XX000 Internal error: For input string: "10.0.0.0/8"
-- begin-expected
-- columns: ?column?:int8
-- row: 2
-- rowcount: 1
-- end-expected
SELECT '10.1.2.5'::inet - '10.1.2.3';
--   PG: 2 | mg: 22P02 invalid input syntax for type double precision
-- begin-expected
-- columns: ?column?:interval
-- row: 1 day 01:00:00
-- rowcount: 1
-- end-expected
SELECT interval '1 day' + '1 hour';
--   PG: 1 day 01:00:00 | mg: 22P02 invalid input syntax for type double precision
-- begin-expected
-- columns: ?column?:interval
-- row: 1 day 01:00:00
-- rowcount: 1
-- end-expected
SELECT '1 hour' + interval '1 day';
--   PG: 1 day 01:00:00 | mg: 42883 operator does not exist: text + text
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type box: "(1,1)"
-- end-expected-error
SELECT '((0,0),(2,2))'::box @> '(1,1)';
-- PG: 22P02 invalid input for box | mg: true
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type circle: "(1,1)"
-- end-expected-error
SELECT '<(0,0),5>'::circle @> '(1,1)';
-- PG: 22P02                       | mg: true
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed range literal: "2020-03-01"
-- end-expected-error
SELECT '[2020-01-01,2020-06-01)'::daterange @> '2020-03-01';
--   PG: 22P02 malformed range literal | mg: true
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed multirange literal: "[1,2)"
-- end-expected-error
SELECT '{[1,3),[5,7)}'::int4multirange @> '[1,2)';
--   PG: 22P02 malformed multirange literal | mg: true
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "3"
-- end-expected-error
SELECT ARRAY[1,2] || '3';
-- PG: 22P02 malformed array literal | mg: {1,2,3}
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: malformed array literal: "c"
-- end-expected-error
SELECT ARRAY['a','b'] || 'c';
-- PG: 22P02                          | mg: {a,b,c}
-- begin-expected-error
-- sqlstate: 42804
-- message-like: could not determine polymorphic type because input has type unknown
-- end-expected-error
SELECT array_to_string('{1,2,3}', '-');
--   PG: 42804 could not determine polymorphic type because input has type unknown | mg: 1-2-3
-- begin-expected-error
-- sqlstate: 42804
-- message-like: could not determine polymorphic type because input has type unknown
-- end-expected-error
SELECT array_length('{1,2,3}', 1);
-- PG: 42804 | mg: 3
-- begin-expected-error
-- sqlstate: 42804
-- message-like: could not determine polymorphic type because input has type unknown
-- end-expected-error
SELECT cardinality('{1,2,3}');
-- PG: 42804 | mg: 3;
