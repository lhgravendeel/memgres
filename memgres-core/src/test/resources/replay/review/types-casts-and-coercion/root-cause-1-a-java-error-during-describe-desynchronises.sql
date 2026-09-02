-- source: review-2026-08.md
-- finding: Root cause 1: a Java `Error` during Describe desynchronises the connection permanently
-- area: Types, casts and coercion
-- title: Root cause 1: a Java `Error` during Describe desynchronises the connection permanently
-- one connection, in this order
-- begin-expected-error
-- sqlstate: 54001
-- message-like: stack depth limit exceeded
-- end-expected-error
SELECT repeat('a', 400000) LIKE repeat('%a', 400000);
-- begin-expected
-- columns: a:int4
-- row: 111
-- rowcount: 1
-- end-expected
SELECT 111 AS a;
-- begin-expected
-- columns: b:int4
-- row: 222
-- rowcount: 1
-- end-expected
SELECT 222 AS b;
-- begin-expected
-- columns: c:int4
-- row: 333
-- rowcount: 1
-- end-expected
SELECT 333 AS c;
