-- source: investigation-2026-08.md
-- finding: 84
-- title: The geometric tolerance constant is 1e-10 and is applied where PostgreSQL uses FPeq at 1e-6, and where PostgreSQL tests for exact zero
-- begin-expected
-- columns: slope:float8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT slope(point '(1,0)', point '(0,0)');
-- begin-expected
-- columns: slope:float8
-- row: Infinity
-- rowcount: 1
-- end-expected
SELECT slope(point '(0,0)', point '(0.0000005,5)');
-- begin-expected
-- columns: ?column?:point
-- row: (1000000,1000000)
-- rowcount: 1
-- end-expected
SELECT point '(1,1)' / point '(0.000001,0)';
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid line specification: must be two distinct points
-- end-expected-error
SELECT '[(0,0),(0.0000005,0)]'::line;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid line specification: must be two distinct points
-- end-expected-error
SELECT '[(0,0),(0,0)]'::line;
