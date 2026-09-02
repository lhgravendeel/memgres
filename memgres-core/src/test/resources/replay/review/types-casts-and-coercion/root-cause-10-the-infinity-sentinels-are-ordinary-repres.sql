-- source: review-2026-08.md
-- finding: Root cause 10: the infinity sentinels are ordinary representable values
-- area: Types, casts and coercion
-- title: Root cause 10: the infinity sentinels are ordinary representable values
-- begin-expected
-- columns: text:text
-- row: infinity
-- rowcount: 1
-- end-expected
SELECT (timestamptz 'infinity')::text;
-- begin-expected
-- columns: isfinite:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT isfinite(timestamptz 'infinity');
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT '9999-12-31 23:59:59'::timestamp = 'infinity'::timestamp;
-- begin-expected
-- columns: ?column?:bool
-- row: f
-- rowcount: 1
-- end-expected
SELECT '9999-12-31'::date = 'infinity'::date;
-- begin-expected
-- columns: ?column?:date
-- row: infinity
-- rowcount: 1
-- end-expected
SELECT date 'infinity' + 1;
