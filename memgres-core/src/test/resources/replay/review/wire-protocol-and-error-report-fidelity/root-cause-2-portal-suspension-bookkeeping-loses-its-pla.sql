-- source: review-2026-08.md
-- finding: Root cause 2: portal suspension bookkeeping loses its place
-- area: Wire protocol and error-report fidelity
-- title: Root cause 2: portal suspension bookkeeping loses its place
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- Parse 'SELECT g FROM generate_series(1,4) g'; Bind; Execute(maxRows=3) x4
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- Parse 'SELECT g FROM generate_series(1,3) g'; Bind; Execute(maxRows=3);
