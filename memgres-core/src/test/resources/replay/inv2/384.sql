-- source: investigation-2026-08.md
-- finding: 384
-- title: Portal suspension bookkeeping is wrong in both directions: portal.suspendedResult is cleared on the final chunk (so the next Execute re-runs the query from the 
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- Parse 'SELECT g FROM generate_series(1,4) g'; Bind; then Execute(maxRows=3) four times
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- Parse 'SELECT g FROM generate_series(1,3) g'; Bind; Execute(maxRows=3);
