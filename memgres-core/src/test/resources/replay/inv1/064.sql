-- source: investigation.md
-- finding: 64
-- title: Unbounded allocation in the pad family ⚠️ high
-- begin-expected-error
-- sqlstate: 54000
-- message-like: requested length too large
-- end-expected-error
SELECT length(lpad('abc', 400000000, 'x'));
--   PG: 54000 requested length too large | mg: no result in 15s (allocating ~400MB)
-- begin-expected-error
-- sqlstate: 54000
-- message-like: requested length too large
-- end-expected-error
SELECT length(repeat('ab', 1500000000));
--   PG: 54000 requested length too large | mg: XX000 Internal error: -1294967296;
