-- source: review-2026-08.md
-- finding: Unrelated singletons
-- area: System columns, row locking and TABLESAMPLE
-- title: Unrelated singletons
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_srf (id int);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf2_srf VALUES (1),(2),(3);
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: FOR UPDATE is not allowed with set-returning functions in the target list
-- end-expected-error
SELECT generate_series(1,2) FROM zz_vf2_srf FOR UPDATE;
