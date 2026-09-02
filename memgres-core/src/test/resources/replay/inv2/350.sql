-- source: investigation-2026-08.md
-- finding: 350
-- title: A WITH (...) storage-parameter name may only be a bare identifier followed by '=', so PostgreSQL's namespaced and valueless reloption forms are syntax errors — 
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_o2 (a int, s text) WITH (toast.autovacuum_enabled = true);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_w4 (a int) WITH (autovacuum_enabled);
-- begin-expected-error
-- sqlstate: 22023
-- message-like: unrecognized parameter "nosuchoption"
-- end-expected-error
CREATE TABLE zz_vf2_o1 (a int) WITH (toast.nosuchoption = 1);
