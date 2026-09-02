-- source: investigation-2026-08.md
-- finding: 139
-- title: A portal in the extended protocol carries no execution state: the row limit is honoured only for SELECT, there is no exhausted-portal flag, and COPY's failure p
-- extended protocol:
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_re (a int);
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- Parse "zz_sre" = INSERT INTO zz_vf_re VALUES (1) RETURNING a
-- Bind portal "zz_pre"; Execute zz_pre, 0; Execute zz_pre, 0; Sync
-- begin-expected
-- ok: 0
-- end-expected
COMMIT;
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_vf_re;
-- protocol sequence (pgjdbc: CopyManager.copyIn(...).cancelCopy()):
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_cf (a int);
-- begin-expected-error
-- sqlstate: 57014
-- message-like: COPY from stdin failed: COPY commands are only supported using the CopyManager API.
-- end-expected-error
COPY zz_vf_cf FROM STDIN;
-- then send CopyFail ('f') instead of CopyData/CopyDone
-- extended protocol, driven through pgjdbc's QueryExecutor:
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_pq (id int PRIMARY KEY);
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- Parse/Bind "INSERT INTO zz_vf_pq VALUES (600),(601) RETURNING id"
-- Execute(portal, maxRows = 1);
