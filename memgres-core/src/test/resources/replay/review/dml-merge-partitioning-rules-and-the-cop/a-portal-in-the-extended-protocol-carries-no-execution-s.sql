-- source: review-2026-08.md
-- finding: A portal in the extended protocol carries no execution state
-- area: DML, MERGE, partitioning, rules and the COPY/extended-protocol surface
-- title: A portal in the extended protocol carries no execution state
-- Parse/Bind "INSERT INTO zz_vf_pq VALUES (600),(601) RETURNING id"; Execute(portal, maxRows=1)
-- Parse "s" = INSERT INTO t VALUES (1) RETURNING a; Bind portal "p"; Execute p,0; Execute p,0; Sync
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_cf (a int);
-- begin-expected-error
-- sqlstate: 57014
-- message-like: COPY from stdin failed: COPY commands are only supported using the CopyManager API.
-- end-expected-error
COPY zz_vf_cf FROM STDIN;
-- then send CopyFail ('f'); this is pgjdbc's CopyIn.cancelCopy();
