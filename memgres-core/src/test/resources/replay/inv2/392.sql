-- source: investigation-2026-08.md
-- finding: 392
-- title: executeTruncate builds its schema search list as {default schema, public} and never consults the session's temp namespace, unlike CREATE/INSERT/SELECT/DROP whic
-- begin-expected
-- ok: 0
-- end-expected
CREATE TEMP TABLE zz_vf_tr (a int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_tr VALUES (1);
-- begin-expected
-- ok: 0
-- end-expected
TRUNCATE zz_vf_tr;
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_vf_tr;
