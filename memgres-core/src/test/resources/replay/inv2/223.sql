-- source: investigation-2026-08.md
-- finding: 223
-- title: a SET LOCAL value is a transaction override that get() consults first and that neither set, reset nor resetAll removes
-- begin-expected
-- ok: 0
-- end-expected
SET work_mem = '10MB';
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
SET LOCAL work_mem = '18MB';
-- begin-expected
-- ok: 0
-- end-expected
SET work_mem = '19MB';
-- begin-expected
-- columns: work_mem:text
-- row: 19MB
-- rowcount: 1
-- end-expected
SHOW work_mem;
-- begin-expected
-- ok: 0
-- end-expected
COMMIT;
-- begin-expected
-- ok: 0
-- end-expected
SET statement_timeout = '6s';
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
SET LOCAL statement_timeout = '7s';
-- begin-expected
-- ok: 0
-- end-expected
RESET statement_timeout;
-- begin-expected
-- columns: statement_timeout:text
-- row: 0
-- rowcount: 1
-- end-expected
SHOW statement_timeout;
-- begin-expected
-- ok: 0
-- end-expected
COMMIT;
