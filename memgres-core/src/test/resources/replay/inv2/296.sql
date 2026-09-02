-- source: investigation-2026-08.md
-- finding: 296
-- title: REASSIGN OWNED and DROP OWNED each read exactly one role name, and neither statement's tail is checked afterwards
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_qa;
-- begin-expected
-- ok: 0
-- end-expected
CREATE ROLE zz_qb;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_q1 (i int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_q2 (i int);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_q1 OWNER TO zz_qa;
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_q2 OWNER TO zz_qb;
-- begin-expected
-- ok: 0
-- end-expected
DROP OWNED BY zz_qa, zz_qb;
-- begin-expected
-- columns: relname:name
-- rowcount: 0
-- end-expected
SELECT relname FROM pg_class WHERE relname IN ('zz_q1','zz_q2') ORDER BY 1;
