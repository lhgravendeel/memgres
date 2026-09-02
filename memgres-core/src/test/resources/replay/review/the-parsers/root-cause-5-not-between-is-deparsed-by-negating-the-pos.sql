-- source: review-2026-08.md
-- finding: Root cause 5: NOT BETWEEN is deparsed by negating the positive form
-- area: The parsers
-- title: Root cause 5: NOT BETWEEN is deparsed by negating the positive form
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_c (i int);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_vf2_c ADD CONSTRAINT zz_vf2_ck1 CHECK (i NOT BETWEEN 1 AND 10);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_vf2_c ADD CONSTRAINT zz_vf2_ck8 CHECK (i NOT BETWEEN SYMMETRIC 1 AND 10);
-- begin-expected
-- columns: conname:name | pg_get_constraintdef:text
-- row: zz_vf2_ck1 | CHECK (((i < 1) OR (i > 10)))
-- row: zz_vf2_ck8 | CHECK ((((i < 1) OR (i > 10)) AND ((i < 10) OR (i > 1))))
-- rowcount: 2
-- end-expected
SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='zz_vf2_c'::regclass ORDER BY conname;
-- begin-expected
-- ok: 0
-- end-expected
CREATE INDEX zz_vf2_px ON zz_vf2_c ((i + 1)) WHERE i NOT BETWEEN 1 AND 5;
-- begin-expected
-- columns: pg_get_indexdef:text
-- row: CREATE INDEX zz_vf2_px ON public.zz_vf2_c USING btree (((i + 1))) WHERE ((i < 1) OR (i > 5))
-- rowcount: 1
-- end-expected
SELECT pg_get_indexdef('zz_vf2_px'::regclass);
