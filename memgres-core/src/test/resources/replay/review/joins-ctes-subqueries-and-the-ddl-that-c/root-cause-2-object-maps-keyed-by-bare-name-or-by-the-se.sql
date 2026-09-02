-- source: review-2026-08.md
-- finding: Root cause 2: object maps keyed by bare name or by the session's schema
-- area: Joins, CTEs, subqueries — and the DDL that came with them
-- title: Root cause 2: object maps keyed by bare name or by the session's schema
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_shared (id int, n int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_tf2() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.n := 42; RETURN NEW; END $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_tg2 BEFORE INSERT ON zz_shared FOR EACH ROW EXECUTE FUNCTION zz_tf2();
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_sc;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_sc.zz_shared (id int);
-- begin-expected
-- ok: 0
-- end-expected
DROP SCHEMA zz_sc CASCADE;
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO public.zz_shared VALUES (1, 7);
-- begin-expected
-- columns: id:int4 | n:int4
-- row: 1 | 42
-- rowcount: 1
-- end-expected
SELECT id, n FROM public.zz_shared;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_o1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_o1.oq;
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop schema zz_o1 because other objects depend on it
-- end-expected-error
DROP SCHEMA zz_o1;
-- begin-expected
-- ok: 0
-- end-expected
DROP SCHEMA IF EXISTS zz_o1 CASCADE;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_o1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE zz_o1.oq;
-- begin-expected
-- ok: 0
-- end-expected
CREATE SCHEMA zz_s9;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_s9.pt (id int);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_s9.pt ENABLE ROW LEVEL SECURITY;
-- begin-expected
-- ok: 0
-- end-expected
CREATE POLICY zz_pol1 ON zz_s9.pt USING (true);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TEMP TABLE zz_qt (a int);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE pg_temp.zz_qt2 (a int);
