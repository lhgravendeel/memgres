-- source: review-2026-08.md
-- finding: Root cause 5: the whole-table DELETE fast path fires no triggers at all
-- area: Isolation, deadlocks and features in combination
-- title: Root cause 5: the whole-table DELETE fast path fires no triggers at all
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_lg (seq serial, t text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_lf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO zz_lg (t) VALUES (TG_WHEN||'/'||TG_OP||'/'||OLD.id); RETURN OLD; END $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (id int PRIMARY KEY);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_t VALUES (1),(2);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_tb BEFORE DELETE ON zz_t FOR EACH ROW EXECUTE FUNCTION zz_lf();
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_ta AFTER DELETE ON zz_t FOR EACH ROW EXECUTE FUNCTION zz_lf();
-- begin-expected
-- ok: 2
-- end-expected
DELETE FROM zz_t;
-- begin-expected
-- columns: t:text
-- row: BEFORE/DELETE/1
-- row: BEFORE/DELETE/2
-- row: AFTER/DELETE/1
-- row: AFTER/DELETE/2
-- rowcount: 4
-- end-expected
SELECT t FROM zz_lg ORDER BY seq;
