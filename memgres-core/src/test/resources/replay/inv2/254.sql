-- source: investigation-2026-08.md
-- finding: 254
-- title: Referential actions rewrite the child row directly rather than running a real UPDATE/DELETE, so the second write path has none of the first path's trigger firin
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_lg (seq serial, t text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_lf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO zz_lg (t) VALUES (TG_WHEN||'/'||TG_OP); IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW; END $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (id int PRIMARY KEY, pid int REFERENCES zz_t(id) ON DELETE CASCADE);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_t VALUES (1,NULL),(2,1),(3,2);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_tt AFTER DELETE ON zz_t FOR EACH ROW EXECUTE FUNCTION zz_lf();
-- begin-expected
-- ok: 1
-- end-expected
DELETE FROM zz_t WHERE id = 1;
-- begin-expected
-- columns: count:int8
-- row: 3
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_lg;
