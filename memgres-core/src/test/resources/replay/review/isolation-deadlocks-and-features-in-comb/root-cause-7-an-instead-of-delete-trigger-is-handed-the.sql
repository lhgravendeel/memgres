-- source: review-2026-08.md
-- finding: Root cause 7: an INSTEAD OF DELETE trigger is handed the live base row as both NEW and OLD, and its return value is copied back over it
-- area: Isolation, deadlocks and features in combination
-- title: Root cause 7: an INSTEAD OF DELETE trigger is handed the live base row as both NEW and OLD, and its return value is copied back over it
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_b (id int, note text);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_b VALUES (1,'a');
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_v AS SELECT id, note FROM zz_b;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_f() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN UPDATE zz_b SET note = 'deleted-by-trigger' WHERE id = OLD.id; RETURN OLD; END $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_t INSTEAD OF DELETE ON zz_v FOR EACH ROW EXECUTE FUNCTION zz_f();
-- begin-expected
-- ok: 1
-- end-expected
DELETE FROM zz_v WHERE id = 1;
-- begin-expected
-- columns: id:int4 | note:text
-- row: 1 | deleted-by-trigger
-- rowcount: 1
-- end-expected
SELECT id, note FROM zz_b ORDER BY id;
