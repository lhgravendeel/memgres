-- source: review-2026-08.md
-- finding: Root cause 11: a deferred constraint is only deferred inside an explicit transaction block
-- area: Isolation, deadlocks and features in combination
-- title: Root cause 11: a deferred constraint is only deferred inside an explicit transaction block
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (id int PRIMARY KEY, pos int, CONSTRAINT zz_u UNIQUE (pos) DEFERRABLE INITIALLY DEFERRED);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_t VALUES (1,1),(2,2);
-- begin-expected
-- ok: 2
-- end-expected
UPDATE zz_t SET pos = 3 - pos;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_par (id int PRIMARY KEY);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_chi (id int PRIMARY KEY, pid int, CONSTRAINT zz_fk FOREIGN KEY (pid) REFERENCES zz_par(id) DEFERRABLE INITIALLY DEFERRED);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_f() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO zz_par VALUES (NEW.pid); RETURN NEW; END $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_t AFTER INSERT ON zz_chi FOR EACH ROW EXECUTE FUNCTION zz_f();
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_chi VALUES (1, 100);
