-- source: investigation-2026-08.md
-- finding: 255
-- title: ALTER TABLE ADD COLUMN has no partition guard, so parent and leaf end with different arities and a constraint on the added column makes every insert through the
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_ref (id int PRIMARY KEY);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_ref VALUES (1);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_p (id int, k int NOT NULL) PARTITION BY RANGE (k);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_pa PARTITION OF zz_p FOR VALUES FROM (0) TO (100);
-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot add column to a partition
-- end-expected-error
ALTER TABLE zz_pa ADD COLUMN lk int;
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "lk" referenced in foreign key constraint does not exist
-- end-expected-error
ALTER TABLE zz_pa ADD CONSTRAINT zz_pfk FOREIGN KEY (lk) REFERENCES zz_ref(id);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_p (id, k) VALUES (1, 10);
