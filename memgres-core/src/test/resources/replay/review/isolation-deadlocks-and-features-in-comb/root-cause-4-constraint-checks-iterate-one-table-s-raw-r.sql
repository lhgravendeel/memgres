-- source: review-2026-08.md
-- finding: Root cause 4: constraint checks iterate one Table's raw row list — no visibility filter, no partition walk
-- area: Isolation, deadlocks and features in combination
-- title: Root cause 4: constraint checks iterate one Table's raw row list — no visibility filter, no partition walk
-- session A                                   -- session B
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (i int PRIMARY KEY);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_u (id int PRIMARY KEY, p int REFERENCES zz_t(i));
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_t VALUES (7);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_u VALUES (1, 7);
-- begin-expected
-- ok: 0
-- end-expected
ROLLBACK;
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_u ch
                                                WHERE NOT EXISTS (SELECT 1 FROM zz_t p WHERE p.i = ch.p);
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
CREATE TABLE zz_p (k int NOT NULL, r int REFERENCES zz_ref(id)) PARTITION BY RANGE (k);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_pa PARTITION OF zz_p FOR VALUES FROM (0) TO (100);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_p VALUES (1, 1);
-- begin-expected-error
-- sqlstate: 23503
-- message-like: update or delete on table "zz_ref" violates foreign key constraint "zz_p_r_fkey" on table "zz_p"
-- end-expected-error
DELETE FROM zz_ref WHERE id = 1;
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_p c WHERE NOT EXISTS (SELECT 1 FROM zz_ref p WHERE p.id = c.r);
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_ref" already exists
-- end-expected-error
CREATE TABLE zz_ref (id int PRIMARY KEY);
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_p" already exists
-- end-expected-error
CREATE TABLE zz_p (k int NOT NULL, r int REFERENCES zz_ref(id)) PARTITION BY RANGE (k);
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_pa" already exists
-- end-expected-error
CREATE TABLE zz_pa PARTITION OF zz_p FOR VALUES FROM (0) TO (100);
-- begin-expected-error
-- sqlstate: 23503
-- message-like: insert or update on table "zz_pa" violates foreign key constraint "zz_p_r_fkey"
-- end-expected-error
INSERT INTO zz_p VALUES (1, 42);
