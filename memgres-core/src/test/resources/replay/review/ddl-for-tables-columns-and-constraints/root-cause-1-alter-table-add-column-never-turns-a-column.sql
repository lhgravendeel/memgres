-- source: review-2026-08.md
-- finding: Root cause 1: ALTER TABLE ADD COLUMN never turns a column's inline constraints into a constraint
-- area: DDL for tables, columns and constraints
-- title: Root cause 1: ALTER TABLE ADD COLUMN never turns a column's inline constraints into a constraint
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (a int);
-- begin-expected
-- ok: 0
-- end-expected
ALTER TABLE zz_t ADD COLUMN b int UNIQUE;
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_t (b) VALUES (1);
-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "zz_t_b_key"
-- end-expected-error
INSERT INTO zz_t (b) VALUES (1);
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_t" already exists
-- end-expected-error
CREATE TABLE zz_t (a int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_t VALUES (1);
-- begin-expected-error
-- sqlstate: 23514
-- message-like: check constraint "zz_t_e_check" of relation "zz_t" is violated by some row
-- end-expected-error
ALTER TABLE zz_t ADD COLUMN e int CHECK (e > 100) DEFAULT 5;
