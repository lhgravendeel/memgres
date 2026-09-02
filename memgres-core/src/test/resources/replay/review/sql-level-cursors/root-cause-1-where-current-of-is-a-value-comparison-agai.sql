-- source: review-2026-08.md
-- finding: Root cause 1: WHERE CURRENT OF is a value comparison against the first matching row of whatever table the statement names
-- area: SQL-level cursors
-- title: Root cause 1: WHERE CURRENT OF is a value comparison against the first matching row of whatever table the statement names
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_d (id int primary key, nm text);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_d VALUES (1,'dup'),(2,'dup'),(3,'x');
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
DECLARE zz_m1 CURSOR FOR SELECT nm FROM zz_d ORDER BY id DESC;
-- begin-expected
-- columns: nm:text
-- row: x
-- rowcount: 1
-- end-expected
FETCH 1 FROM zz_m1;
-- begin-expected
-- columns: nm:text
-- row: dup
-- rowcount: 1
-- end-expected
FETCH 1 FROM zz_m1;
-- positioned on id = 2
-- begin-expected
-- ok: 1
-- end-expected
UPDATE zz_d SET nm = 'HIT' WHERE CURRENT OF zz_m1;
-- begin-expected
-- columns: id:int4 | nm:text
-- row: 1 | dup
-- row: 2 | HIT
-- row: 3 | x
-- rowcount: 3
-- end-expected
SELECT id, nm FROM zz_d ORDER BY id;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_ct (id int primary key, nm text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_co (id int primary key, nm text);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_ct VALUES (1,'a'),(2,'b'),(3,'c');
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_co VALUES (1,'a'),(2,'b'),(3,'c');
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
DECLARE zz_k1 CURSOR FOR SELECT id, nm FROM zz_ct ORDER BY id FOR UPDATE;
-- begin-expected
-- columns: id:int4 | nm:text
-- row: 1 | a
-- rowcount: 1
-- end-expected
FETCH 1 FROM zz_k1;
-- begin-expected-error
-- sqlstate: 24000
-- message-like: cursor "zz_k1" does not have a FOR UPDATE/SHARE reference to table "zz_co"
-- end-expected-error
DELETE FROM zz_co WHERE CURRENT OF zz_k1;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected
-- ok: 0
-- end-expected
DECLARE zz_k3 CURSOR FOR SELECT g FROM generate_series(1,3) g;
-- begin-expected
-- columns: g:int4
-- row: 1
-- row: 2
-- rowcount: 2
-- end-expected
FETCH 2 FROM zz_k3;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_co" does not exist
-- end-expected-error
DELETE FROM zz_co WHERE CURRENT OF zz_k3;
-- begin-expected-error
-- sqlstate: 25P01
-- message-like: DECLARE CURSOR can only be used in transaction blocks
-- end-expected-error
DECLARE zz_k4 CURSOR FOR SELECT 1 AS id;
-- begin-expected-error
-- sqlstate: 34000
-- message-like: cursor "zz_k4" does not exist
-- end-expected-error
FETCH 1 FROM zz_k4;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_co" does not exist
-- end-expected-error
UPDATE zz_co SET nm = 'Y' WHERE CURRENT OF zz_k4;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_ct" does not exist
-- end-expected-error
DECLARE zz_k6 CURSOR FOR SELECT a.id, a.nm FROM zz_ct a JOIN zz_co b ON a.id=b.id ORDER BY a.id;
-- begin-expected-error
-- sqlstate: 34000
-- message-like: cursor "zz_k6" does not exist
-- end-expected-error
FETCH 1 FROM zz_k6;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_ct" does not exist
-- end-expected-error
UPDATE zz_ct SET nm = 'J' WHERE CURRENT OF zz_k6;
-- begin-expected
-- ok: 0
-- end-expected
BEGIN;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_ct" does not exist
-- end-expected-error
DECLARE zz_k7 CURSOR FOR SELECT id, nm FROM zz_ct ORDER BY id FOR UPDATE;
-- begin-expected-error
-- sqlstate: 34000
-- message-like: cursor "zz_k7" does not exist
-- end-expected-error
FETCH 1 FROM zz_k7;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_ct" does not exist
-- end-expected-error
UPDATE zz_ct SET nm = 'ONE' WHERE CURRENT OF zz_k7;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_ct" does not exist
-- end-expected-error
UPDATE zz_ct SET nm = 'TWO' WHERE CURRENT OF zz_k7;
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zz_ct" does not exist
-- end-expected-error
SELECT id, nm FROM zz_ct ORDER BY id;
