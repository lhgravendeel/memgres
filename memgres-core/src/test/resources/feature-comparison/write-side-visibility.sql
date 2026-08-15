-- A write through an auto-updatable view reaches only the rows the view shows: PostgreSQL
-- rewrites the UPDATE or DELETE onto the base relation with the view's own WHERE added to the
-- statement's, so a row outside the view is not a row the write can touch.

-- setup
CREATE TABLE zzw2j_vb (id int PRIMARY KEY, owner text, a int);
INSERT INTO zzw2j_vb VALUES (1,'me',5),(2,'you',50),(3,'me',8);
CREATE VIEW zzw2j_vv AS SELECT id, a FROM zzw2j_vb WHERE a < 10;

-- stmt 1: an UPDATE naming a row the view does not show changes nothing
-- begin-expected
-- columns: id, a
-- end-expected
UPDATE zzw2j_vv SET a = 6 WHERE id = 2 RETURNING id, a;

-- stmt 2: the base row is untouched
-- begin-expected
-- columns: id, owner, a
-- row: 1 | me | 5
-- row: 2 | you | 50
-- row: 3 | me | 8
-- end-expected
SELECT id, owner, a FROM zzw2j_vb ORDER BY id;

-- stmt 3: an unqualified UPDATE through the view reaches the view's rows and no others
-- begin-expected
-- columns: id, a
-- row: 1 | 6
-- row: 3 | 9
-- end-expected
UPDATE zzw2j_vv SET a = a + 1 RETURNING id, a;

-- stmt 4: a view whose qualification is on a column it does not project
CREATE VIEW zzw2j_mine AS SELECT id, a FROM zzw2j_vb WHERE owner = 'me';

-- stmt 5: a DELETE naming a row that view does not show deletes nothing
-- begin-expected
-- columns: id
-- end-expected
DELETE FROM zzw2j_mine WHERE id = 2 RETURNING id;

-- stmt 6: a view over a view carries both qualifications
CREATE VIEW zzw2j_small AS SELECT id AS k, a AS v FROM zzw2j_mine WHERE a < 7;

-- stmt 7: only the row both views show
-- begin-expected
-- columns: k, v
-- row: 1 | 6
-- end-expected
SELECT k, v FROM zzw2j_small ORDER BY k;

-- stmt 8: an unqualified DELETE through the layered view removes exactly that row
-- begin-expected
-- columns: k
-- row: 1
-- end-expected
DELETE FROM zzw2j_small RETURNING k;

-- stmt 9: everything else is still there
-- begin-expected
-- columns: id, owner, a
-- row: 2 | you | 50
-- row: 3 | me | 9
-- end-expected
SELECT id, owner, a FROM zzw2j_vb ORDER BY id;

-- stmt 10: a view whose FROM item is aliased qualifies through the alias
CREATE VIEW zzw2j_al AS SELECT z.id, z.a FROM zzw2j_vb z WHERE z.a > 40;

-- stmt 11
-- begin-expected
-- columns: id
-- row: 2
-- end-expected
DELETE FROM zzw2j_al RETURNING id;

-- stmt 12
-- begin-expected
-- columns: id, owner, a
-- row: 3 | me | 9
-- end-expected
SELECT id, owner, a FROM zzw2j_vb ORDER BY id;

-- cleanup
DROP VIEW zzw2j_al;
DROP VIEW zzw2j_small;
DROP VIEW zzw2j_mine;
DROP VIEW zzw2j_vv;
DROP TABLE zzw2j_vb;

-- A composite's field is a value of the type the composite declares for it, so a field typed by
-- a domain is judged by that domain's NOT NULL and its CHECKs exactly as a column of it would be.

-- setup
CREATE DOMAIN zzw2j_d AS int CHECK (VALUE > 0);
CREATE DOMAIN zzw2j_dt AS text NOT NULL CHECK (VALUE = lower(VALUE));
CREATE TYPE zzw2j_c AS (a zzw2j_d, b zzw2j_dt);
CREATE TABLE zzw2j_ct (id int, c zzw2j_c);

-- stmt 1: a ROW whose first field the domain refuses
-- begin-expected-error
-- sqlstate: 23514
-- message-like: value for domain zzw2j_d violates check constraint "zzw2j_d_check"
-- end-expected-error
SELECT ROW(-1,'x')::zzw2j_c;

-- stmt 2: and its second
-- begin-expected-error
-- sqlstate: 23514
-- message-like: value for domain zzw2j_dt violates check constraint "zzw2j_dt_check"
-- end-expected-error
SELECT ROW(5,'NOPE')::zzw2j_c;

-- stmt 3: a NOT NULL domain refuses a null field
-- begin-expected-error
-- sqlstate: 23502
-- message-like: domain zzw2j_dt does not allow null values
-- end-expected-error
SELECT ROW(5,NULL)::zzw2j_c;

-- stmt 4: the same composite written as text
-- begin-expected-error
-- sqlstate: 23514
-- message-like: value for domain zzw2j_d violates check constraint "zzw2j_d_check"
-- end-expected-error
SELECT '(-1,x)'::zzw2j_c;

-- stmt 5: a value every field's domain accepts
-- begin-expected
-- columns: row
-- row: (5,ok)
-- end-expected
SELECT ROW(5,'ok')::zzw2j_c AS row;

-- stmt 6: an INSERT of a refused composite
-- begin-expected-error
-- sqlstate: 23514
-- message-like: value for domain zzw2j_d violates check constraint "zzw2j_d_check"
-- end-expected-error
INSERT INTO zzw2j_ct VALUES (2, ROW(-1,'ok')::zzw2j_c);

-- stmt 7: an accepted one
INSERT INTO zzw2j_ct VALUES (5, ROW(5,'ok')::zzw2j_c);

-- stmt 8: an UPDATE to a refused composite
-- begin-expected-error
-- sqlstate: 23514
-- message-like: value for domain zzw2j_d violates check constraint "zzw2j_d_check"
-- end-expected-error
UPDATE zzw2j_ct SET c = ROW(-7,'x')::zzw2j_c WHERE id = 5;

-- stmt 9: an array of composites is built through the same casts
-- begin-expected-error
-- sqlstate: 23514
-- message-like: value for domain zzw2j_d violates check constraint "zzw2j_d_check"
-- end-expected-error
SELECT ARRAY[ROW(5,'a')::zzw2j_c, ROW(-1,'b')::zzw2j_c];

-- stmt 10: only the accepted row is stored
-- begin-expected
-- columns: id, c
-- row: 5 | (5,ok)
-- end-expected
SELECT id, c FROM zzw2j_ct ORDER BY id;

-- cleanup
DROP TABLE zzw2j_ct;
DROP TYPE zzw2j_c;
DROP DOMAIN zzw2j_dt;
DROP DOMAIN zzw2j_d;