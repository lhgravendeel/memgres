-- ============================================================================
-- Where the row-level policies and a view's check option sit against the
-- ON CONFLICT arbiter
--
-- PostgreSQL asks whether the row may be written at all before it judges the row
-- against anything else, so a row no policy admits is refused even where the
-- arbiter would have skipped it for conflicting with one already there, and even
-- where a column it holds is one the relation would have refused anyway.
--
-- The view's check option sits at the other end: PostgreSQL asks it once the row
-- is stored and its indexes have been made, so a row that duplicates a key is
-- refused for the key, a row that breaks a CHECK is refused for the CHECK, and a
-- row the arbiter skipped is never offered to the view at all.
--
-- Every value below was read off PostgreSQL 18 before it was written down.
-- ============================================================================

-- ============================================================================
-- A view's check option is asked last of all
-- ============================================================================

CREATE TABLE zzm3sd_vt (i int PRIMARY KEY, v int CHECK (v < 100), w int NOT NULL DEFAULT 1);
INSERT INTO zzm3sd_vt (i, v) VALUES (60, 1), (70, 1);
CREATE VIEW zzm3sd_vv AS SELECT * FROM zzm3sd_vt WHERE v < 10 WITH CHECK OPTION;

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint
-- end-expected-error
INSERT INTO zzm3sd_vv (i, v) VALUES (60, 20);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint
-- end-expected-error
INSERT INTO zzm3sd_vv (i, v) VALUES (60, 5);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: violates check constraint "zzm3sd_vt_v_check"
-- end-expected-error
INSERT INTO zzm3sd_vv (i, v) VALUES (30, 200);

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "w"
-- end-expected-error
INSERT INTO zzm3sd_vv (i, v, w) VALUES (30, 20, NULL);

-- begin-expected-error
-- sqlstate: 44000
-- message-like: new row violates check option for view "zzm3sd_vv"
-- end-expected-error
INSERT INTO zzm3sd_vv (i, v) VALUES (30, 20);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint
-- end-expected-error
UPDATE zzm3sd_vv SET i = 60, v = 20 WHERE i = 70;

-- begin-expected-error
-- sqlstate: 23514
-- message-like: violates check constraint "zzm3sd_vt_v_check"
-- end-expected-error
UPDATE zzm3sd_vv SET i = 80, v = 200 WHERE i = 70;

-- begin-expected-error
-- sqlstate: 44000
-- message-like: new row violates check option for view "zzm3sd_vv"
-- end-expected-error
UPDATE zzm3sd_vv SET v = 20 WHERE i = 70;

-- the arbiter comes between: a row it skips is never offered to the view
INSERT INTO zzm3sd_vv (i, v) VALUES (60, 20) ON CONFLICT (i) DO NOTHING;

-- begin-expected-error
-- sqlstate: 44000
-- message-like: new row violates check option for view "zzm3sd_vv"
-- end-expected-error
INSERT INTO zzm3sd_vv (i, v) VALUES (30, 20) ON CONFLICT (i) DO NOTHING;

-- begin-expected-error
-- sqlstate: 23514
-- message-like: violates check constraint "zzm3sd_vt_v_check"
-- end-expected-error
INSERT INTO zzm3sd_vv (i, v) VALUES (60, 200) ON CONFLICT (i) DO NOTHING;

-- and the row a conflict clause leaves behind is a row of the view like any
-- other
-- begin-expected-error
-- sqlstate: 44000
-- message-like: new row violates check option for view "zzm3sd_vv"
-- end-expected-error
INSERT INTO zzm3sd_vv (i, v) VALUES (60, 20) ON CONFLICT (i) DO UPDATE SET v = 30;

-- a row the view does admit is written
INSERT INTO zzm3sd_vv (i, v) VALUES (30, 5) ON CONFLICT (i) DO NOTHING;

-- begin-expected
-- columns: i | v | w
-- row: 30 | 5 | 1
-- row: 60 | 1 | 1
-- row: 70 | 1 | 1
-- end-expected
SELECT i, v, w FROM zzm3sd_vt ORDER BY i;

DROP VIEW zzm3sd_vv;
DROP TABLE zzm3sd_vt;

-- ============================================================================
-- The row-level policies are asked first of all
-- ============================================================================

CREATE TABLE zzm3sd_rt2 (i int PRIMARY KEY, v int CHECK (v < 100), w int NOT NULL DEFAULT 1);
INSERT INTO zzm3sd_rt2 (i, v) VALUES (60, 1), (70, 1);
ALTER TABLE zzm3sd_rt2 ENABLE ROW LEVEL SECURITY;
ALTER TABLE zzm3sd_rt2 FORCE ROW LEVEL SECURITY;
CREATE POLICY zzm3sd_pol ON zzm3sd_rt2 FOR ALL USING (true) WITH CHECK (i < 50);
DROP ROLE IF EXISTS zzm3sd_user;
CREATE ROLE zzm3sd_user LOGIN;
GRANT ALL ON zzm3sd_rt2 TO zzm3sd_user;
SET ROLE zzm3sd_user;

-- a row no policy admits is refused even where the arbiter would have skipped it
-- begin-expected-error
-- sqlstate: 42501
-- message-like: new row violates row-level security policy for table "zzm3sd_rt2"
-- end-expected-error
INSERT INTO zzm3sd_rt2 (i, v) VALUES (60, 1) ON CONFLICT (i) DO NOTHING;

-- ...and even where a column it holds is one the relation would have refused
-- begin-expected-error
-- sqlstate: 42501
-- message-like: new row violates row-level security policy for table "zzm3sd_rt2"
-- end-expected-error
INSERT INTO zzm3sd_rt2 (i, v) VALUES (60, 200) ON CONFLICT (i) DO NOTHING;

-- begin-expected-error
-- sqlstate: 42501
-- message-like: new row violates row-level security policy for table "zzm3sd_rt2"
-- end-expected-error
INSERT INTO zzm3sd_rt2 (i, v, w) VALUES (60, 1, NULL) ON CONFLICT (i) DO NOTHING;

-- begin-expected-error
-- sqlstate: 42501
-- message-like: new row violates row-level security policy for table "zzm3sd_rt2"
-- end-expected-error
INSERT INTO zzm3sd_rt2 (i, v) VALUES (60, 1) ON CONFLICT (i) DO UPDATE SET v = 2;

-- begin-expected-error
-- sqlstate: 42501
-- message-like: new row violates row-level security policy for table "zzm3sd_rt2"
-- end-expected-error
INSERT INTO zzm3sd_rt2 (i, v) VALUES (80, 200);

-- begin-expected-error
-- sqlstate: 42501
-- message-like: new row violates row-level security policy for table "zzm3sd_rt2"
-- end-expected-error
INSERT INTO zzm3sd_rt2 (i, v, w) VALUES (80, 1, NULL);

-- begin-expected-error
-- sqlstate: 42501
-- message-like: new row violates row-level security policy for table "zzm3sd_rt2"
-- end-expected-error
UPDATE zzm3sd_rt2 SET i = 80, v = 200 WHERE i = 70;

-- a row the policies do admit is judged against the relation's own rules next
-- begin-expected-error
-- sqlstate: 23514
-- message-like: violates check constraint "zzm3sd_rt2_v_check"
-- end-expected-error
INSERT INTO zzm3sd_rt2 (i, v) VALUES (40, 200) ON CONFLICT (i) DO NOTHING;

INSERT INTO zzm3sd_rt2 (i, v) VALUES (40, 5) ON CONFLICT (i) DO NOTHING;

-- begin-expected
-- columns: i | v | w
-- row: 40 | 5 | 1
-- row: 60 | 1 | 1
-- row: 70 | 1 | 1
-- end-expected
SELECT i, v, w FROM zzm3sd_rt2 ORDER BY i;

RESET ROLE;
DROP TABLE zzm3sd_rt2 CASCADE;
DROP ROLE zzm3sd_user;
