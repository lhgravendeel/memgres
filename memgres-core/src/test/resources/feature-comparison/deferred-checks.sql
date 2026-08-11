-- ============================================================================
-- A DEFERRABLE INITIALLY DEFERRED constraint in autocommit
-- PostgreSQL runs a statement outside a transaction in an implicit transaction
-- of its own, and checks a deferred constraint when that commits -- once the
-- statement is over -- rather than as each row is written.
-- ============================================================================

DROP TABLE IF EXISTS zzed_du CASCADE;
CREATE TABLE zzed_du (id int PRIMARY KEY, pos int,
                      CONSTRAINT zzed_du_u UNIQUE (pos) DEFERRABLE INITIALLY DEFERRED);
INSERT INTO zzed_du VALUES (1,1),(2,2);

-- the swap is a duplicate halfway through and no duplicate at the end
UPDATE zzed_du SET pos = 3 - pos;

-- begin-expected
-- columns: id, pos
-- row: 1, 2
-- row: 2, 1
-- end-expected
SELECT id::text AS id, pos::text AS pos FROM zzed_du ORDER BY id;

-- a duplicate that is still there when the statement ends is still reported
-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint
-- end-expected-error
INSERT INTO zzed_du VALUES (3,1);

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::text AS cnt FROM zzed_du;

DROP TABLE zzed_du;

DROP TABLE IF EXISTS zzed_dpk CASCADE;
CREATE TABLE zzed_dpk (id int, CONSTRAINT zzed_dpk_pk PRIMARY KEY (id)
                       DEFERRABLE INITIALLY DEFERRED);
INSERT INTO zzed_dpk VALUES (1),(2);
UPDATE zzed_dpk SET id = 3 - id;

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id::text AS id FROM zzed_dpk ORDER BY id;

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint
-- end-expected-error
INSERT INTO zzed_dpk VALUES (5),(5);

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::text AS cnt FROM zzed_dpk;

DROP TABLE zzed_dpk;


-- ============================================================================
-- A deferred foreign key satisfied later in the same statement
-- The key may be missing when the row is written and present when the
-- statement ends -- supplied by an AFTER trigger, or by a later arm of a
-- data-modifying WITH. PostgreSQL looks at the end.
-- ============================================================================

DROP TABLE IF EXISTS zzed_chi CASCADE;
DROP TABLE IF EXISTS zzed_par CASCADE;
DROP FUNCTION IF EXISTS zzed_f() CASCADE;
CREATE TABLE zzed_par (id int PRIMARY KEY);
CREATE TABLE zzed_chi (id int PRIMARY KEY, pid int, CONSTRAINT zzed_fk
                       FOREIGN KEY (pid) REFERENCES zzed_par(id) DEFERRABLE INITIALLY DEFERRED);
CREATE FUNCTION zzed_f() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO zzed_par VALUES (NEW.pid); RETURN NEW; END $$;
CREATE TRIGGER zzed_tg AFTER INSERT ON zzed_chi FOR EACH ROW EXECUTE FUNCTION zzed_f();

INSERT INTO zzed_chi VALUES (1, 100);

-- begin-expected
-- columns: children, parents
-- row: 1, 1
-- end-expected
SELECT (SELECT count(*) FROM zzed_chi)::text AS children,
       (SELECT count(*) FROM zzed_par)::text AS parents;

DROP TRIGGER zzed_tg ON zzed_chi;

-- with nothing to supply the key, the deferred check still refuses at the end
-- begin-expected-error
-- sqlstate: 23503
-- message-like: violates foreign key constraint
-- end-expected-error
INSERT INTO zzed_chi VALUES (2, 999);

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::text AS cnt FROM zzed_chi;

DROP TABLE zzed_chi;
DROP TABLE zzed_par;
DROP FUNCTION zzed_f();

DROP TABLE IF EXISTS zzed_chi2 CASCADE;
DROP TABLE IF EXISTS zzed_par2 CASCADE;
CREATE TABLE zzed_par2 (id int PRIMARY KEY);
CREATE TABLE zzed_chi2 (id int PRIMARY KEY, pid int REFERENCES zzed_par2(id)
                        DEFERRABLE INITIALLY DEFERRED);

WITH ins AS (INSERT INTO zzed_chi2 VALUES (1,5) RETURNING pid)
INSERT INTO zzed_par2 SELECT pid FROM ins;

-- begin-expected
-- columns: children, parents
-- row: 1, 1
-- end-expected
SELECT (SELECT count(*) FROM zzed_chi2)::text AS children,
       (SELECT count(*) FROM zzed_par2)::text AS parents;

DROP TABLE zzed_chi2;
DROP TABLE zzed_par2;


-- ============================================================================
-- A deferred CONSTRAINT TRIGGER in autocommit
-- It fires at the end of the transaction, and outside an explicit one that is
-- the end of the statement -- so it runs after every immediate AFTER trigger
-- of the same statement, whatever order the triggers were created in.
-- ============================================================================

DROP TABLE IF EXISTS zzed_ct CASCADE;
DROP TABLE IF EXISTS zzed_ctl CASCADE;
DROP FUNCTION IF EXISTS zzed_ctf() CASCADE;
DROP FUNCTION IF EXISTS zzed_ctg() CASCADE;
CREATE TABLE zzed_ct (i int);
CREATE TABLE zzed_ctl (seq serial, t text);
CREATE FUNCTION zzed_ctf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO zzed_ctl(t) VALUES ('deferred:'||NEW.i); RETURN NULL; END $$;
CREATE FUNCTION zzed_ctg() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO zzed_ctl(t) VALUES ('immediate:'||NEW.i); RETURN NULL; END $$;
-- the deferred one is created first, so creation order alone would put it first
CREATE CONSTRAINT TRIGGER zzed_ctt AFTER INSERT ON zzed_ct DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION zzed_ctf();
CREATE TRIGGER zzed_ctu AFTER INSERT ON zzed_ct FOR EACH ROW EXECUTE FUNCTION zzed_ctg();

INSERT INTO zzed_ct VALUES (1),(2);

-- begin-expected
-- columns: t
-- row: immediate:1
-- row: immediate:2
-- row: deferred:1
-- row: deferred:2
-- end-expected
SELECT t FROM zzed_ctl ORDER BY seq;

DROP TABLE zzed_ct;
DROP TABLE zzed_ctl;
DROP FUNCTION zzed_ctf();
DROP FUNCTION zzed_ctg();


-- ============================================================================
-- ON CONFLICT DO UPDATE and columns the relation computes for itself
-- The SET list is settled while planning, so a write to a generated or an
-- identity column is refused even when nothing conflicts; DEFAULT is the one
-- thing either accepts.
-- ============================================================================

DROP TABLE IF EXISTS zzed_g CASCADE;
CREATE TABLE zzed_g (id int PRIMARY KEY, a int, g int GENERATED ALWAYS AS (a*2) STORED);

-- nothing conflicts, the action never runs, and the statement is still refused
-- begin-expected-error
-- sqlstate: 428C9
-- message-like: column "g" can only be updated to DEFAULT
-- end-expected-error
INSERT INTO zzed_g (id,a) VALUES (1,1) ON CONFLICT (id) DO UPDATE SET g = 1;

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::text AS cnt FROM zzed_g;

INSERT INTO zzed_g (id,a) VALUES (1,1);

-- begin-expected-error
-- sqlstate: 428C9
-- message-like: column "g" can only be updated to DEFAULT
-- end-expected-error
INSERT INTO zzed_g (id,a) VALUES (1,7) ON CONFLICT (id) DO UPDATE SET g = excluded.a;

-- DEFAULT is accepted and means "compute it again from the row this leaves"
INSERT INTO zzed_g (id,a) VALUES (1,9) ON CONFLICT (id) DO UPDATE SET g = DEFAULT, a = 11;

-- begin-expected
-- columns: id, a, g
-- row: 1, 11, 22
-- end-expected
SELECT id::text AS id, a::text AS a, g::text AS g FROM zzed_g ORDER BY id;

DROP TABLE zzed_g;

DROP TABLE IF EXISTS zzed_idt CASCADE;
CREATE TABLE zzed_idt (k int PRIMARY KEY, i int GENERATED ALWAYS AS IDENTITY, j int);
INSERT INTO zzed_idt (k,j) VALUES (1,1);

-- begin-expected-error
-- sqlstate: 428C9
-- message-like: column "i" can only be updated to DEFAULT
-- end-expected-error
INSERT INTO zzed_idt (k,j) VALUES (1,2) ON CONFLICT (k) DO UPDATE SET i = 5;

-- begin-expected
-- columns: k, i, j
-- row: 1, 1, 1
-- end-expected
SELECT k::text AS k, i::text AS i, j::text AS j FROM zzed_idt;

DROP TABLE zzed_idt;

DROP TABLE IF EXISTS zzed_dflt CASCADE;
CREATE TABLE zzed_dflt (id int PRIMARY KEY, v int DEFAULT 42, w int);
INSERT INTO zzed_dflt (id, v, w) VALUES (1, 7, 7);

-- DO UPDATE is an UPDATE, so DEFAULT means the column's own default here too
INSERT INTO zzed_dflt (id, v, w) VALUES (1, 8, 8) ON CONFLICT (id) DO UPDATE SET v = DEFAULT;

-- begin-expected
-- columns: id, v, w
-- row: 1, 42, 7
-- end-expected
SELECT id::text AS id, v::text AS v, w::text AS w FROM zzed_dflt;

-- a column with no default of its own goes to NULL
INSERT INTO zzed_dflt (id, v, w) VALUES (1, 8, 8) ON CONFLICT (id) DO UPDATE SET w = DEFAULT;

-- begin-expected
-- columns: v, w_is_null
-- row: 42, true
-- end-expected
SELECT v::text AS v, (w IS NULL)::text AS w_is_null FROM zzed_dflt;

DROP TABLE zzed_dflt;
