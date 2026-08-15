-- ============================================================================
-- A referential action is an ordinary write on the referencing table
-- ============================================================================

CREATE TABLE zzw2f_p1 (id int PRIMARY KEY);
CREATE TABLE zzw2f_c1 (p int NOT NULL REFERENCES zzw2f_p1(id) ON DELETE SET NULL);
INSERT INTO zzw2f_p1 VALUES (1);
INSERT INTO zzw2f_c1 VALUES (1);

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "p" of relation "zzw2f_c1" violates not-null constraint
-- end-expected-error
DELETE FROM zzw2f_p1 WHERE id = 1;

-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::text AS cnt FROM zzw2f_p1;

DROP TABLE zzw2f_c1;
DROP TABLE zzw2f_p1;

CREATE TABLE zzw2f_p4 (id int PRIMARY KEY);
CREATE TABLE zzw2f_c4 (p int NOT NULL REFERENCES zzw2f_p4(id) ON DELETE SET DEFAULT);
INSERT INTO zzw2f_p4 VALUES (1);
INSERT INTO zzw2f_c4 VALUES (1);

-- begin-expected-error
-- sqlstate: 23502
-- message-like: null value in column "p" of relation "zzw2f_c4" violates not-null constraint
-- end-expected-error
DELETE FROM zzw2f_p4 WHERE id = 1;

-- begin-expected
-- columns: p
-- row: 1
-- end-expected
SELECT p::text AS p FROM zzw2f_c4;

DROP TABLE zzw2f_c4;
DROP TABLE zzw2f_p4;

CREATE DOMAIN zzw2f_dom AS int NOT NULL;
CREATE TABLE zzw2f_p5 (id int PRIMARY KEY);
CREATE TABLE zzw2f_c5 (p zzw2f_dom REFERENCES zzw2f_p5(id) ON DELETE SET NULL);
INSERT INTO zzw2f_p5 VALUES (1);
INSERT INTO zzw2f_c5 VALUES (1);

-- begin-expected-error
-- sqlstate: 23502
-- message-like: domain zzw2f_dom does not allow null values
-- end-expected-error
DELETE FROM zzw2f_p5 WHERE id = 1;

-- begin-expected
-- columns: p
-- row: 1
-- end-expected
SELECT p::text AS p FROM zzw2f_c5;

DROP TABLE zzw2f_c5;
DROP TABLE zzw2f_p5;
DROP DOMAIN zzw2f_dom;

-- ============================================================================
-- The referencing table's CHECK decides whether the parent's write may proceed
-- ============================================================================

CREATE TABLE zzw2f_p3 (id int PRIMARY KEY);
CREATE TABLE zzw2f_c3 (p int REFERENCES zzw2f_p3(id) ON DELETE SET DEFAULT DEFAULT 99, CHECK (p < 50));
INSERT INTO zzw2f_p3 VALUES (1),(99);
INSERT INTO zzw2f_c3 VALUES (1);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "zzw2f_c3" violates check constraint "zzw2f_c3_p_check"
-- end-expected-error
DELETE FROM zzw2f_p3 WHERE id = 1;

-- begin-expected
-- columns: p
-- row: 1
-- end-expected
SELECT p::text AS p FROM zzw2f_c3;

-- begin-expected
-- columns: cnt
-- row: 2
-- end-expected
SELECT count(*)::text AS cnt FROM zzw2f_p3;

DROP TABLE zzw2f_c3;
DROP TABLE zzw2f_p3;

CREATE TABLE zzw2f_p6 (id int PRIMARY KEY);
CREATE TABLE zzw2f_c6 (p int REFERENCES zzw2f_p6(id) ON UPDATE SET DEFAULT DEFAULT 99, CHECK (p < 50));
INSERT INTO zzw2f_p6 VALUES (1),(99);
INSERT INTO zzw2f_c6 VALUES (1);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "zzw2f_c6" violates check constraint "zzw2f_c6_p_check"
-- end-expected-error
UPDATE zzw2f_p6 SET id = 5 WHERE id = 1;

-- begin-expected
-- columns: p
-- row: 1
-- end-expected
SELECT p::text AS p FROM zzw2f_c6;

DROP TABLE zzw2f_c6;
DROP TABLE zzw2f_p6;

CREATE TABLE zzw2f_p7 (id int PRIMARY KEY);
CREATE TABLE zzw2f_c7 (p int REFERENCES zzw2f_p7(id) ON UPDATE CASCADE, CHECK (p < 50));
INSERT INTO zzw2f_p7 VALUES (1);
INSERT INTO zzw2f_c7 VALUES (1);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "zzw2f_c7" violates check constraint "zzw2f_c7_p_check"
-- end-expected-error
UPDATE zzw2f_p7 SET id = 100;

-- begin-expected
-- columns: p, id
-- row: 1, 1
-- end-expected
SELECT (SELECT p::text FROM zzw2f_c7) AS p, (SELECT id::text FROM zzw2f_p7) AS id;

DROP TABLE zzw2f_c7;
DROP TABLE zzw2f_p7;

-- ============================================================================
-- A referential action maintains the referencing table's keys and generated columns
-- ============================================================================

CREATE TABLE zzw2f_q1 (id int PRIMARY KEY);
CREATE TABLE zzw2f_q2 (p int UNIQUE REFERENCES zzw2f_q1(id) ON DELETE SET DEFAULT DEFAULT 99);
INSERT INTO zzw2f_q1 VALUES (1),(2),(99);
INSERT INTO zzw2f_q2 VALUES (1),(2);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "zzw2f_q2_p_key"
-- end-expected-error
DELETE FROM zzw2f_q1 WHERE id IN (1,2);

-- begin-expected
-- columns: p
-- row: 1
-- row: 2
-- end-expected
SELECT p::text AS p FROM zzw2f_q2 ORDER BY p;

-- begin-expected
-- columns: cnt
-- row: 3
-- end-expected
SELECT count(*)::text AS cnt FROM zzw2f_q1;

DROP TABLE zzw2f_q2;
DROP TABLE zzw2f_q1;

CREATE TABLE zzw2f_p8 (id int PRIMARY KEY);
CREATE TABLE zzw2f_c8 (p int REFERENCES zzw2f_p8(id) ON UPDATE CASCADE, g int GENERATED ALWAYS AS (p*10) STORED);
INSERT INTO zzw2f_p8 VALUES (1);
INSERT INTO zzw2f_c8 (p) VALUES (1);
UPDATE zzw2f_p8 SET id = 3;

-- begin-expected
-- columns: p, g
-- row: 3, 30
-- end-expected
SELECT p::text AS p, g::text AS g FROM zzw2f_c8;

DROP TABLE zzw2f_c8;
DROP TABLE zzw2f_p8;

-- ============================================================================
-- The referencing table's row triggers take part in a referential action
-- ============================================================================

CREATE TABLE zzw2f_lg (seq serial, t text);
CREATE FUNCTION zzw2f_lf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO zzw2f_lg (t) VALUES (TG_WHEN||'/'||TG_OP); IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW; END $$;
CREATE TABLE zzw2f_t (id int PRIMARY KEY, pid int REFERENCES zzw2f_t(id) ON DELETE CASCADE);
INSERT INTO zzw2f_t VALUES (1,NULL),(2,1),(3,2);
CREATE TRIGGER zzw2f_tt AFTER DELETE ON zzw2f_t FOR EACH ROW EXECUTE FUNCTION zzw2f_lf();
DELETE FROM zzw2f_t WHERE id = 1;

-- begin-expected
-- columns: fires
-- row: 3
-- end-expected
SELECT count(*)::text AS fires FROM zzw2f_lg;

DROP TABLE zzw2f_t;
DROP TABLE zzw2f_lg;
DROP FUNCTION zzw2f_lf();

CREATE TABLE zzw2f_lg2 (seq serial, t text);
CREATE FUNCTION zzw2f_lf2() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO zzw2f_lg2 (t) VALUES (TG_WHEN||'/'||TG_OP); IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW; END $$;
CREATE TABLE zzw2f_pa (id int PRIMARY KEY);
CREATE TABLE zzw2f_ch (id int PRIMARY KEY, pid int REFERENCES zzw2f_pa(id) ON DELETE CASCADE);
INSERT INTO zzw2f_pa VALUES (1);
INSERT INTO zzw2f_ch VALUES (10,1);
CREATE TRIGGER zzw2f_tt2 BEFORE DELETE ON zzw2f_ch FOR EACH ROW EXECUTE FUNCTION zzw2f_lf2();
CREATE TRIGGER zzw2f_tt2a AFTER DELETE ON zzw2f_ch FOR EACH ROW EXECUTE FUNCTION zzw2f_lf2();
DELETE FROM zzw2f_pa WHERE id = 1;

-- begin-expected
-- columns: t
-- row: BEFORE/DELETE
-- row: AFTER/DELETE
-- end-expected
SELECT t FROM zzw2f_lg2 ORDER BY seq;

DROP TABLE zzw2f_ch;
DROP TABLE zzw2f_pa;
DROP TABLE zzw2f_lg2;
DROP FUNCTION zzw2f_lf2();

CREATE TABLE zzw2f_nl (seq serial, t text);
CREATE FUNCTION zzw2f_nf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO zzw2f_nl (t) VALUES (TG_WHEN||'/'||TG_OP); RETURN NEW; END $$;
CREATE TABLE zzw2f_pn (id int PRIMARY KEY);
CREATE TABLE zzw2f_cn (id int PRIMARY KEY, pid int REFERENCES zzw2f_pn(id) ON DELETE SET NULL);
INSERT INTO zzw2f_pn VALUES (1);
INSERT INTO zzw2f_cn VALUES (10,1);
CREATE TRIGGER zzw2f_nt AFTER UPDATE ON zzw2f_cn FOR EACH ROW EXECUTE FUNCTION zzw2f_nf();
DELETE FROM zzw2f_pn WHERE id = 1;

-- begin-expected
-- columns: t
-- row: AFTER/UPDATE
-- end-expected
SELECT t FROM zzw2f_nl ORDER BY seq;

DROP TABLE zzw2f_cn;
DROP TABLE zzw2f_pn;
DROP TABLE zzw2f_nl;
DROP FUNCTION zzw2f_nf();

CREATE TABLE zzw2f_sl (seq serial, t text);
CREATE FUNCTION zzw2f_sf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO zzw2f_sl (t) VALUES ('saw'); NEW.note := 'touched'; RETURN NEW; END $$;
CREATE TABLE zzw2f_s1 (id int PRIMARY KEY);
CREATE TABLE zzw2f_s2 (id int PRIMARY KEY, p int REFERENCES zzw2f_s1(id) ON UPDATE CASCADE, note text);
INSERT INTO zzw2f_s1 VALUES (1);
INSERT INTO zzw2f_s2 VALUES (10,1,'orig');
CREATE TRIGGER zzw2f_st BEFORE UPDATE ON zzw2f_s2 FOR EACH ROW EXECUTE FUNCTION zzw2f_sf();
UPDATE zzw2f_s1 SET id = 7;

-- begin-expected
-- columns: p, note
-- row: 7, touched
-- end-expected
SELECT p::text AS p, note FROM zzw2f_s2;

DROP TABLE zzw2f_s2;
DROP TABLE zzw2f_s1;
DROP TABLE zzw2f_sl;
DROP FUNCTION zzw2f_sf();

CREATE FUNCTION zzw2f_skip() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$;
CREATE TABLE zzw2f_bp (id int PRIMARY KEY);
CREATE TABLE zzw2f_bd (id int PRIMARY KEY, p int REFERENCES zzw2f_bp(id) ON DELETE CASCADE);
INSERT INTO zzw2f_bp VALUES (1);
INSERT INTO zzw2f_bd VALUES (10,1);
CREATE TRIGGER zzw2f_bt BEFORE DELETE ON zzw2f_bd FOR EACH ROW EXECUTE FUNCTION zzw2f_skip();
DELETE FROM zzw2f_bp WHERE id = 1;

-- begin-expected
-- columns: parents, children
-- row: 0, 1
-- end-expected
SELECT (SELECT count(*)::text FROM zzw2f_bp) AS parents, (SELECT count(*)::text FROM zzw2f_bd) AS children;

DROP TABLE zzw2f_bd;
DROP TABLE zzw2f_bp;
DROP FUNCTION zzw2f_skip();