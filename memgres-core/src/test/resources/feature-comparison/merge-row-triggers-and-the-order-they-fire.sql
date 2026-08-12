-- What a MERGE's arms owe the rows they write, and when the triggers they owe run.
--
-- A MERGE is one statement, and PostgreSQL treats it as one: every BEFORE row trigger it
-- owes runs while the statement is writing, and every AFTER row trigger it owes is held
-- back until the statement has finished writing, so an AFTER trigger reads the relation
-- the whole statement left behind rather than a half-written one. The arm that deletes a
-- row fires that relation's DELETE row triggers, exactly as a DELETE does, and a BEFORE
-- DELETE that answers with nothing keeps the row and owes no AFTER half. An arm that
-- moves a row into another partition is the source partition's delete and the destination
-- partition's insert, which is what PostgreSQL performs a move as; neither partition sees
-- an AFTER UPDATE for it. And an UPDATE OF trigger is owed the rows of every arm of the
-- statement that assigns its column, not only the arm the row took.
--
-- Every value below was measured against PostgreSQL 18.

-- ============================================================================
-- The arm that deletes a row fires the DELETE row triggers of the relation
-- ============================================================================

CREATE TABLE mtf_log (n serial, m text);
CREATE TABLE mtf_d (i int PRIMARY KEY, v int);
CREATE TABLE mtf_ds (i int, v int);
CREATE FUNCTION mtf_note() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF TG_WHEN = 'BEFORE' THEN INSERT INTO mtf_log (m) VALUES ('BEFORE ' || TG_OP || ' ' || TG_TABLE_NAME || ' ' || OLD.i); RETURN OLD; END IF; INSERT INTO mtf_log (m) VALUES ('AFTER ' || TG_OP || ' ' || TG_TABLE_NAME || ' ' || OLD.i || ' left=' || (SELECT count(*) FROM mtf_d)); RETURN NULL; END $$;
CREATE FUNCTION mtf_keep() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO mtf_log (m) VALUES ('kept ' || OLD.i); RETURN NULL; END $$;
INSERT INTO mtf_d VALUES (1,10),(2,20),(3,30);
INSERT INTO mtf_ds VALUES (1,100),(2,200);
CREATE TRIGGER mtf_bd BEFORE DELETE ON mtf_d FOR EACH ROW EXECUTE FUNCTION mtf_note();
CREATE TRIGGER mtf_ad AFTER DELETE ON mtf_d FOR EACH ROW EXECUTE FUNCTION mtf_note();

MERGE INTO mtf_d t USING mtf_ds u ON t.i = u.i WHEN MATCHED THEN DELETE;

-- Both BEFORE halves run while the statement writes, both AFTER halves once it has, and
-- both AFTER halves read the one row the whole statement left behind.
-- begin-expected
-- columns: m
-- row: BEFORE DELETE mtf_d 1
-- row: BEFORE DELETE mtf_d 2
-- row: AFTER DELETE mtf_d 1 left=1
-- row: AFTER DELETE mtf_d 2 left=1
-- end-expected
SELECT m FROM mtf_log ORDER BY n;

-- begin-expected
-- columns: i | v
-- row: 3, 30
-- end-expected
SELECT i, v FROM mtf_d ORDER BY i;

DELETE FROM mtf_log;
INSERT INTO mtf_d VALUES (1,10),(2,20);

MERGE INTO mtf_d t USING mtf_ds u ON t.i = u.i WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- begin-expected
-- columns: m
-- row: BEFORE DELETE mtf_d 3
-- row: AFTER DELETE mtf_d 3 left=2
-- end-expected
SELECT m FROM mtf_log ORDER BY n;

-- begin-expected
-- columns: i | v
-- row: 1, 10
-- row: 2, 20
-- end-expected
SELECT i, v FROM mtf_d ORDER BY i;

DELETE FROM mtf_log;
DROP TRIGGER mtf_bd ON mtf_d;
CREATE TRIGGER mtf_bd BEFORE DELETE ON mtf_d FOR EACH ROW EXECUTE FUNCTION mtf_keep();

MERGE INTO mtf_d t USING mtf_ds u ON t.i = u.i WHEN MATCHED THEN DELETE;

-- The arm counts nothing for a row the BEFORE half kept, and no AFTER half is owed.
-- begin-expected
-- columns: m
-- row: kept 1
-- row: kept 2
-- end-expected
SELECT m FROM mtf_log ORDER BY n;

-- begin-expected
-- columns: i | v
-- row: 1, 10
-- row: 2, 20
-- end-expected
SELECT i, v FROM mtf_d ORDER BY i;

DELETE FROM mtf_log;
DROP TRIGGER mtf_bd ON mtf_d;
CREATE TRIGGER mtf_bd BEFORE DELETE ON mtf_d FOR EACH ROW EXECUTE FUNCTION mtf_note();
DELETE FROM mtf_d;
DELETE FROM mtf_log;
INSERT INTO mtf_d VALUES (1,10),(2,20),(3,30);

-- The same sequence an ordinary DELETE over two of three rows fires: every BEFORE half,
-- then every AFTER half, each of them reading the one row the statement left behind. Which
-- of the two rows a scan reaches first is the plan's business, so the two halves are asked
-- for here as a group rather than in a fixed order.
DELETE FROM mtf_d WHERE i < 3;

-- begin-expected
-- columns: halves | queued
-- row: 4, t
-- end-expected
SELECT count(*) AS halves,
       max(n) FILTER (WHERE m LIKE 'BEFORE%') < min(n) FILTER (WHERE m LIKE 'AFTER%') AS queued
  FROM mtf_log;

-- begin-expected
-- columns: m | c
-- row: AFTER DELETE mtf_d 1 left=1, 1
-- row: AFTER DELETE mtf_d 2 left=1, 1
-- row: BEFORE DELETE mtf_d 1, 1
-- row: BEFORE DELETE mtf_d 2, 1
-- end-expected
SELECT m, count(*) AS c FROM mtf_log GROUP BY m ORDER BY m;

DROP TABLE mtf_d CASCADE;
DROP TABLE mtf_ds;
DROP TABLE mtf_log;
DROP FUNCTION mtf_note();
DROP FUNCTION mtf_keep();


-- ============================================================================
-- A MERGE runs its AFTER row triggers once it has finished writing
-- ============================================================================

CREATE TABLE mtf_l2 (n serial, m text);
CREATE FUNCTION mtf_n2() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO mtf_l2 (m) VALUES (TG_WHEN || ' ' || TG_OP || ' ' || coalesce(OLD.i::text,'-') || '/' || coalesce(NEW.i::text,'-')); IF TG_WHEN = 'BEFORE' AND TG_OP = 'DELETE' THEN RETURN OLD; END IF; IF TG_WHEN = 'BEFORE' THEN RETURN NEW; END IF; RETURN NULL; END $$;
CREATE TABLE mtf_q (i int PRIMARY KEY, v int);
INSERT INTO mtf_q VALUES (1,10),(2,20),(3,30);
CREATE TRIGGER mtf_q1 BEFORE INSERT OR UPDATE OR DELETE ON mtf_q FOR EACH ROW EXECUTE FUNCTION mtf_n2();
CREATE TRIGGER mtf_q2 AFTER INSERT OR UPDATE OR DELETE ON mtf_q FOR EACH ROW EXECUTE FUNCTION mtf_n2();

MERGE INTO mtf_q t USING (VALUES (1,11),(2,22)) u(i,v) ON t.i = u.i WHEN MATCHED THEN UPDATE SET v = u.v;

-- Not BEFORE 1, AFTER 1, BEFORE 2, AFTER 2: both BEFORE halves stand ahead of both AFTER
-- halves, because the statement writes every row it is going to write first.
-- begin-expected
-- columns: m
-- row: BEFORE UPDATE 1/1
-- row: BEFORE UPDATE 2/2
-- row: AFTER UPDATE 1/1
-- row: AFTER UPDATE 2/2
-- end-expected
SELECT m FROM mtf_l2 ORDER BY n;

DELETE FROM mtf_l2;

-- Which is the sequence an ordinary UPDATE over the same two rows fires. Which row a scan
-- reaches first is the plan's business, so what is asked here is the group and the rule.
UPDATE mtf_q SET v = v + 1 WHERE i IN (1,2);

-- begin-expected
-- columns: halves | queued
-- row: 4, t
-- end-expected
SELECT count(*) AS halves,
       max(n) FILTER (WHERE m LIKE 'BEFORE%') < min(n) FILTER (WHERE m LIKE 'AFTER%') AS queued
  FROM mtf_l2;

-- begin-expected
-- columns: m | c
-- row: AFTER UPDATE 1/1, 1
-- row: AFTER UPDATE 2/2, 1
-- row: BEFORE UPDATE 1/1, 1
-- row: BEFORE UPDATE 2/2, 1
-- end-expected
SELECT m, count(*) AS c FROM mtf_l2 GROUP BY m ORDER BY m;

DELETE FROM mtf_l2;

MERGE INTO mtf_q t USING (VALUES (1,0),(4,44)) u(i,v) ON t.i = u.i
  WHEN MATCHED THEN DELETE
  WHEN NOT MATCHED THEN INSERT (i,v) VALUES (u.i,u.v)
  WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = v + 1;

-- Which arm reaches which row first is the plan's business; that no AFTER half stands
-- before the last BEFORE one is the rule, and it holds across all three arms at once.
-- begin-expected
-- columns: kinds | queued
-- row: 8, t
-- end-expected
SELECT count(*) AS kinds,
       max(n) FILTER (WHERE m LIKE 'BEFORE%') < min(n) FILTER (WHERE m LIKE 'AFTER%') AS queued
  FROM mtf_l2;

-- begin-expected
-- columns: m | c
-- row: AFTER DELETE 1/-, 1
-- row: AFTER INSERT -/4, 1
-- row: AFTER UPDATE 2/2, 1
-- row: AFTER UPDATE 3/3, 1
-- row: BEFORE DELETE 1/-, 1
-- row: BEFORE INSERT -/4, 1
-- row: BEFORE UPDATE 2/2, 1
-- row: BEFORE UPDATE 3/3, 1
-- end-expected
SELECT m, count(*) AS c FROM mtf_l2 GROUP BY m ORDER BY m;

-- begin-expected
-- columns: i | v
-- row: 2, 24
-- row: 3, 31
-- row: 4, 44
-- end-expected
SELECT i, v FROM mtf_q ORDER BY i;

DELETE FROM mtf_l2;
DROP TRIGGER mtf_q1 ON mtf_q;

-- With no BEFORE half to run at all, the AFTER halves are still the statement's own.
MERGE INTO mtf_q t USING (VALUES (2),(3)) u(i) ON t.i = u.i WHEN MATCHED THEN DELETE;

-- begin-expected
-- columns: m
-- row: AFTER DELETE 2/-
-- row: AFTER DELETE 3/-
-- end-expected
SELECT m FROM mtf_l2 ORDER BY n;

DROP TABLE mtf_q CASCADE;
DROP TABLE mtf_l2;
DROP FUNCTION mtf_n2();


-- ============================================================================
-- An AFTER row trigger of a MERGE reads the relation the whole statement left
-- ============================================================================

CREATE TABLE mtf_l5 (n serial, m text);
CREATE TABLE mtf_a (i int PRIMARY KEY, v int);
CREATE FUNCTION mtf_n5() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO mtf_l5 (m) VALUES (TG_OP || ' left=' || (SELECT count(*) FROM mtf_a) || ' max=' || coalesce((SELECT max(v)::text FROM mtf_a),'-')); RETURN NULL; END $$;
INSERT INTO mtf_a VALUES (1,10),(2,20),(3,30);
CREATE TRIGGER mtf_a1 AFTER DELETE ON mtf_a FOR EACH ROW EXECUTE FUNCTION mtf_n5();
CREATE TRIGGER mtf_a2 AFTER UPDATE ON mtf_a FOR EACH ROW EXECUTE FUNCTION mtf_n5();

MERGE INTO mtf_a t USING (VALUES (1),(2)) u(i) ON t.i = u.i WHEN MATCHED THEN DELETE
  WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = 99;

-- Every one of the three sees one row left and the update's own value already stored:
-- none of them was run before the statement had finished writing.
-- begin-expected
-- columns: m
-- row: DELETE left=1 max=99
-- row: DELETE left=1 max=99
-- row: UPDATE left=1 max=99
-- end-expected
SELECT m FROM mtf_l5 ORDER BY n;

-- begin-expected
-- columns: i | v
-- row: 3, 99
-- end-expected
SELECT i, v FROM mtf_a ORDER BY i;

DROP TABLE mtf_a CASCADE;
DROP TABLE mtf_l5;
DROP FUNCTION mtf_n5();


-- ============================================================================
-- A row a MERGE moves into another partition leaves one and arrives in the other
-- ============================================================================

CREATE TABLE mtf_l3 (n serial, m text);
CREATE FUNCTION mtf_n3() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO mtf_l3 (m) VALUES (TG_WHEN || ' ' || TG_OP || ' ' || TG_TABLE_NAME || ' ' || coalesce(OLD.i::text,'-') || '/' || coalesce(NEW.i::text,'-')); IF TG_WHEN = 'BEFORE' AND TG_OP = 'DELETE' THEN RETURN OLD; END IF; IF TG_WHEN = 'BEFORE' THEN RETURN NEW; END IF; RETURN NULL; END $$;
CREATE TABLE mtf_p (i int PRIMARY KEY, s text) PARTITION BY RANGE (i);
CREATE TABLE mtf_p1 PARTITION OF mtf_p FOR VALUES FROM (0) TO (10);
CREATE TABLE mtf_p2 PARTITION OF mtf_p FOR VALUES FROM (10) TO (20);
INSERT INTO mtf_p VALUES (1,'a'),(2,'b');
CREATE TABLE mtf_ps (i int, j int, s text);
INSERT INTO mtf_ps VALUES (2,12,'moved');
CREATE TRIGGER mtf_t1 BEFORE INSERT OR UPDATE OR DELETE ON mtf_p1 FOR EACH ROW EXECUTE FUNCTION mtf_n3();
CREATE TRIGGER mtf_t2 AFTER INSERT OR UPDATE OR DELETE ON mtf_p1 FOR EACH ROW EXECUTE FUNCTION mtf_n3();
CREATE TRIGGER mtf_t3 BEFORE INSERT OR UPDATE OR DELETE ON mtf_p2 FOR EACH ROW EXECUTE FUNCTION mtf_n3();
CREATE TRIGGER mtf_t4 AFTER INSERT OR UPDATE OR DELETE ON mtf_p2 FOR EACH ROW EXECUTE FUNCTION mtf_n3();

MERGE INTO mtf_p t USING mtf_ps u ON t.i = u.i WHEN MATCHED THEN UPDATE SET i = u.j, s = u.s;

-- The partition the row left is asked first, as an update and then as a delete; the one it
-- reached is asked for an insert. Neither is given an AFTER UPDATE, because a move is not
-- an update on either of them.
-- begin-expected
-- columns: m
-- row: BEFORE UPDATE mtf_p1 2/12
-- row: BEFORE DELETE mtf_p1 2/-
-- row: BEFORE INSERT mtf_p2 -/12
-- row: AFTER DELETE mtf_p1 2/-
-- row: AFTER INSERT mtf_p2 -/12
-- end-expected
SELECT m FROM mtf_l3 ORDER BY n;

-- begin-expected
-- columns: c1 | c2
-- row: 1, 1
-- end-expected
SELECT (SELECT count(*) FROM mtf_p1) AS c1, (SELECT count(*) FROM mtf_p2) AS c2;

-- begin-expected
-- columns: i | s
-- row: 1, a
-- row: 12, moved
-- end-expected
SELECT i, s FROM mtf_p ORDER BY i;

DELETE FROM mtf_l3;

-- A row that stays where it is is an update, and only its own partition hears of it.
MERGE INTO mtf_p t USING (VALUES (12,15)) u(i,j) ON t.i = u.i WHEN MATCHED THEN UPDATE SET i = u.j;

-- begin-expected
-- columns: m
-- row: BEFORE UPDATE mtf_p2 12/15
-- row: AFTER UPDATE mtf_p2 12/15
-- end-expected
SELECT m FROM mtf_l3 ORDER BY n;

DELETE FROM mtf_l3;

-- Which is the sequence an ordinary UPDATE that moves a row the other way fires.
UPDATE mtf_p SET i = 5 WHERE i = 15;

-- begin-expected
-- columns: m
-- row: BEFORE UPDATE mtf_p2 15/5
-- row: BEFORE DELETE mtf_p2 15/-
-- row: BEFORE INSERT mtf_p1 -/5
-- row: AFTER DELETE mtf_p2 15/-
-- row: AFTER INSERT mtf_p1 -/5
-- end-expected
SELECT m FROM mtf_l3 ORDER BY n;

DELETE FROM mtf_l3;

-- The delete arm through the parent fires the storing partition's DELETE triggers.
MERGE INTO mtf_p t USING (VALUES (5)) u(i) ON t.i = u.i WHEN MATCHED THEN DELETE;

-- begin-expected
-- columns: m
-- row: BEFORE DELETE mtf_p1 5/-
-- row: AFTER DELETE mtf_p1 5/-
-- end-expected
SELECT m FROM mtf_l3 ORDER BY n;

-- begin-expected
-- columns: i | s
-- row: 1, a
-- end-expected
SELECT i, s FROM mtf_p ORDER BY i;

DROP TABLE mtf_p CASCADE;
DROP TABLE mtf_ps;
DROP TABLE mtf_l3;
DROP FUNCTION mtf_n3();


-- ============================================================================
-- An UPDATE OF trigger fires for what the whole MERGE assigns
-- ============================================================================

CREATE TABLE mtf_l4 (n serial, m text);
CREATE FUNCTION mtf_n4() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO mtf_l4 (m) VALUES (TG_NAME || ' ' || OLD.i); RETURN NEW; END $$;
CREATE TABLE mtf_u (i int PRIMARY KEY, v int, w int);
CREATE TABLE mtf_us (i int, v int);
INSERT INTO mtf_u VALUES (1,10,100),(2,20,200),(3,30,300);
INSERT INTO mtf_us VALUES (1,11),(2,22);
CREATE TRIGGER mtf_ov BEFORE UPDATE OF v ON mtf_u FOR EACH ROW EXECUTE FUNCTION mtf_n4();
CREATE TRIGGER mtf_ow BEFORE UPDATE OF w ON mtf_u FOR EACH ROW EXECUTE FUNCTION mtf_n4();

MERGE INTO mtf_u t USING mtf_us u ON t.i = u.i WHEN MATCHED THEN UPDATE SET w = u.v;

-- begin-expected
-- columns: m | c
-- row: mtf_ow 1, 1
-- row: mtf_ow 2, 1
-- end-expected
SELECT m, count(*) AS c FROM mtf_l4 GROUP BY m ORDER BY m;

DELETE FROM mtf_l4;

MERGE INTO mtf_u t USING mtf_us u ON t.i = u.i
  WHEN MATCHED THEN UPDATE SET w = u.v
  WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = 0;

-- The columns a statement assigns are the columns all of its arms assign, so the trigger
-- written for v is owed the rows the arm that assigns w wrote as well, and the other way.
-- begin-expected
-- columns: m | c
-- row: mtf_ov 1, 1
-- row: mtf_ov 2, 1
-- row: mtf_ov 3, 1
-- row: mtf_ow 1, 1
-- row: mtf_ow 2, 1
-- row: mtf_ow 3, 1
-- end-expected
SELECT m, count(*) AS c FROM mtf_l4 GROUP BY m ORDER BY m;

DELETE FROM mtf_l4;

-- An ordinary UPDATE is unchanged: it assigns w and only that trigger hears of it.
UPDATE mtf_u SET w = 5 WHERE i = 1;

-- begin-expected
-- columns: m
-- row: mtf_ow 1
-- end-expected
SELECT m FROM mtf_l4 ORDER BY n;

DROP TABLE mtf_u CASCADE;
DROP TABLE mtf_us;
DROP TABLE mtf_l4;
DROP FUNCTION mtf_n4();
