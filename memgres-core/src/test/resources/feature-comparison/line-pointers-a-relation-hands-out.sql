-- ============================================================================
-- The line pointers a relation hands out
--
-- A tuple is numbered by the place it takes in the file of the relation that
-- stores it. That file belongs to the relation and not to the name over it: a
-- rename or a move to another schema carries the numbering along, a TRUNCATE
-- hands the relation a new file, and a relation created after another was
-- dropped under the same name begins at one again.
--
-- The place is taken when the row is written, which is before the relation's
-- indexes are maintained and before a referenced row is looked for. So a
-- duplicate key, a missing parent, an exclusion conflict or an AFTER trigger
-- that raises all leave the place spent, while a NOT NULL, a CHECK, a
-- partition bound, a generation expression and a BEFORE trigger -- all settled
-- before the row reaches the relation -- cost nothing.
--
-- Every value below was measured against PostgreSQL 18.
-- ============================================================================

-- ============================================================================
-- A relation created under a name that has been used before begins at one
-- ============================================================================
CREATE TABLE lph_a (i int);
INSERT INTO lph_a VALUES (1),(2);

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 1
-- row: (0,2) | 2
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_a ORDER BY i;

DROP TABLE lph_a;
CREATE TABLE lph_a (i int);
INSERT INTO lph_a VALUES (1),(2);

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 1
-- row: (0,2) | 2
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_a ORDER BY i;

-- the same inside one transaction: the name reaches a relation of its own
-- afterwards, with a file of its own
BEGIN;
DROP TABLE lph_a;
CREATE TABLE lph_a (i int);
INSERT INTO lph_a VALUES (7);
COMMIT;

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 7
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_a ORDER BY i;

-- a DROP that is rolled back leaves the relation, and its numbering, alone
BEGIN;
DROP TABLE lph_a;
ROLLBACK;
INSERT INTO lph_a VALUES (8);

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 7
-- row: (0,2) | 8
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_a ORDER BY i;

DROP TABLE lph_a;

-- a drop AND a re-create rolled back together leave the first relation, and
-- its numbering, exactly where they were
CREATE TABLE lph_b (i int);
INSERT INTO lph_b VALUES (1),(2);
BEGIN;
DROP TABLE lph_b;
CREATE TABLE lph_b (i int);
INSERT INTO lph_b VALUES (9);

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 9
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_b ORDER BY i;

ROLLBACK;
INSERT INTO lph_b VALUES (3);

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 1
-- row: (0,2) | 2
-- row: (0,3) | 3
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_b ORDER BY i;

DROP TABLE lph_b;

-- a partitioned relation re-created under the same name begins at one in each
-- of its partitions
CREATE TABLE lph_p (i int) PARTITION BY RANGE (i);
CREATE TABLE lph_p0 PARTITION OF lph_p FOR VALUES FROM (0) TO (100);
INSERT INTO lph_p VALUES (1),(2);
DROP TABLE lph_p;
CREATE TABLE lph_p (i int) PARTITION BY RANGE (i);
CREATE TABLE lph_p0 PARTITION OF lph_p FOR VALUES FROM (0) TO (100);
INSERT INTO lph_p VALUES (3),(4);

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 3
-- row: (0,2) | 4
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_p ORDER BY i;

DROP TABLE lph_p;

-- ============================================================================
-- The numbering belongs to the relation, not to the name over it
-- ============================================================================
CREATE TABLE lph_c (i int);
INSERT INTO lph_c VALUES (1),(2);
ALTER TABLE lph_c RENAME TO lph_c2;
INSERT INTO lph_c2 VALUES (3);

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 1
-- row: (0,2) | 2
-- row: (0,3) | 3
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_c2 ORDER BY i;

CREATE SCHEMA lph_s;
ALTER TABLE lph_c2 SET SCHEMA lph_s;
INSERT INTO lph_s.lph_c2 VALUES (4);

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 1
-- row: (0,2) | 2
-- row: (0,3) | 3
-- row: (0,4) | 4
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_s.lph_c2 ORDER BY i;

DROP TABLE lph_s.lph_c2;

-- two relations of one name in two schemas number their tuples apart, and
-- dropping one leaves the other where it stood
CREATE TABLE lph_d (i int);
CREATE TABLE lph_s.lph_d (i int);
INSERT INTO lph_d VALUES (1),(2),(3);
INSERT INTO lph_s.lph_d VALUES (1);
DROP TABLE lph_d;
INSERT INTO lph_s.lph_d VALUES (2);

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 1
-- row: (0,2) | 2
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_s.lph_d ORDER BY i;

CREATE TABLE lph_d (i int);
INSERT INTO lph_d VALUES (7);

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 7
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_d ORDER BY i;

DROP TABLE lph_d;
DROP SCHEMA lph_s CASCADE;

-- ============================================================================
-- TRUNCATE hands the relation a new file; a rolled-back TRUNCATE and a DELETE
-- of every row do not
-- ============================================================================
CREATE TABLE lph_e (i int);
INSERT INTO lph_e VALUES (1),(2);
TRUNCATE lph_e;
INSERT INTO lph_e VALUES (3),(4);

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 3
-- row: (0,2) | 4
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_e ORDER BY i;

BEGIN;
TRUNCATE lph_e;
ROLLBACK;
INSERT INTO lph_e VALUES (5);

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 3
-- row: (0,2) | 4
-- row: (0,3) | 5
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_e ORDER BY i;

DELETE FROM lph_e;
INSERT INTO lph_e VALUES (6);

-- begin-expected
-- columns: ctid | i
-- row: (0,4) | 6
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_e ORDER BY i;

DROP TABLE lph_e;

-- ============================================================================
-- A write refused after its row was written has already spent the place
-- ============================================================================
CREATE TABLE lph_f (i int PRIMARY KEY);
INSERT INTO lph_f VALUES (1);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint
-- end-expected-error
INSERT INTO lph_f VALUES (1);

INSERT INTO lph_f VALUES (2);

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 1
-- row: (0,3) | 2
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_f ORDER BY i;

-- every row the statement had already written took a place as well
-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint
-- end-expected-error
INSERT INTO lph_f VALUES (5),(1),(6);

INSERT INTO lph_f VALUES (3);

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 1
-- row: (0,3) | 2
-- row: (0,6) | 3
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_f ORDER BY i;

DROP TABLE lph_f;

-- an INSERT ... SELECT is the same write in another spelling
CREATE TABLE lph_g (i int PRIMARY KEY);
CREATE TABLE lph_gs (i int);
INSERT INTO lph_g VALUES (1);
INSERT INTO lph_gs VALUES (5),(1),(6);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint
-- end-expected-error
INSERT INTO lph_g SELECT i FROM lph_gs ORDER BY i;

INSERT INTO lph_g VALUES (9);

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 1
-- row: (0,3) | 9
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_g ORDER BY i;

DROP TABLE lph_g;
DROP TABLE lph_gs;

-- a missing parent row is looked for after the write too
CREATE TABLE lph_hp (i int PRIMARY KEY);
INSERT INTO lph_hp VALUES (1);
CREATE TABLE lph_h (i int REFERENCES lph_hp(i));
INSERT INTO lph_h VALUES (1);

-- begin-expected-error
-- sqlstate: 23503
-- message-like: violates foreign key constraint
-- end-expected-error
INSERT INTO lph_h VALUES (9);

INSERT INTO lph_h VALUES (1);

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 1
-- row: (0,3) | 1
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_h ORDER BY ctid;

DROP TABLE lph_h;
DROP TABLE lph_hp;

-- a reference left to the end of the transaction spends the place just the
-- same: the row was written when the statement ran
CREATE TABLE lph_jp (i int PRIMARY KEY);
INSERT INTO lph_jp VALUES (1);
CREATE TABLE lph_j (i int REFERENCES lph_jp(i) DEFERRABLE INITIALLY DEFERRED);
INSERT INTO lph_j VALUES (1);
BEGIN;
INSERT INTO lph_j VALUES (9);

-- begin-expected-error
-- sqlstate: 23503
-- message-like: violates foreign key constraint
-- end-expected-error
COMMIT;

INSERT INTO lph_j VALUES (1);

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 1
-- row: (0,3) | 1
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_j ORDER BY ctid;

DROP TABLE lph_j;
DROP TABLE lph_jp;

-- a partition carries a copy of the index the partitioned table declares, and
-- refuses the row where that copy already holds the key
CREATE TABLE lph_k (i int PRIMARY KEY) PARTITION BY RANGE (i);
CREATE TABLE lph_k0 PARTITION OF lph_k FOR VALUES FROM (0) TO (100);
INSERT INTO lph_k VALUES (1);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint
-- end-expected-error
INSERT INTO lph_k VALUES (1);

INSERT INTO lph_k VALUES (2);

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 1
-- row: (0,3) | 2
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_k ORDER BY i;

DROP TABLE lph_k;

-- a MERGE that inserts is the same write again
CREATE TABLE lph_m (i int PRIMARY KEY, s text);
CREATE TABLE lph_ms (i int, s text);
INSERT INTO lph_m VALUES (1,'a');
INSERT INTO lph_ms VALUES (1,'b');

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint
-- end-expected-error
MERGE INTO lph_m t USING lph_ms u ON t.i = u.i + 100
  WHEN NOT MATCHED THEN INSERT VALUES (u.i, u.s);

INSERT INTO lph_m VALUES (2,'c');

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 1
-- row: (0,3) | 2
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_m ORDER BY i;

DROP TABLE lph_m;
DROP TABLE lph_ms;

-- an exclusion constraint is read off an index too
CREATE TABLE lph_n (i int, EXCLUDE (i WITH =));
INSERT INTO lph_n VALUES (1);

-- begin-expected-error
-- sqlstate: 23P01
-- message-like: conflicting key value violates exclusion constraint
-- end-expected-error
INSERT INTO lph_n VALUES (1);

INSERT INTO lph_n VALUES (2);

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 1
-- row: (0,3) | 2
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_n ORDER BY i;

DROP TABLE lph_n;

-- an AFTER trigger runs once the row is in the relation, so its refusal costs
-- the place; a BEFORE trigger runs first and costs nothing
CREATE FUNCTION lph_raise() RETURNS trigger AS $$ BEGIN IF NEW.i = 99 THEN RAISE EXCEPTION 'refused by the trigger'; END IF; RETURN NEW; END $$ LANGUAGE plpgsql;

CREATE TABLE lph_q (i int);
CREATE TRIGGER lph_q_after AFTER INSERT ON lph_q
  FOR EACH ROW EXECUTE FUNCTION lph_raise();
INSERT INTO lph_q VALUES (1);

-- begin-expected-error
-- sqlstate: P0001
-- message-like: refused by the trigger
-- end-expected-error
INSERT INTO lph_q VALUES (99);

INSERT INTO lph_q VALUES (2);

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 1
-- row: (0,3) | 2
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_q ORDER BY i;

DROP TABLE lph_q;

CREATE TABLE lph_r (i int);
CREATE TRIGGER lph_r_before BEFORE INSERT ON lph_r
  FOR EACH ROW EXECUTE FUNCTION lph_raise();
INSERT INTO lph_r VALUES (1);

-- begin-expected-error
-- sqlstate: P0001
-- message-like: refused by the trigger
-- end-expected-error
INSERT INTO lph_r VALUES (99);

INSERT INTO lph_r VALUES (2);

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 1
-- row: (0,2) | 2
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_r ORDER BY i;

DROP TABLE lph_r;
DROP FUNCTION lph_raise();

-- ============================================================================
-- A write refused before its row reached the relation costs nothing
-- ============================================================================
CREATE TABLE lph_t (i int NOT NULL CHECK (i > 0));
INSERT INTO lph_t VALUES (1);

-- begin-expected-error
-- sqlstate: 23502
-- message-like: violates not-null constraint
-- end-expected-error
INSERT INTO lph_t VALUES (NULL);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: violates check constraint
-- end-expected-error
INSERT INTO lph_t VALUES (-1);

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer
-- end-expected-error
INSERT INTO lph_t VALUES ('x');

INSERT INTO lph_t VALUES (2);

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 1
-- row: (0,2) | 2
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_t ORDER BY i;

DROP TABLE lph_t;

-- a row that belongs in no partition never reached a relation either
CREATE TABLE lph_u (i int) PARTITION BY RANGE (i);
CREATE TABLE lph_u0 PARTITION OF lph_u FOR VALUES FROM (0) TO (10);
INSERT INTO lph_u VALUES (1);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: no partition of relation
-- end-expected-error
INSERT INTO lph_u VALUES (50);

INSERT INTO lph_u VALUES (2);

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 1
-- row: (0,2) | 2
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_u ORDER BY i;

DROP TABLE lph_u;

-- a generation expression is worked out before the row is written
CREATE TABLE lph_v (i int, j int GENERATED ALWAYS AS (10 / i) STORED);
INSERT INTO lph_v VALUES (1);

-- begin-expected-error
-- sqlstate: 22012
-- message-like: division by zero
-- end-expected-error
INSERT INTO lph_v VALUES (0);

INSERT INTO lph_v VALUES (2);

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 1
-- row: (0,2) | 2
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_v ORDER BY i;

DROP TABLE lph_v;

-- ============================================================================
-- What the transaction becomes does not give the place back
-- ============================================================================
CREATE TABLE lph_w (i int PRIMARY KEY);
INSERT INTO lph_w VALUES (1);
BEGIN;
SAVEPOINT sp;

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint
-- end-expected-error
INSERT INTO lph_w VALUES (1);

ROLLBACK TO SAVEPOINT sp;
INSERT INTO lph_w VALUES (2);
COMMIT;

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 1
-- row: (0,3) | 2
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_w ORDER BY i;

DROP TABLE lph_w;

-- a row a whole transaction takes back leaves its place spent as well
CREATE TABLE lph_x (i int PRIMARY KEY);
INSERT INTO lph_x VALUES (1);
BEGIN;
INSERT INTO lph_x VALUES (2);
ROLLBACK;
INSERT INTO lph_x VALUES (3);

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 1
-- row: (0,3) | 3
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_x ORDER BY i;

DROP TABLE lph_x;

-- ============================================================================
-- An UPDATE writes its new version somewhere else, and a refused one has
-- already spent the place it wrote it to
-- ============================================================================
CREATE TABLE lph_y (i int UNIQUE);
INSERT INTO lph_y VALUES (1),(2);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint
-- end-expected-error
UPDATE lph_y SET i = 1 WHERE i = 2;

INSERT INTO lph_y VALUES (3);

-- begin-expected
-- columns: ctid | i
-- row: (0,1) | 1
-- row: (0,2) | 2
-- row: (0,4) | 3
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_y ORDER BY i;

UPDATE lph_y SET i = i * 10 WHERE i = 1;
INSERT INTO lph_y VALUES (4);

-- begin-expected
-- columns: ctid | i
-- row: (0,2) | 2
-- row: (0,4) | 3
-- row: (0,6) | 4
-- row: (0,5) | 10
-- end-expected
SELECT ctid::text AS ctid, i FROM lph_y ORDER BY i;

DROP TABLE lph_y;

-- ============================================================================
-- ON CONFLICT: the row DO NOTHING passed over costs nothing, and the one
-- DO UPDATE rewrote lives somewhere else afterwards
-- ============================================================================
CREATE TABLE lph_z (i int PRIMARY KEY, s text);
INSERT INTO lph_z VALUES (1,'a');
INSERT INTO lph_z VALUES (1,'b') ON CONFLICT DO NOTHING;
INSERT INTO lph_z VALUES (2,'c');

-- begin-expected
-- columns: ctid | i | s
-- row: (0,1) | 1 | a
-- row: (0,2) | 2 | c
-- end-expected
SELECT ctid::text AS ctid, i, s FROM lph_z ORDER BY i;

INSERT INTO lph_z VALUES (1,'d') ON CONFLICT (i) DO UPDATE SET s = 'e';
INSERT INTO lph_z VALUES (3,'f');

-- begin-expected
-- columns: ctid | i | s
-- row: (0,3) | 1 | e
-- row: (0,2) | 2 | c
-- row: (0,4) | 3 | f
-- end-expected
SELECT ctid::text AS ctid, i, s FROM lph_z ORDER BY i;

DROP TABLE lph_z;
