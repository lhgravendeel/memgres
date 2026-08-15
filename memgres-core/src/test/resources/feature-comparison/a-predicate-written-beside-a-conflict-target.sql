-- ============================================================================
-- A predicate written beside a conflict target names an index, it does not
-- describe one. PostgreSQL asks that the index's own predicate be implied by
-- what was written: an index with no predicate of its own is reached by any
-- predicate at all, and a partial index only by a predicate that entails its
-- own. Which rows then collide is the arbiter index's predicate; the one
-- written says nothing about what the index holds.
-- ============================================================================

-- ============================================================================
-- An index that is not partial has no predicate to imply
-- ============================================================================
CREATE TABLE arbp_a (i int PRIMARY KEY, k int);
INSERT INTO arbp_a VALUES (1,1);

-- any predicate at all reaches the primary key's index
INSERT INTO arbp_a VALUES (1,2) ON CONFLICT (i) WHERE i > 0 DO NOTHING;
INSERT INTO arbp_a VALUES (1,3) ON CONFLICT (i) WHERE k > 0 DO NOTHING;
INSERT INTO arbp_a VALUES (1,4) ON CONFLICT (i) WHERE false DO NOTHING;

-- begin-expected
-- columns: i | k
-- row: 1 | 1
-- end-expected
SELECT i, k FROM arbp_a ORDER BY i;

-- and the predicate decides nothing about which rows collide
INSERT INTO arbp_a VALUES (1,5) ON CONFLICT (i) WHERE i > 0 DO UPDATE SET k = 9;

-- begin-expected
-- columns: i | k
-- row: 1 | 9
-- end-expected
SELECT i, k FROM arbp_a ORDER BY i;

-- a row that collides with nothing is written, predicate or no predicate
INSERT INTO arbp_a VALUES (2,1) ON CONFLICT (i) WHERE i > 0 DO UPDATE SET k = 9;

-- begin-expected
-- columns: i | k
-- row: 1 | 9
-- row: 2 | 1
-- end-expected
SELECT i, k FROM arbp_a ORDER BY i;

-- the target written as an expression reaches the same index
INSERT INTO arbp_a VALUES (1,6) ON CONFLICT ((i)) WHERE i > 0 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbp_a VALUES (1,7) ON CONFLICT ((i+0)) WHERE i > 0 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbp_a VALUES (1,8) ON CONFLICT (k) WHERE i > 0 DO NOTHING;

-- begin-expected
-- columns: i | k
-- row: 1 | 9
-- row: 2 | 1
-- end-expected
SELECT i, k FROM arbp_a ORDER BY i;

DROP TABLE arbp_a;

-- ============================================================================
-- A partial index is reached only by a predicate that entails its own
-- ============================================================================
CREATE TABLE arbp_b (i int, k int, s text);
CREATE UNIQUE INDEX arbp_b_u ON arbp_b (i) WHERE i > 0;
INSERT INTO arbp_b VALUES (1,1,'a');

-- the index's own predicate, and predicates narrower than it
INSERT INTO arbp_b VALUES (1,2,'b') ON CONFLICT (i) WHERE i > 0 DO NOTHING;
INSERT INTO arbp_b VALUES (1,3,'b') ON CONFLICT (i) WHERE i > 5 DO NOTHING;
INSERT INTO arbp_b VALUES (1,4,'b') ON CONFLICT (i) WHERE i >= 1 DO NOTHING;
INSERT INTO arbp_b VALUES (1,5,'b') ON CONFLICT (i) WHERE i = 5 DO NOTHING;
INSERT INTO arbp_b VALUES (1,6,'b') ON CONFLICT (i) WHERE i > 0 AND k > 0 DO NOTHING;
INSERT INTO arbp_b VALUES (1,7,'b') ON CONFLICT (i) WHERE i > 0 AND i IS NOT NULL DO NOTHING;

-- and the same predicate spelled otherwise
INSERT INTO arbp_b VALUES (1,8,'b') ON CONFLICT (i) WHERE arbp_b.i > 0 DO NOTHING;
INSERT INTO arbp_b VALUES (1,9,'b') ON CONFLICT (i) WHERE 0 < i DO NOTHING;
INSERT INTO arbp_b VALUES (1,10,'b') ON CONFLICT (i) WHERE i > 0::int DO NOTHING;
INSERT INTO arbp_b VALUES (1,11,'b') ON CONFLICT (i) WHERE (i > 0) DO NOTHING;
INSERT INTO arbp_b VALUES (1,12,'b') ON CONFLICT (i) WHERE i > 0 AND true DO NOTHING;

-- a predicate that admits rows the index does not hold entails nothing about it
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbp_b VALUES (1,13,'b') ON CONFLICT (i) WHERE i > -5 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbp_b VALUES (1,14,'b') ON CONFLICT (i) WHERE i >= 0 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbp_b VALUES (1,15,'b') ON CONFLICT (i) WHERE i = 0 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbp_b VALUES (1,16,'b') ON CONFLICT (i) WHERE k > 0 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbp_b VALUES (1,17,'b') ON CONFLICT (i) WHERE i > 0 OR k > 0 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbp_b VALUES (1,18,'b') ON CONFLICT (i) WHERE true DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbp_b VALUES (1,19,'b') ON CONFLICT (i) WHERE i IS NOT NULL DO NOTHING;

-- a false constant takes the whole conjunction with it, and what is left proves nothing
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbp_b VALUES (1,20,'b') ON CONFLICT (i) WHERE false DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbp_b VALUES (1,21,'b') ON CONFLICT (i) WHERE i > 0 AND false DO NOTHING;

-- and no predicate at all reaches no partial index
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbp_b VALUES (1,22,'b') ON CONFLICT (i) DO NOTHING;

-- the action the target carries makes no difference to which index it reaches
INSERT INTO arbp_b VALUES (1,23,'b') ON CONFLICT (i) WHERE i > 0 DO UPDATE SET s = 'up';

-- begin-expected
-- columns: i | k | s
-- row: 1 | 1 | up
-- end-expected
SELECT i, k, s FROM arbp_b ORDER BY i, k;

DROP TABLE arbp_b;

-- ============================================================================
-- The DO UPDATE form is refused and accepted on the same grounds
-- ============================================================================
CREATE TABLE arbp_c (i int, k int, s text);
CREATE UNIQUE INDEX arbp_c_u ON arbp_c (i) WHERE i > 0;
INSERT INTO arbp_c VALUES (1,1,'a');

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbp_c VALUES (1,2,'b') ON CONFLICT (i) WHERE i >= 0 DO UPDATE SET s = 'up';

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbp_c VALUES (1,3,'b') ON CONFLICT (i) DO UPDATE SET s = 'up';

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbp_c VALUES (1,4,'b') ON CONFLICT (i) WHERE k > 0 DO UPDATE SET s = 'up';

-- none of the refused statements wrote anything
-- begin-expected
-- columns: i | k | s
-- row: 1 | 1 | a
-- end-expected
SELECT i, k, s FROM arbp_c ORDER BY i, k;

INSERT INTO arbp_c VALUES (1,5,'b') ON CONFLICT (i) WHERE i > 0 DO UPDATE SET s = EXCLUDED.s;

-- begin-expected
-- columns: i | k | s
-- row: 1 | 1 | b
-- end-expected
SELECT i, k, s FROM arbp_c ORDER BY i, k;

DROP TABLE arbp_c;

-- a relation carrying no unique index at all is reached by nothing
CREATE TABLE arbp_d (i int, k int);
INSERT INTO arbp_d VALUES (1,1);

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbp_d VALUES (1,2) ON CONFLICT (i) WHERE i > 0 DO UPDATE SET k = 5;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbp_d VALUES (1,3) ON CONFLICT (i) DO NOTHING;

-- but a conflict target of no columns asks for no index
INSERT INTO arbp_d VALUES (1,4) ON CONFLICT DO NOTHING;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*) AS n FROM arbp_d;

DROP TABLE arbp_d;

-- ============================================================================
-- A comparison answers nothing where its operand is null, which is the proof
-- of IS NOT NULL
-- ============================================================================
CREATE TABLE arbp_e (i int, k int);
CREATE UNIQUE INDEX arbp_e_u ON arbp_e (i) WHERE i IS NOT NULL;
INSERT INTO arbp_e VALUES (1,1);

INSERT INTO arbp_e VALUES (1,2) ON CONFLICT (i) WHERE i > 0 DO NOTHING;
INSERT INTO arbp_e VALUES (1,3) ON CONFLICT (i) WHERE i IS NOT NULL DO NOTHING;
INSERT INTO arbp_e VALUES (1,4) ON CONFLICT (i) WHERE i = 1 DO NOTHING;

-- but only about the expression it compares
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbp_e VALUES (1,5) ON CONFLICT (i) WHERE k IS NOT NULL DO NOTHING;

-- begin-expected
-- columns: i | k
-- row: 1 | 1
-- end-expected
SELECT i, k FROM arbp_e ORDER BY k;

DROP TABLE arbp_e;

CREATE TABLE arbp_f (i int, k int, s text);
CREATE UNIQUE INDEX arbp_f_u ON arbp_f (i) WHERE s IS NOT NULL;
INSERT INTO arbp_f VALUES (1,1,'a');

INSERT INTO arbp_f VALUES (1,2,'b') ON CONFLICT (i) WHERE s = 'a' DO NOTHING;
INSERT INTO arbp_f VALUES (1,3,'b') ON CONFLICT (i) WHERE s <> 'a' DO NOTHING;
INSERT INTO arbp_f VALUES (1,4,'b') ON CONFLICT (i) WHERE s IS NOT NULL DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbp_f VALUES (1,5,'b') ON CONFLICT (i) WHERE s IS NULL DO NOTHING;

-- an equality that reaches the index still names it, and the row that collides is
-- the one the index holds
INSERT INTO arbp_f VALUES (1,6,'b') ON CONFLICT (i) WHERE s = 'a' DO UPDATE SET k = 8;

-- begin-expected
-- columns: i | k | s
-- row: 1 | 8 | a
-- end-expected
SELECT i, k, s FROM arbp_f ORDER BY i;

DROP TABLE arbp_f;

-- ============================================================================
-- Every part of the index's predicate has to be entailed
-- ============================================================================
CREATE TABLE arbp_g (i int, k int, s text);
CREATE UNIQUE INDEX arbp_g_u ON arbp_g (i) WHERE s = 'a' AND k > 0;
INSERT INTO arbp_g VALUES (1,1,'a');

INSERT INTO arbp_g VALUES (1,1,'a') ON CONFLICT (i) WHERE s = 'a' AND k > 0 DO NOTHING;
INSERT INTO arbp_g VALUES (1,1,'a') ON CONFLICT (i) WHERE k > 0 AND s = 'a' DO NOTHING;
INSERT INTO arbp_g VALUES (1,1,'a') ON CONFLICT (i) WHERE s = 'a' AND k > 5 AND i > 0 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbp_g VALUES (1,1,'a') ON CONFLICT (i) WHERE s = 'a' DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbp_g VALUES (1,1,'a') ON CONFLICT (i) WHERE s = 'A' AND k > 0 DO NOTHING;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM arbp_g;

DROP TABLE arbp_g;

-- ============================================================================
-- What the arbiter index holds is what collides
-- ============================================================================
CREATE TABLE arbp_h (i int, k int, s text);
CREATE UNIQUE INDEX arbp_h_u ON arbp_h (i) WHERE s = 'a';
INSERT INTO arbp_h VALUES (1,1,'a');

-- a row the index would not hold collides with nothing there
INSERT INTO arbp_h VALUES (1,2,'b') ON CONFLICT (i) WHERE s = 'a' DO NOTHING;
INSERT INTO arbp_h VALUES (1,3,'a') ON CONFLICT (i) WHERE s = 'a' DO NOTHING;
INSERT INTO arbp_h VALUES (1,4,'a') ON CONFLICT (i) WHERE s = 'a' DO UPDATE SET k = 99;

-- begin-expected
-- columns: i | k | s
-- row: 1 | 2 | b
-- row: 1 | 99 | a
-- end-expected
SELECT i, k, s FROM arbp_h ORDER BY k;

DROP TABLE arbp_h;

-- a predicate narrower than the index's does not narrow what the index holds:
-- the stored row has k = -1 and collides all the same
CREATE TABLE arbp_i (i int, k int, s text);
CREATE UNIQUE INDEX arbp_i_u ON arbp_i (i) WHERE s = 'a';
INSERT INTO arbp_i VALUES (1,-1,'a');
INSERT INTO arbp_i VALUES (1,50,'a') ON CONFLICT (i) WHERE s = 'a' AND k > 0 DO UPDATE SET k = 77;

-- begin-expected
-- columns: i | k | s
-- row: 1 | 77 | a
-- end-expected
SELECT i, k, s FROM arbp_i ORDER BY i, k;

DROP TABLE arbp_i;

-- ============================================================================
-- An expression target is settled the same way
-- ============================================================================
CREATE TABLE arbp_j (i int, s text, k int);
CREATE UNIQUE INDEX arbp_j_u ON arbp_j (lower(s)) WHERE k > 0;
INSERT INTO arbp_j VALUES (1,'a',1);

INSERT INTO arbp_j VALUES (2,'A',2) ON CONFLICT ((lower(s))) WHERE k > 0 DO NOTHING;
INSERT INTO arbp_j VALUES (3,'A',3) ON CONFLICT ((lower(s))) WHERE k > 5 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbp_j VALUES (4,'A',4) ON CONFLICT ((lower(s))) WHERE k > -1 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbp_j VALUES (5,'A',5) ON CONFLICT ((lower(s))) DO NOTHING;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM arbp_j;

DROP TABLE arbp_j;

-- an expression index that is not partial takes a predicate and takes none
CREATE TABLE arbp_k (i int, s text, k int);
CREATE UNIQUE INDEX arbp_k_u ON arbp_k (lower(s));
INSERT INTO arbp_k VALUES (1,'a',1);
INSERT INTO arbp_k VALUES (2,'A',2) ON CONFLICT ((lower(s))) WHERE k > 0 DO NOTHING;
INSERT INTO arbp_k VALUES (3,'A',3) ON CONFLICT ((lower(s))) DO NOTHING;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM arbp_k;

DROP TABLE arbp_k;

-- ============================================================================
-- The names the arbiter predicate holds are the relation's own
-- ============================================================================
CREATE TABLE arbp_l (i int PRIMARY KEY, k int);
INSERT INTO arbp_l VALUES (1,1);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
INSERT INTO arbp_l VALUES (1,2) ON CONFLICT (i) WHERE nosuchcol > 0 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "zzz"
-- end-expected-error
INSERT INTO arbp_l VALUES (1,2) ON CONFLICT (i) WHERE zzz.i > 0 DO NOTHING;

-- EXCLUDED is the row the action reads, and an index predicate is no action
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: missing FROM-clause entry for table "excluded"
-- end-expected-error
INSERT INTO arbp_l VALUES (1,2) ON CONFLICT (i) WHERE excluded.i > 0 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column arbp_l.nosuchcol does not exist
-- end-expected-error
INSERT INTO arbp_l VALUES (1,2) ON CONFLICT (i) WHERE arbp_l.nosuchcol > 0 DO NOTHING;

-- the predicate is judged against one row, with no query around it for a nested one
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in index predicate
-- end-expected-error
INSERT INTO arbp_l VALUES (1,2) ON CONFLICT (i) WHERE (SELECT true) DO NOTHING;

-- the relation is in the statement under the name the statement gave it
INSERT INTO arbp_l AS z VALUES (1,2) ON CONFLICT (i) WHERE z.i > 0 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: invalid reference to FROM-clause entry for table "arbp_l"
-- end-expected-error
INSERT INTO arbp_l AS z VALUES (1,2) ON CONFLICT (i) WHERE arbp_l.i > 0 DO NOTHING;

-- every relation carries the system columns whether or not anybody declared them
INSERT INTO arbp_l VALUES (1,2) ON CONFLICT (i) WHERE ctid IS NOT NULL DO NOTHING;

-- the target's own columns are read before the predicate beside them
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
INSERT INTO arbp_l VALUES (1,2) ON CONFLICT (nosuchcol) WHERE i > 0 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
INSERT INTO arbp_l VALUES (1,2) ON CONFLICT (k) WHERE nosuchcol > 0 DO NOTHING;

-- begin-expected
-- columns: i | k
-- row: 1 | 1
-- end-expected
SELECT i, k FROM arbp_l;

DROP TABLE arbp_l;

-- ============================================================================
-- A target names an index's columns, in whatever order it writes them
-- ============================================================================
CREATE TABLE arbp_m (i int, k int, UNIQUE (i,k));
INSERT INTO arbp_m VALUES (1,2);
INSERT INTO arbp_m VALUES (1,2) ON CONFLICT (k,i) DO NOTHING;
INSERT INTO arbp_m VALUES (1,2) ON CONFLICT (k,i) WHERE i > 0 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbp_m VALUES (1,2) ON CONFLICT (i) WHERE i > 0 DO NOTHING;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM arbp_m;

DROP TABLE arbp_m;

CREATE TABLE arbp_n (i int, k int);
CREATE UNIQUE INDEX arbp_n_u ON arbp_n (i, k) WHERE i > 0;
INSERT INTO arbp_n VALUES (1,2);
INSERT INTO arbp_n VALUES (1,2) ON CONFLICT (k,i) WHERE i > 0 DO NOTHING;
INSERT INTO arbp_n VALUES (1,2) ON CONFLICT (i,k) WHERE i > 0 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbp_n VALUES (1,2) ON CONFLICT (i,k) DO NOTHING;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM arbp_n;

DROP TABLE arbp_n;

-- ============================================================================
-- Where more than one index could answer, each is judged on its own predicate
-- ============================================================================
CREATE TABLE arbp_o (i int, k int);
CREATE UNIQUE INDEX arbp_o_p ON arbp_o (i) WHERE i > 0;
CREATE UNIQUE INDEX arbp_o_f ON arbp_o (i);
INSERT INTO arbp_o VALUES (1,1);

-- the index with no predicate of its own answers for all three
INSERT INTO arbp_o VALUES (1,2) ON CONFLICT (i) WHERE i > 0 DO NOTHING;
INSERT INTO arbp_o VALUES (1,3) ON CONFLICT (i) DO NOTHING;
INSERT INTO arbp_o VALUES (1,4) ON CONFLICT (i) WHERE i > 5 DO NOTHING;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM arbp_o;

DROP TABLE arbp_o;

CREATE TABLE arbp_p (i int, k int);
CREATE UNIQUE INDEX arbp_p_1 ON arbp_p (i) WHERE i > 0;
CREATE UNIQUE INDEX arbp_p_2 ON arbp_p (i) WHERE i > 10;
INSERT INTO arbp_p VALUES (20,1);

INSERT INTO arbp_p VALUES (20,2) ON CONFLICT (i) WHERE i > 0 DO NOTHING;
INSERT INTO arbp_p VALUES (20,3) ON CONFLICT (i) WHERE i > 10 DO NOTHING;
INSERT INTO arbp_p VALUES (20,4) ON CONFLICT (i) WHERE i > 30 DO NOTHING;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM arbp_p;

DROP TABLE arbp_p;

-- ============================================================================
-- A unique constraint's index is an index like any other, and ON CONSTRAINT
-- takes no predicate at all
-- ============================================================================
CREATE TABLE arbp_q (i int, k int, s text, CONSTRAINT arbp_q_uq UNIQUE (i));
INSERT INTO arbp_q VALUES (1,1,'a');
INSERT INTO arbp_q VALUES (1,2,'b') ON CONFLICT (i) WHERE i > 0 DO NOTHING;

-- the predicate is no filter on the row that collides
INSERT INTO arbp_q VALUES (1,3,'b') ON CONFLICT (i) WHERE s = 'zzz' DO UPDATE SET k = 7;

-- begin-expected
-- columns: i | k | s
-- row: 1 | 7 | a
-- end-expected
SELECT i, k, s FROM arbp_q ORDER BY i;

INSERT INTO arbp_q VALUES (1,4,'b') ON CONFLICT ON CONSTRAINT arbp_q_uq DO NOTHING;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "WHERE"
-- end-expected-error
INSERT INTO arbp_q VALUES (1,5,'b') ON CONFLICT ON CONSTRAINT arbp_q_uq WHERE i > 0 DO NOTHING;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM arbp_q;

DROP TABLE arbp_q;

-- ============================================================================
-- The predicate is read to settle which index arbitrates and never evaluated,
-- so what CREATE INDEX would refuse stands beside a conflict target
-- ============================================================================
CREATE TABLE arbp_r (i int PRIMARY KEY, k int);
INSERT INTO arbp_r VALUES (1,1);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
CREATE INDEX arbp_r_x ON arbp_r (k) WHERE i;

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: functions in index predicate must be marked IMMUTABLE
-- end-expected-error
CREATE INDEX arbp_r_y ON arbp_r (k) WHERE random() > 0.5;

-- both stand beside the target, because the index with no predicate answers first
INSERT INTO arbp_r VALUES (1,2) ON CONFLICT (i) WHERE i DO NOTHING;
INSERT INTO arbp_r VALUES (1,3) ON CONFLICT (i) WHERE random() > 0.5 DO UPDATE SET k = 55;

-- begin-expected
-- columns: i | k
-- row: 1 | 55
-- end-expected
SELECT i, k FROM arbp_r ORDER BY i;

DROP TABLE arbp_r;

-- over a partial index the same predicate proves nothing
CREATE TABLE arbp_s (i int, k int);
CREATE UNIQUE INDEX arbp_s_u ON arbp_s (i) WHERE i > 0;
INSERT INTO arbp_s VALUES (1,1);

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbp_s VALUES (1,2) ON CONFLICT (i) WHERE i DO NOTHING;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM arbp_s;

DROP TABLE arbp_s;

-- ============================================================================
-- RETURNING answers for the row the action wrote, and for none where the
-- action was DO NOTHING
-- ============================================================================
CREATE TABLE arbp_t (i int, k int);
CREATE UNIQUE INDEX arbp_t_u ON arbp_t (i) WHERE k > 0;
INSERT INTO arbp_t VALUES (1,1);

-- begin-expected
-- columns: i | k
-- row: 1 | 5
-- end-expected
INSERT INTO arbp_t VALUES (1,2) ON CONFLICT (i) WHERE k > 0 DO UPDATE SET k = 5 RETURNING i, k;

-- begin-expected
-- columns: i | k
-- end-expected
INSERT INTO arbp_t VALUES (1,3) ON CONFLICT (i) WHERE k > 0 DO NOTHING RETURNING i, k;

-- begin-expected
-- columns: i | k
-- row: 1 | 5
-- end-expected
SELECT i, k FROM arbp_t ORDER BY i, k;

DROP TABLE arbp_t;
