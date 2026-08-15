-- ============================================================================
-- A row a BEFORE trigger rewrote still belongs in the partition it was put in
-- ============================================================================
-- Which partition holds an inserted row is settled before any row trigger of
-- that partition runs, so a BEFORE INSERT FOR EACH ROW trigger that rewrites
-- the partition key cannot send the row somewhere else. A trigger written on
-- the partition itself leaves a row that fails that partition's own bound, and
-- the write is refused under the partition's name. A copy the partitioned
-- table handed down to the partition is stopped at the trigger that rewrote
-- the row instead, which PostgreSQL reports as a move it does not support,
-- naming the trigger and the partition the row was on its way to. Routing
-- running first settles the other half of it too: a row no partition will take
-- is refused there and then, with no trigger given the chance to rewrite the
-- key into one that fits. An UPDATE is a different matter -- a row whose key
-- an UPDATE changes is meant to move, and it moves.
-- ============================================================================

CREATE FUNCTION trw_bump() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.k := NEW.k + 10; RETURN NEW; END $$;
CREATE FUNCTION trw_touch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.s := NEW.s || '!'; RETURN NEW; END $$;
CREATE FUNCTION trw_pass() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE FUNCTION trw_veto() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$;
CREATE FUNCTION trw_settle() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.k := 5; RETURN NEW; END $$;

-- ----------------------------------------------------------------------------
-- 1. A trigger written on the partition itself leaves the row to fail that
--    partition's bound, whichever relation the statement named
-- ----------------------------------------------------------------------------
CREATE TABLE trw_p (i int, k int, s text) PARTITION BY RANGE (k);
CREATE TABLE trw_p0 PARTITION OF trw_p FOR VALUES FROM (0) TO (10);
CREATE TABLE trw_p1 PARTITION OF trw_p FOR VALUES FROM (10) TO (20);
CREATE TRIGGER trw_a_move BEFORE INSERT ON trw_p0 FOR EACH ROW EXECUTE FUNCTION trw_bump();

-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "trw_p0" violates partition constraint
-- detail-like: Failing row contains (1, 15, a).
-- end-expected-error
INSERT INTO trw_p VALUES (1, 5, 'a');

-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "trw_p0" violates partition constraint
-- detail-like: Failing row contains (2, 16, b).
-- end-expected-error
INSERT INTO trw_p0 VALUES (2, 6, 'b');

-- 1c. Neither row was stored, in the partition its rewritten key named or
--     anywhere else
-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) AS count FROM trw_p;

-- 1d. Nothing tests the bound of a partition the statement named until that
--     partition's triggers are done with the row, so a trigger there may
--     settle a key the partition does hold
DROP TRIGGER trw_a_move ON trw_p0;
CREATE TRIGGER trw_a_move BEFORE INSERT ON trw_p1 FOR EACH ROW EXECUTE FUNCTION trw_bump();
INSERT INTO trw_p1 VALUES (3, 1, 'c');

-- 1e. and routing reads the key as the statement wrote it, not as the trigger
--     of the partition that key names would have left it: this row is stored
--     where 1 belongs, with the key it came in with, and trw_p1's trigger
--     never sees it
INSERT INTO trw_p VALUES (4, 1, 'd');

-- begin-expected
-- columns: tableoid, i, k, s
-- row: trw_p1 | 3 | 11 | c
-- row: trw_p0 | 4 | 1 | d
-- end-expected
SELECT tableoid::regclass::text AS tableoid, i, k, s FROM trw_p ORDER BY i;

DROP TABLE trw_p;

-- ----------------------------------------------------------------------------
-- 2. A copy of the partitioned table's own trigger: the move is refused at the
--    trigger rather than at the bound
-- ----------------------------------------------------------------------------
CREATE TABLE trw_q (i int, k int, s text) PARTITION BY RANGE (k);
CREATE TABLE trw_q0 PARTITION OF trw_q FOR VALUES FROM (0) TO (10);
CREATE TABLE trw_q1 PARTITION OF trw_q FOR VALUES FROM (10) TO (20);
CREATE TRIGGER trw_b_move BEFORE INSERT ON trw_q FOR EACH ROW EXECUTE FUNCTION trw_bump();

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: moving row to another partition during a BEFORE FOR EACH ROW trigger is not supported
-- detail-like: Before executing trigger "trw_b_move", the row was to be in partition "public.trw_q0".
-- end-expected-error
INSERT INTO trw_q VALUES (1, 5, 'a');

-- 2b. and the same for a write that named the partition
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: moving row to another partition during a BEFORE FOR EACH ROW trigger is not supported
-- detail-like: Before executing trigger "trw_b_move", the row was to be in partition "public.trw_q0".
-- end-expected-error
INSERT INTO trw_q0 VALUES (2, 6, 'b');

-- 2c. A copy that leaves the row where it belongs moved nothing
INSERT INTO trw_q1 VALUES (3, 1, 'c');

-- begin-expected
-- columns: tableoid, i, k, s
-- row: trw_q1 | 3 | 11 | c
-- end-expected
SELECT tableoid::regclass::text AS tableoid, i, k, s FROM trw_q ORDER BY i;

DELETE FROM trw_q;
DROP TRIGGER trw_b_move ON trw_q;

-- ----------------------------------------------------------------------------
-- 3. Which of the two reports comes out is settled by which trigger last
--    rewrote the row, not by which one moved the key
-- ----------------------------------------------------------------------------
CREATE TRIGGER trw_a_move BEFORE INSERT ON trw_q0 FOR EACH ROW EXECUTE FUNCTION trw_bump();
CREATE TRIGGER trw_b_touch BEFORE INSERT ON trw_q FOR EACH ROW EXECUTE FUNCTION trw_touch();

-- 3a. The partition's own trigger moved the key and the copy that ran after it
--     changed a column that has nothing to do with the key: the copy is still
--     the trigger the move is reported against
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: moving row to another partition during a BEFORE FOR EACH ROW trigger is not supported
-- detail-like: Before executing trigger "trw_b_touch", the row was to be in partition "public.trw_q0".
-- end-expected-error
INSERT INTO trw_q VALUES (1, 5, 'a');

DROP TRIGGER trw_b_touch ON trw_q;
CREATE TRIGGER trw_b_pass BEFORE INSERT ON trw_q FOR EACH ROW EXECUTE FUNCTION trw_pass();

-- 3b. A copy that hands the row back untouched rewrote nothing, so the row is
--     left to fail the bound of the partition it was routed to
-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "trw_q0" violates partition constraint
-- detail-like: Failing row contains (2, 15, b).
-- end-expected-error
INSERT INTO trw_q VALUES (2, 5, 'b');

DROP TRIGGER trw_a_move ON trw_q0;
DROP TRIGGER trw_b_pass ON trw_q;
CREATE TRIGGER trw_c_touch BEFORE INSERT ON trw_q FOR EACH ROW EXECUTE FUNCTION trw_touch();

-- 3c. A copy that changes a column outside the key leaves the row where it is
INSERT INTO trw_q VALUES (3, 5, 'c');

-- begin-expected
-- columns: tableoid, i, k, s
-- row: trw_q0 | 3 | 5 | c!
-- end-expected
SELECT tableoid::regclass::text AS tableoid, i, k, s FROM trw_q ORDER BY i;

DROP TABLE trw_q;

-- ----------------------------------------------------------------------------
-- 4. A trigger that answers with nothing keeps the row out altogether, which
--    is no move
-- ----------------------------------------------------------------------------
CREATE TABLE trw_v (i int, k int) PARTITION BY RANGE (k);
CREATE TABLE trw_v0 PARTITION OF trw_v FOR VALUES FROM (0) TO (10);
CREATE TABLE trw_v1 PARTITION OF trw_v FOR VALUES FROM (10) TO (20);
CREATE TRIGGER trw_a_veto BEFORE INSERT ON trw_v0 FOR EACH ROW EXECUTE FUNCTION trw_veto();
INSERT INTO trw_v VALUES (1, 5);
INSERT INTO trw_v VALUES (2, 15);

-- begin-expected
-- columns: tableoid, i, k
-- row: trw_v1 | 2 | 15
-- end-expected
SELECT tableoid::regclass::text AS tableoid, i, k FROM trw_v ORDER BY i;

DROP TABLE trw_v;

-- ----------------------------------------------------------------------------
-- 5. In a sub-partitioned hierarchy the partition named is the leaf the row
--    was routed all the way down to
-- ----------------------------------------------------------------------------
CREATE TABLE trw_s (i int, k int, j int) PARTITION BY RANGE (k);
CREATE TABLE trw_s0 PARTITION OF trw_s FOR VALUES FROM (0) TO (10) PARTITION BY RANGE (j);
CREATE TABLE trw_s00 PARTITION OF trw_s0 FOR VALUES FROM (0) TO (100);
CREATE TABLE trw_s1 PARTITION OF trw_s FOR VALUES FROM (10) TO (20) PARTITION BY RANGE (j);
CREATE TABLE trw_s10 PARTITION OF trw_s1 FOR VALUES FROM (0) TO (100);
CREATE TRIGGER trw_a_move BEFORE INSERT ON trw_s00 FOR EACH ROW EXECUTE FUNCTION trw_bump();

-- 5a. The leaf's own trigger leaves a row that fails a bound belonging to the
--     level above it, and the leaf is what the report names
-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "trw_s00" violates partition constraint
-- detail-like: Failing row contains (1, 15, 7).
-- end-expected-error
INSERT INTO trw_s VALUES (1, 5, 7);

DROP TRIGGER trw_a_move ON trw_s00;

-- 5b. A copy handed down from the level between names the leaf it fired on
CREATE TRIGGER trw_b_move BEFORE INSERT ON trw_s0 FOR EACH ROW EXECUTE FUNCTION trw_bump();

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: moving row to another partition during a BEFORE FOR EACH ROW trigger is not supported
-- detail-like: Before executing trigger "trw_b_move", the row was to be in partition "public.trw_s00".
-- end-expected-error
INSERT INTO trw_s VALUES (2, 5, 7);

DROP TRIGGER trw_b_move ON trw_s0;

-- 5c. and so does one handed down from the root
CREATE TRIGGER trw_c_move BEFORE INSERT ON trw_s FOR EACH ROW EXECUTE FUNCTION trw_bump();

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: moving row to another partition during a BEFORE FOR EACH ROW trigger is not supported
-- detail-like: Before executing trigger "trw_c_move", the row was to be in partition "public.trw_s00".
-- end-expected-error
INSERT INTO trw_s VALUES (3, 5, 7);

DROP TRIGGER trw_c_move ON trw_s;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) AS count FROM trw_s;

DROP TABLE trw_s;

-- ----------------------------------------------------------------------------
-- 6. A partitioned table that is itself a partition tests its own bound before
--    it routes, so that bound is reported under its own name
-- ----------------------------------------------------------------------------
CREATE TABLE trw_x (i int, k int, j int) PARTITION BY RANGE (k);
CREATE TABLE trw_x0 PARTITION OF trw_x FOR VALUES FROM (0) TO (10) PARTITION BY RANGE (j);
CREATE TABLE trw_x00 PARTITION OF trw_x0 FOR VALUES FROM (0) TO (100);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "trw_x0" violates partition constraint
-- detail-like: Failing row contains (1, 99, 7).
-- end-expected-error
INSERT INTO trw_x0 VALUES (1, 99, 7);

-- begin-expected-error
-- sqlstate: 23514
-- message-like: no partition of relation "trw_x0" found for row
-- detail-like: Partition key of the failing row contains (j) = (999).
-- end-expected-error
INSERT INTO trw_x0 VALUES (2, 5, 999);

DROP TABLE trw_x;

-- ----------------------------------------------------------------------------
-- 7. Routing runs first, so no trigger gets to settle a key no partition takes
-- ----------------------------------------------------------------------------
CREATE TABLE trw_n (i int, k int) PARTITION BY RANGE (k);
CREATE TABLE trw_n0 PARTITION OF trw_n FOR VALUES FROM (0) TO (10);
CREATE TRIGGER trw_a_settle BEFORE INSERT ON trw_n FOR EACH ROW EXECUTE FUNCTION trw_settle();

-- begin-expected-error
-- sqlstate: 23514
-- message-like: no partition of relation "trw_n" found for row
-- detail-like: Partition key of the failing row contains (k) = (99).
-- end-expected-error
INSERT INTO trw_n VALUES (1, 99);

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) AS count FROM trw_n;

DROP TABLE trw_n;

-- ----------------------------------------------------------------------------
-- 8. The DEFAULT partition holds what no sibling claims, and a trigger on it
--    is read against exactly that
-- ----------------------------------------------------------------------------
CREATE TABLE trw_d (i int, k int) PARTITION BY RANGE (k);
CREATE TABLE trw_d0 PARTITION OF trw_d FOR VALUES FROM (0) TO (10);
CREATE TABLE trw_dd PARTITION OF trw_d DEFAULT;
CREATE TRIGGER trw_a_move BEFORE INSERT ON trw_dd FOR EACH ROW EXECUTE FUNCTION trw_bump();

-- 8a. A key no sibling claims before the trigger and none claims after it
--     leaves the row in the default partition
INSERT INTO trw_d VALUES (1, 50);

-- begin-expected
-- columns: tableoid, i, k
-- row: trw_dd | 1 | 60
-- end-expected
SELECT tableoid::regclass::text AS tableoid, i, k FROM trw_d ORDER BY i;

-- 8b. A trigger on a bounded partition may not hand the row to the default one
CREATE TRIGGER trw_a_move BEFORE INSERT ON trw_d0 FOR EACH ROW EXECUTE FUNCTION trw_bump();

-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "trw_d0" violates partition constraint
-- detail-like: Failing row contains (2, 15).
-- end-expected-error
INSERT INTO trw_d VALUES (2, 5);

-- begin-expected
-- columns: tableoid, i, k
-- row: trw_dd | 1 | 60
-- end-expected
SELECT tableoid::regclass::text AS tableoid, i, k FROM trw_d ORDER BY i;

DROP TABLE trw_d;

-- ----------------------------------------------------------------------------
-- 9. An UPDATE is a different matter: a row whose key it changes is meant to
--    move, and a BEFORE UPDATE trigger on the partition moves it
-- ----------------------------------------------------------------------------
CREATE TABLE trw_u (i int, k int) PARTITION BY RANGE (k);
CREATE TABLE trw_u0 PARTITION OF trw_u FOR VALUES FROM (0) TO (10);
CREATE TABLE trw_u1 PARTITION OF trw_u FOR VALUES FROM (10) TO (20);
INSERT INTO trw_u VALUES (1, 5), (2, 6);
CREATE TRIGGER trw_a_move BEFORE UPDATE ON trw_u0 FOR EACH ROW EXECUTE FUNCTION trw_bump();
UPDATE trw_u SET i = i WHERE i = 1;

-- begin-expected
-- columns: tableoid, i, k
-- row: trw_u1 | 1 | 15
-- row: trw_u0 | 2 | 6
-- end-expected
SELECT tableoid::regclass::text AS tableoid, i, k FROM trw_u ORDER BY i;

-- 9b. An UPDATE that named the partition has nowhere to move the row to, so
--     the row it leaves simply fails that partition's bound
-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "trw_u0" violates partition constraint
-- detail-like: Failing row contains (2, 16).
-- end-expected-error
UPDATE trw_u0 SET i = i WHERE i = 2;

-- 9c. and a copy handed down from the partitioned table moves the row just as
--     readily, because a change of key by an UPDATE is a move PostgreSQL
--     carries out
DROP TRIGGER trw_a_move ON trw_u0;
CREATE TRIGGER trw_b_move BEFORE UPDATE ON trw_u FOR EACH ROW EXECUTE FUNCTION trw_bump();
UPDATE trw_u SET i = i WHERE i = 2;

-- begin-expected
-- columns: tableoid, i, k
-- row: trw_u1 | 1 | 15
-- row: trw_u1 | 2 | 16
-- end-expected
SELECT tableoid::regclass::text AS tableoid, i, k FROM trw_u ORDER BY i;

DROP TABLE trw_u;

-- ----------------------------------------------------------------------------
-- 10. The insert half of a row that changed partition is an insert like any
--     other: the destination's copy may not carry it further, and the
--     destination's own trigger leaves it to fail the destination's bound
-- ----------------------------------------------------------------------------
CREATE TABLE trw_r (i int, k int) PARTITION BY RANGE (k);
CREATE TABLE trw_r0 PARTITION OF trw_r FOR VALUES FROM (0) TO (10);
CREATE TABLE trw_r1 PARTITION OF trw_r FOR VALUES FROM (10) TO (20);
CREATE TABLE trw_r2 PARTITION OF trw_r FOR VALUES FROM (20) TO (30);
INSERT INTO trw_r VALUES (1, 5);
CREATE TRIGGER trw_a_move BEFORE INSERT ON trw_r1 FOR EACH ROW EXECUTE FUNCTION trw_bump();

-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "trw_r1" violates partition constraint
-- detail-like: Failing row contains (1, 25).
-- end-expected-error
UPDATE trw_r SET k = 15 WHERE i = 1;

DROP TRIGGER trw_a_move ON trw_r1;
CREATE TRIGGER trw_b_move BEFORE INSERT ON trw_r FOR EACH ROW EXECUTE FUNCTION trw_bump();

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: moving row to another partition during a BEFORE FOR EACH ROW trigger is not supported
-- detail-like: Before executing trigger "trw_b_move", the row was to be in partition "public.trw_r1".
-- end-expected-error
UPDATE trw_r SET k = 15 WHERE i = 1;

-- 10c. and the row is still where it was
-- begin-expected
-- columns: tableoid, i, k
-- row: trw_r0 | 1 | 5
-- end-expected
SELECT tableoid::regclass::text AS tableoid, i, k FROM trw_r ORDER BY i;

DROP TABLE trw_r;

-- ----------------------------------------------------------------------------
-- 11. A MERGE's insert arm and an INSERT ... ON CONFLICT read the same way
-- ----------------------------------------------------------------------------
CREATE TABLE trw_src (i int, k int);
INSERT INTO trw_src VALUES (1, 5), (2, 5);
CREATE TABLE trw_m (i int, k int, PRIMARY KEY (i, k)) PARTITION BY RANGE (k);
CREATE TABLE trw_m0 PARTITION OF trw_m FOR VALUES FROM (0) TO (10);
CREATE TABLE trw_m1 PARTITION OF trw_m FOR VALUES FROM (10) TO (20);
CREATE TRIGGER trw_a_move BEFORE INSERT ON trw_m0 FOR EACH ROW EXECUTE FUNCTION trw_bump();

-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "trw_m0" violates partition constraint
-- detail-like: Failing row contains (1, 15).
-- end-expected-error
MERGE INTO trw_m t USING trw_src s ON t.i = s.i WHEN NOT MATCHED THEN INSERT (i, k) VALUES (s.i, s.k);

DROP TRIGGER trw_a_move ON trw_m0;
CREATE TRIGGER trw_b_move BEFORE INSERT ON trw_m FOR EACH ROW EXECUTE FUNCTION trw_bump();

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: moving row to another partition during a BEFORE FOR EACH ROW trigger is not supported
-- detail-like: Before executing trigger "trw_b_move", the row was to be in partition "public.trw_m0".
-- end-expected-error
MERGE INTO trw_m t USING trw_src s ON t.i = s.i WHEN NOT MATCHED THEN INSERT (i, k) VALUES (s.i, s.k);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: moving row to another partition during a BEFORE FOR EACH ROW trigger is not supported
-- detail-like: Before executing trigger "trw_b_move", the row was to be in partition "public.trw_m0".
-- end-expected-error
INSERT INTO trw_m VALUES (9, 5) ON CONFLICT (i, k) DO NOTHING;

-- 11c. and an ON CONFLICT arm that would have passed the row over is reached
--      only once the row has been found to belong where it was put
DROP TRIGGER trw_b_move ON trw_m;
INSERT INTO trw_m VALUES (1, 5);
CREATE TRIGGER trw_a_move BEFORE INSERT ON trw_m0 FOR EACH ROW EXECUTE FUNCTION trw_bump();

-- begin-expected-error
-- sqlstate: 23514
-- message-like: new row for relation "trw_m0" violates partition constraint
-- detail-like: Failing row contains (1, 15).
-- end-expected-error
INSERT INTO trw_m VALUES (1, 5) ON CONFLICT (i, k) DO NOTHING;

-- begin-expected
-- columns: tableoid, i, k
-- row: trw_m0 | 1 | 5
-- end-expected
SELECT tableoid::regclass::text AS tableoid, i, k FROM trw_m ORDER BY i;

DROP TABLE trw_m;
DROP TABLE trw_src;
DROP FUNCTION trw_bump();
DROP FUNCTION trw_touch();
DROP FUNCTION trw_pass();
DROP FUNCTION trw_veto();
DROP FUNCTION trw_settle();
