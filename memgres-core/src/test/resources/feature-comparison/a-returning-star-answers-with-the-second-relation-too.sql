-- ============================================================================
-- What a bare star in a RETURNING clause stands for
--
-- A write that brings in a second relation -- an UPDATE with a FROM, a DELETE
-- with a USING -- reads its RETURNING list in the scope of both, so a bare star
-- there is every column of the target followed by every column the second
-- relation supplies. However that relation was written -- a table, a join of
-- two, a derived query, a VALUES list, a set-returning call -- the star answers
-- with the columns it supplies. The order is the order the statement named its
-- relations in, which is why MERGE, whose source is named before its target,
-- answers with the source's columns first.
--
-- A star written out against the target names the target's columns alone, and a
-- write with no second relation answers as it always did.
--
-- Every value below was measured against PostgreSQL 18.
-- ============================================================================

-- setup
CREATE TABLE zzt4f_rx (i int, v text, w text);
CREATE TABLE zzt4f_rxs (j int, p text, q text);
CREATE TABLE zzt4f_rxt (k int, r text);
INSERT INTO zzt4f_rx VALUES (1,'a','L1'),(2,'b','L2');
INSERT INTO zzt4f_rxs VALUES (1,'p','R1'),(3,'z','R3');
INSERT INTO zzt4f_rxt VALUES (1,'T1'),(4,'T4');

-- ============================================================================
-- The target's columns, then the FROM relation's
-- ============================================================================

-- begin-expected
-- columns: i | v | w | j | p | q
-- row: 1 | y | L1 | 1 | p | R1
-- end-expected
UPDATE zzt4f_rx t SET v='y' FROM zzt4f_rxs u WHERE t.i=u.j RETURNING *;

-- a join in the FROM adds its second relation just the same
-- begin-expected
-- columns: i | v | w | j | p | q | k | r
-- row: 1 | y2 | L1 | 1 | p | R1 | 1 | T1
-- end-expected
UPDATE zzt4f_rx t SET v='y2' FROM zzt4f_rxs u JOIN zzt4f_rxt s ON s.k=u.j WHERE t.i=u.j RETURNING *;

-- and so does a second FROM item written beside the first
-- begin-expected
-- columns: i | v | w | j | p | q | k | r
-- row: 1 | y3 | L1 | 1 | p | R1 | 1 | T1
-- end-expected
UPDATE zzt4f_rx t SET v='y3' FROM zzt4f_rxs u, zzt4f_rxt s WHERE t.i=u.j AND t.i=s.k RETURNING *;

-- a derived FROM item supplies its select list, not the columns underneath it
-- begin-expected
-- columns: i | v | w | j | p
-- row: 1 | y4 | L1 | 1 | p
-- end-expected
UPDATE zzt4f_rx t SET v='y4' FROM (SELECT j, p FROM zzt4f_rxs) u WHERE t.i=u.j RETURNING *;

-- a VALUES list supplies the names its alias list gave it
-- begin-expected
-- columns: i | v | w | j | p
-- row: 1 | y5 | L1 | 1 | k
-- end-expected
UPDATE zzt4f_rx t SET v='y5' FROM (VALUES (1,'k')) u(j,p) WHERE t.i=u.j RETURNING *;

-- a set-returning call supplies the one column it is
-- begin-expected
-- columns: i | v | w | g
-- row: 1 | y6 | L1 | 1
-- end-expected
UPDATE zzt4f_rx t SET v='y6' FROM generate_series(1,1) g WHERE t.i=g RETURNING *;

-- a LATERAL item reading a relation of its own supplies its select list
-- begin-expected
-- columns: i | v | w | j | p | q | s
-- row: 1 | y7 | L1 | 1 | p | R1 | 2
-- end-expected
UPDATE zzt4f_rx t SET v='y7' FROM zzt4f_rxs u, LATERAL (SELECT u.j + 1 AS s) l WHERE t.i=u.j RETURNING *;

-- the star stands where it was written, and what follows it is answered after
-- begin-expected
-- columns: i | v | w | j | p | q | ?column?
-- row: 1 | y8 | L1 | 1 | p | R1 | p!
-- end-expected
UPDATE zzt4f_rx t SET v='y8' FROM zzt4f_rxs u WHERE t.i=u.j RETURNING *, u.p || '!';

-- ============================================================================
-- DELETE ... USING reads its USING relation the same way
-- ============================================================================

-- begin-expected
-- columns: i | v | w | j | p | q
-- row: 1 | y8 | L1 | 1 | p | R1
-- end-expected
DELETE FROM zzt4f_rx t USING zzt4f_rxs u WHERE t.i=u.j RETURNING *;

INSERT INTO zzt4f_rx VALUES (1,'a','L1');

-- begin-expected
-- columns: i | v | w | j | p
-- row: 1 | a | L1 | 1 | p
-- end-expected
DELETE FROM zzt4f_rx t USING (SELECT j, p FROM zzt4f_rxs) u WHERE t.i=u.j RETURNING *;

INSERT INTO zzt4f_rx VALUES (1,'a','L1');

-- ============================================================================
-- A pairing that reaches no row still answers with the columns of both
-- ============================================================================

-- begin-expected
-- columns: i | v | w | j | p | q
-- end-expected
UPDATE zzt4f_rx t SET v='n' FROM zzt4f_rxs u WHERE false RETURNING *;

-- and one whose second relation holds no row it can pair with
-- begin-expected
-- columns: i | v | w | j | p | q
-- end-expected
UPDATE zzt4f_rx t SET v='n' FROM zzt4f_rxs u WHERE t.i=u.j AND t.i>100 RETURNING *;

-- ============================================================================
-- MERGE names its source first
-- ============================================================================

-- MERGE writes its source ahead of its target in the range table, and the star
-- follows the range table -- so the source's columns come first here, the other
-- way round from the UPDATE above.
-- begin-expected
-- columns: j | p | q | i | v | w
-- row: 1 | p | R1 | 1 | m | L1
-- end-expected
MERGE INTO zzt4f_rx t USING zzt4f_rxs u ON t.i=u.j WHEN MATCHED THEN UPDATE SET v='m' RETURNING *;

-- begin-expected
-- columns: i | v | w
-- row: 1 | m2 | L1
-- end-expected
MERGE INTO zzt4f_rx t USING zzt4f_rxs u ON t.i=u.j WHEN MATCHED THEN UPDATE SET v='m2' RETURNING t.*;

-- an arm that pairs nothing still answers with the columns of both
-- begin-expected
-- columns: j | p | q | i | v | w
-- end-expected
MERGE INTO zzt4f_rx t USING zzt4f_rxs u ON t.i=u.j AND t.i>100 WHEN MATCHED THEN UPDATE SET v='m3' RETURNING *;

-- ============================================================================
-- What the star does not reach
-- ============================================================================

-- a star written out against the target is the target's columns alone
-- begin-expected
-- columns: i | v | w
-- row: 1 | y9 | L1
-- end-expected
UPDATE zzt4f_rx t SET v='y9' FROM zzt4f_rxs u WHERE t.i=u.j RETURNING t.*;

-- a write with no second relation answers with its own columns
-- begin-expected
-- columns: i | v | w
-- row: 5 | s | L5
-- end-expected
INSERT INTO zzt4f_rx VALUES (5,'s','L5') RETURNING *;

-- begin-expected
-- columns: i | v | w
-- row: 5 | t5 | L5
-- end-expected
UPDATE zzt4f_rx SET v='t5' WHERE i=5 RETURNING *;

-- begin-expected
-- columns: i | v | w
-- row: 5 | t5 | L5
-- end-expected
DELETE FROM zzt4f_rx WHERE i=5 RETURNING *;

-- begin-expected
-- columns: i | v | w
-- row: 1, y9, L1
-- row: 2, b, L2
-- end-expected
SELECT i, v, w FROM zzt4f_rx ORDER BY i;

-- teardown
DROP TABLE zzt4f_rx;
DROP TABLE zzt4f_rxs;
DROP TABLE zzt4f_rxt;
