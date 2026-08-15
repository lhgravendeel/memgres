-- ============================================================================
-- What a RETURNING clause resolves when a second relation stands beside the
-- target
--
-- A write that brings in a second relation -- an UPDATE with a FROM, a DELETE
-- with a USING, a MERGE with its source -- reads its RETURNING list in the
-- scope of both. So a bare name only the second relation holds answers to that
-- relation, a bare name both hold answers to neither, and a bare name neither
-- holds is reported as missing. The second relation supplies its names however
-- it was written: a table, a derived query, a VALUES list, a set-returning
-- call.
-- ============================================================================

CREATE TABLE rsr_x (i int PRIMARY KEY, v text, c text);
CREATE TABLE rsr_xs (j int, w text, c text);
CREATE TABLE rsr_xt (k int, y text);
INSERT INTO rsr_x VALUES (1,'a','L1'),(2,'b','L2'),(3,'c','L3');
INSERT INTO rsr_xs VALUES (1,'p','R1'),(2,'q','R2'),(4,'r','R4');
INSERT INTO rsr_xt VALUES (1,'P'),(2,'Q');

-- ============================================================================
-- UPDATE ... FROM: the FROM relation's own columns are in scope
-- ============================================================================

-- begin-expected
-- columns: i | w
-- row: 1 | p
-- row: 2 | q
-- end-expected
UPDATE rsr_x t SET v='z' FROM rsr_xs u WHERE t.i = u.j RETURNING i, w;

-- writing the same name out against the relation answers the same
-- begin-expected
-- columns: i | w
-- row: 1 | p
-- row: 2 | q
-- end-expected
UPDATE rsr_x t SET v='y' FROM rsr_xs u WHERE t.i = u.j RETURNING i, u.w;

-- begin-expected
-- columns: i | j
-- row: 1 | 1
-- row: 2 | 2
-- end-expected
UPDATE rsr_x t SET v='y' FROM rsr_xs u WHERE t.i = u.j RETURNING i, j;

-- and it is a name under an operator just as much as a name on its own
-- begin-expected
-- columns: i | ?column?
-- row: 1 | p!
-- row: 2 | q!
-- end-expected
UPDATE rsr_x t SET v='z' FROM rsr_xs u WHERE t.i = u.j RETURNING i, w || '!';

-- two second relations, each supplying its own
-- begin-expected
-- columns: i | w | y
-- row: 1 | p | P
-- row: 2 | q | Q
-- end-expected
UPDATE rsr_x t SET v='z' FROM rsr_xs u, rsr_xt s WHERE t.i = u.j AND t.i = s.k RETURNING i, w, y;

-- A name both relations hold answers to neither.
-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "c" is ambiguous
-- end-expected-error
UPDATE rsr_x t SET v='y' FROM rsr_xs u WHERE t.i = u.j RETURNING i, c;

-- A name neither holds is still reported, whether or not a row is written.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" does not exist
-- end-expected-error
UPDATE rsr_x t SET v='y' FROM rsr_xs u WHERE t.i = u.j RETURNING i, nosuch;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" does not exist
-- end-expected-error
UPDATE rsr_x t SET v='y' FROM rsr_xs u WHERE false RETURNING i, nosuch;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" does not exist
-- end-expected-error
UPDATE rsr_x SET v='z' RETURNING nosuch;

-- A pairing that reaches no row still answers with the clause's own columns.
-- begin-expected
-- columns: i | w
-- end-expected
UPDATE rsr_x t SET v='y' FROM rsr_xs u WHERE false RETURNING i, w;

-- ============================================================================
-- A derived FROM item, a VALUES list and a call supply names the same way
-- ============================================================================

-- begin-expected
-- columns: i | w
-- row: 1 | p
-- row: 2 | q
-- end-expected
UPDATE rsr_x t SET v='z' FROM (SELECT j, w FROM rsr_xs) u WHERE t.i = u.j RETURNING i, w;

-- what such an item does not hold is reported the same way
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" does not exist
-- end-expected-error
UPDATE rsr_x t SET v='z' FROM (SELECT j, w FROM rsr_xs) u WHERE t.i = u.j RETURNING i, nosuch;

-- an item that produces no row at all supplies its names just the same
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" does not exist
-- end-expected-error
UPDATE rsr_x t SET v='z' FROM (SELECT j, w FROM rsr_xs WHERE false) u WHERE t.i = u.j RETURNING i, nosuch;

-- begin-expected
-- columns: i | w
-- row: 1 | k
-- end-expected
UPDATE rsr_x t SET v='z' FROM (VALUES (1,'k')) u(j,w) WHERE t.i = u.j RETURNING i, w;

-- begin-expected
-- columns: i | g
-- row: 1 | 1
-- row: 2 | 2
-- end-expected
UPDATE rsr_x t SET v='z' FROM generate_series(1,2) g WHERE t.i = g RETURNING i, g;

-- ============================================================================
-- DELETE ... USING reads its USING relation the same way
-- ============================================================================

-- begin-expected
-- columns: i | w
-- row: 1 | p
-- row: 2 | q
-- end-expected
DELETE FROM rsr_x t USING rsr_xs u WHERE t.i = u.j RETURNING i, w;

INSERT INTO rsr_x VALUES (1,'a','L1'),(2,'b','L2');

-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "c" is ambiguous
-- end-expected-error
DELETE FROM rsr_x t USING rsr_xs u WHERE t.i = u.j RETURNING i, c;

-- begin-expected
-- columns: i | j
-- row: 1 | 1
-- row: 2 | 2
-- end-expected
DELETE FROM rsr_x t USING (SELECT j FROM rsr_xs) u WHERE t.i = u.j RETURNING i, j;

INSERT INTO rsr_x VALUES (1,'a','L1'),(2,'b','L2');

-- ============================================================================
-- MERGE reads its source the same way, in every arm
-- ============================================================================

-- begin-expected
-- columns: i | w
-- row: 1 | p
-- row: 2 | q
-- end-expected
MERGE INTO rsr_x t USING rsr_xs u ON t.i = u.j WHEN MATCHED THEN UPDATE SET v='m' RETURNING i, w;

-- begin-expected
-- columns: i | j
-- row: 1 | 1
-- row: 2 | 2
-- end-expected
MERGE INTO rsr_x t USING rsr_xs u ON t.i = u.j WHEN MATCHED THEN UPDATE SET v='m' RETURNING i, j;

-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "c" is ambiguous
-- end-expected-error
MERGE INTO rsr_x t USING rsr_xs u ON t.i = u.j WHEN MATCHED THEN UPDATE SET v='m' RETURNING i, c;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuch" does not exist
-- end-expected-error
MERGE INTO rsr_x t USING rsr_xs u ON t.i = u.j WHEN MATCHED THEN UPDATE SET v='m' RETURNING i, nosuch;

-- A target row no source row paired with reads the source's column as nothing.
-- begin-expected
-- columns: i | w
-- row: 3 | NULL
-- end-expected
MERGE INTO rsr_x t USING rsr_xs u ON t.i = u.j WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v='s' RETURNING i, w;

-- begin-expected
-- columns: i | w
-- row: 4 | r
-- end-expected
MERGE INTO rsr_x t USING rsr_xs u ON t.i = u.j WHEN NOT MATCHED THEN INSERT VALUES (u.j, u.w) RETURNING i, w;

-- An arm that writes nothing answers with the clause's own columns.
-- begin-expected
-- columns: i | w
-- end-expected
MERGE INTO rsr_x t USING rsr_xs u ON t.i = u.j WHEN MATCHED THEN DO NOTHING RETURNING i, w;

-- begin-expected
-- columns: i | w
-- row: 1 | p
-- row: 2 | q
-- row: 4 | r
-- end-expected
MERGE INTO rsr_x t USING rsr_xs u ON t.i = u.j WHEN MATCHED THEN DELETE RETURNING i, w;

-- begin-expected
-- columns: i | v | c
-- row: 3 | s | L3
-- end-expected
SELECT i, v, c FROM rsr_x ORDER BY i;

INSERT INTO rsr_x VALUES (1,'a','L1'),(2,'b','L2');

-- A source that is a derived relation or a VALUES list supplies its names too.
-- begin-expected
-- columns: i | w
-- row: 1 | p
-- row: 2 | q
-- end-expected
MERGE INTO rsr_x t USING (SELECT j, w FROM rsr_xs) u ON t.i = u.j WHEN MATCHED THEN UPDATE SET v='m' RETURNING i, w;

-- begin-expected
-- columns: i | w
-- row: 1 | k
-- end-expected
MERGE INTO rsr_x t USING (VALUES (1,'k')) u(j,w) ON t.i = u.j WHEN MATCHED THEN UPDATE SET v='n' RETURNING i, w;

-- A source that is not a relation at all is reported ahead of the missing name.
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "rsr_nosuch" does not exist
-- end-expected-error
MERGE INTO rsr_x t USING rsr_nosuch u ON t.i = u.j WHEN MATCHED THEN UPDATE SET v='m' RETURNING nosuch;

-- begin-expected
-- columns: i | v | c
-- row: 1 | n | L1
-- row: 2 | m | L2
-- row: 3 | s | L3
-- end-expected
SELECT i, v, c FROM rsr_x ORDER BY i;

DROP TABLE rsr_x;
DROP TABLE rsr_xs;
DROP TABLE rsr_xt;
