-- ============================================================================
-- A bare RETURNING name that both relations of a write hold
--
-- The RETURNING clause of a write that brings in a second relation stands in
-- the scope of both of them, so a column name they both hold answers to
-- neither: PostgreSQL refuses to choose and raises 42702. It reads the clause
-- while it analyses the statement, so the refusal is owed whether or not a row
-- would have been written, whichever arm of a MERGE would have written it, and
-- even when the second relation holds no row at all.
--
-- Writing the relation the name belongs to settles it, a second relation
-- holding neither name was never in question, and a name a sub-select's own
-- FROM supplies is judged against that FROM instead.
--
-- Every value below was measured against PostgreSQL 18.
-- ============================================================================

-- setup
CREATE TABLE zzbr_k (i int PRIMARY KEY, v text);
CREATE TABLE zzbr_ks (i int, v text);
CREATE TABLE zzbr_ke (i int, v text);
CREATE TABLE zzbr_kd (j int, w text);
INSERT INTO zzbr_k VALUES (1,'a'),(2,'b'),(3,'c');
INSERT INTO zzbr_ks VALUES (1,'x'),(2,'y'),(4,'z');
INSERT INTO zzbr_kd VALUES (1,'x'),(2,'y');

-- ============================================================================
-- Every arm of a MERGE is read the same way
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "i" is ambiguous
-- end-expected-error
MERGE INTO zzbr_k t USING zzbr_ks u ON t.i = u.i WHEN MATCHED THEN DELETE RETURNING i, v;

-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "i" is ambiguous
-- end-expected-error
MERGE INTO zzbr_k t USING zzbr_ks u ON t.i = u.i WHEN MATCHED THEN UPDATE SET v = u.v RETURNING i, v;

-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "i" is ambiguous
-- end-expected-error
MERGE INTO zzbr_k t USING zzbr_ks u ON t.i = u.i WHEN NOT MATCHED THEN INSERT VALUES (u.i, u.v) RETURNING i, v;

-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "i" is ambiguous
-- end-expected-error
MERGE INTO zzbr_k t USING zzbr_ks u ON t.i = u.i WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = 'q' RETURNING i, v;

-- An arm that writes nothing is refused just the same: the clause is read while
-- the statement is analysed, before any row is reached.
-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "i" is ambiguous
-- end-expected-error
MERGE INTO zzbr_k t USING zzbr_ks u ON t.i = u.i WHEN MATCHED THEN DO NOTHING RETURNING i;

-- and so is one whose ON condition pairs nothing
-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "i" is ambiguous
-- end-expected-error
MERGE INTO zzbr_k t USING zzbr_ks u ON t.i = u.i AND t.i > 100 WHEN MATCHED THEN DELETE RETURNING i;

-- and so is one whose source holds no row at all
-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "i" is ambiguous
-- end-expected-error
MERGE INTO zzbr_k t USING zzbr_ke u ON t.i = u.i WHEN MATCHED THEN DELETE RETURNING i;

-- ============================================================================
-- However the second relation was brought in
-- ============================================================================

-- a source that is a query of its own brings its columns in the same way
-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "i" is ambiguous
-- end-expected-error
MERGE INTO zzbr_k t USING (SELECT i, v FROM zzbr_ks) u ON t.i = u.i WHEN MATCHED THEN DO NOTHING RETURNING i;

-- as does a VALUES list under a column alias list
-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "i" is ambiguous
-- end-expected-error
MERGE INTO zzbr_k t USING (VALUES (1,'x')) AS u(i,v) ON t.i = u.i WHEN MATCHED THEN DO NOTHING RETURNING i;

-- and so does the target itself, read a second time under another name
-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "i" is ambiguous
-- end-expected-error
MERGE INTO zzbr_k t USING zzbr_k u ON t.i = u.i WHEN MATCHED THEN DO NOTHING RETURNING i;

-- ============================================================================
-- Where the name stands in the clause makes no difference
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "v" is ambiguous
-- end-expected-error
MERGE INTO zzbr_k t USING zzbr_ks u ON t.i = u.i WHEN MATCHED THEN DELETE RETURNING v;

-- a name under an operator is read no differently
-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "i" is ambiguous
-- end-expected-error
MERGE INTO zzbr_k t USING zzbr_ks u ON t.i = u.i WHEN MATCHED THEN DELETE RETURNING i + 1;

-- nor is one standing beside a clause the statement did settle
-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "i" is ambiguous
-- end-expected-error
MERGE INTO zzbr_k t USING zzbr_ks u ON t.i = u.i WHEN MATCHED THEN DELETE RETURNING merge_action(), i;

-- ============================================================================
-- The same rule on the ordinary UPDATE and DELETE paths
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "i" is ambiguous
-- end-expected-error
UPDATE zzbr_k t SET v = 'p' FROM zzbr_ks u WHERE t.i = u.i RETURNING i, v;

-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "i" is ambiguous
-- end-expected-error
UPDATE zzbr_k t SET v = 'p' FROM zzbr_ks u WHERE t.i = u.i AND t.i > 100 RETURNING i;

-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "i" is ambiguous
-- end-expected-error
UPDATE zzbr_k t SET v = 'p' FROM zzbr_ke u WHERE t.i = u.i RETURNING i;

-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "i" is ambiguous
-- end-expected-error
DELETE FROM zzbr_k t USING zzbr_ks u WHERE t.i = u.i RETURNING i, v;

-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "i" is ambiguous
-- end-expected-error
DELETE FROM zzbr_k t USING zzbr_ks u WHERE t.i = u.i AND t.i > 100 RETURNING i;

-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "i" is ambiguous
-- end-expected-error
DELETE FROM zzbr_k t USING zzbr_ke u WHERE t.i = u.i RETURNING i;

-- Nothing any of the refusals above wrote or took away.
-- begin-expected
-- columns: i | v
-- row: 1, a
-- row: 2, b
-- row: 3, c
-- end-expected
SELECT i, v FROM zzbr_k ORDER BY i;

-- ============================================================================
-- What the rule does not reach
-- ============================================================================

-- a second relation holding neither name
-- begin-expected
-- columns: i
-- end-expected
UPDATE zzbr_k t SET v = 'p' FROM zzbr_kd d WHERE t.i = d.j AND t.i > 100 RETURNING i;

-- begin-expected
-- columns: i
-- end-expected
DELETE FROM zzbr_k t USING zzbr_kd d WHERE t.i = d.j AND t.i > 100 RETURNING i;

-- begin-expected
-- columns: i
-- end-expected
MERGE INTO zzbr_k t USING zzbr_kd d ON t.i = d.j WHEN MATCHED THEN DO NOTHING RETURNING i;

-- a name a sub-select's own FROM supplies is judged against that
-- begin-expected
-- columns: m
-- end-expected
UPDATE zzbr_k t SET v = 'p' FROM zzbr_ks u WHERE t.i = u.i AND t.i > 100 RETURNING (SELECT max(i) FROM zzbr_ke) AS m;

-- a name written out against one of the two relations
-- begin-expected
-- columns: i | v
-- row: 1, a
-- row: 2, b
-- end-expected
DELETE FROM zzbr_k t USING zzbr_ks u WHERE t.i = u.i RETURNING t.i, t.v;

-- a write with no second relation reads its own column as it always did
-- begin-expected
-- columns: i | v
-- row: 3, c
-- end-expected
DELETE FROM zzbr_k WHERE i = 3 RETURNING i, v;

-- begin-expected
-- columns: count
-- row: 0
-- end-expected
SELECT count(*) FROM zzbr_k;

-- teardown
DROP TABLE zzbr_k;
DROP TABLE zzbr_ks;
DROP TABLE zzbr_ke;
DROP TABLE zzbr_kd;
