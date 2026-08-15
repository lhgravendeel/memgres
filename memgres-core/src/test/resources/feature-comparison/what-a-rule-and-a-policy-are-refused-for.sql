-- ============================================================================
-- What a rule and a policy are refused for
--
-- A rule's RETURNING list has to describe the relation the rule is on, and a
-- view's columns are its own rather than those of the relation the action
-- writes to. An UPDATE event carries the row as it was and the row as it will
-- be, so a column written in the rule's own qualification with no row named in
-- front of it answers to both at once. A policy decides which of a relation's
-- rows a role may see, and only a table has rows of its own to decide about.
-- DROP POLICY names one policy. And a view carrying an INSTEAD rule that
-- speaks only for some of its rows cannot be written through at all.
--
-- Every value below was read off PostgreSQL 18.
-- ============================================================================
CREATE TABLE zzm3sr_wd (i int, j int, k int);
CREATE VIEW zzm3sr_wv AS SELECT i, j, k FROM zzm3sr_wd;

-- A RETURNING list is matched against the relation the rule is on, which here
-- is the view: two entries where the view has three columns is too few, and
-- four is too many. PostgreSQL sends neither a DETAIL nor a HINT with either.

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: RETURNING list has too few entries
-- end-expected-error
CREATE RULE zzm3sr_wr1 AS ON DELETE TO zzm3sr_wv DO INSTEAD DELETE FROM zzm3sr_wd WHERE i = old.i RETURNING i, j;

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: RETURNING list has too many entries
-- end-expected-error
CREATE RULE zzm3sr_wr3 AS ON DELETE TO zzm3sr_wv DO INSTEAD DELETE FROM zzm3sr_wd WHERE i = old.i RETURNING i, j, k, 1;

-- One entry per column of the view is taken, and nothing the refusals named
-- was written.
CREATE RULE zzm3sr_wr2 AS ON DELETE TO zzm3sr_wv DO INSTEAD DELETE FROM zzm3sr_wd WHERE i = old.i RETURNING i, j, k;

-- begin-expected
-- columns: rulename
-- row: zzm3sr_wr2
-- end-expected
SELECT rulename FROM pg_rules WHERE tablename = 'zzm3sr_wv' ORDER BY rulename;

DROP VIEW zzm3sr_wv;
DROP TABLE zzm3sr_wd;

-- ============================================================================
-- The entries are matched to the view's columns one by one, by type
-- ============================================================================
CREATE TABLE zzm3sr_td (i int, s text, k int);
CREATE VIEW zzm3sr_vd AS SELECT i, s FROM zzm3sr_td;

-- A star stands for the columns of the relation the action writes to, and
-- there are more of them than the view has.

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: RETURNING list has too many entries
-- end-expected-error
CREATE RULE zzm3sr_y2 AS ON DELETE TO zzm3sr_vd DO INSTEAD DELETE FROM zzm3sr_td WHERE i = old.i RETURNING *;

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: RETURNING list has too few entries
-- end-expected-error
CREATE RULE zzm3sr_y4 AS ON UPDATE TO zzm3sr_vd DO INSTEAD UPDATE zzm3sr_td SET s = new.s WHERE i = old.i RETURNING i;

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: RETURNING list's entry 2 has different type from column "s"
-- detail-like: RETURNING list entry has type integer, but column has type text.
-- end-expected-error
CREATE RULE zzm3sr_y1 AS ON DELETE TO zzm3sr_vd DO INSTEAD DELETE FROM zzm3sr_td WHERE i = old.i RETURNING i, k;

-- OLD and NEW are the view's own rows, so a list written from them fits it.
CREATE RULE zzm3sr_y3 AS ON DELETE TO zzm3sr_vd DO INSTEAD DELETE FROM zzm3sr_td WHERE i = old.i RETURNING old.i, old.s;
CREATE RULE zzm3sr_y5 AS ON INSERT TO zzm3sr_vd DO INSTEAD INSERT INTO zzm3sr_td VALUES (new.i, new.s) RETURNING i, s;

-- begin-expected
-- columns: rulename
-- row: zzm3sr_y3
-- row: zzm3sr_y5
-- end-expected
SELECT rulename FROM pg_rules WHERE tablename = 'zzm3sr_vd' ORDER BY rulename;

DROP VIEW zzm3sr_vd;
DROP TABLE zzm3sr_td;

-- ============================================================================
-- A bare column in an UPDATE rule's own qualification
-- ============================================================================
CREATE TABLE zzm3sr_qt (i int, s text);

-- An UPDATE has both rows in scope for its qualification, so neither of them
-- is the one a bare column names.

-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "i" is ambiguous
-- end-expected-error
CREATE RULE zzm3sr_q1 AS ON UPDATE TO zzm3sr_qt WHERE i <> 0 DO ALSO NOTHING;

-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "s" is ambiguous
-- end-expected-error
CREATE RULE zzm3sr_q8 AS ON UPDATE TO zzm3sr_qt WHERE s <> 'x' DO INSTEAD NOTHING;

-- The same wherever in the qualification it stands, and inside a call as much
-- as beside a comparison.

-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "i" is ambiguous
-- end-expected-error
CREATE RULE zzm3sr_q9 AS ON UPDATE TO zzm3sr_qt WHERE i <> 0 AND old.s = 'a' DO ALSO NOTHING;

-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "s" is ambiguous
-- end-expected-error
CREATE RULE zzm3sr_qa AS ON UPDATE TO zzm3sr_qt WHERE upper(s) = 'A' DO ALSO NOTHING;

-- A column read as a condition of its own is resolved before it is asked to be
-- a boolean.

-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "i" is ambiguous
-- end-expected-error
CREATE RULE zzm3sr_q5 AS ON UPDATE TO zzm3sr_qt WHERE i DO ALSO NOTHING;

-- The qualification is read before the actions are.

-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "i" is ambiguous
-- end-expected-error
CREATE RULE zzm3sr_q6 AS ON UPDATE TO zzm3sr_qt WHERE i <> 0 DO ALSO INSERT INTO zzm3sr_nn VALUES (1);

-- A system column stands in both rows too.

-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "xmin" is ambiguous
-- end-expected-error
CREATE RULE zzm3sr_qx AS ON UPDATE TO zzm3sr_qt WHERE xmin IS NOT NULL DO ALSO NOTHING;

-- A name no row holds is missing rather than ambiguous, and a row named in
-- front of it settles which row it is.

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
CREATE RULE zzm3sr_q4 AS ON UPDATE TO zzm3sr_qt WHERE nosuchcol <> 0 DO ALSO NOTHING;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column old.nosuch does not exist
-- end-expected-error
CREATE RULE zzm3sr_q7 AS ON UPDATE TO zzm3sr_qt WHERE old.nosuch = 1 AND i = 1 DO ALSO NOTHING;

-- An INSERT carries only the new row and a DELETE only the old one, so the
-- same column written there is that row's; a query inside the qualification
-- reads its own FROM.
CREATE RULE zzm3sr_q2 AS ON UPDATE TO zzm3sr_qt WHERE old.i <> 0 DO ALSO NOTHING;
CREATE RULE zzm3sr_q3 AS ON INSERT TO zzm3sr_qt WHERE i <> 0 DO ALSO NOTHING;
CREATE RULE zzm3sr_qb AS ON DELETE TO zzm3sr_qt WHERE i <> 0 DO ALSO NOTHING;
CREATE RULE zzm3sr_qc AS ON UPDATE TO zzm3sr_qt WHERE (SELECT count(*) FROM zzm3sr_qt) > 0 DO ALSO NOTHING;
CREATE RULE zzm3sr_qd AS ON UPDATE TO zzm3sr_qt WHERE EXISTS (SELECT 1 FROM zzm3sr_qt z WHERE z.i = i) DO ALSO NOTHING;

-- begin-expected
-- columns: rulename
-- row: zzm3sr_q2
-- row: zzm3sr_q3
-- row: zzm3sr_qb
-- row: zzm3sr_qc
-- row: zzm3sr_qd
-- end-expected
SELECT rulename FROM pg_rules WHERE tablename = 'zzm3sr_qt' ORDER BY rulename;

-- A rule on a view is read the same way: both of its rows are the view's.
CREATE VIEW zzm3sr_qw AS SELECT i, s FROM zzm3sr_qt;

-- begin-expected-error
-- sqlstate: 42702
-- message-like: column reference "i" is ambiguous
-- end-expected-error
CREATE RULE zzm3sr_w1 AS ON UPDATE TO zzm3sr_qw WHERE i <> 0 DO ALSO NOTHING;

DROP VIEW zzm3sr_qw;
DROP TABLE zzm3sr_qt CASCADE;

-- ============================================================================
-- What relation a policy may be put on
-- ============================================================================
CREATE TABLE zzm3sr_pp (i int, s text);
CREATE SEQUENCE zzm3sr_sq;
CREATE VIEW zzm3sr_pv AS SELECT i FROM zzm3sr_pp;
CREATE MATERIALIZED VIEW zzm3sr_pm AS SELECT i FROM zzm3sr_pp;
CREATE INDEX zzm3sr_pi ON zzm3sr_pp (i);
CREATE TYPE zzm3sr_ct AS (a int, b text);

-- Only a table has rows of its own for a policy to decide about, and the
-- relation is named without its schema in that refusal.

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zzm3sr_sq" is not a table
-- end-expected-error
CREATE POLICY zzm3sr_po ON zzm3sr_sq USING (true);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zzm3sr_pv" is not a table
-- end-expected-error
CREATE POLICY zzm3sr_po ON zzm3sr_pv USING (true);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zzm3sr_pm" is not a table
-- end-expected-error
CREATE POLICY zzm3sr_po ON zzm3sr_pm USING (true);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zzm3sr_pi" is not a table
-- end-expected-error
CREATE POLICY zzm3sr_po ON zzm3sr_pi USING (true);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "zzm3sr_ct" is not a table
-- end-expected-error
CREATE POLICY zzm3sr_po ON zzm3sr_ct USING (true);

-- A name that reaches nothing is a missing relation, whatever kind was wanted.

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "zzm3sr_nn" does not exist
-- end-expected-error
CREATE POLICY zzm3sr_po ON zzm3sr_nn USING (true);

-- A table takes one, and none of the refusals above left anything behind.
CREATE POLICY zzm3sr_po ON zzm3sr_pp USING (true);

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_policies WHERE policyname = 'zzm3sr_po';

DROP POLICY zzm3sr_po ON zzm3sr_pp;
DROP TYPE zzm3sr_ct;
DROP INDEX zzm3sr_pi;
DROP MATERIALIZED VIEW zzm3sr_pm;
DROP VIEW zzm3sr_pv;
DROP SEQUENCE zzm3sr_sq;
DROP TABLE zzm3sr_pp;

-- A partitioned table and a partition of it each take one of their own.
CREATE TABLE zzm3sr_pt (id int, a int) PARTITION BY RANGE (id);
CREATE TABLE zzm3sr_p1 PARTITION OF zzm3sr_pt FOR VALUES FROM (0) TO (10);
CREATE POLICY zzm3sr_pq ON zzm3sr_pt USING (true);
CREATE POLICY zzm3sr_pr ON zzm3sr_p1 USING (true);

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*)::int AS n FROM pg_policies WHERE policyname IN ('zzm3sr_pq','zzm3sr_pr');

DROP POLICY zzm3sr_pq ON zzm3sr_pt;
DROP POLICY zzm3sr_pr ON zzm3sr_p1;
DROP TABLE zzm3sr_pt;

-- ============================================================================
-- DROP POLICY names one policy
-- ============================================================================
CREATE TABLE zzm3sr_dp (i int, s text);
CREATE POLICY zzm3sr_x1 ON zzm3sr_dp USING (true);
CREATE POLICY zzm3sr_x2 ON zzm3sr_dp USING (true);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ","
-- end-expected-error
DROP POLICY zzm3sr_x1 ON zzm3sr_dp, zzm3sr_x2 ON zzm3sr_dp;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ","
-- end-expected-error
DROP POLICY zzm3sr_x1 ON zzm3sr_dp, zzm3sr_x2;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ","
-- end-expected-error
DROP POLICY IF EXISTS zzm3sr_x1 ON zzm3sr_dp, zzm3sr_x2 ON zzm3sr_dp;

-- The comma is refused before the relation is looked for, so a relation that
-- is not there is not what is reported.

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near ","
-- end-expected-error
DROP POLICY zzm3sr_x1 ON zzm3sr_nn, zzm3sr_x2 ON zzm3sr_nn;

-- Nothing of the list was taken down.

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*)::int AS n FROM pg_policies WHERE tablename = 'zzm3sr_dp';

DROP POLICY zzm3sr_x1 ON zzm3sr_dp;
DROP POLICY zzm3sr_x2 ON zzm3sr_dp;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_policies WHERE tablename = 'zzm3sr_dp';

DROP TABLE zzm3sr_dp;

-- ============================================================================
-- A view carrying only a conditional DO INSTEAD rule
-- ============================================================================
CREATE TABLE zzm3sr_bs (id int, a int, b text);
INSERT INTO zzm3sr_bs VALUES (1, 10, 'x');
INSERT INTO zzm3sr_bs VALUES (2, 20, 'y');
CREATE VIEW zzm3sr_cv AS SELECT id, a, b FROM zzm3sr_bs;
CREATE RULE zzm3sr_cr AS ON UPDATE TO zzm3sr_cv WHERE old.id = 1 DO INSTEAD UPDATE zzm3sr_bs SET a = new.a WHERE id = old.id;

-- The rows the rule does not claim would have to be written to the view
-- itself, so the write is refused outright, with a DETAIL and a HINT.

-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot update view "zzm3sr_cv"
-- detail-like: Views with conditional DO INSTEAD rules are not automatically updatable.
-- hint-like: To enable updating the view, provide an INSTEAD OF UPDATE trigger or an unconditional ON UPDATE DO INSTEAD rule.
-- end-expected-error
UPDATE zzm3sr_cv SET a = DEFAULT;

-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot update view "zzm3sr_cv"
-- detail-like: Views with conditional DO INSTEAD rules are not automatically updatable.
-- end-expected-error
UPDATE zzm3sr_cv SET a = 99;

-- The refusal comes ahead of the RETURNING list a conditional rule cannot
-- answer.

-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot update view "zzm3sr_cv"
-- detail-like: Views with conditional DO INSTEAD rules are not automatically updatable.
-- end-expected-error
UPDATE zzm3sr_cv SET a = 1 RETURNING id;

-- Nothing was written.

-- begin-expected
-- columns: id | a | b
-- row: 1 | 10 | x
-- row: 2 | 20 | y
-- end-expected
SELECT id, a, b FROM zzm3sr_bs ORDER BY id;

-- The event the rule is on is the only one refused: a DELETE the view carries
-- no rule for is still rewritten onto the relation behind it.
DELETE FROM zzm3sr_cv WHERE id = 2;

-- begin-expected
-- columns: id | a | b
-- row: 1 | 10 | x
-- end-expected
SELECT id, a, b FROM zzm3sr_bs ORDER BY id;

-- The catalogue does not read the rule: it answers for the view's shape alone.

-- begin-expected
-- columns: u
-- row: 28
-- end-expected
SELECT pg_relation_is_updatable('zzm3sr_cv'::regclass, false) AS u;

-- begin-expected
-- columns: is_updatable | is_insertable_into
-- row: YES | YES
-- end-expected
SELECT is_updatable, is_insertable_into FROM information_schema.views WHERE table_name = 'zzm3sr_cv';

-- An INSERT and a DELETE are refused the same way once the rule is theirs.
CREATE RULE zzm3sr_ci AS ON INSERT TO zzm3sr_cv WHERE new.id = 1 DO INSTEAD INSERT INTO zzm3sr_bs VALUES (new.id, new.a, new.b);

-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot insert into view "zzm3sr_cv"
-- detail-like: Views with conditional DO INSTEAD rules are not automatically updatable.
-- hint-like: To enable inserting into the view, provide an INSTEAD OF INSERT trigger or an unconditional ON INSERT DO INSTEAD rule.
-- end-expected-error
INSERT INTO zzm3sr_cv VALUES (7, 70, 'q');

CREATE RULE zzm3sr_cd AS ON DELETE TO zzm3sr_cv WHERE old.id = 1 DO INSTEAD DELETE FROM zzm3sr_bs WHERE id = old.id;

-- begin-expected-error
-- sqlstate: 55000
-- message-like: cannot delete from view "zzm3sr_cv"
-- detail-like: Views with conditional DO INSTEAD rules are not automatically updatable.
-- hint-like: To enable deleting from the view, provide an INSTEAD OF DELETE trigger or an unconditional ON DELETE DO INSTEAD rule.
-- end-expected-error
DELETE FROM zzm3sr_cv WHERE id = 1;

-- begin-expected
-- columns: id | a | b
-- row: 1 | 10 | x
-- end-expected
SELECT id, a, b FROM zzm3sr_bs ORDER BY id;

-- A rule that runs beside the statement rather than in place of it leaves the
-- view as writable as it was, so taking the conditional INSTEAD rule off makes
-- the write go again.
CREATE RULE zzm3sr_ca AS ON UPDATE TO zzm3sr_cv WHERE old.id = 1 DO ALSO INSERT INTO zzm3sr_bs VALUES (9, 9, 'z');
DROP RULE zzm3sr_cr ON zzm3sr_cv;
UPDATE zzm3sr_cv SET a = 5;

-- begin-expected
-- columns: id | a | b
-- row: 1 | 5 | x
-- row: 9 | 5 | z
-- end-expected
SELECT id, a, b FROM zzm3sr_bs ORDER BY id, b;

DROP VIEW zzm3sr_cv;
DROP TABLE zzm3sr_bs;

-- A conditional INSTEAD rule on a table is not touched by any of this: it
-- claims the rows it matches and leaves the rest to the statement.
CREATE TABLE zzm3sr_tb (id int, a int);
CREATE TABLE zzm3sr_tl (id int, a int);
INSERT INTO zzm3sr_tb VALUES (1, 1);
INSERT INTO zzm3sr_tb VALUES (2, 2);
CREATE RULE zzm3sr_tr AS ON UPDATE TO zzm3sr_tb WHERE old.id = 1 DO INSTEAD INSERT INTO zzm3sr_tl VALUES (old.id, new.a);
UPDATE zzm3sr_tb SET a = 8;

-- begin-expected
-- columns: id | a
-- row: 1 | 1
-- row: 2 | 8
-- end-expected
SELECT id, a FROM zzm3sr_tb ORDER BY id;

-- begin-expected
-- columns: id | a
-- row: 1 | 8
-- end-expected
SELECT id, a FROM zzm3sr_tl ORDER BY id;

DROP TABLE zzm3sr_tb;
DROP TABLE zzm3sr_tl;
