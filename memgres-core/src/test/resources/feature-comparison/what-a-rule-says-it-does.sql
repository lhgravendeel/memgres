-- ============================================================================
-- What a rule says it does: the stored definition is the tree the action was
-- analysed into, not the text it was typed as
--
-- pg_get_ruledef, and the pg_rules.definition that reads it, deparse the analysed
-- action: the schema comes off wherever the reader's search path reaches the
-- relation, a column written bare comes back under the relation it resolved to,
-- and every constant carries the type its column read it as. Every value below
-- was read off PostgreSQL 18.
-- ============================================================================
CREATE TABLE wrs_k (i int, j int, s text);
CREATE TABLE wrs_d (i int, j int, s text);
CREATE SCHEMA wrs_sc;
CREATE TABLE wrs_sc.wrs_c (i int, j int);

-- A DELETE action loses the qualifier the reader's search path reaches, and the
-- columns of its qualification come back under the relation they resolved to.
CREATE RULE wrs_r1 AS ON DELETE TO wrs_k DO ALSO DELETE FROM public.wrs_d WHERE i = old.i;

-- begin-expected
-- columns: d
-- row: CREATE RULE wrs_r1 AS~    ON DELETE TO public.wrs_k DO  DELETE FROM wrs_d~  WHERE (wrs_d.i = old.i);
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'wrs_r1';

-- An action with no qualification has no line after the relation.
CREATE RULE wrs_r2 AS ON DELETE TO wrs_k DO ALSO DELETE FROM wrs_d;

-- begin-expected
-- columns: d
-- row: CREATE RULE wrs_r2 AS~    ON DELETE TO public.wrs_k DO  DELETE FROM wrs_d;
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'wrs_r2';

-- ONLY is kept; an alias is kept and the AS that introduced it is not, because
-- what was analysed is the alias rather than the word in front of it.
CREATE RULE wrs_r3 AS ON DELETE TO wrs_k DO ALSO DELETE FROM ONLY wrs_d AS z WHERE z.i = old.i;

-- begin-expected
-- columns: d
-- row: CREATE RULE wrs_r3 AS~    ON DELETE TO public.wrs_k DO  DELETE FROM ONLY wrs_d z~  WHERE (z.i = old.i);
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'wrs_r3';

-- IN becomes = ANY (ARRAY[...]) and IS NULL is bracketed, as everywhere else the
-- analysed tree is printed.
CREATE RULE wrs_r4 AS ON DELETE TO wrs_k DO ALSO DELETE FROM wrs_d WHERE i IN (1,2) AND s IS NULL;

-- begin-expected
-- columns: d
-- row: CREATE RULE wrs_r4 AS~    ON DELETE TO public.wrs_k DO  DELETE FROM wrs_d~  WHERE ((wrs_d.i = ANY (ARRAY[1, 2])) AND (wrs_d.s IS NULL));
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'wrs_r4';

-- A schema the reader's search path does not reach stays in front of the relation,
-- and the columns are still written under the relation's bare name.
CREATE RULE wrs_r5 AS ON DELETE TO wrs_k DO ALSO DELETE FROM wrs_sc.wrs_c WHERE i = old.i;

-- begin-expected
-- columns: d
-- row: CREATE RULE wrs_r5 AS~    ON DELETE TO public.wrs_k DO  DELETE FROM wrs_sc.wrs_c~  WHERE (wrs_c.i = old.i);
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'wrs_r5';

-- An UPDATE action writes its assignments on one line, each value in the type the
-- column read it as, and the qualification on the next.
CREATE RULE wrs_u1 AS ON UPDATE TO wrs_k DO ALSO UPDATE wrs_d SET j = new.j, s = 'x' WHERE i = old.i AND s <> 'q';

-- begin-expected
-- columns: d
-- row: CREATE RULE wrs_u1 AS~    ON UPDATE TO public.wrs_k DO  UPDATE wrs_d SET j = new.j, s = 'x'::text~  WHERE ((wrs_d.i = old.i) AND (wrs_d.s <> 'q'::text));
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'wrs_u1';

-- An assignment reading the column it sets reads it under the relation's name.
CREATE RULE wrs_u2 AS ON UPDATE TO wrs_k DO ALSO UPDATE wrs_d SET j = j + 1;

-- begin-expected
-- columns: d
-- row: CREATE RULE wrs_u2 AS~    ON UPDATE TO public.wrs_k DO  UPDATE wrs_d SET j = (wrs_d.j + 1);
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'wrs_u2';

-- A call in an assignment reads its argument under the relation's name too.
CREATE RULE wrs_u3 AS ON UPDATE TO wrs_k DO ALSO UPDATE wrs_d SET s = upper(s) WHERE i = old.i;

-- begin-expected
-- columns: d
-- row: CREATE RULE wrs_u3 AS~    ON UPDATE TO public.wrs_k DO  UPDATE wrs_d SET s = upper(wrs_d.s)~  WHERE (wrs_d.i = old.i);
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'wrs_u3';

-- DEFAULT stands as the word it is, and a row the event has is read under its own
-- name wherever the assignment puts it.
CREATE RULE wrs_u4 AS ON UPDATE TO wrs_k DO ALSO UPDATE wrs_d SET j = DEFAULT WHERE i = old.i;
CREATE RULE wrs_u5 AS ON DELETE TO wrs_k DO ALSO UPDATE wrs_d SET s = old.s, j = old.j WHERE i = old.i;

-- begin-expected
-- columns: d
-- row: CREATE RULE wrs_u4 AS~    ON UPDATE TO public.wrs_k DO  UPDATE wrs_d SET j = DEFAULT~  WHERE (wrs_d.i = old.i);
-- row: CREATE RULE wrs_u5 AS~    ON DELETE TO public.wrs_k DO  UPDATE wrs_d SET s = old.s, j = old.j~  WHERE (wrs_d.i = old.i);
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules
  WHERE rulename IN ('wrs_u4','wrs_u5') ORDER BY rulename;

-- NOTHING is not a statement, so it is not written after the space a deparsed
-- statement brings with it: DO NOTHING, where an action gives DO  DELETE FROM.
CREATE RULE wrs_n1 AS ON DELETE TO wrs_k DO ALSO NOTHING;
CREATE RULE wrs_n2 AS ON UPDATE TO wrs_k DO INSTEAD NOTHING;

-- begin-expected
-- columns: d
-- row: CREATE RULE wrs_n1 AS~    ON DELETE TO public.wrs_k DO NOTHING;
-- row: CREATE RULE wrs_n2 AS~    ON UPDATE TO public.wrs_k DO INSTEAD NOTHING;
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules
  WHERE rulename IN ('wrs_n1','wrs_n2') ORDER BY rulename;

-- The rule's own qualification begins a line of its own, and is resolved against
-- the rows the event has: an INSERT has only the new one, a DELETE only the old
-- one, and an UPDATE both.
CREATE RULE wrs_n3 AS ON INSERT TO wrs_k WHERE i > 5 DO ALSO NOTHING;
CREATE RULE wrs_n4 AS ON DELETE TO wrs_k WHERE s = 'p' DO ALSO NOTHING;
CREATE RULE wrs_n5 AS ON UPDATE TO wrs_k WHERE new.i > old.i DO ALSO NOTHING;
CREATE RULE wrs_n6 AS ON INSERT TO wrs_k WHERE new.s IS NOT NULL AND new.j BETWEEN 1 AND 3 DO INSTEAD NOTHING;

-- begin-expected
-- columns: d
-- row: CREATE RULE wrs_n3 AS~    ON INSERT TO public.wrs_k~   WHERE (new.i > 5) DO NOTHING;
-- row: CREATE RULE wrs_n4 AS~    ON DELETE TO public.wrs_k~   WHERE (old.s = 'p'::text) DO NOTHING;
-- row: CREATE RULE wrs_n5 AS~    ON UPDATE TO public.wrs_k~   WHERE (new.i > old.i) DO NOTHING;
-- row: CREATE RULE wrs_n6 AS~    ON INSERT TO public.wrs_k~   WHERE ((new.s IS NOT NULL) AND ((new.j >= 1) AND (new.j <= 3))) DO INSTEAD NOTHING;
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules
  WHERE rulename IN ('wrs_n3','wrs_n4','wrs_n5','wrs_n6') ORDER BY rulename;

-- A rule whose action list holds several statements is written with a semicolon between them, and
-- this harness reads a file one semicolon at a time, so such a rule cannot be written here at all:
-- it would arrive at both engines cut in half. Both engines print it the same way -- the actions
-- wrapped in parentheses, each closed by its own semicolon, the closing parenthesis on a line of
-- its own -- and that is asserted over JDBC instead, where the statement arrives whole.

-- pg_get_ruledef reads the same text pg_rules does.

-- begin-expected
-- columns: d
-- row: CREATE RULE wrs_r1 AS~    ON DELETE TO public.wrs_k DO  DELETE FROM wrs_d~  WHERE (wrs_d.i = old.i);
-- end-expected
SELECT replace(pg_get_ruledef(oid), chr(10), '~') AS d FROM pg_rewrite WHERE rulename = 'wrs_r1';

-- A RETURNING list begins a line of its own and writes every item after the first
-- on a line of its own; a star is the columns it stood for, and a label the item
-- already answers to is not written out again.
CREATE VIEW wrs_v AS SELECT i, j, s FROM wrs_k;
CREATE RULE wrs_g1 AS ON DELETE TO wrs_v DO INSTEAD DELETE FROM wrs_d WHERE i = old.i RETURNING i, j, s;
CREATE RULE wrs_g2 AS ON UPDATE TO wrs_v DO INSTEAD UPDATE wrs_d SET j = new.j WHERE i = old.i RETURNING *;
CREATE RULE wrs_g3 AS ON INSERT TO wrs_v DO INSTEAD INSERT INTO wrs_d VALUES (new.i, new.j, new.s) RETURNING i, j AS jj, s;

-- begin-expected
-- columns: d
-- row: CREATE RULE wrs_g1 AS~    ON DELETE TO public.wrs_v DO INSTEAD  DELETE FROM wrs_d~  WHERE (wrs_d.i = old.i)~  RETURNING wrs_d.i,~    wrs_d.j,~    wrs_d.s;
-- row: CREATE RULE wrs_g2 AS~    ON UPDATE TO public.wrs_v DO INSTEAD  UPDATE wrs_d SET j = new.j~  WHERE (wrs_d.i = old.i)~  RETURNING wrs_d.i,~    wrs_d.j,~    wrs_d.s;
-- row: CREATE RULE wrs_g3 AS~    ON INSERT TO public.wrs_v DO INSTEAD  INSERT INTO wrs_d (i, j, s)~  VALUES (new.i, new.j, new.s)~  RETURNING wrs_d.i,~    wrs_d.j AS jj,~    wrs_d.s;
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules
  WHERE rulename IN ('wrs_g1','wrs_g2','wrs_g3') ORDER BY rulename;

-- A view's own ON SELECT rule is that view's query, deparsed the same way. It is
-- reached through pg_rewrite, because pg_rules leaves _RETURN out.

-- begin-expected
-- columns: d
-- row: CREATE RULE "_RETURN" AS~    ON SELECT TO public.wrs_v DO INSTEAD  SELECT i,~    j,~    s~   FROM wrs_k;
-- end-expected
SELECT replace(pg_get_ruledef(oid), chr(10), '~') AS d FROM pg_rewrite
  WHERE rulename = '_RETURN' AND ev_class = 'wrs_v'::regclass;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_rules WHERE rulename = '_RETURN';

-- A RETURNING list is only allowed where the rewrite can hand it back.

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: RETURNING lists are not supported in non-INSTEAD rules
-- end-expected-error
CREATE RULE wrs_g4 AS ON DELETE TO wrs_k DO ALSO DELETE FROM wrs_d WHERE i = old.i RETURNING i;

-- A relation the qualifier names is what the rule is filed under, and pg_rules
-- reports the schema holding it.
CREATE RULE wrs_q1 AS ON DELETE TO wrs_sc.wrs_c DO ALSO DELETE FROM public.wrs_d WHERE i = old.i;

-- begin-expected
-- columns: schemaname, tablename, d
-- row: wrs_sc, wrs_c, CREATE RULE wrs_q1 AS~    ON DELETE TO wrs_sc.wrs_c DO  DELETE FROM wrs_d~  WHERE (wrs_d.i = old.i);
-- end-expected
SELECT schemaname, tablename, replace(definition, chr(10), '~') AS d FROM pg_rules
  WHERE rulename = 'wrs_q1';

DROP RULE wrs_q1 ON wrs_sc.wrs_c;
DROP VIEW wrs_v;
DROP TABLE wrs_k CASCADE;
DROP TABLE wrs_d CASCADE;
DROP TABLE wrs_sc.wrs_c CASCADE;
DROP SCHEMA wrs_sc;
