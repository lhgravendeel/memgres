-- ============================================================================
-- A query inside a rule action is deparsed, not echoed
--
-- pg_get_ruledef, and the pg_rules.definition that reads it, write out the
-- whole analysed action. An action that only reads, an INSERT whose rows come
-- from a query, an UPDATE reading a second relation and every sub-link inside
-- an action's qualification are therefore laid out exactly as a stored query
-- is: the select list one item to a line, each clause keyword starting a line
-- of its own, and every column written under the relation it resolved to,
-- because the range table a rule is analysed against holds OLD and NEW beside
-- whatever the action reads. Every value below was read off PostgreSQL 18.
-- ============================================================================
CREATE TABLE zzm3sr_k (i int, j int, s text);
CREATE TABLE zzm3sr_d (i int, j int, s text);
CREATE TABLE zzm3sr_t (i int, s text, n numeric);

-- An action that only reads is written out like a view's query: every column under the
-- relation it resolved to, the select list broken one item to a line, and the clauses laid out.
CREATE RULE zzm3sr_r01 AS ON UPDATE TO zzm3sr_t DO ALSO SELECT i, s FROM zzm3sr_t WHERE i = old.i ORDER BY i;

-- begin-expected
-- columns: d
-- row: CREATE RULE zzm3sr_r01 AS~    ON UPDATE TO public.zzm3sr_t DO  SELECT zzm3sr_t.i,~    zzm3sr_t.s~   FROM zzm3sr_t~  WHERE (zzm3sr_t.i = old.i)~  ORDER BY zzm3sr_t.i;
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'zzm3sr_r01';

-- A constant with no name of its own is published under the placeholder, because a reader of
-- an action that stands on its own does see the names its columns answer to.
CREATE RULE zzm3sr_r04 AS ON UPDATE TO zzm3sr_t DO ALSO SELECT 1;

-- begin-expected
-- columns: d
-- row: CREATE RULE zzm3sr_r04 AS~    ON UPDATE TO public.zzm3sr_t DO  SELECT 1 AS "?column?";
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'zzm3sr_r04';

-- A star is settled into the columns it stood for when the rule is written.
CREATE RULE zzm3sr_r12 AS ON UPDATE TO zzm3sr_t DO ALSO SELECT * FROM zzm3sr_k k;

-- begin-expected
-- columns: d
-- row: CREATE RULE zzm3sr_r12 AS~    ON UPDATE TO public.zzm3sr_t DO  SELECT k.i,~    k.j,~    k.s~   FROM zzm3sr_k k;
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'zzm3sr_r12';

-- A call is published under its own name, which is the name a reader would see.
CREATE RULE zzm3sr_r14 AS ON UPDATE TO zzm3sr_t DO ALSO SELECT count(*) FROM zzm3sr_k;

-- begin-expected
-- columns: d
-- row: CREATE RULE zzm3sr_r14 AS~    ON UPDATE TO public.zzm3sr_t DO  SELECT count(*) AS count~   FROM zzm3sr_k;
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'zzm3sr_r14';

-- Two relations read side by side each go on a line of their own.
CREATE RULE zzm3sr_r15 AS ON UPDATE TO zzm3sr_t DO ALSO SELECT k.i, d.j FROM zzm3sr_k k, zzm3sr_d d WHERE k.i = d.i;

-- begin-expected
-- columns: d
-- row: CREATE RULE zzm3sr_r15 AS~    ON UPDATE TO public.zzm3sr_t DO  SELECT k.i,~    d.j~   FROM zzm3sr_k k,~    zzm3sr_d d~  WHERE (k.i = d.i);
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'zzm3sr_r15';

-- A join is bracketed once, with its condition bracketed again.
CREATE RULE zzm3sr_r16 AS ON UPDATE TO zzm3sr_t DO ALSO SELECT k.i FROM zzm3sr_k k JOIN zzm3sr_d d ON k.i = d.i;

-- begin-expected
-- columns: d
-- row: CREATE RULE zzm3sr_r16 AS~    ON UPDATE TO public.zzm3sr_t DO  SELECT k.i~   FROM (zzm3sr_k k~     JOIN zzm3sr_d d ON ((k.i = d.i)));
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'zzm3sr_r16';

-- The arms of a set operation are laid out under the operator, and only the first shows the
-- names it gives its columns.
CREATE RULE zzm3sr_r17 AS ON UPDATE TO zzm3sr_t DO ALSO SELECT i FROM zzm3sr_k UNION ALL SELECT j FROM zzm3sr_d;

-- begin-expected
-- columns: d
-- row: CREATE RULE zzm3sr_r17 AS~    ON UPDATE TO public.zzm3sr_t DO  SELECT zzm3sr_k.i~   FROM zzm3sr_k~UNION ALL~ SELECT zzm3sr_d.j~   FROM zzm3sr_d;
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'zzm3sr_r17';

-- INSTEAD stands before the action and the space where ALSO would have been is still there.
CREATE RULE zzm3sr_r19 AS ON UPDATE TO zzm3sr_t DO INSTEAD SELECT i FROM zzm3sr_k;

-- begin-expected
-- columns: d
-- row: CREATE RULE zzm3sr_r19 AS~    ON UPDATE TO public.zzm3sr_t DO INSTEAD  SELECT zzm3sr_k.i~   FROM zzm3sr_k;
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'zzm3sr_r19';

-- A WITH item is written out at the level its body belongs to.
CREATE RULE zzm3sr_r20 AS ON UPDATE TO zzm3sr_t DO ALSO WITH c AS (SELECT i FROM zzm3sr_k) SELECT i FROM c;

-- begin-expected
-- columns: d
-- row: CREATE RULE zzm3sr_r20 AS~    ON UPDATE TO public.zzm3sr_t DO  WITH c AS (~         SELECT zzm3sr_k.i~           FROM zzm3sr_k~        )~ SELECT c.i~   FROM c;
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'zzm3sr_r20';

-- So is a query read as a relation.
CREATE RULE zzm3sr_r21 AS ON UPDATE TO zzm3sr_t DO ALSO SELECT x.i FROM (SELECT i FROM zzm3sr_k) x;

-- begin-expected
-- columns: d
-- row: CREATE RULE zzm3sr_r21 AS~    ON UPDATE TO public.zzm3sr_t DO  SELECT x.i~   FROM ( SELECT zzm3sr_k.i~           FROM zzm3sr_k) x;
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'zzm3sr_r21';

-- DISTINCT stands where it was written.
CREATE RULE zzm3sr_r22 AS ON UPDATE TO zzm3sr_t DO ALSO SELECT DISTINCT i FROM zzm3sr_k;

-- begin-expected
-- columns: d
-- row: CREATE RULE zzm3sr_r22 AS~    ON UPDATE TO public.zzm3sr_t DO  SELECT DISTINCT zzm3sr_k.i~   FROM zzm3sr_k;
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'zzm3sr_r22';

-- GROUP BY and HAVING each begin a line, and HAVING one column further out.
CREATE RULE zzm3sr_r23 AS ON UPDATE TO zzm3sr_t DO ALSO SELECT i FROM zzm3sr_k GROUP BY i HAVING count(*) > 1;

-- begin-expected
-- columns: d
-- row: CREATE RULE zzm3sr_r23 AS~    ON UPDATE TO public.zzm3sr_t DO  SELECT zzm3sr_k.i~   FROM zzm3sr_k~  GROUP BY zzm3sr_k.i~ HAVING (count(*) > 1);
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'zzm3sr_r23';

-- A constant in the query's qualification carries the type its column read it as.
CREATE RULE zzm3sr_r26 AS ON UPDATE TO zzm3sr_t DO ALSO SELECT i FROM zzm3sr_k WHERE s = 'a';

-- begin-expected
-- columns: d
-- row: CREATE RULE zzm3sr_r26 AS~    ON UPDATE TO public.zzm3sr_t DO  SELECT zzm3sr_k.i~   FROM zzm3sr_k~  WHERE (zzm3sr_k.s = 'a'::text);
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'zzm3sr_r26';

-- ============================================================================
-- An INSERT reads its rows from a query written under the columns it writes
-- ============================================================================

-- An INSERT whose rows come from a query lists the columns it writes and lays the query out
-- one step further in than the statement holding it.
CREATE RULE zzm3sr_r03 AS ON INSERT TO zzm3sr_t DO ALSO INSERT INTO zzm3sr_d SELECT i, j, s FROM zzm3sr_k WHERE i = new.i;

-- begin-expected
-- columns: d
-- row: CREATE RULE zzm3sr_r03 AS~    ON INSERT TO public.zzm3sr_t DO  INSERT INTO zzm3sr_d (i, j, s)  SELECT zzm3sr_k.i,~            zzm3sr_k.j,~            zzm3sr_k.s~           FROM zzm3sr_k~          WHERE (zzm3sr_k.i = new.i);
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'zzm3sr_r03';

-- Including the star an INSERT reads its rows through.
CREATE RULE zzm3sr_r13 AS ON INSERT TO zzm3sr_t DO ALSO INSERT INTO zzm3sr_d SELECT * FROM zzm3sr_k;

-- begin-expected
-- columns: d
-- row: CREATE RULE zzm3sr_r13 AS~    ON INSERT TO public.zzm3sr_t DO  INSERT INTO zzm3sr_d (i, j, s)  SELECT zzm3sr_k.i,~            zzm3sr_k.j,~            zzm3sr_k.s~           FROM zzm3sr_k;
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'zzm3sr_r13';

-- The columns the INSERT was written with are the columns it is read back with.
CREATE RULE zzm3sr_r24 AS ON INSERT TO zzm3sr_t DO ALSO INSERT INTO zzm3sr_d (i, s) SELECT i, s FROM zzm3sr_k WHERE s = 'c';

-- begin-expected
-- columns: d
-- row: CREATE RULE zzm3sr_r24 AS~    ON INSERT TO public.zzm3sr_t DO  INSERT INTO zzm3sr_d (i, s)  SELECT zzm3sr_k.i,~            zzm3sr_k.s~           FROM zzm3sr_k~          WHERE (zzm3sr_k.s = 'c'::text);
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'zzm3sr_r24';

-- An INSERT naming one column reads one column out of the query it is given, and that column
-- keeps the name the call gives it.
CREATE RULE zzm3sr_r25 AS ON INSERT TO zzm3sr_t DO ALSO INSERT INTO zzm3sr_d (j) SELECT count(*) FROM zzm3sr_k;

-- begin-expected
-- columns: d
-- row: CREATE RULE zzm3sr_r25 AS~    ON INSERT TO public.zzm3sr_t DO  INSERT INTO zzm3sr_d (j)  SELECT count(*) AS count~           FROM zzm3sr_k;
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'zzm3sr_r25';

-- ============================================================================
-- An UPDATE writes the relation it reads beside the one it writes to
-- ============================================================================

-- An UPDATE action reading a second relation writes that relation on a line of its own.
CREATE RULE zzm3sr_r02 AS ON UPDATE TO zzm3sr_t DO ALSO UPDATE zzm3sr_d SET j = z.j FROM zzm3sr_k z WHERE z.i = zzm3sr_d.i;

-- begin-expected
-- columns: d
-- row: CREATE RULE zzm3sr_r02 AS~    ON UPDATE TO public.zzm3sr_t DO  UPDATE zzm3sr_d SET j = z.j~   FROM zzm3sr_k z~  WHERE (z.i = zzm3sr_d.i);
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'zzm3sr_r02';

-- Both aliases are kept and neither AS that introduced one is.
CREATE RULE zzm3sr_r27 AS ON UPDATE TO zzm3sr_t DO ALSO UPDATE zzm3sr_d d SET j = k.j FROM zzm3sr_k k WHERE k.i = d.i AND d.j > 0;

-- begin-expected
-- columns: d
-- row: CREATE RULE zzm3sr_r27 AS~    ON UPDATE TO public.zzm3sr_t DO  UPDATE zzm3sr_d d SET j = k.j~   FROM zzm3sr_k k~  WHERE ((k.i = d.i) AND (d.j > 0));
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'zzm3sr_r27';

-- An UPDATE may read a relation its qualification never names.
CREATE RULE zzm3sr_r30 AS ON UPDATE TO zzm3sr_t DO ALSO UPDATE zzm3sr_d SET j = 1 FROM zzm3sr_k;

-- begin-expected
-- columns: d
-- row: CREATE RULE zzm3sr_r30 AS~    ON UPDATE TO public.zzm3sr_t DO  UPDATE zzm3sr_d SET j = 1~   FROM zzm3sr_k;
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'zzm3sr_r30';

-- ============================================================================
-- A query standing inside an action's qualification keeps the sub-link it was
-- ============================================================================

-- IN over a query is left the sub-link it was, where IN over a value list becomes = ANY.
CREATE RULE zzm3sr_r05 AS ON UPDATE TO zzm3sr_t DO ALSO DELETE FROM zzm3sr_d WHERE i IN (SELECT i FROM zzm3sr_k WHERE j > 0);

-- begin-expected
-- columns: d
-- row: CREATE RULE zzm3sr_r05 AS~    ON UPDATE TO public.zzm3sr_t DO  DELETE FROM zzm3sr_d~  WHERE (zzm3sr_d.i IN ( SELECT zzm3sr_k.i~           FROM zzm3sr_k~          WHERE (zzm3sr_k.j > 0)));
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'zzm3sr_r05';

-- NOT IN over a query is that same sub-link with a NOT around it.
CREATE RULE zzm3sr_r06 AS ON UPDATE TO zzm3sr_t DO ALSO DELETE FROM zzm3sr_d WHERE i NOT IN (SELECT i FROM zzm3sr_k WHERE s = 'b');

-- begin-expected
-- columns: d
-- row: CREATE RULE zzm3sr_r06 AS~    ON UPDATE TO public.zzm3sr_t DO  DELETE FROM zzm3sr_d~  WHERE (NOT (zzm3sr_d.i IN ( SELECT zzm3sr_k.i~           FROM zzm3sr_k~          WHERE (zzm3sr_k.s = 'b'::text))));
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'zzm3sr_r06';

-- EXISTS is bracketed twice: once for the sub-link and once for the query inside it.
CREATE RULE zzm3sr_r07 AS ON UPDATE TO zzm3sr_t DO ALSO DELETE FROM zzm3sr_d WHERE EXISTS (SELECT 1 FROM zzm3sr_k WHERE zzm3sr_k.i = zzm3sr_d.i);

-- begin-expected
-- columns: d
-- row: CREATE RULE zzm3sr_r07 AS~    ON UPDATE TO public.zzm3sr_t DO  DELETE FROM zzm3sr_d~  WHERE (EXISTS ( SELECT 1~           FROM zzm3sr_k~          WHERE (zzm3sr_k.i = zzm3sr_d.i)));
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'zzm3sr_r07';

-- A query standing where a value is wanted is written the same way, and its column keeps the
-- name the call gives it because nothing outside the sub-link reads it.
CREATE RULE zzm3sr_r08 AS ON UPDATE TO zzm3sr_t DO ALSO UPDATE zzm3sr_d SET j = (SELECT max(i) FROM zzm3sr_k);

-- begin-expected
-- columns: d
-- row: CREATE RULE zzm3sr_r08 AS~    ON UPDATE TO public.zzm3sr_t DO  UPDATE zzm3sr_d SET j = ( SELECT max(zzm3sr_k.i) AS max~           FROM zzm3sr_k);
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'zzm3sr_r08';

-- Equality against ANY of a query is the one sub-link PostgreSQL writes back as IN.
CREATE RULE zzm3sr_r09 AS ON UPDATE TO zzm3sr_t DO ALSO DELETE FROM zzm3sr_d WHERE i = ANY (SELECT i FROM zzm3sr_k);

-- begin-expected
-- columns: d
-- row: CREATE RULE zzm3sr_r09 AS~    ON UPDATE TO public.zzm3sr_t DO  DELETE FROM zzm3sr_d~  WHERE (zzm3sr_d.i IN ( SELECT zzm3sr_k.i~           FROM zzm3sr_k));
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'zzm3sr_r09';

-- ALL keeps the spelling it was written with.
CREATE RULE zzm3sr_r10 AS ON UPDATE TO zzm3sr_t DO ALSO DELETE FROM zzm3sr_d WHERE i <> ALL (SELECT i FROM zzm3sr_k);

-- begin-expected
-- columns: d
-- row: CREATE RULE zzm3sr_r10 AS~    ON UPDATE TO public.zzm3sr_t DO  DELETE FROM zzm3sr_d~  WHERE (zzm3sr_d.i <> ALL ( SELECT zzm3sr_k.i~           FROM zzm3sr_k));
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'zzm3sr_r10';

-- So does ANY under any operator but equality.
CREATE RULE zzm3sr_r11 AS ON UPDATE TO zzm3sr_t DO ALSO DELETE FROM zzm3sr_d WHERE i > ANY (SELECT i FROM zzm3sr_k);

-- begin-expected
-- columns: d
-- row: CREATE RULE zzm3sr_r11 AS~    ON UPDATE TO public.zzm3sr_t DO  DELETE FROM zzm3sr_d~  WHERE (zzm3sr_d.i > ANY ( SELECT zzm3sr_k.i~           FROM zzm3sr_k));
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'zzm3sr_r11';

-- ============================================================================
-- What the shapes already printed did not move
-- ============================================================================

-- An INSERT of a value list still writes VALUES on a line of its own.
CREATE RULE zzm3sr_r28 AS ON INSERT TO zzm3sr_t DO ALSO INSERT INTO zzm3sr_d VALUES (new.i, 1, 'x');

-- begin-expected
-- columns: d
-- row: CREATE RULE zzm3sr_r28 AS~    ON INSERT TO public.zzm3sr_t DO  INSERT INTO zzm3sr_d (i, j, s)~  VALUES (new.i, 1, 'x'::text);
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'zzm3sr_r28';

-- A value list is still a comparison against an array, and IS NULL is still bracketed.
CREATE RULE zzm3sr_r29 AS ON DELETE TO zzm3sr_t DO ALSO DELETE FROM zzm3sr_d WHERE i IN (1,2) AND s IS NULL;

-- begin-expected
-- columns: d
-- row: CREATE RULE zzm3sr_r29 AS~    ON DELETE TO public.zzm3sr_t DO  DELETE FROM zzm3sr_d~  WHERE ((zzm3sr_d.i = ANY (ARRAY[1, 2])) AND (zzm3sr_d.s IS NULL));
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'zzm3sr_r29';

DROP TABLE zzm3sr_t CASCADE;
DROP TABLE zzm3sr_d CASCADE;
DROP TABLE zzm3sr_k CASCADE;
