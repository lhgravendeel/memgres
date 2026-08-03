-- Where PostgreSQL requires a boolean, and what it says when it is handed something else.
--
-- A condition is not "anything that can be read as true or false". PostgreSQL coerces the
-- expression to boolean while it transforms the clause the expression stands in, and a type with
-- no coercion to boolean is refused there and then, naming the clause: 42804 "argument of WHERE
-- must be type boolean, not type integer". The clauses that do this are WHERE, HAVING, a JOIN's
-- ON, a searched CASE's WHEN, FILTER, AND, OR, NOT, a CHECK constraint, a policy expression, a
-- partial index's predicate and a rule's qualification.
--
-- Three things decide the answer, all measured against PostgreSQL 18:
--
--  1. The clause is named. The same integer is "argument of WHERE", "argument of HAVING",
--     "argument of JOIN/ON", "argument of CASE/WHEN", "argument of FILTER", "argument of AND".
--  2. How the expression was WRITTEN decides which error, never what it evaluates to. A bare
--     string literal is still of type unknown, so boolean's input function reads it and a word it
--     does not know is 22P02, not 42804 -- while the same text spelled 'x'::text is 42804.
--  3. Being unable to settle a type means saying nothing. A column of a derived table or a CTE
--     carries whatever type the engine inferred while building the result, so those are left
--     alone rather than guessed at; PostgreSQL refuses some of them and memgres does not, which
--     is a gap and not a divergence in the other direction.
--
-- And the ordinary shapes keep working: a boolean column, a comparison, AND/OR/NOT, IS NULL, IN,
-- EXISTS, LIKE, BETWEEN, a boolean-returning function, a CASE returning boolean, a scalar
-- sub-query returning boolean, and the boolean words 't', 'true', '1', 'f', 'no', 'off' written
-- as bare literals.

-- setup
DROP TABLE IF EXISTS bc_t CASCADE;
DROP TABLE IF EXISTS bc_u CASCADE;

CREATE TABLE bc_t (id int PRIMARY KEY, i int, n numeric, s text, b boolean, arr int[], j jsonb);
INSERT INTO bc_t VALUES (1, 1, 1.5, 'x', true, ARRAY[1,2], '{"a":1}');
INSERT INTO bc_t VALUES (2, 0, 0.0, 'y', false, ARRAY[3], '{"a":2}');

CREATE TABLE bc_u (id int PRIMARY KEY, k int);
INSERT INTO bc_u VALUES (1, 1), (2, 2);

-- 1: WHERE names itself, and every type that is not a boolean

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
SELECT id FROM bc_t WHERE 1;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
SELECT id FROM bc_t WHERE i;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type numeric
-- end-expected-error
SELECT id FROM bc_t WHERE 1.5;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type numeric
-- end-expected-error
SELECT id FROM bc_t WHERE n;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type text
-- end-expected-error
SELECT id FROM bc_t WHERE s;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type text
-- end-expected-error
SELECT id FROM bc_t WHERE 'x'::text;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer[]
-- end-expected-error
SELECT id FROM bc_t WHERE arr;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type jsonb
-- end-expected-error
SELECT id FROM bc_t WHERE j;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
SELECT id FROM bc_t WHERE (SELECT 1);

-- an operator's result type is worked out, not only a bare column's
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
SELECT id FROM bc_t WHERE i + 1;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type text
-- end-expected-error
SELECT id FROM bc_t WHERE s || 'a';

-- a call is resolved by its argument types, and upper(text) is text
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type text
-- end-expected-error
SELECT id FROM bc_t WHERE upper(s);

-- an explicit NULL of a type is still that type
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
SELECT id FROM bc_t WHERE NULL::int;

-- 2: the refusal does not depend on there being a row to try it on

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of AND must be type boolean, not type integer
-- end-expected-error
SELECT id FROM bc_t WHERE id < 0 AND i > 0 AND i;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
SELECT 1 WHERE (SELECT 1);

-- 3: a bare string literal is of type unknown, so the input function reads it

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type boolean: "zzz"
-- end-expected-error
SELECT id FROM bc_t WHERE 'zzz';

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM bc_t WHERE 't' ORDER BY id;

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM bc_t WHERE 'true' ORDER BY id;

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM bc_t WHERE '1' ORDER BY id;

-- 'f', 'no' and 'off' are all false, and were all being read as true
-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*) AS c FROM bc_t WHERE 'f';

-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*) AS c FROM bc_t WHERE 'no';

-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*) AS c FROM bc_t WHERE 'off';

-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*) AS c FROM bc_t WHERE NOT 'off';

-- 4: AND, OR and NOT each name themselves

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of AND must be type boolean, not type integer
-- end-expected-error
SELECT id FROM bc_t WHERE b AND i;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of OR must be type boolean, not type text
-- end-expected-error
SELECT id FROM bc_t WHERE b OR s;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of NOT must be type boolean, not type numeric
-- end-expected-error
SELECT id FROM bc_t WHERE NOT n;

-- outside a WHERE just as much as inside one
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of AND must be type boolean, not type integer
-- end-expected-error
SELECT i AND true FROM bc_t;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of OR must be type boolean, not type bigint
-- end-expected-error
SELECT count(*) OR false FROM bc_t;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of NOT must be type boolean, not type jsonb
-- end-expected-error
SELECT NOT j FROM bc_t;

-- 5: HAVING, with and without a GROUP BY

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of HAVING must be type boolean, not type integer
-- end-expected-error
SELECT count(*) FROM bc_t HAVING 1;

-- an aggregate's own result type is what HAVING complains about
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of HAVING must be type boolean, not type bigint
-- end-expected-error
SELECT count(*) FROM bc_t HAVING count(*);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of HAVING must be type boolean, not type integer
-- end-expected-error
SELECT count(*) FROM bc_t GROUP BY i HAVING i;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type boolean: "zzz"
-- end-expected-error
SELECT count(*) FROM bc_t HAVING 'zzz';

-- 6: a JOIN's ON condition

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of JOIN/ON must be type boolean, not type integer
-- end-expected-error
SELECT count(*) FROM bc_t JOIN bc_u ON 1;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of JOIN/ON must be type boolean, not type text
-- end-expected-error
SELECT count(*) FROM bc_t LEFT JOIN bc_u ON s;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type boolean: "zzz"
-- end-expected-error
SELECT count(*) FROM bc_t JOIN bc_u ON 'zzz';

-- 7: a searched CASE's WHEN. A simple CASE compares its WHEN with the operand instead, so
-- CASE i WHEN 1 THEN ... is not a boolean context at all.

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of CASE/WHEN must be type boolean, not type integer
-- end-expected-error
SELECT CASE WHEN 1 THEN 1 ELSE 2 END;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of CASE/WHEN must be type boolean, not type integer
-- end-expected-error
SELECT CASE WHEN i THEN 1 END FROM bc_t;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of CASE/WHEN must be type boolean, not type bigint
-- end-expected-error
SELECT CASE WHEN count(*) THEN 1 END FROM bc_t;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type boolean: "abc"
-- end-expected-error
SELECT CASE WHEN 'abc' THEN 1 END;

-- begin-expected
-- columns: r
-- row: 1
-- row: 2
-- end-expected
SELECT CASE i WHEN 1 THEN 1 ELSE 2 END AS r FROM bc_t ORDER BY id;

-- 8: FILTER, on an aggregate and on a call that is not one. PostgreSQL coerces the predicate
-- while it transforms the call, which is before it resolves the function -- so a FILTER that is
-- not a condition outranks the complaint that abs is not an aggregate.

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of FILTER must be type boolean, not type integer
-- end-expected-error
SELECT count(*) FILTER (WHERE 1) FROM bc_t;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of FILTER must be type boolean, not type integer
-- end-expected-error
SELECT count(*) FILTER (WHERE i) FROM bc_t;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of FILTER must be type boolean, not type integer
-- end-expected-error
SELECT abs(i) FILTER (WHERE 1) FROM bc_t;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type boolean: "zzz"
-- end-expected-error
SELECT abs(i) FILTER (WHERE 'zzz') FROM bc_t;

-- 9: UPDATE and DELETE resolve their target themselves, and their WHERE is a WHERE

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
UPDATE bc_t SET i = i WHERE i;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type jsonb
-- end-expected-error
DELETE FROM bc_t WHERE j;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type boolean: "zzz"
-- end-expected-error
UPDATE bc_t SET i = i WHERE 'zzz';

-- a CASE inside a SET list is a boolean context like any other
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of CASE/WHEN must be type boolean, not type integer
-- end-expected-error
UPDATE bc_t SET i = CASE WHEN i THEN 1 ELSE 2 END;

-- nothing was written by any of those
-- begin-expected
-- columns: id | i
-- row: 1, 1
-- row: 2, 0
-- end-expected
SELECT id, i FROM bc_t ORDER BY id;

-- 10: a MERGE's WHEN condition. Its ON is deliberately not checked: PostgreSQL transforms a
-- MERGE's join condition without coercing it, so MERGE ... ON (1) is accepted where the same
-- condition in a JOIN is not.

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHEN must be type boolean, not type numeric
-- end-expected-error
MERGE INTO bc_t t USING bc_u u ON t.id = u.id WHEN MATCHED AND (n) THEN DO NOTHING;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type boolean: "zzz"
-- end-expected-error
MERGE INTO bc_t t USING bc_u u ON t.id = u.id WHEN MATCHED AND ('zzz') THEN DO NOTHING;

-- 11: definitions that store a condition -- a CHECK, a policy, a partial index, a rule

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of CHECK must be type boolean, not type integer
-- end-expected-error
CREATE TABLE bc_bad (i int, CHECK (i));

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of CHECK must be type boolean, not type integer
-- end-expected-error
CREATE TABLE bc_bad (i int, CHECK (i + 1));

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type boolean: "zzz"
-- end-expected-error
CREATE TABLE bc_bad (i int, CHECK ('zzz'));

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
CREATE INDEX bc_ix ON bc_t (id) WHERE i;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type text
-- end-expected-error
CREATE INDEX bc_ix ON bc_t (id) WHERE s;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type boolean: "zzz"
-- end-expected-error
CREATE INDEX bc_ix ON bc_t (id) WHERE 'zzz';

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of POLICY must be type boolean, not type integer
-- end-expected-error
CREATE POLICY bc_p ON bc_t USING (i);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of POLICY must be type boolean, not type jsonb
-- end-expected-error
CREATE POLICY bc_p ON bc_t WITH CHECK (j);

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type boolean: "zzz"
-- end-expected-error
CREATE POLICY bc_p ON bc_t USING ('zzz');

-- an aggregate in a policy is refused before its type is judged
-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in policy expressions
-- end-expected-error
CREATE POLICY bc_p ON bc_t USING (count(*));

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
CREATE RULE bc_r AS ON INSERT TO bc_t WHERE new.i DO INSTEAD NOTHING;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type boolean: "zzz"
-- end-expected-error
CREATE RULE bc_r AS ON INSERT TO bc_t WHERE 'zzz' DO INSTEAD NOTHING;

-- a trigger's WHEN is resolved against the row it fires for
-- setup
CREATE FUNCTION bc_f() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHEN must be type boolean, not type integer
-- end-expected-error
CREATE TRIGGER bc_tg BEFORE UPDATE ON bc_t FOR EACH ROW WHEN (new.i)
    EXECUTE FUNCTION bc_f();

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type boolean: "zzz"
-- end-expected-error
CREATE TRIGGER bc_tg BEFORE UPDATE ON bc_t FOR EACH ROW WHEN ('zzz')
    EXECUTE FUNCTION bc_f();

-- 12: PL/pgSQL is different, and deliberately so. It has the value already, so it does not raise
-- the type system's 42804: it puts the value through boolean's input function. IF i where i is 1
-- therefore runs -- "1" is boolean input -- while IF i + 1 fails on the text "2".

DO $$ DECLARE i int := 1; BEGIN IF i THEN NULL; END IF; END $$;

-- begin-expected
-- columns: r
-- row: ran
-- end-expected
SELECT 'ran' AS r;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type boolean: "2"
-- end-expected-error
DO $$ DECLARE i int := 1; BEGIN IF i + 1 THEN NULL; END IF; END $$;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type boolean: "1.5"
-- end-expected-error
DO $$ DECLARE n numeric := 1.5; BEGIN WHILE n LOOP EXIT; END LOOP; END $$;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type boolean: "x"
-- end-expected-error
DO $$ DECLARE s text := 'x'; BEGIN IF s THEN NULL; END IF; END $$;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type boolean: "{1}"
-- end-expected-error
DO $$ DECLARE arr int[] := ARRAY[1]; BEGIN IF arr THEN NULL; END IF; END $$;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type boolean: "3"
-- end-expected-error
DO $$ BEGIN LOOP EXIT WHEN 3; END LOOP; END $$;

-- 13: the ordinary shapes, in every clause that wants a boolean

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM bc_t WHERE b;

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM bc_t WHERE i = 1;

-- begin-expected
-- columns: id
-- row: 1
-- end-expected
SELECT id FROM bc_t WHERE b AND true;

-- begin-expected
-- columns: id
-- row: 2
-- end-expected
SELECT id FROM bc_t WHERE NOT b AND (b OR i = 0);

-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*) AS c FROM bc_t WHERE i IS NULL;

-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*) AS c FROM bc_t WHERE i IN (0, 1);

-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*) AS c FROM bc_t WHERE EXISTS (SELECT 1 FROM bc_u);

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*) AS c FROM bc_t WHERE s LIKE 'x';

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*) AS c FROM bc_t WHERE i BETWEEN 1 AND 2;

-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*) AS c FROM bc_t WHERE (SELECT true);

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*) AS c FROM bc_t WHERE starts_with(s, 'x');

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*) AS c FROM bc_t WHERE CASE WHEN i = 1 THEN true ELSE false END;

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*) AS c FROM bc_t WHERE b IS TRUE;

-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*) AS c FROM bc_t WHERE length(s) = 1;

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*) AS c FROM bc_t WHERE coalesce(b, false);

-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*) AS c FROM bc_t WHERE arr[1] IS NOT NULL;

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*) AS c FROM bc_t WHERE j ->> 'a' = '1';

-- a correlated reference is resolved against the enclosing relation and typed there
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*) AS c FROM bc_t x WHERE EXISTS (SELECT 1 FROM bc_u WHERE x.b);

-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*) AS c FROM bc_t JOIN bc_u ON bc_t.id = bc_u.id;

-- begin-expected
-- columns: c
-- row: 4
-- end-expected
SELECT count(*) AS c FROM bc_t LEFT JOIN bc_u ON bc_t.b OR bc_u.k > 0;

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*) FILTER (WHERE b) AS c FROM bc_t;

-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*) AS c FROM bc_t HAVING count(*) > 1;

-- the rest of the words boolean input knows
-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*) AS c FROM bc_t WHERE 'y';

-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*) AS c FROM bc_t WHERE 'on';

-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*) AS c FROM bc_t WHERE 'false';

-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*) AS c FROM bc_t WHERE '0';

-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*) AS c FROM bc_t WHERE NULL;

-- an operator's result type follows the operands
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type numeric
-- end-expected-error
SELECT id FROM bc_t WHERE i * n;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
SELECT id FROM bc_t WHERE length(s);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of NOT must be type boolean, not type integer
-- end-expected-error
SELECT NOT 1;

-- a PL/pgSQL searched CASE reads its value the same way an IF does
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type boolean: "7"
-- end-expected-error
DO $$ BEGIN CASE WHEN 7 THEN NULL; ELSE NULL; END CASE; END $$;

-- 'off' is false, so the condition below does not raise
DO $$ DECLARE s text := 'off'; BEGIN IF s THEN RAISE EXCEPTION 'off is not true'; END IF; END $$;

-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*) AS c FROM bc_t LEFT JOIN bc_u ON bc_t.id = bc_u.id AND bc_u.k > 0;

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*) AS c FROM bc_t WHERE i = ANY (ARRAY[1, 5]);

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*) AS c FROM bc_t WHERE s ~ 'x';

-- a definition that is a condition is stored without complaint
-- setup
CREATE TABLE bc_ok (i int, b boolean, CHECK (i > 0), CHECK (b), CHECK (i IS NOT NULL));
CREATE INDEX bc_ix_ok ON bc_t (id) WHERE b;
CREATE INDEX bc_ix_ok2 ON bc_t (id) WHERE i > 0 AND s IS NOT NULL;
CREATE POLICY bc_p_ok ON bc_t USING (b);
CREATE POLICY bc_p_ok2 ON bc_t WITH CHECK (i > 0 OR s LIKE 'x');
CREATE RULE bc_r_ok AS ON INSERT TO bc_t WHERE new.b DO INSTEAD NOTHING;
CREATE TRIGGER bc_tg_ok BEFORE UPDATE ON bc_t FOR EACH ROW WHEN (new.i > old.i)
    EXECUTE FUNCTION bc_f();

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*) AS c FROM pg_catalog.pg_indexes WHERE indexname = 'bc_ix_ok';

-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*) AS c FROM pg_catalog.pg_policies WHERE tablename = 'bc_t';

-- and a PL/pgSQL condition that is a boolean runs
DO $$ DECLARE c int := 0; BEGIN WHILE c < 3 LOOP c := c + 1; END LOOP;
  CREATE TEMP TABLE bc_loop AS SELECT c AS r; END $$;

-- begin-expected
-- columns: r
-- row: 3
-- end-expected
SELECT r FROM bc_loop;

-- cleanup
DROP TABLE IF EXISTS bc_loop CASCADE;
DROP TRIGGER IF EXISTS bc_tg_ok ON bc_t;
DROP RULE IF EXISTS bc_r_ok ON bc_t;
DROP TABLE IF EXISTS bc_ok CASCADE;
DROP TABLE IF EXISTS bc_bad CASCADE;
DROP FUNCTION IF EXISTS bc_f() CASCADE;
DROP TABLE IF EXISTS bc_t CASCADE;
DROP TABLE IF EXISTS bc_u CASCADE;

-- ============================================================================
-- A column a derived relation supplies is that relation's, not an outer one's
-- ============================================================================
-- The boolean checks read a column's declared type. A derived table, a CTE, a
-- view and a VALUES list expose columns whose type was worked out rather than
-- declared, and two things went wrong with that: the worked-out type was
-- trusted even where it was wrong, and a name the derived relation supplied but
-- could not type was followed out to the enclosing query level, which typed a
-- different column of the same name. An ordinary join and an ordinary sub-query
-- were both refused as a result.
DROP TABLE IF EXISTS bctx_t CASCADE;
DROP TABLE IF EXISTS bctx_u CASCADE;
DROP VIEW IF EXISTS bctx_v CASCADE;
CREATE TABLE bctx_t (id int PRIMARY KEY, txt text, d date, n int);
CREATE TABLE bctx_u (id int PRIMARY KEY, flag boolean);
INSERT INTO bctx_t VALUES (1,'aa','2020-01-01',1),(2,'ab','2020-01-02',0);
INSERT INTO bctx_u VALUES (1,true),(2,false);
CREATE VIEW bctx_v AS SELECT id, flag AS n FROM bctx_u;

-- a join whose condition is a boolean the sub-query computed
-- begin-expected
-- columns: n
-- row: 4
-- end-expected
SELECT count(*)::text AS n FROM bctx_t a
  JOIN (SELECT id, starts_with(txt,'a') AS f FROM bctx_t) s ON s.f;

-- begin-expected
-- columns: n
-- row: 4
-- end-expected
SELECT count(*)::text AS n FROM bctx_t a
  JOIN (SELECT id, isfinite(d) AS f FROM bctx_t) s ON s.f;

-- the same through a WITH item
-- begin-expected
-- columns: n
-- row: 4
-- end-expected
WITH q AS (SELECT id, starts_with(txt,'a') AS f FROM bctx_t)
SELECT count(*)::text AS n FROM bctx_t a JOIN q ON q.f;

-- an inner WHERE over a boolean the derived relation supplies, where the outer
-- relation has an integer of the same name
-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id::text AS id FROM bctx_t
 WHERE EXISTS (SELECT 1 FROM (SELECT flag AS n FROM bctx_u) q WHERE n) ORDER BY id;

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id::text AS id FROM bctx_t
 WHERE EXISTS (SELECT 1 FROM bctx_v WHERE n) ORDER BY id;

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id::text AS id FROM bctx_t
 WHERE EXISTS (SELECT 1 FROM (VALUES (true)) v(n) WHERE n) ORDER BY id;

-- and the refusals a declared column still earns
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
SELECT id FROM bctx_t WHERE n;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of JOIN/ON must be type boolean, not type integer
-- end-expected-error
SELECT count(*) FROM bctx_t a JOIN bctx_t b ON a.n;

DROP VIEW IF EXISTS bctx_v CASCADE;
DROP TABLE IF EXISTS bctx_t CASCADE;
DROP TABLE IF EXISTS bctx_u CASCADE;

-- ============================================================================
-- The type a derived relation's definition settles is the column's type
-- ============================================================================
-- A derived table, a WITH item, a view, a VALUES list and a function in FROM
-- are all built by running something and describing what came back, so their
-- columns carry the type the builder read off a value. That guess was wrong
-- often enough that nothing could be refused on the strength of it. The type
-- is now worked out from the definition instead -- the select-list expression,
-- the non-recursive term of a recursive WITH item, the view's query, what the
-- function returns -- and a condition over a column whose type that settles is
-- refused exactly as one over a declared column is. Where the definition
-- settles nothing the column stays as untyped as it was.
DROP VIEW IF EXISTS bcdt_v CASCADE;
DROP TABLE IF EXISTS bcdt_u CASCADE;
DROP TABLE IF EXISTS bcdt_t CASCADE;
CREATE TABLE bcdt_t (id int PRIMARY KEY, i int, n numeric, s text, b boolean, d date);
INSERT INTO bcdt_t VALUES (1,1,1.5,'aa',true,'2020-01-01'),(2,0,0.0,'ab',false,'2020-01-02');
CREATE TABLE bcdt_u (id int PRIMARY KEY, flag boolean);
INSERT INTO bcdt_u VALUES (1,true),(2,false);
CREATE VIEW bcdt_v AS SELECT id, flag AS n FROM bcdt_u;

-- a column a derived table, a WITH item, a view or a VALUES list supplies
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
SELECT id FROM (SELECT id, i FROM bcdt_t) q WHERE i;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
WITH c AS (SELECT id, i FROM bcdt_t) SELECT id FROM c WHERE i;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
SELECT count(*) FROM (VALUES (1)) v(k) WHERE k;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
SELECT count(*) FROM (SELECT * FROM bcdt_v) q WHERE id;

-- a star stands for the columns of the relations behind it, types and all
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
SELECT id FROM (SELECT * FROM bcdt_t) d WHERE i;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type numeric
-- end-expected-error
SELECT id FROM (SELECT * FROM bcdt_t) d WHERE n;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type text
-- end-expected-error
SELECT id FROM (SELECT * FROM bcdt_t) d WHERE s;

-- what a function in FROM returns
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
SELECT g FROM generate_series(1, 2) g WHERE g;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
SELECT count(*) FROM (SELECT * FROM generate_series(1, 2) AS g(x)) q WHERE x;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
SELECT count(*) FROM unnest(ARRAY[1,2]) u WHERE u;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::text AS n FROM unnest(ARRAY[true,false]) u WHERE u;

-- a recursive WITH item's columns are its non-recursive term's
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
WITH RECURSIVE r(k) AS (SELECT 1 UNION ALL SELECT k + 1 FROM r WHERE k < 3)
SELECT count(*) FROM r WHERE k;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
WITH RECURSIVE r(k) AS (SELECT true UNION ALL SELECT false FROM r WHERE k)
SELECT count(*)::text AS n FROM r WHERE k;

-- a set operation's arms have to agree before the type is the column's
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
SELECT count(*) FROM (SELECT id FROM bcdt_t UNION SELECT id FROM bcdt_t) q WHERE id;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*)::text AS n
  FROM (SELECT b FROM bcdt_t UNION ALL SELECT NOT b FROM bcdt_t) q WHERE b;

-- renaming a column does not retype it, at any depth
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
SELECT count(*) FROM (SELECT i FROM bcdt_t) q(z) WHERE z;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::text AS n FROM (SELECT b FROM bcdt_t) q(z) WHERE z;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
SELECT count(*) FROM bcdt_t x(c1,c2,c3,c4,c5,c6) WHERE c2;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::text AS n FROM bcdt_t x(c1,c2,c3,c4,c5,c6) WHERE c5;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
SELECT count(*) FROM (SELECT * FROM (SELECT i AS k FROM bcdt_t) y) z WHERE k;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::text AS n FROM (SELECT * FROM (SELECT b AS k FROM bcdt_t) y) z WHERE k;

-- a condition over a join's derived side
-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of JOIN/ON must be type boolean, not type integer
-- end-expected-error
SELECT count(*) FROM bcdt_t a JOIN (SELECT id, i FROM bcdt_t) x ON x.i;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of JOIN/ON must be type boolean, not type integer
-- end-expected-error
SELECT count(*) FROM bcdt_t a JOIN bcdt_v x ON x.id;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of JOIN/ON must be type boolean, not type integer
-- end-expected-error
SELECT count(*) FROM (SELECT i FROM bcdt_t) q JOIN (SELECT flag FROM bcdt_u) r ON q.i;

-- ...and the ordinary ones over a boolean the definition computed, which are
-- what refusing on a guessed type used to reject. Every join form, and LATERAL.
-- begin-expected
-- columns: n
-- row: 4
-- end-expected
SELECT count(*)::text AS n FROM bcdt_t a
  JOIN (SELECT id, starts_with(s,'a') AS f FROM bcdt_t) x ON x.f;

-- begin-expected
-- columns: n
-- row: 4
-- end-expected
SELECT count(*)::text AS n FROM bcdt_t a
  LEFT JOIN (SELECT id, starts_with(s,'a') AS f FROM bcdt_t) x ON x.f;

-- begin-expected
-- columns: n
-- row: 4
-- end-expected
SELECT count(*)::text AS n FROM bcdt_t a
  RIGHT JOIN (SELECT id, isfinite(d) AS f FROM bcdt_t) x ON x.f;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*)::text AS n FROM bcdt_t a
  JOIN LATERAL (SELECT starts_with(a.s,'a') AS f) x ON x.f;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*)::text AS n FROM bcdt_t a,
  LATERAL (SELECT starts_with(a.s,'a') AS f) x WHERE x.f;

-- begin-expected
-- columns: n
-- row: 4
-- end-expected
WITH q AS (SELECT id, starts_with(s,'a') AS f FROM bcdt_t)
SELECT count(*)::text AS n FROM bcdt_t a JOIN q ON q.f;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*)::text AS n FROM bcdt_t a JOIN bcdt_v x ON x.n;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*)::text AS n FROM bcdt_t a JOIN (VALUES (1,true)) v(k,f) ON v.f;

-- a name the derived relation supplies is that relation's, whatever an
-- enclosing relation calls its own column of the same name
-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*)::text AS n FROM bcdt_t a
 WHERE EXISTS (SELECT 1 FROM (SELECT b AS i FROM bcdt_t) q WHERE i);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: argument of WHERE must be type boolean, not type integer
-- end-expected-error
SELECT count(*) FROM bcdt_u a
 WHERE EXISTS (SELECT 1 FROM (SELECT id AS flag FROM bcdt_t) q WHERE flag);

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id::text AS id FROM bcdt_t
 WHERE EXISTS (SELECT 1 FROM (SELECT flag AS n FROM bcdt_u) q WHERE n) ORDER BY id;

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id::text AS id FROM bcdt_t WHERE EXISTS (SELECT 1 FROM bcdt_v WHERE n) ORDER BY id;

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id::text AS id FROM bcdt_t
 WHERE EXISTS (SELECT 1 FROM (VALUES (true)) v(n) WHERE n) ORDER BY id;

-- a record-returning call still answers to its own column names
-- begin-expected
-- columns: key
-- row: a
-- end-expected
SELECT key FROM jsonb_each('{"a":1}');

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::text AS n FROM (SELECT * FROM jsonb_each('{"a":1}')) q WHERE key = 'a';

-- and everything else a derived column is written into
-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*)::text AS n FROM (SELECT g FROM generate_series(1,2) g) q WHERE g > 0;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::text AS n FROM (SELECT * FROM bcdt_t a JOIN bcdt_u u USING (id)) q WHERE flag;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*)::text AS n FROM (SELECT * FROM bcdt_t a CROSS JOIN bcdt_u u) q WHERE flag;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*)::text AS n FROM (SELECT (SELECT true) AS k FROM bcdt_t) q WHERE k;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::text AS n FROM (SELECT count(*) AS k FROM bcdt_t) q WHERE k > 0;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::text AS n FROM (SELECT n AS k FROM bcdt_t) q WHERE k > 0;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*)::text AS n FROM (SELECT d AS k FROM bcdt_t) q WHERE k > DATE '2019-01-01';

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*)::text AS n
  FROM (SELECT id, row_number() OVER () AS k FROM bcdt_t) q WHERE k > 0;

DROP VIEW IF EXISTS bcdt_v CASCADE;
DROP TABLE IF EXISTS bcdt_u CASCADE;
DROP TABLE IF EXISTS bcdt_t CASCADE;
