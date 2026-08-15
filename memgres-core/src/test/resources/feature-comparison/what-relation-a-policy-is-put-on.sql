-- ============================================================================
-- What relation a policy is put on: CREATE, ALTER and DROP POLICY take a
-- schema-qualified relation
--
-- The qualifier in front of the relation names the schema that holds it, so a
-- policy reaches a relation the session's search path does not, and pg_policies
-- reports the schema the relation is really in. Every value below was read off
-- PostgreSQL 18.
-- ============================================================================
CREATE TABLE wpo_t (i int, j int);
CREATE SCHEMA wpo_s;
CREATE TABLE wpo_s.wpo_u (i int, j int);
CREATE VIEW wpo_s.wpo_w AS SELECT 1 AS i;

CREATE POLICY wpo_p ON public.wpo_t USING (true);

-- begin-expected
-- columns: schemaname | tablename | policyname | permissive | roles | cmd | qual | with_check
-- row: public | wpo_t | wpo_p | PERMISSIVE | {public} | ALL | true | NULL
-- end-expected
SELECT schemaname, tablename, policyname, permissive, roles::text AS roles, cmd, qual, with_check
  FROM pg_policies WHERE policyname = 'wpo_p';

ALTER POLICY wpo_p ON public.wpo_t USING (i > 1);

-- begin-expected
-- columns: qual
-- row: (i > 1)
-- end-expected
SELECT qual FROM pg_policies WHERE policyname = 'wpo_p';

DROP POLICY wpo_p ON public.wpo_t;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_policies WHERE tablename = 'wpo_t';

-- A relation in a schema the search path does not reach is reached by writing the
-- schema, and pg_policies then reports that schema.
CREATE POLICY wpo_q ON wpo_s.wpo_u FOR SELECT USING (i > 0);

-- begin-expected
-- columns: schemaname | tablename | policyname | cmd | qual
-- row: wpo_s | wpo_u | wpo_q | SELECT | (i > 0)
-- end-expected
SELECT schemaname, tablename, policyname, cmd, qual
  FROM pg_policies WHERE policyname = 'wpo_q';

ALTER POLICY wpo_q ON wpo_s.wpo_u RENAME TO wpo_q2;

-- begin-expected
-- columns: schemaname | tablename | policyname
-- row: wpo_s | wpo_u | wpo_q2
-- end-expected
SELECT schemaname, tablename, policyname FROM pg_policies WHERE policyname LIKE 'wpo!_q%' ESCAPE '!';

DROP POLICY wpo_q2 ON wpo_s.wpo_u;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_policies WHERE schemaname = 'wpo_s';

-- An INSERT policy has a WITH CHECK and no USING, and the qualifier reaches the
-- relation for that too.
CREATE POLICY wpo_r ON wpo_s.wpo_u FOR INSERT WITH CHECK (j > 2);

-- begin-expected
-- columns: schemaname | tablename | policyname | cmd | qual | with_check
-- row: wpo_s | wpo_u | wpo_r | INSERT | NULL | (j > 2)
-- end-expected
SELECT schemaname, tablename, policyname, cmd, qual, with_check
  FROM pg_policies WHERE policyname = 'wpo_r';

DROP POLICY wpo_r ON wpo_s.wpo_u;

-- A quoted qualifier names the same schema an unquoted one does, and RESTRICTIVE,
-- the command and both expressions are kept beside it.
CREATE POLICY wpo_qq ON "wpo_s"."wpo_u" AS RESTRICTIVE FOR DELETE USING (i > 3);

-- begin-expected
-- columns: schemaname | tablename | policyname | permissive | cmd | qual
-- row: wpo_s | wpo_u | wpo_qq | RESTRICTIVE | DELETE | (i > 3)
-- end-expected
SELECT schemaname, tablename, policyname, permissive, cmd, qual
  FROM pg_policies WHERE policyname = 'wpo_qq';

CREATE POLICY wpo_qu ON wpo_s.wpo_u FOR UPDATE TO PUBLIC USING (i > 1) WITH CHECK (i > 2);

-- begin-expected
-- columns: schemaname | roles | cmd | qual | with_check
-- row: wpo_s | {public} | UPDATE | (i > 1) | (i > 2)
-- end-expected
SELECT schemaname, roles::text AS roles, cmd, qual, with_check
  FROM pg_policies WHERE policyname = 'wpo_qu';

ALTER POLICY wpo_qu ON wpo_s.wpo_u USING (i > 5) WITH CHECK (i > 6);

-- begin-expected
-- columns: schemaname | qual | with_check
-- row: wpo_s | (i > 5) | (i > 6)
-- end-expected
SELECT schemaname, qual, with_check FROM pg_policies WHERE policyname = 'wpo_qu';

-- A policy written without a qualifier is put on the relation the search path
-- reaches, so two relations of one name in two schemas keep their own policies.
CREATE TABLE public.wpo_u (i int, j int);
CREATE POLICY wpo_pub ON wpo_u USING (i > 0);

-- begin-expected
-- columns: schemaname | tablename | policyname | qual
-- row: public | wpo_u | wpo_pub | (i > 0)
-- end-expected
SELECT schemaname, tablename, policyname, qual FROM pg_policies WHERE policyname = 'wpo_pub';

-- begin-expected
-- columns: n1 | n2
-- row: 1 | 2
-- end-expected
SELECT (SELECT count(*)::int FROM pg_policies WHERE schemaname = 'public' AND tablename = 'wpo_u') AS n1,
       (SELECT count(*)::int FROM pg_policies WHERE schemaname = 'wpo_s' AND tablename = 'wpo_u') AS n2;

DROP POLICY wpo_pub ON public.wpo_u;
DROP POLICY wpo_qq ON wpo_s.wpo_u;

-- IF EXISTS reaches the qualified relation like the plain form does.
DROP POLICY IF EXISTS wpo_qu ON wpo_s.wpo_u;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_policies WHERE policyname LIKE 'wpo%';

-- A qualifier naming no schema is a missing schema, not a missing relation.

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "wpo_nosuch" does not exist
-- end-expected-error
CREATE POLICY wpo_z ON wpo_nosuch.wpo_u USING (true);

-- A relation the named schema does not hold is reported under the whole name.

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "wpo_s.wpo_nope" does not exist
-- end-expected-error
CREATE POLICY wpo_z ON wpo_s.wpo_nope USING (true);

-- A view carries no row security, and it is named without its schema in the refusal.

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "wpo_w" is not a table
-- end-expected-error
CREATE POLICY wpo_z ON wpo_s.wpo_w USING (true);

-- ALTER and DROP name the relation without its schema when the policy is missing.

-- begin-expected-error
-- sqlstate: 42704
-- message-like: policy "wpo_z" for table "wpo_u" does not exist
-- end-expected-error
ALTER POLICY wpo_z ON wpo_s.wpo_u USING (true);

-- begin-expected-error
-- sqlstate: 42704
-- message-like: policy "wpo_z" for table "wpo_u" does not exist
-- end-expected-error
DROP POLICY wpo_z ON wpo_s.wpo_u;

-- ALTER and DROP report the schema and the relation the same way CREATE does.

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "wpo_nosuch" does not exist
-- end-expected-error
ALTER POLICY wpo_z ON wpo_nosuch.wpo_u USING (true);

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "wpo_s.wpo_nope" does not exist
-- end-expected-error
ALTER POLICY wpo_z ON wpo_s.wpo_nope USING (true);

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "wpo_s.wpo_nope" does not exist
-- end-expected-error
DROP POLICY wpo_z ON wpo_s.wpo_nope;

-- A name already taken on that relation is refused under the relation's bare name.
CREATE POLICY wpo_a1 ON wpo_s.wpo_u USING (i > 7);
CREATE POLICY wpo_a2 ON wpo_s.wpo_u USING (i > 8);

-- begin-expected-error
-- sqlstate: 42710
-- message-like: policy "wpo_a2" for table "wpo_u" already exists
-- end-expected-error
ALTER POLICY wpo_a1 ON wpo_s.wpo_u RENAME TO wpo_a2;

-- IF EXISTS is silent for a policy that is not there and for a schema that is not.
DROP POLICY IF EXISTS wpo_z ON wpo_s.wpo_u;
DROP POLICY IF EXISTS wpo_z ON wpo_nosuch.wpo_u;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "wpo_nosuch" does not exist
-- end-expected-error
DROP POLICY wpo_z ON wpo_nosuch.wpo_u;

-- What was refused took nothing with it: both policies still stand and the
-- qualified relation still takes a new one.

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*)::int AS n FROM pg_policies WHERE schemaname = 'wpo_s';

CREATE POLICY wpo_a3 ON wpo_s.wpo_u FOR SELECT USING (j > 9);

-- begin-expected
-- columns: schemaname | tablename | policyname | cmd | qual
-- row: wpo_s | wpo_u | wpo_a3 | SELECT | (j > 9)
-- end-expected
SELECT schemaname, tablename, policyname, cmd, qual FROM pg_policies WHERE policyname = 'wpo_a3';

DROP POLICY wpo_a1 ON wpo_s.wpo_u;
DROP POLICY wpo_a2 ON wpo_s.wpo_u;
DROP POLICY wpo_a3 ON wpo_s.wpo_u;
DROP VIEW wpo_s.wpo_w;
DROP TABLE wpo_s.wpo_u;
DROP SCHEMA wpo_s;
DROP TABLE public.wpo_u;
DROP TABLE wpo_t;
