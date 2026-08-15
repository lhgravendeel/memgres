-- A schema is what everything in it hangs from, so DROP SCHEMA answers for all of it. Without
-- CASCADE it refuses while the schema still holds anything and says what stands in the way; with
-- CASCADE it takes the schema's own objects and reaches past the schema to a view, a row security
-- policy or a column default elsewhere that depends on one of them. A drop that is rolled back
-- never happened, so the schema comes back holding what it held -- its rows, its keys, its
-- sequence's place and the rules on its relations.
--
-- A rule's action names its relation once and for all when the rule is written, and the stored
-- definition reads back with the qualification the reading session's own search path calls for.
--
-- Every answer below was measured against PostgreSQL 18.

-- ============================================================================
-- RESTRICT refuses while the schema still holds a relation, and leaves it there
-- ============================================================================
CREATE SCHEMA wsd_a;
CREATE TABLE wsd_a.r (i int);
INSERT INTO wsd_a.r VALUES (1);

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop schema wsd_a because other objects depend on it
-- detail-like: table wsd_a.r depends on schema wsd_a
-- hint-like: Use DROP ... CASCADE to drop the dependent objects too.
-- end-expected-error
DROP SCHEMA wsd_a RESTRICT;

-- RESTRICT is the default, and the refusal is the same one
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop schema wsd_a because other objects depend on it
-- end-expected-error
DROP SCHEMA wsd_a;

-- the refusal changed nothing: the schema is there and so is its row
-- begin-expected
-- columns: n | r
-- row: 1, 1
-- end-expected
SELECT (SELECT count(*) FROM pg_namespace WHERE nspname = 'wsd_a') AS n,
       (SELECT count(*) FROM wsd_a.r) AS r;

DROP SCHEMA wsd_a CASCADE;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_namespace WHERE nspname = 'wsd_a';

-- ============================================================================
-- A sequence, a type or a function of its own stands in the way just as a table does
-- ============================================================================
CREATE SCHEMA wsd_b;
CREATE SEQUENCE wsd_b.sq;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop schema wsd_b because other objects depend on it
-- detail-like: sequence wsd_b.sq depends on schema wsd_b
-- end-expected-error
DROP SCHEMA wsd_b RESTRICT;

DROP SEQUENCE wsd_b.sq;
CREATE TYPE wsd_b.e AS ENUM ('a');

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop schema wsd_b because other objects depend on it
-- detail-like: type wsd_b.e depends on schema wsd_b
-- end-expected-error
DROP SCHEMA wsd_b RESTRICT;

DROP TYPE wsd_b.e;
CREATE FUNCTION wsd_b.f(a int) RETURNS int AS 'SELECT a' LANGUAGE sql;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop schema wsd_b because other objects depend on it
-- detail-like: function wsd_b.f(integer) depends on schema wsd_b
-- end-expected-error
DROP SCHEMA wsd_b RESTRICT;

DROP FUNCTION wsd_b.f(int);

-- with nothing left to hang from it the schema drops without CASCADE
DROP SCHEMA wsd_b RESTRICT;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_namespace WHERE nspname = 'wsd_b';

-- ============================================================================
-- CASCADE reaches a view outside the schema that reads a relation inside it
-- ============================================================================
CREATE SCHEMA wsd_c;
CREATE TABLE wsd_c.t (i int);
CREATE VIEW public.wsd_cv AS SELECT i FROM wsd_c.t;
CREATE VIEW public.wsd_cv2 AS SELECT i FROM public.wsd_cv;
CREATE TABLE public.wsd_ck (i int);
CREATE VIEW public.wsd_ckv AS SELECT i FROM public.wsd_ck;

-- the outside view is what stands in the way of a drop without CASCADE
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop schema wsd_c because other objects depend on it
-- detail-like: view wsd_cv depends on table wsd_c.t
-- end-expected-error
DROP SCHEMA wsd_c RESTRICT;

DROP SCHEMA wsd_c CASCADE;

-- the view that read it is gone, and so is the view that read that one
-- begin-expected
-- columns: v | v2 | k
-- row: 0, 0, 1
-- end-expected
SELECT (SELECT count(*) FROM pg_views WHERE viewname = 'wsd_cv') AS v,
       (SELECT count(*) FROM pg_views WHERE viewname = 'wsd_cv2') AS v2,
       (SELECT count(*) FROM pg_views WHERE viewname = 'wsd_ckv') AS k;

-- the view that read nothing of the schema's still answers
INSERT INTO public.wsd_ck VALUES (7);

-- begin-expected
-- columns: i
-- row: 7
-- end-expected
SELECT i FROM public.wsd_ckv;

DROP VIEW public.wsd_ckv;
DROP TABLE public.wsd_ck;

-- ============================================================================
-- CASCADE reaches a policy outside the schema and leaves its relation standing
-- ============================================================================
CREATE SCHEMA wsd_d;
CREATE TABLE wsd_d.t (i int);
CREATE TABLE public.wsd_dt (i int);
CREATE POLICY wsd_dp ON wsd_dt USING (i IN (SELECT i FROM wsd_d.t));
CREATE TABLE public.wsd_du (i int);
CREATE POLICY wsd_dq ON wsd_du USING (i > 0);

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop schema wsd_d because other objects depend on it
-- detail-like: policy wsd_dp on table wsd_dt depends on table wsd_d.t
-- end-expected-error
DROP SCHEMA wsd_d RESTRICT;

DROP SCHEMA wsd_d CASCADE;

-- the policy that read it is gone; the relation it was on and the other policy stay
-- begin-expected
-- columns: p | q | t
-- row: 0, 1, 1
-- end-expected
SELECT (SELECT count(*) FROM pg_policies WHERE policyname = 'wsd_dp') AS p,
       (SELECT count(*) FROM pg_policies WHERE policyname = 'wsd_dq') AS q,
       (SELECT count(*) FROM pg_tables WHERE tablename = 'wsd_dt') AS t;

DROP TABLE public.wsd_dt;
DROP TABLE public.wsd_du;

-- ============================================================================
-- CASCADE reaches the default of a column on a relation outside the schema
-- ============================================================================
CREATE SCHEMA wsd_e;
CREATE SEQUENCE wsd_e.sq;
CREATE TABLE public.wsd_ed (i int, j int DEFAULT nextval('wsd_e.sq'));
INSERT INTO public.wsd_ed (i) VALUES (1);

-- begin-expected
-- columns: i | j
-- row: 1, 1
-- end-expected
SELECT i, j FROM public.wsd_ed;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop schema wsd_e because other objects depend on it
-- detail-like: default value for column j of table wsd_ed depends on sequence wsd_e.sq
-- end-expected-error
DROP SCHEMA wsd_e RESTRICT;

-- rolled back, the sequence is back and the default outside the schema goes on drawing from it
BEGIN;
DROP SCHEMA wsd_e CASCADE;
ROLLBACK;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM pg_namespace WHERE nspname = 'wsd_e';

INSERT INTO public.wsd_ed (i) VALUES (2);

-- begin-expected
-- columns: i | j
-- row: 1, 1
-- row: 2, 2
-- end-expected
SELECT i, j FROM public.wsd_ed ORDER BY i;

DROP TABLE public.wsd_ed;
DROP SCHEMA wsd_e CASCADE;

-- ============================================================================
-- A DROP SCHEMA rolled back never happened
-- ============================================================================
CREATE SCHEMA wsd_f;
CREATE TABLE wsd_f.t (i int);
CREATE TABLE wsd_f.l (m text);
CREATE SEQUENCE wsd_f.sq;
CREATE VIEW wsd_f.v AS SELECT i FROM wsd_f.t;
CREATE RULE wsd_fr AS ON INSERT TO wsd_f.t DO ALSO INSERT INTO wsd_f.l VALUES ('z');
INSERT INTO wsd_f.t VALUES (1);

-- begin-expected
-- columns: t | l
-- row: 1, 1
-- end-expected
SELECT (SELECT count(*) FROM wsd_f.t) AS t, (SELECT count(*) FROM wsd_f.l) AS l;

BEGIN;
DROP SCHEMA wsd_f CASCADE;

-- inside the transaction the schema is gone
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_namespace WHERE nspname = 'wsd_f';

ROLLBACK;

-- and afterwards it is back, with its relations, its rows and its rule
-- begin-expected
-- columns: n | r
-- row: 1, 1
-- end-expected
SELECT (SELECT count(*) FROM pg_namespace WHERE nspname = 'wsd_f') AS n,
       (SELECT count(*) FROM pg_rules WHERE rulename = 'wsd_fr') AS r;

-- begin-expected
-- columns: t | l | v
-- row: 1, 1, 1
-- end-expected
SELECT (SELECT count(*) FROM wsd_f.t) AS t, (SELECT count(*) FROM wsd_f.l) AS l,
       (SELECT count(*) FROM wsd_f.v) AS v;

-- the rule the drop took goes on firing
INSERT INTO wsd_f.t VALUES (2);

-- begin-expected
-- columns: t | l
-- row: 2, 2
-- end-expected
SELECT (SELECT count(*) FROM wsd_f.t) AS t, (SELECT count(*) FROM wsd_f.l) AS l;

-- and the rule still stands in the way of dropping what its action names
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table wsd_f.l because other objects depend on it
-- detail-like: rule wsd_fr on table wsd_f.t depends on table wsd_f.l
-- end-expected-error
DROP TABLE wsd_f.l;

-- the sequence the drop took goes on counting from where it was
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT nextval('wsd_f.sq') AS n;

DROP SCHEMA wsd_f CASCADE;

-- ============================================================================
-- A rolled-back drop puts back the keys and the sequence's place
-- ============================================================================
CREATE SCHEMA wsd_g;
CREATE TABLE wsd_g.k (i int PRIMARY KEY, m text UNIQUE);
CREATE SEQUENCE wsd_g.sq;
INSERT INTO wsd_g.k VALUES (1, 'a');

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT nextval('wsd_g.sq') AS n;

-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT nextval('wsd_g.sq') AS n;

BEGIN;
DROP SCHEMA wsd_g CASCADE;
ROLLBACK;

-- the sequence goes on from where it was, not from the beginning
-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT nextval('wsd_g.sq') AS n;

-- and both keys are back, refusing the values already there
-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint
-- end-expected-error
INSERT INTO wsd_g.k VALUES (1, 'b');

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint
-- end-expected-error
INSERT INTO wsd_g.k VALUES (2, 'a');

INSERT INTO wsd_g.k VALUES (2, 'b');

-- begin-expected
-- columns: k | x
-- row: 2, 2
-- end-expected
SELECT (SELECT count(*) FROM wsd_g.k) AS k,
       (SELECT count(*) FROM pg_indexes WHERE schemaname = 'wsd_g') AS x;

DROP SCHEMA wsd_g CASCADE;

-- ============================================================================
-- A rolled-back drop puts back a view and a policy CASCADE reached outside it
-- ============================================================================
CREATE SCHEMA wsd_h;
CREATE TABLE wsd_h.t (i int);
CREATE VIEW public.wsd_hv AS SELECT i FROM wsd_h.t;
CREATE TABLE public.wsd_ht (i int);
CREATE POLICY wsd_hp ON wsd_ht USING (i IN (SELECT i FROM wsd_h.t));

BEGIN;
DROP SCHEMA wsd_h CASCADE;
ROLLBACK;

-- begin-expected
-- columns: n | v | p
-- row: 1, 1, 1
-- end-expected
SELECT (SELECT count(*) FROM pg_namespace WHERE nspname = 'wsd_h') AS n,
       (SELECT count(*) FROM pg_views WHERE viewname = 'wsd_hv') AS v,
       (SELECT count(*) FROM pg_policies WHERE policyname = 'wsd_hp') AS p;

INSERT INTO wsd_h.t VALUES (3);

-- begin-expected
-- columns: i
-- row: 3
-- end-expected
SELECT i FROM public.wsd_hv;

DROP SCHEMA wsd_h CASCADE;
DROP TABLE public.wsd_ht;

-- ============================================================================
-- A drop rolled back to a savepoint, and a drop that commits
-- ============================================================================
CREATE SCHEMA wsd_i;
CREATE TABLE wsd_i.t (i int);
INSERT INTO wsd_i.t VALUES (9);

BEGIN;
SAVEPOINT s1;
DROP SCHEMA wsd_i CASCADE;
ROLLBACK TO SAVEPOINT s1;

-- begin-expected
-- columns: n | t
-- row: 1, 1
-- end-expected
SELECT (SELECT count(*) FROM pg_namespace WHERE nspname = 'wsd_i') AS n,
       (SELECT count(*) FROM wsd_i.t) AS t;

COMMIT;

-- the rest of the transaction committed, and the schema the savepoint put back is still there
-- begin-expected
-- columns: n | t
-- row: 1, 1
-- end-expected
SELECT (SELECT count(*) FROM pg_namespace WHERE nspname = 'wsd_i') AS n,
       (SELECT count(*) FROM wsd_i.t) AS t;

BEGIN;
DROP SCHEMA wsd_i CASCADE;
COMMIT;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_namespace WHERE nspname = 'wsd_i';

-- ============================================================================
-- A rule's action names its relation once, when the rule is written
-- ============================================================================
CREATE SCHEMA wsd_j;
CREATE TABLE wsd_j.d (m text);
CREATE TABLE public.wsd_js (i int);
SET search_path = wsd_j, public;
CREATE RULE wsd_jr AS ON INSERT TO public.wsd_js DO ALSO INSERT INTO d VALUES ('x');

-- the schema ahead on the path is the one the action named, and the definition writes the
-- relation bare because that schema is on the reading session's path too
-- begin-expected
-- columns: d
-- row: CREATE RULE wsd_jr AS~    ON INSERT TO public.wsd_js DO  INSERT INTO d (m)~  VALUES ('x'::text);
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'wsd_jr';

SET search_path = public;

-- read by a session the schema is not on the path of, the same rule writes it out
-- begin-expected
-- columns: d
-- row: CREATE RULE wsd_jr AS~    ON INSERT TO public.wsd_js DO  INSERT INTO wsd_j.d (m)~  VALUES ('x'::text);
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'wsd_jr';

CREATE TABLE public.wsd_jd (m text);
INSERT INTO public.wsd_js VALUES (1);

-- the write went where the rule was written to send it, not where the path points now
-- begin-expected
-- columns: g | p
-- row: 1, 0
-- end-expected
SELECT (SELECT count(*) FROM wsd_j.d) AS g, (SELECT count(*) FROM public.wsd_jd) AS p;

DROP TABLE public.wsd_jd;
DROP TABLE public.wsd_js CASCADE;
DROP SCHEMA wsd_j CASCADE;
RESET search_path;

-- ============================================================================
-- A relation of the name that appears later is nothing to a rule already written
-- ============================================================================
CREATE TABLE public.wsd_kd (m text);
CREATE TABLE public.wsd_kk (i int);
CREATE RULE wsd_kr AS ON INSERT TO public.wsd_kk DO ALSO INSERT INTO wsd_kd VALUES ('t');
CREATE SCHEMA wsd_k;
CREATE TABLE wsd_k.wsd_kd (m text);
SET search_path = wsd_k, public;
INSERT INTO public.wsd_kk VALUES (1);

-- begin-expected
-- columns: p | s
-- row: 1, 0
-- end-expected
SELECT (SELECT count(*) FROM public.wsd_kd) AS p, (SELECT count(*) FROM wsd_k.wsd_kd) AS s;

RESET search_path;
DROP SCHEMA wsd_k CASCADE;

-- with the schema that shared the name gone the rule fires exactly as before
INSERT INTO public.wsd_kk VALUES (2);

-- begin-expected
-- columns: p
-- row: 2
-- end-expected
SELECT count(*) AS p FROM public.wsd_kd;

DROP TABLE public.wsd_kk CASCADE;
DROP TABLE public.wsd_kd CASCADE;

-- ============================================================================
-- The same rule read through pg_get_ruledef, and a rule written qualified
-- ============================================================================
CREATE SCHEMA wsd_l;
CREATE TABLE wsd_l.l (m text);
CREATE TABLE public.wsd_lk (i int);
CREATE RULE wsd_lr AS ON INSERT TO public.wsd_lk DO ALSO INSERT INTO wsd_l.l VALUES ('q');

-- begin-expected
-- columns: d
-- row: CREATE RULE wsd_lr AS~    ON INSERT TO public.wsd_lk DO  INSERT INTO wsd_l.l (m)~  VALUES ('q'::text);
-- end-expected
SELECT replace(pg_get_ruledef(r.oid), chr(10), '~') AS d FROM pg_rewrite r
 WHERE r.rulename = 'wsd_lr';

SET search_path = wsd_l, public;

-- with the schema on the path the action's relation is written without it, through either reader
-- begin-expected
-- columns: d
-- row: CREATE RULE wsd_lr AS~    ON INSERT TO public.wsd_lk DO  INSERT INTO l (m)~  VALUES ('q'::text);
-- end-expected
SELECT replace(pg_get_ruledef(r.oid), chr(10), '~') AS d FROM pg_rewrite r
 WHERE r.rulename = 'wsd_lr';

-- begin-expected
-- columns: d
-- row: CREATE RULE wsd_lr AS~    ON INSERT TO public.wsd_lk DO  INSERT INTO l (m)~  VALUES ('q'::text);
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'wsd_lr';

SET search_path = public;
INSERT INTO public.wsd_lk VALUES (1);

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM wsd_l.l;

RESET search_path;
DROP TABLE public.wsd_lk CASCADE;
DROP SCHEMA wsd_l CASCADE;

-- ============================================================================
-- A rule written with the qualifier public reads back without it
-- ============================================================================
CREATE TABLE public.wsd_md (i int);
CREATE TABLE public.wsd_mk (i int);
CREATE RULE wsd_mr AS ON INSERT TO public.wsd_mk DO ALSO
  INSERT INTO public.wsd_md VALUES (new.i);

-- begin-expected
-- columns: d
-- row: CREATE RULE wsd_mr AS~    ON INSERT TO public.wsd_mk DO  INSERT INTO wsd_md (i)~  VALUES (new.i);
-- end-expected
SELECT replace(definition, chr(10), '~') AS d FROM pg_rules WHERE rulename = 'wsd_mr';

INSERT INTO public.wsd_mk VALUES (5);

-- begin-expected
-- columns: i
-- row: 5
-- end-expected
SELECT i FROM public.wsd_md;

DROP TABLE public.wsd_mk CASCADE;
DROP TABLE public.wsd_md CASCADE;

-- ============================================================================
-- IF EXISTS on a schema that is not there is nothing to anybody
-- ============================================================================
DROP SCHEMA IF EXISTS wsd_nothere CASCADE;
DROP SCHEMA IF EXISTS wsd_nothere;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "wsd_nothere" does not exist
-- end-expected-error
DROP SCHEMA wsd_nothere;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM pg_namespace WHERE nspname LIKE 'wsd!_%' ESCAPE '!';
