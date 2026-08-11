-- A rule belongs to a relation, and a relation belongs to a schema, so two schemas holding a
-- relation of the same name hold two separate sets of rules: a write fires only the rules of the
-- relation its name reaches, creating or dropping one schema's relation leaves the other's rules
-- alone, and pg_rules and pg_rewrite name the schema the rule's relation is in. DROP RULE, ALTER
-- RULE and DROP TRIGGER reach the relation the qualifier names. What an action may read is
-- settled while the rule is written, and PostgreSQL says which relation holds a name it may not
-- read from there.

-- setup
CREATE SCHEMA rbs_s;
CREATE TABLE public.rbs_t (i int);
CREATE TABLE public.rbs_g (i int);
CREATE TABLE rbs_s.rbs_t (i int);
CREATE TABLE rbs_s.rbs_g (i int);
CREATE RULE rbs_rp AS ON INSERT TO public.rbs_t DO ALSO INSERT INTO public.rbs_g VALUES (new.i);
CREATE RULE rbs_rs AS ON INSERT TO rbs_s.rbs_t DO ALSO INSERT INTO rbs_s.rbs_g VALUES (new.i + 100);

-- stmt 1: the catalogue names the schema the rule's relation is in
-- begin-expected
-- columns: d
-- row: public/rbs_t/rbs_rp
-- row: rbs_s/rbs_t/rbs_rs
-- end-expected
SELECT schemaname || '/' || tablename || '/' || rulename AS d FROM pg_rules WHERE rulename IN ('rbs_rp','rbs_rs') ORDER BY rulename;
-- begin-expected
-- columns: d
-- row: rbs_s/rbs_t
-- end-expected
SELECT n.nspname || '/' || c.relname AS d FROM pg_rewrite r JOIN pg_class c ON c.oid = r.ev_class JOIN pg_namespace n ON n.oid = c.relnamespace WHERE r.rulename = 'rbs_rs';

-- stmt 2: a write fires the rules of the relation its name reaches, and no others
INSERT INTO public.rbs_t VALUES (1);
INSERT INTO rbs_s.rbs_t VALUES (2);
-- begin-expected
-- columns: d
-- row: 1
-- end-expected
SELECT count(*)::text AS d FROM public.rbs_g;
-- begin-expected
-- columns: d
-- row: 102
-- end-expected
SELECT i::text AS d FROM rbs_s.rbs_g;

-- stmt 3: dropping and recreating one schema's relation leaves the other's rules alone
DROP TABLE rbs_s.rbs_t;
CREATE TABLE rbs_s.rbs_t (i int);
-- begin-expected
-- columns: d
-- row: 1
-- end-expected
SELECT count(*)::text AS d FROM pg_rules WHERE rulename = 'rbs_rp';
-- begin-expected
-- columns: d
-- row: true
-- end-expected
SELECT relhasrules::text AS d FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = 'rbs_t' AND n.nspname = 'public';
INSERT INTO public.rbs_t VALUES (4);
-- begin-expected
-- columns: d
-- row: 2
-- end-expected
SELECT count(*)::text AS d FROM public.rbs_g;

-- stmt 4: nor does dropping the whole schema the other relation is in
DROP SCHEMA rbs_s CASCADE;
-- begin-expected
-- columns: d
-- row: 1
-- end-expected
SELECT count(*)::text AS d FROM pg_rules WHERE rulename = 'rbs_rp';
INSERT INTO public.rbs_t VALUES (6);
-- begin-expected
-- columns: d
-- row: 3
-- end-expected
SELECT count(*)::text AS d FROM public.rbs_g;
DROP TABLE public.rbs_t CASCADE;
DROP TABLE public.rbs_g;

-- stmt 5: DROP RULE, ALTER RULE and DROP TRIGGER reach the relation the qualifier names
CREATE SCHEMA rbs_q;
CREATE TABLE rbs_q.rbs_qt (i int);
CREATE TABLE rbs_q.rbs_ql (i int);
CREATE RULE rbs_qr AS ON INSERT TO rbs_q.rbs_qt DO ALSO INSERT INTO rbs_q.rbs_ql VALUES (new.i);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: rule "rbs_nope" for relation "rbs_qt" does not exist
-- end-expected-error
DROP RULE rbs_nope ON rbs_q.rbs_qt;
ALTER RULE rbs_qr ON rbs_q.rbs_qt RENAME TO rbs_qr2;
-- begin-expected
-- columns: d
-- row: rbs_qr2
-- end-expected
SELECT rulename AS d FROM pg_rules WHERE tablename = 'rbs_qt';
-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table rbs_q.rbs_ql because other objects depend on it
-- end-expected-error
DROP TABLE rbs_q.rbs_ql;
DROP RULE rbs_qr2 ON rbs_q.rbs_qt;
-- begin-expected
-- columns: d
-- row: 0
-- end-expected
SELECT count(*)::text AS d FROM pg_rules WHERE tablename = 'rbs_qt';
CREATE FUNCTION rbs_q.rbs_tf() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql;
CREATE TRIGGER rbs_tr BEFORE INSERT ON rbs_q.rbs_qt FOR EACH ROW EXECUTE FUNCTION rbs_q.rbs_tf();
-- begin-expected-error
-- sqlstate: 42704
-- message-like: trigger "rbs_nope" for table "rbs_qt" does not exist
-- end-expected-error
DROP TRIGGER rbs_nope ON rbs_q.rbs_qt;
DROP TRIGGER rbs_tr ON rbs_q.rbs_qt;
-- begin-expected
-- columns: d
-- row: 0
-- end-expected
SELECT count(*)::text AS d FROM pg_trigger WHERE tgname = 'rbs_tr';
DROP SCHEMA rbs_q CASCADE;

-- stmt 6: a rule a CASCADE took off some other relation comes back with the rolled-back block
CREATE TABLE rbs_ra (i int);
CREATE TABLE rbs_rb (i int);
CREATE RULE rbs_rcr AS ON INSERT TO rbs_ra DO ALSO INSERT INTO rbs_rb VALUES (new.i);
BEGIN;
DROP TABLE rbs_rb CASCADE;
-- begin-expected
-- columns: d
-- row: 0
-- end-expected
SELECT count(*)::text AS d FROM pg_rules WHERE rulename = 'rbs_rcr';
ROLLBACK;
-- begin-expected
-- columns: d
-- row: 1
-- end-expected
SELECT count(*)::text AS d FROM pg_rules WHERE rulename = 'rbs_rcr';
-- begin-expected
-- columns: d
-- row: rbs_ra
-- end-expected
SELECT tablename AS d FROM pg_rules WHERE rulename = 'rbs_rcr';
INSERT INTO rbs_ra VALUES (5);
-- begin-expected
-- columns: d
-- row: 1
-- end-expected
SELECT count(*)::text AS d FROM rbs_rb;
DROP TABLE rbs_ra CASCADE;
DROP TABLE rbs_rb;

-- stmt 7: what an action may read is settled while the rule is written
CREATE TABLE rbs_ca (i int, j int);
CREATE TABLE rbs_cb (i int, k int);
CREATE TABLE rbs_cc (zx int, zy int, zv int);
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "i" does not exist
-- end-expected-error
CREATE RULE rbs_x1 AS ON INSERT TO rbs_ca DO ALSO INSERT INTO rbs_cb VALUES (i, 2);
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "k" does not exist
-- end-expected-error
CREATE RULE rbs_x2 AS ON INSERT TO rbs_ca DO ALSO INSERT INTO rbs_cb (i) VALUES (k);
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "kk" does not exist
-- end-expected-error
CREATE RULE rbs_x3 AS ON INSERT TO rbs_ca DO ALSO INSERT INTO rbs_cb VALUES (kk, 2);
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "kk" does not exist
-- end-expected-error
CREATE RULE rbs_x4 AS ON INSERT TO rbs_ca DO ALSO INSERT INTO rbs_cb SELECT i, 2 FROM rbs_cb WHERE kk = 1;
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "zz" does not exist
-- end-expected-error
CREATE RULE rbs_x5 AS ON INSERT TO rbs_ca DO ALSO INSERT INTO rbs_cc (zx) VALUES (zz);
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "jj" does not exist
-- end-expected-error
CREATE RULE rbs_x9 AS ON INSERT TO rbs_ca WHERE jj > 1 DO INSTEAD NOTHING;
-- begin-expected
-- columns: d
-- row: 0
-- end-expected
SELECT count(*)::text AS d FROM pg_rules WHERE tablename = 'rbs_ca';

-- stmt 8: what an action hands back is checked as the rule is written
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: RETURNING lists are not supported in non-INSTEAD rules
-- end-expected-error
CREATE RULE rbs_x6 AS ON INSERT TO rbs_ca DO ALSO INSERT INTO rbs_cb VALUES (new.i, 2) RETURNING i;
-- begin-expected-error
-- sqlstate: 42P17
-- message-like: RETURNING list has too few entries
-- end-expected-error
CREATE RULE rbs_x7 AS ON INSERT TO rbs_ca DO INSTEAD INSERT INTO rbs_cb VALUES (new.i, 2) RETURNING i;
-- begin-expected-error
-- sqlstate: 42P17
-- message-like: RETURNING list has too many entries
-- end-expected-error
CREATE RULE rbs_x8 AS ON INSERT TO rbs_ca DO INSTEAD INSERT INTO rbs_cb VALUES (new.i, 2) RETURNING i, k, 3;

-- stmt 9: a qualification reads the ruled relation, and a list of the right length is accepted
CREATE RULE rbs_ok0 AS ON INSERT TO rbs_ca WHERE j > 1 DO INSTEAD NOTHING;
CREATE RULE rbs_ok1 AS ON INSERT TO rbs_ca DO ALSO INSERT INTO rbs_cb SELECT i, 2 FROM rbs_cb;
CREATE RULE rbs_ok2 AS ON UPDATE TO rbs_ca DO ALSO UPDATE rbs_cb SET k = 1 WHERE i = 3;
CREATE RULE rbs_ok3 AS ON INSERT TO rbs_ca DO INSTEAD INSERT INTO rbs_cb VALUES (new.i, 2) RETURNING i, k;
CREATE RULE rbs_ok4 AS ON INSERT TO rbs_ca DO INSTEAD INSERT INTO rbs_cb VALUES (new.i, 2) RETURNING *;
-- begin-expected
-- columns: d
-- row: rbs_ok0,rbs_ok1,rbs_ok2,rbs_ok3,rbs_ok4
-- end-expected
SELECT string_agg(rulename, ',' ORDER BY rulename) AS d FROM pg_rules WHERE tablename = 'rbs_ca';

-- cleanup
DROP TABLE rbs_ca CASCADE;
DROP TABLE rbs_cb;
DROP TABLE rbs_cc;
