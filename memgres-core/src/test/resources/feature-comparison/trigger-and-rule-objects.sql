-- ============================================================================
-- A row trigger on a partitioned table is cloned onto every partition
-- ============================================================================
DROP TABLE IF EXISTS dtr_p CASCADE;
DROP FUNCTION IF EXISTS dtr_tf() CASCADE;
CREATE FUNCTION dtr_tf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$;
CREATE TABLE dtr_p (i int) PARTITION BY RANGE (i);
CREATE TABLE dtr_pa PARTITION OF dtr_p FOR VALUES FROM (0) TO (100);
CREATE TRIGGER dtr_ptg AFTER INSERT ON dtr_p FOR EACH ROW EXECUTE FUNCTION dtr_tf();
-- a partition created after the trigger was written carries it too
CREATE TABLE dtr_pb PARTITION OF dtr_p FOR VALUES FROM (100) TO (200);

-- begin-expected
-- columns: relname, n
-- row: dtr_p, 1
-- row: dtr_pa, 1
-- row: dtr_pb, 1
-- end-expected
SELECT c.relname::text AS relname, count(*)::text AS n
  FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
 WHERE c.relname IN ('dtr_p','dtr_pa','dtr_pb') AND NOT t.tgisinternal
 GROUP BY c.relname ORDER BY c.relname;

-- the copies point back at the trigger they came from; the original does not
-- begin-expected
-- columns: relname, has_parent
-- row: dtr_p, f
-- row: dtr_pa, t
-- row: dtr_pb, t
-- end-expected
SELECT c.relname::text AS relname, (t.tgparentid <> 0)::text AS has_parent
  FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
 WHERE c.relname IN ('dtr_p','dtr_pa','dtr_pb') AND NOT t.tgisinternal
 ORDER BY c.relname;

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop trigger dtr_ptg on table dtr_pa because trigger dtr_ptg on table dtr_p requires it
-- hint-like: You can drop trigger dtr_ptg on table dtr_p instead.
-- end-expected-error
DROP TRIGGER dtr_ptg ON dtr_pa;

-- a statement-level trigger fires once for the statement and is not cloned
CREATE TRIGGER dtr_stg AFTER INSERT ON dtr_p FOR EACH STATEMENT EXECUTE FUNCTION dtr_tf();
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
 WHERE c.relname IN ('dtr_pa','dtr_pb') AND t.tgname = 'dtr_stg';

-- and it is there on the partitioned table itself, which is what it was written on
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::text AS n FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
 WHERE c.relname = 'dtr_p' AND t.tgname = 'dtr_stg';

-- a name a partition already carries is a collision reported against the partition
CREATE TRIGGER dtr_ptg2 AFTER INSERT ON dtr_pa FOR EACH ROW EXECUTE FUNCTION dtr_tf();
-- begin-expected-error
-- sqlstate: 42710
-- message-like: trigger "dtr_ptg2" for relation "dtr_pa" already exists
-- end-expected-error
CREATE TRIGGER dtr_ptg2 AFTER INSERT ON dtr_p FOR EACH ROW EXECUTE FUNCTION dtr_tf();

-- dropping the original takes every copy with it
DROP TRIGGER dtr_ptg ON dtr_p;
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM pg_trigger WHERE tgname = 'dtr_ptg';

DROP TABLE dtr_p CASCADE;
DROP FUNCTION dtr_tf() CASCADE;

-- ============================================================================
-- Which relations can carry a trigger, and which shape of trigger they take
-- ============================================================================
DROP TABLE IF EXISTS dtr_kt CASCADE;
DROP MATERIALIZED VIEW IF EXISTS dtr_kmv;
DROP SEQUENCE IF EXISTS dtr_ks;
DROP FUNCTION IF EXISTS dtr_kf() CASCADE;
CREATE FUNCTION dtr_kf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$;
CREATE TABLE dtr_kt (i int);
CREATE VIEW dtr_kv AS SELECT i FROM dtr_kt;
CREATE VIEW dtr_kvk AS SELECT 1 AS i;
CREATE SEQUENCE dtr_ks;
CREATE MATERIALIZED VIEW dtr_kmv AS SELECT 1 AS i;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: relation "dtr_ks" cannot have triggers
-- detail-like: This operation is not supported for sequences.
-- end-expected-error
CREATE TRIGGER dtr_ktg1 BEFORE INSERT ON dtr_ks FOR EACH ROW EXECUTE FUNCTION dtr_kf();

-- begin-expected-error
-- sqlstate: 42809
-- message-like: relation "dtr_kmv" cannot have triggers
-- detail-like: This operation is not supported for materialized views.
-- end-expected-error
CREATE TRIGGER dtr_ktg2 BEFORE INSERT ON dtr_kmv FOR EACH ROW EXECUTE FUNCTION dtr_kf();

-- a view takes a statement-level trigger, whether or not it could be written through
CREATE TRIGGER dtr_ktg3 AFTER INSERT ON dtr_kv FOR EACH STATEMENT EXECUTE FUNCTION dtr_kf();
CREATE TRIGGER dtr_ktg4 BEFORE INSERT ON dtr_kv FOR EACH STATEMENT EXECUTE FUNCTION dtr_kf();
CREATE TRIGGER dtr_ktg5 AFTER INSERT ON dtr_kvk FOR EACH STATEMENT EXECUTE FUNCTION dtr_kf();

-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT count(*)::text AS n FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
 WHERE c.relname IN ('dtr_kv','dtr_kvk') AND NOT t.tgisinternal;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "dtr_kv" is a view
-- detail-like: Views cannot have row-level BEFORE or AFTER triggers.
-- end-expected-error
CREATE TRIGGER dtr_ktg6 AFTER INSERT ON dtr_kv FOR EACH ROW EXECUTE FUNCTION dtr_kf();

-- begin-expected-error
-- sqlstate: 42809
-- message-like: "dtr_kv" is a view
-- detail-like: Triggers on views cannot have transition tables.
-- end-expected-error
CREATE TRIGGER dtr_ktg7 AFTER INSERT ON dtr_kv REFERENCING NEW TABLE AS dtr_nt
  FOR EACH STATEMENT EXECUTE FUNCTION dtr_kf();

DROP VIEW dtr_kvk;
DROP VIEW dtr_kv;
DROP MATERIALIZED VIEW dtr_kmv;
DROP SEQUENCE dtr_ks;
DROP TABLE dtr_kt CASCADE;
DROP FUNCTION dtr_kf() CASCADE;

-- ============================================================================
-- Transition tables, system columns in WHEN, and the constraint-trigger grammar
-- ============================================================================
DROP TABLE IF EXISTS dtr_xp CASCADE;
DROP TABLE IF EXISTS dtr_xt CASCADE;
DROP FUNCTION IF EXISTS dtr_xf() CASCADE;
CREATE FUNCTION dtr_xf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$;
CREATE TABLE dtr_xp (i int) PARTITION BY RANGE (i);
CREATE TABLE dtr_xpa PARTITION OF dtr_xp FOR VALUES FROM (0) TO (100);
CREATE TABLE dtr_xt (i int);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ROW triggers with transition tables are not supported on partitions
-- end-expected-error
CREATE TRIGGER dtr_xtg1 AFTER INSERT ON dtr_xpa REFERENCING NEW TABLE AS dtr_nt
  FOR EACH ROW EXECUTE FUNCTION dtr_xf();

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: "dtr_xp" is a partitioned table
-- detail-like: ROW triggers with transition tables are not supported on partitioned tables.
-- end-expected-error
CREATE TRIGGER dtr_xtg2 AFTER INSERT ON dtr_xp REFERENCING NEW TABLE AS dtr_nt
  FOR EACH ROW EXECUTE FUNCTION dtr_xf();

-- the statement-level spelling is accepted on both
CREATE TRIGGER dtr_xtg3 AFTER INSERT ON dtr_xpa REFERENCING NEW TABLE AS dtr_nt
  FOR EACH STATEMENT EXECUTE FUNCTION dtr_xf();
CREATE TRIGGER dtr_xtg4 AFTER INSERT ON dtr_xp REFERENCING NEW TABLE AS dtr_nt
  FOR EACH STATEMENT EXECUTE FUNCTION dtr_xf();

-- a WHEN condition may read the system columns every relation carries
CREATE TRIGGER dtr_xtg5 AFTER INSERT ON dtr_xt FOR EACH ROW
  WHEN (NEW.ctid IS NOT NULL) EXECUTE FUNCTION dtr_xf();
CREATE TRIGGER dtr_xtg6 AFTER INSERT ON dtr_xt FOR EACH ROW
  WHEN (NEW.xmin IS NOT NULL) EXECUTE FUNCTION dtr_xf();

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column new.nosuch does not exist
-- end-expected-error
CREATE TRIGGER dtr_xtg7 AFTER INSERT ON dtr_xt FOR EACH ROW
  WHEN (NEW.nosuch IS NOT NULL) EXECUTE FUNCTION dtr_xf();

-- a constraint trigger's grammar has no REFERENCING clause
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "REFERENCING"
-- end-expected-error
CREATE CONSTRAINT TRIGGER dtr_xtg8 AFTER INSERT ON dtr_xt REFERENCING NEW TABLE AS dtr_nt2
  FOR EACH ROW EXECUTE FUNCTION dtr_xf();

CREATE CONSTRAINT TRIGGER dtr_xtg9 AFTER INSERT ON dtr_xt DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION dtr_xf();

DROP TABLE dtr_xp CASCADE;
DROP TABLE dtr_xt CASCADE;
DROP FUNCTION dtr_xf() CASCADE;

-- ============================================================================
-- A function a trigger executes, and a relation a rule writes to, are depended on
-- ============================================================================
DROP TABLE IF EXISTS dtr_ft CASCADE;
DROP FUNCTION IF EXISTS dtr_gf() CASCADE;
CREATE TABLE dtr_ft (i int);
CREATE FUNCTION dtr_gf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$;
CREATE TRIGGER dtr_gtg AFTER INSERT ON dtr_ft FOR EACH ROW EXECUTE FUNCTION dtr_gf();

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop function dtr_gf() because other objects depend on it
-- detail-like: trigger dtr_gtg on table dtr_ft depends on function dtr_gf()
-- hint-like: Use DROP ... CASCADE to drop the dependent objects too.
-- end-expected-error
DROP FUNCTION dtr_gf();

DROP FUNCTION dtr_gf() CASCADE;
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM pg_trigger WHERE tgname = 'dtr_gtg';

-- dropping the trigger first leaves the function free to go
CREATE FUNCTION dtr_gf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$;
CREATE TRIGGER dtr_gtg AFTER INSERT ON dtr_ft FOR EACH ROW EXECUTE FUNCTION dtr_gf();
DROP TRIGGER dtr_gtg ON dtr_ft;
DROP FUNCTION dtr_gf();
DROP TABLE dtr_ft CASCADE;

DROP TABLE IF EXISTS dtr_dep CASCADE;
DROP TABLE IF EXISTS dtr_deplog CASCADE;
CREATE TABLE dtr_dep (i int);
CREATE TABLE dtr_deplog (i int);
CREATE RULE dtr_depr AS ON INSERT TO dtr_dep DO ALSO INSERT INTO dtr_deplog VALUES (NEW.i);

-- begin-expected-error
-- sqlstate: 2BP01
-- message-like: cannot drop table dtr_deplog because other objects depend on it
-- detail-like: rule dtr_depr on table dtr_dep depends on table dtr_deplog
-- hint-like: Use DROP ... CASCADE to drop the dependent objects too.
-- end-expected-error
DROP TABLE dtr_deplog;

-- the ruled relation can still be written to, and the rule still fires
INSERT INTO dtr_dep VALUES (1);
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::text AS n FROM dtr_deplog;

-- CASCADE takes the rule with it, and the writes go on
DROP TABLE dtr_deplog CASCADE;
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::text AS n FROM pg_rules WHERE tablename = 'dtr_dep';
INSERT INTO dtr_dep VALUES (2);
DROP TABLE dtr_dep CASCADE;

-- ============================================================================
-- CREATE RULE analyses its actions, and judges what kind of relation it is on
-- ============================================================================
DROP TABLE IF EXISTS dtr_at CASCADE;
DROP TABLE IF EXISTS dtr_rl1 CASCADE;
DROP MATERIALIZED VIEW IF EXISTS dtr_amv;
DROP SEQUENCE IF EXISTS dtr_aseq;
CREATE TABLE dtr_at (i int);
CREATE TABLE dtr_rl1 (i int, t text);

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "dtr_nosuchtable" does not exist
-- end-expected-error
CREATE RULE dtr_ar1 AS ON INSERT TO dtr_at DO ALSO INSERT INTO dtr_nosuchtable VALUES (1);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column new.nosuchcol does not exist
-- end-expected-error
CREATE RULE dtr_ar2 AS ON INSERT TO dtr_at DO ALSO INSERT INTO dtr_rl1 VALUES (NEW.nosuchcol);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "CREATE"
-- end-expected-error
CREATE RULE dtr_ar3 AS ON INSERT TO dtr_at DO ALSO CREATE TABLE dtr_illegal (x int);

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "dtr_nosuch2" does not exist
-- end-expected-error
CREATE RULE dtr_ar4 AS ON INSERT TO dtr_at DO ALSO UPDATE dtr_nosuch2 SET i = 1;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "dtr_nosuch3" does not exist
-- end-expected-error
CREATE RULE dtr_ar5 AS ON INSERT TO dtr_at DO ALSO SELECT * FROM dtr_nosuch3;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column old.nosuchcol does not exist
-- end-expected-error
CREATE RULE dtr_ar6 AS ON DELETE TO dtr_at DO ALSO INSERT INTO dtr_rl1 (i) VALUES (OLD.nosuchcol);

-- what must keep being accepted: a system column of NEW, a NOTIFY, an action on a real relation
CREATE RULE dtr_ar7 AS ON INSERT TO dtr_at DO ALSO INSERT INTO dtr_rl1 (t) VALUES (NEW.ctid::text);
CREATE RULE dtr_ar8 AS ON INSERT TO dtr_at DO ALSO NOTIFY dtr_chan;
CREATE RULE dtr_ar9 AS ON INSERT TO dtr_at DO ALSO INSERT INTO dtr_rl1 (i) VALUES (NEW.i);

-- begin-expected
-- columns: rulename
-- row: dtr_ar7
-- row: dtr_ar8
-- row: dtr_ar9
-- end-expected
SELECT rulename::text AS rulename FROM pg_rules WHERE tablename = 'dtr_at' ORDER BY 1;

CREATE SEQUENCE dtr_aseq;
-- begin-expected-error
-- sqlstate: 42809
-- message-like: relation "dtr_aseq" cannot have rules
-- detail-like: This operation is not supported for sequences.
-- end-expected-error
CREATE RULE dtr_asr AS ON INSERT TO dtr_aseq DO INSTEAD NOTHING;

CREATE VIEW dtr_av AS SELECT 1 AS i;
-- begin-expected-error
-- sqlstate: 55000
-- message-like: "dtr_av" is already a view
-- end-expected-error
CREATE RULE dtr_avr AS ON SELECT TO dtr_av DO INSTEAD SELECT 2 AS i;

CREATE MATERIALIZED VIEW dtr_amv AS SELECT 1 AS i;
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: rules on materialized views are not supported
-- end-expected-error
CREATE RULE dtr_amr AS ON INSERT TO dtr_amv DO INSTEAD NOTHING;

DROP MATERIALIZED VIEW dtr_amv;
DROP VIEW dtr_av;
DROP SEQUENCE dtr_aseq;
DROP TABLE dtr_at CASCADE;
DROP TABLE dtr_rl1 CASCADE;