-- source: investigation-2026-08.md
-- finding: 256
-- title: Unrelated singletons in this area
-- A: CREATE TABLE zz_t (i int PRIMARY KEY);
--    CREATE TABLE zz_u (id int PRIMARY KEY, p int REFERENCES zz_t(i));
--    BEGIN; INSERT INTO zz_t VALUES (7);
-- B: INSERT INTO zz_u VALUES (1, 7);
-- A: ROLLBACK;
-- B: SELECT count(*) FROM zz_u ch WHERE NOT EXISTS (SELECT 1 FROM zz_t p WHERE p.i = ch.p);
-- B: BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT count(*) FROM zz_t;
-- A: DELETE FROM zz_t WHERE i = 2;
-- B: DELETE FROM zz_t WHERE i = 2; SELECT 1;
-- B: BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT 1;
-- A: CREATE TABLE zz_u (i int); INSERT INTO zz_u VALUES (1),(2);
-- B: SELECT count(*) FROM pg_class WHERE relname = 'zz_u';
-- B: SELECT count(*) FROM zz_u;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_e AS ENUM ('lo','mid','hi');
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_p (e zz_e NOT NULL) PARTITION BY RANGE (e);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_p1 PARTITION OF zz_p FOR VALUES FROM ('lo') TO ('hi');
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_p VALUES ('mid');
-- begin-expected
-- columns: e:zz_e
-- row: mid
-- rowcount: 1
-- end-expected
SELECT e FROM zz_p;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_t (id int PRIMARY KEY, v int);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_t VALUES (1, 0);
-- begin-expected
-- columns: ca:int8 | cb:int8
-- row: 1 | 0
-- rowcount: 1
-- end-expected
WITH a AS (UPDATE zz_t SET v = 100 WHERE id = 1 RETURNING id),
     b AS (UPDATE zz_t SET v = 200 WHERE id = 1 RETURNING id)
SELECT (SELECT count(*) FROM a) AS ca, (SELECT count(*) FROM b) AS cb;
-- begin-expected
-- columns: id:int4 | v:int4
-- row: 1 | 100
-- rowcount: 1
-- end-expected
SELECT id, v FROM zz_t;
-- A: BEGIN; UPDATE zz_t SET v = 99 WHERE i = 1;
-- B: UPDATE zz_t SET s = 'q' WHERE i IN (SELECT i FROM zz_t WHERE v = 10);  -- blocks
-- A: COMMIT;
-- A: BEGIN; UPDATE zz_t SET v=v+1 WHERE id=1;   (left open)
-- B: SELECT ctid::text FROM zz_t WHERE id=1;
-- A: ROLLBACK;
-- B: SELECT ctid::text FROM zz_t WHERE id=1;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_b (id int, note text);
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_v AS SELECT id, note FROM zz_b;
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_f() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN OLD; END $$;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TRIGGER zz_t INSTEAD OF DELETE ON zz_v FOR EACH ROW EXECUTE FUNCTION zz_f();
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid WHERE c.relname = 'zz_v';
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_v" already exists
-- end-expected-error
CREATE VIEW zz_v WITH (security_barrier) AS SELECT 1 AS x;
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_v2 WITH (security_invoker) AS SELECT 1 AS x;
