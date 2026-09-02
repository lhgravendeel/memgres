-- source: review-2026-08.md
-- finding: Unrelated singletons
-- area: Isolation, deadlocks and features in combination
-- title: Unrelated singletons
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_ref (id int PRIMARY KEY);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_ref VALUES (1);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_p (id int, k int NOT NULL) PARTITION BY RANGE (k);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_pa PARTITION OF zz_p FOR VALUES FROM (0) TO (100);
-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot add column to a partition
-- end-expected-error
ALTER TABLE zz_pa ADD COLUMN lk int;
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "lk" referenced in foreign key constraint does not exist
-- end-expected-error
ALTER TABLE zz_pa ADD CONSTRAINT zz_pfk FOREIGN KEY (lk) REFERENCES zz_ref(id);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_p (id, k) VALUES (1, 10);
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
-- B: UPDATE zz_t SET s = 'q' WHERE i IN (SELECT i FROM zz_t WHERE v = 10);   -- blocks
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
-- begin-expected
-- ok: 0
-- end-expected
CREATE TYPE zz_e AS ENUM ('lo','mid','hi');
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_p" already exists
-- end-expected-error
CREATE TABLE zz_p (e zz_e NOT NULL) PARTITION BY RANGE (e);
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "lo"
-- end-expected-error
CREATE TABLE zz_p1 PARTITION OF zz_p FOR VALUES FROM ('lo') TO ('hi');
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "mid"
-- end-expected-error
INSERT INTO zz_p VALUES ('mid');
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "e" does not exist
-- end-expected-error
SELECT e FROM zz_p;
-- begin-expected-error
-- sqlstate: 42P07
-- message-like: relation "zz_v" already exists
-- end-expected-error
CREATE VIEW zz_v WITH (security_barrier) AS SELECT 1 AS x;
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_v2 WITH (security_invoker) AS SELECT 1 AS x;
