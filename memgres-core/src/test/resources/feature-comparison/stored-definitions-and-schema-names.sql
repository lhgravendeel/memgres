-- ============================================================================
-- Stored definitions, schema-qualified names, and the name a type answers to
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- Three rules, each with the ordinary shapes around it:
--
--   A stored expression -- a CHECK on a column and on a table, a DEFAULT, an
--   index expression and predicate, a generated column, a policy, a rule's
--   qualification, a domain constraint, a partition bound, a trigger's WHEN --
--   is analysed exactly like an expression written in a query. A call carrying
--   FILTER, DISTINCT or an aggregate ORDER BY without being an aggregate is
--   refused there too, and which of a definition's faults is reported follows
--   the order the expression is read in rather than the order of the checks.
--
--   A schema qualifier has to name a schema. On a type name that is 3F000
--   wherever a type is written; on a relation a DDL statement names it is 3F000
--   as well; on a relation a query reads it is 42P01 naming the relation, which
--   is a different lookup and a different answer.
--
--   A domain is a type of its own, and every place a type is named back to the
--   client -- pg_typeof, format_type, information_schema, the RowDescription --
--   answers with the domain rather than with the type it is built on.
-- ============================================================================

DROP TABLE IF EXISTS sdsn_t CASCADE;
DROP SCHEMA IF EXISTS sdsn_s CASCADE;
DROP DOMAIN IF EXISTS sdsn_yn CASCADE;
DROP DOMAIN IF EXISTS sdsn_pos CASCADE;
DROP DOMAIN IF EXISTS sdsn_arr CASCADE;
DROP TYPE IF EXISTS sdsn_mood CASCADE;
CREATE SCHEMA sdsn_s;
CREATE TYPE sdsn_mood AS ENUM ('ok','bad');
CREATE DOMAIN sdsn_yn AS boolean;
CREATE DOMAIN sdsn_pos AS int;
CREATE DOMAIN sdsn_arr AS int[];
CREATE TABLE sdsn_t (id int PRIMARY KEY, v int, txt text,
                     y sdsn_yn, p sdsn_pos, a sdsn_arr, e sdsn_mood);
INSERT INTO sdsn_t VALUES (1,1,'aa',true,3,'{1,2}','ok'),
                          (2,2,'ab',false,4,'{3}','bad');

-- ============================================================================
-- 1. A clause only an aggregate may carry, inside a stored definition
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
CREATE TABLE sdsn_g1 (i int CHECK (abs(i) FILTER (WHERE true) > 0));

-- begin-expected-error
-- sqlstate: 42809
-- message-like: DISTINCT specified, but abs is not an aggregate function
-- end-expected-error
CREATE TABLE sdsn_g2 (i int CHECK (abs(DISTINCT i) > 0));

-- begin-expected-error
-- sqlstate: 42809
-- message-like: ORDER BY specified, but abs is not an aggregate function
-- end-expected-error
CREATE TABLE sdsn_g3 (i int CHECK (abs(i ORDER BY i) > 0));

-- a table-level CHECK is the same expression in a different place
-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
CREATE TABLE sdsn_g4 (i int, CHECK (abs(i) FILTER (WHERE true) > 0));

-- begin-expected-error
-- sqlstate: 42809
-- message-like: ORDER BY specified, but abs is not an aggregate function
-- end-expected-error
CREATE TABLE sdsn_g5 (i int DEFAULT (abs(1 ORDER BY 1)));

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
CREATE TABLE sdsn_g6 (i int, j int GENERATED ALWAYS AS (abs(i) FILTER (WHERE true)) STORED);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: ORDER BY specified, but abs is not an aggregate function
-- end-expected-error
CREATE INDEX sdsn_g7 ON sdsn_t ((abs(id ORDER BY id)));

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
CREATE INDEX sdsn_g8 ON sdsn_t (id) WHERE abs(id) FILTER (WHERE true) > 0;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
CREATE POLICY sdsn_g9 ON sdsn_t USING (abs(id) FILTER (WHERE true) > 0);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
CREATE POLICY sdsn_g10 ON sdsn_t FOR INSERT WITH CHECK (abs(id) FILTER (WHERE true) > 0);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
CREATE RULE sdsn_g11 AS ON DELETE TO sdsn_t
    WHERE abs(old.id) FILTER (WHERE true) > 0 DO INSTEAD NOTHING;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: DISTINCT specified, but abs is not an aggregate function
-- end-expected-error
CREATE DOMAIN sdsn_g12 AS int CHECK (abs(DISTINCT VALUE) > 0);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
ALTER TABLE sdsn_t ADD CONSTRAINT sdsn_g13 CHECK (abs(v) FILTER (WHERE true) > 0);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: ORDER BY specified, but abs is not an aggregate function
-- end-expected-error
ALTER TABLE sdsn_t ALTER COLUMN v SET DEFAULT (abs(1 ORDER BY 1));

-- begin-expected-error
-- sqlstate: 42809
-- message-like: ORDER BY specified, but abs is not an aggregate function
-- end-expected-error
ALTER TABLE sdsn_t ADD COLUMN sdsn_c int DEFAULT (abs(1 ORDER BY 1));

-- a partition bound is settled once, and it is judged the same way
CREATE TABLE sdsn_pt (a int) PARTITION BY RANGE (a);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
CREATE TABLE sdsn_pt1 PARTITION OF sdsn_pt FOR VALUES FROM (abs(1) FILTER (WHERE true)) TO (10);

DROP TABLE sdsn_pt CASCADE;

-- a view body already goes through the ordinary path, and stays there
-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
CREATE VIEW sdsn_g14 AS SELECT abs(id) FILTER (WHERE true) FROM sdsn_t;

-- a trigger's WHEN is analysed against the relation before the function it will
-- call is looked for, so the condition is what is reported when both are wrong
CREATE FUNCTION sdsn_tf() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
CREATE TRIGGER sdsn_g15 BEFORE INSERT ON sdsn_t FOR EACH ROW
    WHEN (abs(NEW.id) FILTER (WHERE true) > 0) EXECUTE FUNCTION sdsn_nofn();

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column new.nosuchcol does not exist
-- end-expected-error
CREATE TRIGGER sdsn_g16 BEFORE INSERT ON sdsn_t FOR EACH ROW
    WHEN (NEW.nosuchcol > 0) EXECUTE FUNCTION sdsn_tf();

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in trigger WHEN conditions
-- end-expected-error
CREATE TRIGGER sdsn_g17 BEFORE INSERT ON sdsn_t FOR EACH ROW
    WHEN (count(*) > 0) EXECUTE FUNCTION sdsn_tf();

-- ...and a condition that stands is what leaves the missing function to be reported
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function sdsn_nofn() does not exist
-- end-expected-error
CREATE TRIGGER sdsn_g18 BEFORE INSERT ON sdsn_t FOR EACH ROW
    WHEN (NEW.v > 0) EXECUTE FUNCTION sdsn_nofn();

CREATE TRIGGER sdsn_g19 BEFORE INSERT ON sdsn_t FOR EACH ROW
    WHEN (NEW.v > 0) EXECUTE FUNCTION sdsn_tf();
DROP TRIGGER sdsn_g19 ON sdsn_t;
DROP FUNCTION sdsn_tf() CASCADE;

-- ============================================================================
-- 2. Which of a definition's faults is reported
-- ============================================================================
-- Every kind of fault is found by one walk, so the leftmost one is named.

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in check constraints
-- end-expected-error
CREATE TABLE sdsn_o1 (i int CHECK (count(*) > 0 AND abs(i) FILTER (WHERE true) > 0));

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
CREATE TABLE sdsn_o2 (i int CHECK (abs(i) FILTER (WHERE true) > 0 AND count(*) > 0));

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in check constraint
-- end-expected-error
CREATE TABLE sdsn_o3 (i int CHECK ((SELECT 1) > 0 AND abs(i) FILTER (WHERE true) > 0));

-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
CREATE TABLE sdsn_o4 (i int CHECK (abs(i) FILTER (WHERE true) > (SELECT 1)));

-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in check constraints
-- end-expected-error
CREATE TABLE sdsn_o5 (i int CHECK (count(*) > 0 AND (SELECT 1) > 0));

-- the FILTER is judged before the type of what it stands in
-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but abs is not an aggregate function
-- end-expected-error
CREATE TABLE sdsn_o6 (i int CHECK (abs(i) FILTER (WHERE true)));

-- a qualified built-in is named the way it was written
-- begin-expected-error
-- sqlstate: 42809
-- message-like: FILTER specified, but pg_catalog.abs is not an aggregate function
-- end-expected-error
CREATE TABLE sdsn_o7 (i int CHECK (pg_catalog.abs(i) FILTER (WHERE true) > 0));

-- DISTINCT is read before an aggregate ORDER BY, so it is the one named
-- begin-expected-error
-- sqlstate: 42809
-- message-like: DISTINCT specified, but abs is not an aggregate function
-- end-expected-error
CREATE TABLE sdsn_o8 (i int CHECK (abs(DISTINCT i ORDER BY i) > 0));

-- a DEFAULT that names a column is refused for that first
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use column reference in DEFAULT expression
-- end-expected-error
CREATE TABLE sdsn_o9 (i int, j int DEFAULT (abs(i) FILTER (WHERE true)));

-- and a DEFAULT holding an aggregate is refused for that
-- begin-expected-error
-- sqlstate: 42803
-- message-like: aggregate functions are not allowed in DEFAULT expressions
-- end-expected-error
CREATE TABLE sdsn_o10 (i int DEFAULT (count(*) FILTER (WHERE true)));

-- ============================================================================
-- 3. The ordinary shapes, which are still accepted
-- ============================================================================
-- An aggregate is what FILTER, DISTINCT and an aggregate ORDER BY are for.

CREATE VIEW sdsn_ok1 AS SELECT count(*) FILTER (WHERE v > 1) AS c,
                               count(DISTINCT v) AS d,
                               string_agg(txt, ',' ORDER BY id) AS s FROM sdsn_t;

-- begin-expected
-- columns: c | d | s
-- row: 1, 2, aa,ab
-- end-expected
SELECT c, d, s FROM sdsn_ok1;

DROP VIEW sdsn_ok1;

-- a plain call with no such clause is ordinary everywhere a definition is stored
CREATE TABLE sdsn_ok2 (i int CHECK (abs(i) > 0 AND coalesce(i, 0) >= 0),
                       j int DEFAULT greatest(1, 2),
                       g int GENERATED ALWAYS AS (abs(i) * 2) STORED);
INSERT INTO sdsn_ok2 (i) VALUES (5);

-- begin-expected
-- columns: i | j | g
-- row: 5, 2, 10
-- end-expected
SELECT i, j, g FROM sdsn_ok2;

DROP TABLE sdsn_ok2;

CREATE INDEX sdsn_ok3 ON sdsn_t ((lower(txt)), (abs(v))) WHERE abs(v) > 0;
DROP INDEX sdsn_ok3;
CREATE INDEX sdsn_ok4 ON sdsn_t ((id::bigint));
DROP INDEX sdsn_ok4;
CREATE DOMAIN sdsn_ok5 AS text CHECK (length(VALUE) > 0);
DROP DOMAIN sdsn_ok5;
CREATE POLICY sdsn_ok6 ON sdsn_t USING (abs(id) > 0 AND txt IS NOT NULL);
DROP POLICY sdsn_ok6 ON sdsn_t;
-- a policy is a query fragment, so a sub-query in one is ordinary
CREATE POLICY sdsn_ok7 ON sdsn_t USING (id IN (SELECT id FROM sdsn_t));
DROP POLICY sdsn_ok7 ON sdsn_t;
CREATE RULE sdsn_ok8 AS ON DELETE TO sdsn_t WHERE abs(old.id) > 100 DO INSTEAD NOTHING;
DROP RULE sdsn_ok8 ON sdsn_t;

-- a window call in a stored definition keeps its own complaint
-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in check constraints
-- end-expected-error
CREATE TABLE sdsn_ok9 (i int CHECK (row_number() OVER () > 0));

-- begin-expected-error
-- sqlstate: 42809
-- message-like: window function row_number requires an OVER clause
-- end-expected-error
CREATE TABLE sdsn_ok10 (i int CHECK (row_number() > 0));

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: window functions are not allowed in policy expressions
-- end-expected-error
CREATE POLICY sdsn_ok11 ON sdsn_t USING (row_number() OVER () > 0);

-- and so does a sub-query where a definition may not hold one
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in check constraint
-- end-expected-error
CREATE TABLE sdsn_ok12 (i int CHECK (i IN (SELECT 1)));

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in index predicate
-- end-expected-error
CREATE INDEX sdsn_ok13 ON sdsn_t (id) WHERE id IN (SELECT 1);

-- ============================================================================
-- 4. A schema qualifier on a type name
-- ============================================================================

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "sdsn_no" does not exist
-- end-expected-error
SELECT 1::sdsn_no.int4;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "sdsn_no" does not exist
-- end-expected-error
SELECT CAST(1 AS sdsn_no.int4);

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "sdsn_no" does not exist
-- end-expected-error
SELECT ARRAY[1]::sdsn_no.int4[];

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "sdsn_no" does not exist
-- end-expected-error
CREATE TABLE sdsn_e1 (a sdsn_no.int4);

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "sdsn_no" does not exist
-- end-expected-error
ALTER TABLE sdsn_t ADD COLUMN sdsn_e2 sdsn_no.int4;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "sdsn_no" does not exist
-- end-expected-error
ALTER TABLE sdsn_t ALTER COLUMN v TYPE sdsn_no.int4;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "sdsn_no" does not exist
-- end-expected-error
CREATE DOMAIN sdsn_e3 AS sdsn_no.int4;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "sdsn_no" does not exist
-- end-expected-error
CREATE FUNCTION sdsn_e4(a sdsn_no.int4) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "sdsn_no" does not exist
-- end-expected-error
CREATE FUNCTION sdsn_e5(a int) RETURNS sdsn_no.int4 AS $$ SELECT 1 $$ LANGUAGE sql;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "sdsn_no" does not exist
-- end-expected-error
PREPARE sdsn_e6 (sdsn_no.int4) AS SELECT 1;

-- the type is resolved while the statement is analysed, so it is refused whether
-- or not the expression holding it would ever have been evaluated
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "sdsn_no" does not exist
-- end-expected-error
SELECT CASE WHEN false THEN 1::sdsn_no.int4 ELSE 0 END;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "sdsn_no" does not exist
-- end-expected-error
WITH c AS (SELECT 1::sdsn_no.int4) SELECT 1;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "sdsn_no" does not exist
-- end-expected-error
SELECT (SELECT 1::sdsn_no.int4);

-- and inside a definition kept as the text it was written as
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "sdsn_no" does not exist
-- end-expected-error
CREATE INDEX sdsn_e7 ON sdsn_t ((id::sdsn_no.int4));

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "sdsn_no" does not exist
-- end-expected-error
CREATE TABLE sdsn_e8 (i int CHECK (i::sdsn_no.int4 > 0));

-- the leftmost qualifier is the one named
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "sdsn_no1" does not exist
-- end-expected-error
SELECT 1::sdsn_no1.int4, 1::sdsn_no2.int4;

-- but the range table is built first, so a relation that is not there outranks it
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "sdsn_nosuch" does not exist
-- end-expected-error
SELECT 1::sdsn_no.int4 FROM sdsn_nosuch;

-- ...while a column that is not there does not
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "sdsn_no" does not exist
-- end-expected-error
SELECT sdsn_nocol::sdsn_no.int4 FROM sdsn_t;

-- ============================================================================
-- 5. A schema qualifier on a relation, which is not the same lookup
-- ============================================================================

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "sdsn_no" does not exist
-- end-expected-error
CREATE INDEX sdsn_r1 ON sdsn_no.sdsn_t (id);

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "sdsn_no" does not exist
-- end-expected-error
ALTER TABLE sdsn_no.sdsn_t ADD COLUMN c int;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "sdsn_no" does not exist
-- end-expected-error
CREATE TABLE sdsn_no.sdsn_r2 (id int);

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "sdsn_no" does not exist
-- end-expected-error
CREATE VIEW sdsn_no.sdsn_r3 AS SELECT 1;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "sdsn_no" does not exist
-- end-expected-error
CREATE SEQUENCE sdsn_no.sdsn_r4;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "sdsn_no" does not exist
-- end-expected-error
CREATE DOMAIN sdsn_no.sdsn_r5 AS int;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "sdsn_no" does not exist
-- end-expected-error
CREATE FUNCTION sdsn_no.sdsn_r6() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "sdsn_no" does not exist
-- end-expected-error
CREATE TABLE sdsn_no.sdsn_r7 AS SELECT 1 AS a;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "sdsn_no" does not exist
-- end-expected-error
TRUNCATE sdsn_no.sdsn_t;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "sdsn_no" does not exist
-- end-expected-error
DROP TABLE sdsn_no.sdsn_t;

-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "sdsn_no" does not exist
-- end-expected-error
SELECT 1 OPERATOR(sdsn_no.+) 1;

-- a relation a query reads is 42P01 naming the relation, not 3F000
-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "sdsn_no.sdsn_t" does not exist
-- end-expected-error
SELECT * FROM sdsn_no.sdsn_t;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "sdsn_no.sdsn_t" does not exist
-- end-expected-error
INSERT INTO sdsn_no.sdsn_t VALUES (9);

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "sdsn_no.sdsn_t" does not exist
-- end-expected-error
UPDATE sdsn_no.sdsn_t SET v = 1;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "sdsn_no.sdsn_t" does not exist
-- end-expected-error
DELETE FROM sdsn_no.sdsn_t;

-- begin-expected-error
-- sqlstate: 42P01
-- message-like: relation "sdsn_no.sdsn_t" does not exist
-- end-expected-error
CREATE VIEW sdsn_r8 AS SELECT * FROM sdsn_no.sdsn_t;

-- IF EXISTS skips on the schema by name rather than refusing
DROP TABLE IF EXISTS sdsn_no.sdsn_t;

-- ============================================================================
-- 6. The ordinary shapes around a schema qualifier
-- ============================================================================

-- begin-expected
-- columns: a | b | c
-- row: 1, x, 1
-- end-expected
SELECT 1::pg_catalog.int4 AS a, 'x'::pg_catalog.text AS b,
       CAST(1 AS pg_catalog.numeric(5,0)) AS c;

-- begin-expected
-- columns: a
-- row: {1,2}
-- end-expected
SELECT ARRAY[1,2]::pg_catalog.int4[] AS a;

CREATE TABLE sdsn_s.sdsn_q1 (i pg_catalog.int4, j pg_catalog.text);
INSERT INTO sdsn_s.sdsn_q1 VALUES (1, 'x');

-- begin-expected
-- columns: i | j
-- row: 1, x
-- end-expected
SELECT i, j FROM sdsn_s.sdsn_q1;

CREATE DOMAIN sdsn_s.sdsn_q2 AS pg_catalog.int4;
CREATE TABLE sdsn_s.sdsn_q3 (k sdsn_s.sdsn_q2);
INSERT INTO sdsn_s.sdsn_q3 VALUES (7);

-- begin-expected
-- columns: k
-- row: 7
-- end-expected
SELECT k FROM sdsn_s.sdsn_q3;

CREATE FUNCTION sdsn_s.sdsn_q4(a pg_catalog.int4) RETURNS pg_catalog.int4
    AS $$ SELECT a + 1 $$ LANGUAGE sql;

-- begin-expected
-- columns: r
-- row: 2
-- end-expected
SELECT sdsn_s.sdsn_q4(1) AS r;

CREATE VIEW sdsn_s.sdsn_q5 AS SELECT id FROM sdsn_t;

-- begin-expected
-- columns: id
-- row: 1
-- row: 2
-- end-expected
SELECT id FROM sdsn_s.sdsn_q5 ORDER BY id;

CREATE INDEX sdsn_q6 ON public.sdsn_t (v);
DROP INDEX public.sdsn_q6;
ALTER TABLE public.sdsn_t ADD COLUMN sdsn_q7 pg_catalog.int4;
ALTER TABLE public.sdsn_t DROP COLUMN sdsn_q7;

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT pg_catalog.abs(-1) AS a;

-- begin-expected
-- columns: a
-- row: 2
-- end-expected
SELECT 1 OPERATOR(pg_catalog.+) 1 AS a;

-- ============================================================================
-- 7. The name a type answers to
-- ============================================================================

-- begin-expected
-- columns: t
-- row: sdsn_yn
-- end-expected
SELECT pg_typeof(y)::text AS t FROM sdsn_t LIMIT 1;

-- begin-expected
-- columns: t
-- row: sdsn_pos
-- end-expected
SELECT pg_typeof(p)::text AS t FROM sdsn_t LIMIT 1;

-- begin-expected
-- columns: t
-- row: sdsn_arr
-- end-expected
SELECT pg_typeof(a)::text AS t FROM sdsn_t LIMIT 1;

-- begin-expected
-- columns: t
-- row: sdsn_mood
-- end-expected
SELECT pg_typeof(e)::text AS t FROM sdsn_t LIMIT 1;

-- begin-expected
-- columns: t
-- row: integer
-- end-expected
SELECT pg_typeof(id)::text AS t FROM sdsn_t LIMIT 1;

-- a derived table, a CTE and a join all keep the name
-- begin-expected
-- columns: t
-- row: sdsn_yn
-- end-expected
SELECT pg_typeof(b)::text AS t FROM (SELECT y AS b FROM sdsn_t) q LIMIT 1;

-- begin-expected
-- columns: t
-- row: sdsn_yn
-- end-expected
WITH q AS (SELECT y AS b FROM sdsn_t) SELECT pg_typeof(b)::text AS t FROM q LIMIT 1;

-- begin-expected
-- columns: t
-- row: sdsn_pos
-- end-expected
SELECT pg_typeof(x.p)::text AS t FROM sdsn_t x JOIN sdsn_t z ON x.id = z.id LIMIT 1;

-- a cast to the domain answers with the domain
-- begin-expected
-- columns: t
-- row: sdsn_pos
-- end-expected
SELECT pg_typeof(1::sdsn_pos)::text AS t;

-- an expression over a domain-typed column is of the base type again
-- begin-expected
-- columns: t
-- row: integer
-- end-expected
SELECT pg_typeof(p + 1)::text AS t FROM sdsn_t LIMIT 1;

-- format_type reads the same name
-- begin-expected
-- columns: t
-- row: sdsn_yn
-- end-expected
SELECT format_type(at.atttypid, at.atttypmod) AS t
FROM pg_attribute at JOIN pg_class c ON c.oid = at.attrelid
WHERE c.relname = 'sdsn_t' AND at.attname = 'y';

-- begin-expected
-- columns: t
-- row: sdsn_arr
-- end-expected
SELECT format_type(at.atttypid, at.atttypmod) AS t
FROM pg_attribute at JOIN pg_class c ON c.oid = at.attrelid
WHERE c.relname = 'sdsn_t' AND at.attname = 'a';

-- and so does information_schema
-- begin-expected
-- columns: data_type | domain_name | udt_name
-- row: boolean, sdsn_yn, bool
-- end-expected
SELECT data_type, domain_name, udt_name FROM information_schema.columns
WHERE table_name = 'sdsn_t' AND column_name = 'y';

-- begin-expected
-- columns: data_type | domain_name | udt_name
-- row: integer, sdsn_pos, int4
-- end-expected
SELECT data_type, domain_name, udt_name FROM information_schema.columns
WHERE table_name = 'sdsn_t' AND column_name = 'p';

-- begin-expected
-- columns: data_type | domain_name | udt_name
-- row: USER-DEFINED, null, sdsn_mood
-- end-expected
SELECT data_type, domain_name, udt_name FROM information_schema.columns
WHERE table_name = 'sdsn_t' AND column_name = 'e';

-- the RowDescription a client reads
-- begin-expected
-- columns: y | p | a | e
-- row: true, 3, {1,2}, ok
-- end-expected
SELECT y, p, a, e FROM sdsn_t WHERE id = 1;

DROP TABLE IF EXISTS sdsn_t CASCADE;
DROP SCHEMA IF EXISTS sdsn_s CASCADE;
DROP DOMAIN IF EXISTS sdsn_yn CASCADE;
DROP DOMAIN IF EXISTS sdsn_pos CASCADE;
DROP DOMAIN IF EXISTS sdsn_arr CASCADE;
DROP TYPE IF EXISTS sdsn_mood CASCADE;
