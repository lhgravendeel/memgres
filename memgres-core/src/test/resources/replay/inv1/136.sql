-- source: investigation.md
-- finding: 136
-- title: Exception-handler identifiers leak into normal scope ⚠️
-- unrunnable: the report wrote this reproducer abbreviated
-- a function body outside any exception handler referring to SQLSTATE / SQLERRM
--   PG: 42703 column "sqlstate" does not exist
--   mg: returns the literal text 'sqlstate sqlerrm'
CREATE FUNCTION … $$ declare v text; begin get diagnostics v = message_text; return v; end $$;
--   PG: 42601 diagnostics item MESSAGE_TEXT is not allowed in GET CURRENT DIAGNOSTICS | mg: created
foreach x in array null::int[] loop … end loop;
-- PG: 22004 FOREACH expression must not be null | mg: 0 iterations
foreach x slice 2 in array array[1,2,3] …         -- PG: 2202E slice dimension out of range | mg: OK
foreach x slice 1 …  -- x is scalar               -- PG: 42804 SLICE loop variable must be an array | mg: OK
foreach x in array 42 loop …                      -- PG: 42804 must yield an array | mg: OK
declare x int;
begin foreach x in array array[[1,2],[3,4]] loop s := s || x || ',';
end loop;
--   PG: '1,2,3,4,' — a scalar FOREACH variable iterates the flattened array
--   mg: 42883 operator does not exist: text[] || integer[]
return query select a from zr7_rq;
-- fewer columns than the SETOF type
--   PG: 42804 structure of query does not match function result type | mg: returns 2 rows
return next 'notanint';
-- SETOF int
--   PG: 22P02 invalid input syntax for integer | mg: returns 1 row
return 1;
-- in a SETOF function    -- PG: 42804 RETURN cannot have a parameter … | mg: created
return next 1;
-- in a non-SETOF function-- PG: 42804 cannot use RETURN NEXT … | mg: created
CREATE FUNCTION f() RETURNS int AS $$ begin null; end $$;
-- no RETURN at all
SELECT f();
--   PG: 2F005 control reached end of function without RETURN
--   mg: returns NULL
exception when zr7_no_such_condition then null;
-- PG: 42704 unrecognized exception condition | mg: created
exception when SQLSTATE 'notavalidstate' then null;
-- PG: 42601 invalid SQLSTATE code | mg: created
assert 42;
-- PG: 22P02 invalid input syntax for type boolean: "42" | mg: assertion passes
get stacked diagnostics st = returned_sqlstate;
-- outside any handler
--   PG: 0Z002 cannot be used outside an exception handler | mg: created, returns NULL
get stacked diagnostics n = row_count;
-- PG: 42601 not allowed in GET STACKED | mg: created
get diagnostics v = zr7_no_such_item;
-- PG: 42601 unrecognized item | mg: created
CREATE FUNCTION f(IN a int, OUT b int) … $$ begin return 1; end $$;
--   PG: 42804 RETURN cannot have a parameter in function with OUT parameters | mg: created
CREATE FUNCTION f(VARIADIC a int) …       -- PG: 42P13 VARIADIC parameter must be an array | mg: created
CREATE FUNCTION f(VARIADIC a int[], b int) … -- PG: 42P13 must be the last input parameter | mg: created
CREATE FUNCTION f(a int DEFAULT 1, b int) … -- PG: 42P13 parameters after one with a default … | mg: created
CREATE PROCEDURE p() … $$ begin return 1; end $$;
-- PG: 42601 RETURN cannot have a parameter … | mg: created
execute 'select … where a = $1 and b = $2' into v using 1;
--   PG: 42P02 there is no parameter $2 | mg: returns NULL
execute 'select … where a = 99' into strict v;
-- no rows
--   PG: P0002 query returned no rows | mg: returns NULL
CREATE FUNCTION f() … $$ #variable_conflict use_column
declare a int := 99; … $$;
--   PG: works | mg: 42601 syntax error at or near "variable_conflict"
<<outer_blk>> declare x int := 1;
begin declare x int := 2;
begin return outer_blk.x::text || '/' || x::text;
end;
end
--   PG: '1/2' | mg: 42P01 missing FROM-clause entry for table "outer_blk"
declare x int := y;
y int := 5;
begin return x;
end
--   PG: 42703 column "y" does not exist (forward reference)  | mg: returns the string 'y'
declare r zr7_rr%rowtype;
v int;
begin select * into r …;
v := r;
return v::text;
end
--   PG: 22P02 invalid input syntax for type integer: "(1,x)" | mg: returns the string 'v'
declare x int := 1;
x int := 2;
-- PG: 42601 duplicate declaration | mg: created, x = 2
declare c scroll cursor for select …;
--   mg: XX000 Internal error: missing semicolon after variable declaration at position 18
declare c cursor for select …;
begin open c;
move absolute 5 from c;
…
--   mg: 34000 cursor "c" does not exist
open c;
close c;
close c;
-- PG: 34000 cursor does not exist | mg: OK
c1 refcursor := 'dup';
c2 refcursor := 'dup';
open c1 …;
open c2 …;
--   PG: 42P03 cursor "dup" already in use | mg: OK
declare c refcursor;
begin fetch c into v;
-- never opened
--   PG: 22004 cursor variable "c" is null | mg: returns NULL
SELECT row(1,null)::zr7_rr …    -- zr7_rr is an existing table
--   PG: works | mg: 42704 type "zr7_rr" does not exist
ALTER TABLE child RENAME COLUMN a TO z;
-- a is inherited
--   PG: 42P16 cannot rename inherited column | mg: OK
ALTER TABLE a_view RENAME COLUMN i TO k;
--   PG: works — RENAME is permitted on a view through ALTER TABLE
--   mg: 55000 cannot insert into view "zr7_rn_v"
ALTER TABLE t ALTER COLUMN i DROP NOT NULL;
-- i is the PK
--   PG: 42P16 column "i" is in a primary key | mg: OK
ALTER TABLE t ADD CONSTRAINT c CHECK (i);
-- PG: 42804 argument of CHECK must be type boolean | mg: OK
CREATE POLICY p ON t FOR SELECT USING (i);
-- PG: 42804 argument of POLICY must be type boolean | mg: OK
ALTER TABLE t ALTER COLUMN i TYPE bigint COLLATE "C";
--   PG: 42804 collations are not supported by type bigint | mg: OK
CREATE TABLE t (i text GENERATED ALWAYS AS IDENTITY);
--   PG: 22023 identity column type must be smallint, integer, or bigint | mg: OK
CREATE TABLE t (i int GENERATED ALWAYS AS IDENTITY DEFAULT 1);
--   PG: 42601 both default and identity specified | mg: OK
ALTER TABLE t ALTER COLUMN i DROP IDENTITY;
-- i is not an identity column
--   PG: 55000 is not an identity column | mg: OK
ALTER TABLE t ALTER COLUMN i ADD GENERATED ALWAYS AS IDENTITY;
-- column holds nulls
--   PG: 55000 must be declared NOT NULL before identity can be added | mg: OK
CREATE TABLE t (a int, b int GENERATED ALWAYS AS (a) VIRTUAL PRIMARY KEY);
--   PG: 0A000 primary keys on virtual generated columns are not supported | mg: OK
ALTER TABLE zr7_mo SET SCHEMA zr7_no_such_schema;
--   PG: 3F000 schema "zr7_no_such_schema" does not exist
--   mg: OK — and from this point every statement naming zr7_mo fails with 42P01
ALTER TABLE t ALTER COLUMN c SET STORAGE NONSENSE;
-- PG: 22023 invalid storage type | mg: OK
ALTER TABLE t ALTER COLUMN i SET STORAGE EXTERNAL;
-- i is integer
--   PG: 0A000 column data type integer can only have storage PLAIN | mg: OK
ALTER TABLE t ALTER COLUMN c SET COMPRESSION nosuchmethod;
--   PG: 22023 invalid compression method | mg: OK
ALTER TABLE t SET WITHOUT CLUSTER;
-- PG: works | mg: 42601 syntax error at or near "WITHOUT"
ALTER TABLE t DISABLE RULE r;
-- PG: works | mg: 42601 syntax error at or near "RULE"
SET CONSTRAINTS zr7_no_such_constraint DEFERRED;
-- PG: 42704 | mg: OK
ATTACH PARTITION p FOR VALUES FROM (30) TO (20);
-- PG: 42P17 empty range bound | mg: OK
ATTACH PARTITION p FOR VALUES IN (100);
-- parent is RANGE-partitioned
--   PG: 42P16 invalid bound specification for a range partition | mg: OK
ALTER TABLE plain_table ATTACH PARTITION x …;
-- PG: 42809 | mg: OK
CREATE TABLE d2 PARTITION OF p DEFAULT;
-- p already has a default
--   PG: 42P17 conflicts with existing default partition | mg: OK
ATTACH PARTITION ok FOR VALUES FROM (400) TO (600);
-- default partition holds a row in that range
--   PG: 23514 updated partition constraint … would be violated by some row | mg: OK
ALTER TABLE p DETACH PARTITION x;
-- x is not a partition of p
--   PG: 42P01 relation is not a partition of relation | mg: OK
DROP TABLE zr7_pt;
-- partitioned, with partitions attached
--   PG: succeeds, dropping the partitions with it
--   mg: 2BP01 cannot drop table zr7_pt because other objects depend on it
ALTER TABLE ONLY partitioned_parent ADD COLUMN extra2 text;
--   PG: 42P16 column must be added to child tables too | mg: OK
INSTEAD OF INSERT ON v FOR EACH STATEMENT …   -- PG: 0A000 INSTEAD OF triggers must be FOR EACH ROW | mg: OK
INSTEAD OF INSERT ON v FOR EACH ROW WHEN (true) …  -- PG: 0A000 cannot have WHEN conditions | mg: OK
BEFORE UPDATE OF nosuch ON t …                -- PG: 42703 column does not exist | mg: OK
BEFORE DELETE … WHEN (NEW.i > 0)              -- PG: 42P17 cannot reference NEW values | mg: OK
BEFORE INSERT … WHEN (OLD.i > 0)              -- PG: 42P17 cannot reference OLD values | mg: OK
FOR EACH STATEMENT WHEN (NEW.i > 0)           -- PG: 42P17 cannot reference column values | mg: OK
… WHEN ((SELECT count(*) FROM t) > 0)         -- PG: 0A000 cannot use subquery in WHEN | mg: OK
BEFORE TRUNCATE … FOR EACH ROW                -- PG: 0A000 not supported | mg: OK
CREATE TRIGGER t13 …;
CREATE TRIGGER t13 …;
-- PG: 42710 already exists | mg: OK
BEFORE INSERT … REFERENCING NEW TABLE AS nt   -- PG: 42P17 only for an AFTER trigger | mg: OK
AFTER INSERT … REFERENCING OLD TABLE AS ot    -- PG: 42P17 OLD TABLE only for DELETE or UPDATE | mg: OK
ALTER TABLE t DISABLE TRIGGER zr7_no_such;
-- PG: 42704 | mg: OK
ON INSERT … VALUES (OLD.j)     -- PG: 42P17 ON INSERT rule cannot use OLD | mg: OK
ON DELETE … VALUES (NEW.j)     -- PG: 42P17 ON DELETE rule cannot use NEW | mg: OK
CREATE RULE r1 …;
CREATE RULE r1 …;
-- PG: 42710 already exists | mg: OK
CREATE RULE "_RETURN" AS ON SELECT TO t WHERE i > 0 DO INSTEAD …  -- PG: 42P17 | mg: OK
CREATE RULE r AS ON SELECT TO t DO INSTEAD SELECT …  -- PG: 42809 relation cannot have ON SELECT rules | mg: OK
CREATE RULE r AS ON SELECT TO t DO ALSO SELECT …     -- PG: 42809 | mg: OK
CREATE RULE r AS ON UPDATE TO t DO ALSO ( INSERT INTO log VALUES ('u1')
--   PG: 42601 syntax error at end of input | mg: OK
CREATE VIEW v AS SELECT i, i FROM t;
-- PG: 42701 column "i" specified more than once | mg: OK
CREATE VIEW v AS SELECT DISTINCT i FROM t WITH CHECK OPTION;
--   PG: 0A000 WITH CHECK OPTION is supported only on automatically updatable views | mg: OK
SELECT has_table_privilege('zr7_pv', 'NOSUCHPRIV');
-- PG: 22023 | mg: true
SELECT has_table_privilege('zr7_no_such_table', 'SELECT');
-- PG: 42P01 | mg: true
SELECT has_column_privilege('zr7_pv', 'nosuchcol', 'SELECT');
-- PG: 42703 | mg: true
SELECT has_schema_privilege('zr7_no_such_schema', 'USAGE');
-- PG: 3F000 | mg: true
SELECT has_table_privilege('zr7_no_such_role', 't', 'SELECT');
-- PG: 42704 | mg: false
GRANT DELETE (i) ON t TO PUBLIC;
-- PG: 0LP01 invalid privilege type DELETE for column | mg: OK
GRANT SELECT ON t TO PUBLIC WITH GRANT OPTION;
-- PG: 0LP01 grant options can only be granted to roles | mg: OK
ALTER DEFAULT PRIVILEGES IN SCHEMA nosuch GRANT SELECT ON TABLES TO PUBLIC;
-- PG: 3F000 | mg: OK
CREATE POLICY p1 …;
CREATE POLICY p1 …;
-- PG: 42710 already exists | mg: OK
FOR SELECT WITH CHECK (…)                      -- PG: 42601 WITH CHECK cannot be applied to SELECT or DELETE | mg: OK
FOR DELETE WITH CHECK (…)                      -- PG: 42601 | mg: OK
FOR INSERT USING (…)                           -- PG: 42601 only WITH CHECK allowed for INSERT | mg: OK
FOR SELECT USING (i)                           -- PG: 42804 argument of POLICY must be boolean | mg: OK
FOR SELECT USING (nosuchcol = 1)               -- PG: 42703 | mg: OK
CREATE POLICY p TO zr7_no_such_role …          -- PG: 42704 role does not exist | mg: OK
CREATE POLICY p ON a_view …                    -- PG: 42809 "v" is not a table | mg: OK
ALTER TABLE a_view ENABLE ROW LEVEL SECURITY;
-- PG: 42809 | mg: OK
ALTER POLICY p ON t FOR UPDATE USING (true);
-- PG: 42601 (command cannot be changed) | mg: OK
CREATE POLICY p ON t AS NONSENSE FOR SELECT …  -- PG: 42601 unrecognized row security option | mg: OK
CREATE RULE r AS ON INSERT TO t DO ALSO ( INSERT INTO log VALUES ('a');
INSERT INTO log VALUES ('b');
);
--   PG: works — both actions run | mg: 42601 syntax error at or near ")"
declare a int[] := array[1,2,3];
begin …
--   PG: works | mg: 42601 syntax error at or near "ARRAY"
declare v fg_outer;
-- fg_outer is (x int, y fg_inner)
begin v.x := 1;
v.y.a := 2;
v.y.b := 3;
return v::text;
end
--   PG: (1,"(2,3)")   | mg: returns the string 'v'
CREATE FUNCTION f(n int) RETURNS int … $$ begin
  if n <= 0 then return 0; end if; return 1 + f(n - 1); end $$;
SELECT f(500);
--   PG: 500 | mg: XX000 Internal error: StackOverflowError
CREATE VIEW v1 AS SELECT i FROM vt;
CREATE VIEW v2 AS SELECT i FROM v1;
CREATE OR REPLACE VIEW v1 AS SELECT i FROM v2;
-- closes the loop
SELECT count(*) FROM v1;
--   PG: 42P17 infinite recursion detected in rules for relation "v1"
--   mg: XX000 Internal error: StackOverflowError

-- an AFTER INSERT trigger that inserts into its own table
--   PG: 54001 stack depth limit exceeded | mg: XX000 StackOverflowError
-- a DO ALSO rule that inserts into its own table
--   PG: 42P17 infinite recursion detected in rules | mg: XX000 StackOverflowError
CREATE AGGREGATE a(int) (STYPE = int);
-- PG: 42P13 sfunc must be specified | mg: OK
CREATE AGGREGATE a(int) (SFUNC = f);
-- PG: 42P13 stype must be specified | mg: OK
CREATE AGGREGATE a(text) (SFUNC = f, STYPE = int);
-- f is (int,int)
--   PG: 42883 function f(integer, text) does not exist | mg: OK
CREATE AGGREGATE mysum(int) (…);
-- already exists       -- PG: 42723 | mg: OK
CREATE AGGREGATE os(float8 ORDER BY float8) (… FINALFUNC = nonexistent_signature …);
-- PG: 42883 | mg: OK
DROP AGGREGATE mysum(text);
-- wrong signature      -- PG: 42883 | mg: OK
ALTER AGGREGATE nosuch(int) RENAME TO x;
-- PG: 42883 | mg: OK
SELECT zr8_mysum(1);
-- a user-defined aggregate over a constant
--   PG: 1 | mg: 42883 function zr8_mysum(integer) does not exist
… EXECUTE FUNCTION f();
-- f does not return event_trigger
--   PG: 42P17 function must return type event_trigger | mg: OK
… WHEN NOSUCHVAR IN ('CREATE TABLE') …   -- PG: 42601 unrecognized filter variable | mg: OK
… WHEN TAG IN ('SELECT') …               -- PG: 0A000 event triggers are not supported for SELECT | mg: OK
CREATE EVENT TRIGGER et7 …;
CREATE EVENT TRIGGER et7 …;
-- PG: 42710 | mg: OK
DROP EVENT TRIGGER nosuch;
-- PG: 42704 | mg: OK
CREATE STATISTICS s ON a FROM t;
-- PG: 42P17 require at least 2 columns | mg: OK
CREATE STATISTICS s ON a, nosuchcol FROM t;
-- PG: 42703 | mg: OK
CREATE STATISTICS s (nosuchkind) ON a,b FROM t;
-- PG: 42601 unrecognized statistics kind | mg: OK
CREATE STATISTICS s ON a, a FROM t;
-- PG: 42701 duplicate column name | mg: OK
CREATE STATISTICS s ON a, b FROM nosuchtable;
-- PG: 42P01 | mg: OK
CREATE STATISTICS s ON a, b FROM a_view;
-- PG: 42809 cannot define statistics for relation | mg: OK
ALTER STATISTICS nosuch RENAME TO x;
-- PG: 42704 | mg: OK
DROP STATISTICS nosuch;
-- PG: 42704 | mg: OK
CREATE OPERATOR ===== (LEFTARG = int, RIGHTARG = int);
-- PG: 42P13 operator function must be specified | mg: OK
CREATE OPERATOR ab (…);
-- "ab" is not a valid operator name; PG: 42601 | mg: OK
CREATE CAST (int AS ct) WITH FUNCTION f(int);
-- already exists; PG: 42710 | mg: OK
CREATE CAST (text AS ct) WITH FUNCTION f(int);
--   PG: 42P17 argument of cast function must match the source data type | mg: OK
CREATE CAST (int AS int) WITH FUNCTION f(int);
--   PG: 42P17 return data type of cast function must match the target | mg: OK
CREATE CAST (bigint AS ct) WITH FUNCTION nosuch(bigint);
-- PG: 42883 | mg: OK
CREATE CAST (int AS text) WITHOUT FUNCTION;
--   PG: 42P17 source and target data types are not physically compatible | mg: OK
CREATE TYPE t AS (a int, a text);
-- PG: 42701 column "a" specified more than once | mg: OK
CREATE TYPE t AS (a nosuchtype);
-- PG: 42704 | mg: OK
ALTER TYPE t DROP ATTRIBUTE nosuchattr;
-- PG: 42703 | mg: OK
ALTER TYPE t ADD ATTRIBUTE a int;
-- a already exists; PG: 42701 | mg: OK
CREATE TYPE e AS ENUM ('a','a');
-- PG: 23505 duplicate key | mg: OK
CREATE TYPE r AS RANGE (COLLATION = "C");
-- PG: 42601 type attribute "subtype" is required | mg: OK
CREATE TYPE r AS RANGE (SUBTYPE = nosuchtype);
-- PG: 42704 | mg: OK
DROP TYPE t;
-- a table uses it; PG: 2BP01 | mg: OK
CREATE TYPE zr8_shell;
-- PG: works, creates a shell type | mg: 42601 syntax error at or near ""
CREATE INDEX i ON t ((SELECT 1));
-- PG: 42601 | mg: OK
CREATE INDEX i ON t ((count(x)));
-- PG: 42803 aggregates not allowed in index expressions | mg: OK
CREATE INDEX i ON t (x) WHERE x IN (SELECT 1);
-- PG: 0A000 cannot use subquery in index predicate | mg: OK
CREATE INDEX i ON t (x) WHERE x > random();
-- PG: 42P17 must be marked IMMUTABLE | mg: OK
CREATE INDEX i ON t USING hash (x) INCLUDE (y);
-- PG: 0A000 hash does not support included columns | mg: OK
CREATE INDEX i ON t USING nosuchmethod (x);
-- PG: 42704 access method does not exist | mg: OK
CREATE INDEX i ON t (int_col text_pattern_ops);
-- PG: 42804 opclass does not accept data type | mg: OK
CREATE INDEX i ON t USING hash (x, y);
-- PG: 0A000 no multicolumn hash indexes | mg: OK
CREATE INDEX i ON t USING hash (x DESC);
-- PG: 0A000 hash does not support ASC/DESC | mg: OK
CREATE INDEX i ON a_view (x);
-- PG: 42809 cannot create index on relation | mg: OK
CREATE SEQUENCE s MINVALUE 5 MAXVALUE 10 START 1;
-- PG: 22023 START cannot be less than MINVALUE | mg: OK
CREATE SEQUENCE s CACHE 0;
-- PG: 22023 CACHE must be greater than zero | mg: OK
CREATE SEQUENCE s AS smallint MAXVALUE 100000;
-- PG: 22023 out of range for smallint | mg: OK
CREATE SEQUENCE s AS text;
-- PG: 22023 must be smallint, integer, or bigint | mg: OK
ALTER SEQUENCE s RESTART WITH 1000;
-- exceeds MAXVALUE; PG: 22023 | mg: OK
CREATE SEQUENCE s OWNED BY t.nosuchcol;
-- PG: 42703 | mg: OK
SELECT pg_get_serial_sequence('t','nosuchcol');
-- PG: 42703 | mg: NULL
ALTER SEQUENCE IF EXISTS nosuch RESTART;
-- PG: works | mg: 42P01 relation "if" does not exist
CREATE TABLE t (i int) ON COMMIT DELETE ROWS;
-- not temporary; PG: 42P16 | mg: OK
CREATE TEMP TABLE t (i int REFERENCES permanent_table(i));
--   PG: 42P16 constraints on temporary tables may reference only temporary tables | mg: OK
CREATE TABLE t (i int REFERENCES temp_table(i));
--   PG: 42P16 constraints on permanent tables may reference only permanent tables | mg: OK
CREATE TABLE t AS SELECT 1 AS x, 2 AS x;
-- PG: 42701 column "x" specified more than once | mg: OK
CREATE TABLE t (LIKE src INCLUDING NOSUCHOPT);
-- PG: 42601 | mg: OK
BEGIN;
SELECT 1;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
--   PG: 25001 must be called before any query | mg: OK
SET TRANSACTION ISOLATION LEVEL NONSENSE;
-- PG: 42601 | mg: OK
SAVEPOINT sp;
-- outside a transaction block; PG: 25P01 | mg: OK
… ON CONFLICT (i) DO UPDATE SET nosuchcol = 'x';
-- PG: 42703 | mg: OK, row written
… ON CONFLICT ON CONSTRAINT nosuch DO NOTHING;
-- PG: 42704 | mg: OK
… ON CONFLICT ON CONSTRAINT a_check_constraint DO NOTHING;
--   PG: 42809 constraint in ON CONFLICT clause has no associated index | mg: OK
INSERT … VALUES (20,'p',200),(20,'q',201) ON CONFLICT (i) DO UPDATE …;
--   PG: 21000 cannot affect row a second time | mg: OK, both applied
MERGE … WHEN MATCHED THEN UPDATE …;
-- two source rows match one target row
--   PG: 21000 MERGE command cannot affect row a second time | mg: OK
MERGE … WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = s.v;
--   PG: 42P01 invalid reference to FROM-clause entry for table "s" | mg: OK
GROUP BY ()                             -- PG: one total row | mg: 42601 syntax error at or near ")"
GROUP BY GROUPING SETS (a, (a, b))      -- bare column ref; PG: 6 rows | mg: 42601 syntax error at "a"
GROUP BY GROUPING SETS (ROLLUP (a, b))  -- nested; PG: 7 rows | mg: 42601 syntax error at "ROLLUP"
GROUP BY GROUPING SETS (alias_a)        -- an output alias; PG: 2 rows | mg: 42601
SELECT v FILTER (WHERE b = 1) FROM t;
--   PG: 42601 syntax error at or near "FILTER"
--   mg: returns a column named "filter" containing the string 'v'
SELECT sum(v) WITHIN GROUP (ORDER BY v) FROM t;
--   PG: 42883 function sum(integer, integer) does not exist | mg: NULL
SELECT * FROM f();
-- f RETURNS SETOF record
--   PG: 42601 a column definition list is required for functions returning "record" | mg: OK
SELECT * FROM f() AS t(x int, y int, z int);
-- wrong shape
--   PG: 42P13 return type mismatch in function declared to return record | mg: OK
SELECT * FROM g() AS t(x int, y text);
-- g has OUT parameters
--   PG: 42601 a column definition list is redundant | mg: OK
SELECT string_agg((f()).y, ',');
--   PG: 0A000 aggregate function calls cannot contain set-returning function calls | mg: NULL
SELECT * FROM abs(1) WITH ORDINALITY AS t(v, o);
--   PG: (1, 1) — any function may appear in FROM | mg: 42883 function abs does not exist
INSERT … RETURNING count(*);
-- PG: 42803 | mg: returns NULL
INSERT … RETURNING row_number() OVER ();
-- PG: 42P20 | mg: returns NULL
SELECT … FROM (INSERT INTO t VALUES (…) RETURNING j) x;
--   PG: 42601 syntax error at or near "INTO" — only a top-level CTE may modify data
--   mg: runs the INSERT and returns its rows
SELECT … WHERE EXISTS (WITH x AS (INSERT INTO t … RETURNING i) SELECT 1 FROM x);
--   PG: 0A000 WITH clause containing a data-modifying statement must be at the top level | mg: OK
CREATE MATERIALIZED VIEW mv AS SELECT …;
DROP VIEW mv;
--   PG: 42809 "mv" is not a view — refuses
--   mg: succeeds, and the materialized view is gone
INSERT INTO mv VALUES (9,'z');
-- PG: 42809 cannot change materialized view | mg: 1 row inserted
REFRESH MATERIALIZED VIEW CONCURRENTLY mv;
-- no unique index
--   PG: 55000 cannot refresh concurrently | mg: OK
ALTER SCHEMA s2 RENAME TO s2b;
SELECT count(*) FROM s2b.t;
--   PG: 2 | mg: 3F000 schema "s2b" does not exist
CREATE FUNCTION s1.f() RETURNS int …;
-- OK
CREATE FUNCTION s2.f() RETURNS int …;
-- PG: OK, a different function
--   mg: 42723 function "f" already exists with same argument types
ALTER SCHEMA nosuch RENAME TO x;
-- PG: 3F000 | mg: OK
CREATE FUNCTION f() RETURNS int LANGUAGE nosuchlang AS $$ x $$;
-- PG: 42704 | mg: OK
CREATE FUNCTION f() RETURNS bigint LANGUAGE sql BEGIN ATOMIC SELECT count(*) FROM t;
END;
DROP TABLE t;
--   PG: 2BP01 cannot drop table because other objects depend on it | mg: OK
CREATE FUNCTION f() RETURNS void LANGUAGE sql AS $$ SELECT 1 $$;
--   PG: works, the value is discarded | mg: 42P13 return type mismatch in function declared to return void
CREATE OR REPLACE FUNCTION f(p int) RETURNS int …;
-- was RETURNS text
--   PG: 42P13 cannot change return type of existing function | mg: OK
CREATE OR REPLACE FUNCTION f(q int) RETURNS text …;
-- parameter was named p
--   PG: 42P13 cannot change name of input parameter "p" | mg: OK
declare a int[] := '{1,2,3}';
begin a[2] := 99;
-- PG: {1,99,3}      | mg: 42601 syntax error at "{1,2,3}"
declare a int[] := '{1,2,3}';
begin a[5] := 7;
-- PG: {1,2,3,NULL,7}| mg: 42601
declare a int[];
begin a[1] := 5;
-- PG: {5}           | mg: 42601
declare a int[];
begin a := array[1,2,3];
a[1] := 42;
-- PG: {42,2,3} | mg: 42601 at "ARRAY"
declare a int[] := '{1,2,3,4}';
begin a[2:3] := '{8,9}';
-- PG: {1,8,9,4} | mg: 42601
declare a int[] := '{{1,2},{3,4}}';
begin a[1][2] := 20;
-- PG: {{1,20},{3,4}} | mg: 42601
declare a pc[]  := '{"(1,a)"}';
begin a[1].x := 9;
-- PG: {"(9,a)"} | mg: 42601
-- session A:  SELECT nextval('s');    -- returns 1
-- session B:  SELECT currval('s');
--   PG: 55000 currval of sequence "s" is not yet defined in this session
--   mg: 1
-- session A (open transaction):  INSERT INTO t VALUES (9, 1);   -- not committed
-- session B:                     INSERT INTO t VALUES (9, 2);
--   PG: blocks — it cannot know yet whether A will commit
--   mg: 23505 unique violation, immediately
SET statement_timeout = '1s';
SELECT count(*) FROM generate_series(1, 200000000);
--   PG: 57014 canceling statement due to statement timeout
--   mg: still running after 30s — the probe's watchdog had to kill it
CREATE TABLE t (id int PRIMARY KEY, other text);
SELECT id, other FROM t GROUP BY id;
--   PG: works — grouping by the primary key functionally determines every other column
--   mg: 42803 column "other" must appear in the GROUP BY clause …
SELECT count(*) FROM t HAVING i > 0;
-- i is ungrouped; PG: 42803 | mg: returns 4
SELECT i FROM t ORDER BY sum(v);
-- ungrouped query; PG: 42803 | mg: returns 4 rows
SELECT count(generate_series(1,3));
-- PG: 0A000 aggregate cannot contain an SRF | mg: 1
SELECT json_object_agg(t, i) FROM …;
-- t contains a NULL
--   PG: 22004 null value not allowed for object key | mg: silently drops the NULL-keyed entry
SELECT DISTINCT ON (g) i FROM t ORDER BY i;
--   PG: 42P10 SELECT DISTINCT ON expressions must match initial ORDER BY expressions
--   mg: returns rows 1 and 3
SELECT count(*) FROM zr9_a JOIN zr9_a ON true;
-- PG: 42712 table name specified more than once | mg: 9
SELECT count(*) FROM zr9_a t1, zr9_b t1;
-- PG: 42712 table name "t1" specified more than once | mg: 9
… JOIN b ON a.id;
-- PG: 42804 argument of JOIN/ON must be type boolean | mg: 9 rows
… JOIN b ON count(*) > 0;
-- PG: 42803 aggregates not allowed in JOIN conditions | mg: 0 rows
… JOIN b USING (id, id);
-- PG: 42701 column name appears more than once | mg: 2 rows
SELECT count(*) FROM (a JOIN b ON a.id = b.id) AS j WHERE j.id = 2;
--   PG: 42702 column reference "id" is ambiguous — j exposes both a.id and b.id
--   mg: 1
SELECT count(*) FROM a t RIGHT JOIN LATERAL (SELECT t.x) s ON true;
--   PG: 42P10 invalid reference to FROM-clause entry for table "t"
--   mg: 3 rows
SELECT count(*) FROM a FULL JOIN b ON a.x < b.y;
--   PG: 0A000 FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
--   mg: 9 rows
… OVER nosuchwindow                                    -- PG: 42704 window does not exist | mg: OK
… WINDOW w AS (), w AS ()                              -- PG: 42P20 window "w" is already defined | mg: OK
… OVER (w PARTITION BY g) WINDOW w AS (PARTITION BY g) -- PG: 42P20 cannot override PARTITION BY | mg: OK
… OVER (w ORDER BY i)     WINDOW w AS (ORDER BY v)     -- PG: 42P20 cannot override ORDER BY | mg: OK
ORDER BY g, v RANGE BETWEEN 1 PRECEDING …   -- PG: 42P20 RANGE offset requires exactly one ORDER BY column | mg: OK
RANGE BETWEEN 1 PRECEDING AND CURRENT ROW   -- no ORDER BY at all; PG: 42P20 | mg: OK
GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW  -- no ORDER BY; PG: 42P20 GROUPS mode requires ORDER BY | mg: OK
ROWS BETWEEN CURRENT ROW AND 1 PRECEDING    -- PG: 42P20 frame starting from current row cannot have preceding rows | mg: OK
ROWS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW  -- PG: 42P20 | mg: OK
ROWS BETWEEN NULL PRECEDING AND CURRENT ROW -- PG: 22004 frame starting offset must not be null | mg: OK
GROUP BY rank() OVER (ORDER BY v)          -- PG: 42P20 window functions not allowed in GROUP BY | mg: OK
sum(rank() OVER (ORDER BY v)) OVER ()      -- PG: 42P20 window function calls cannot be nested | mg: OK
rank() FILTER (WHERE v > 15) OVER (…)      -- PG: 0A000 FILTER not implemented for non-aggregate window functions | mg: OK
SELECT lag(v, -1) OVER (ORDER BY i) FROM t;
--   PG: 20,20,30,40,NULL — a negative lag looks forward, equivalent to lead()
--   mg: XX000 Internal error: Index 5 out of bounds for length 5;;
