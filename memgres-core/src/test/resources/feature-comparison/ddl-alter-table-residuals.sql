-- ============================================================================
-- Feature Comparison: ALTER TABLE residuals
-- Target: PostgreSQL 18 vs Memgres
--
-- Four things ALTER TABLE was getting wrong about a column it does not own
-- alone, all re-measured against PostgreSQL 18.
--
--   A. A DEFAULT is judged by its type, not by its value. SET DEFAULT
--      2147483648 on an integer column is accepted; the error is 22003 at the
--      insert that takes the default. memgres refused the ALTER outright --
--      the dangerous direction, so the ordinary defaults are pinned beside it.
--   B. ENABLE / DISABLE reach a rule or a trigger in all four firing modes.
--      REPLICA is not a reserved word, so matchKeyword("REPLICA") never saw it
--      and the whole form was a syntax error.
--   C. An inherited column belongs to its parent: the child may not retype it,
--      and a retype on the parent reaches every descendant.
--   D. ONLY is refused for the actions a table's children would have to share.
-- ============================================================================

DROP TABLE IF EXISTS ba_dv, ba_dc, ba_dn, ba_da CASCADE;
DROP TABLE IF EXISTS ba_ch, ba_ch2, ba_par CASCADE;
DROP TABLE IF EXISTS ba_g3, ba_g2, ba_g1 CASCADE;
DROP TABLE IF EXISTS ba_m3, ba_m2, ba_m1 CASCADE;
DROP TABLE IF EXISTS ba_pt1, ba_pt, ba_pu, ba_pv CASCADE;
DROP TABLE IF EXISTS ba_ip, ba_ic, ba_ref CASCADE;
DROP TABLE IF EXISTS ba_rt, ba_tt CASCADE;
DROP FUNCTION IF EXISTS ba_trgf() CASCADE;

-- ============================================================================
-- SECTION A: a DEFAULT is judged by its type, not by its value
-- ============================================================================

CREATE TABLE ba_dv (id int primary key, c1 int);

-- PostgreSQL records the default: bigint assigns to integer, and whether this
-- particular bigint fits is the insert's business.
ALTER TABLE ba_dv ALTER COLUMN c1 SET DEFAULT 2147483648;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: integer out of range
-- end-expected-error
INSERT INTO ba_dv (id) VALUES (1);

-- A numeric past bigint is no different.
ALTER TABLE ba_dv ALTER COLUMN c1 SET DEFAULT 99999999999999999999;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: integer out of range
-- end-expected-error
INSERT INTO ba_dv (id) VALUES (2);

DROP TABLE ba_dv;
CREATE TABLE ba_dv (id int primary key, c1 smallint, c2 bigint, c3 numeric(4,0),
                    c4 varchar(3));
ALTER TABLE ba_dv ALTER COLUMN c1 SET DEFAULT 40000;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: smallint out of range
-- end-expected-error
INSERT INTO ba_dv (id) VALUES (1);

ALTER TABLE ba_dv ALTER COLUMN c1 DROP DEFAULT;
ALTER TABLE ba_dv ALTER COLUMN c2 SET DEFAULT 99999999999999999999;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: bigint out of range
-- end-expected-error
INSERT INTO ba_dv (id) VALUES (2);

ALTER TABLE ba_dv ALTER COLUMN c2 DROP DEFAULT;
ALTER TABLE ba_dv ALTER COLUMN c3 SET DEFAULT 99999;

-- begin-expected-error
-- sqlstate: 22003
-- message-like: numeric field overflow
-- end-expected-error
INSERT INTO ba_dv (id) VALUES (3);

ALTER TABLE ba_dv ALTER COLUMN c3 DROP DEFAULT;

-- A literal longer than the column is stored as written; the length is the
-- row's rule, not the default's.
ALTER TABLE ba_dv ALTER COLUMN c4 SET DEFAULT 'abcdefg';

-- begin-expected-error
-- sqlstate: 22001
-- message-like: value too long for type character varying(3)
-- end-expected-error
INSERT INTO ba_dv (id) VALUES (4);

-- The same at CREATE TABLE.
CREATE TABLE ba_dc (id int primary key, c1 int DEFAULT 2147483648);

-- begin-expected-error
-- sqlstate: 22003
-- message-like: integer out of range
-- end-expected-error
INSERT INTO ba_dc (id) VALUES (1);

-- A default whose type has no assignment cast to the column's is still refused,
-- and this is where PostgreSQL names both types.
DROP TABLE ba_dv;
CREATE TABLE ba_dv (id int primary key, c1 int, c2 date, c3 text);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "c1" is of type integer but default expression is of type boolean
-- end-expected-error
ALTER TABLE ba_dv ALTER COLUMN c1 SET DEFAULT true;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "c1" is of type integer but default expression is of type text
-- end-expected-error
ALTER TABLE ba_dv ALTER COLUMN c1 SET DEFAULT 'x'::text;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "c2" is of type date but default expression is of type integer
-- end-expected-error
ALTER TABLE ba_dv ALTER COLUMN c2 SET DEFAULT 5;

-- A bare string literal has no type of its own, so it is read as a value of the
-- column's type at once and a bad one is invalid input rather than a mismatch.
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer
-- end-expected-error
ALTER TABLE ba_dv ALTER COLUMN c1 SET DEFAULT 'abc';

-- ... and the ordinary defaults all still go in, whatever their type.
DROP TABLE ba_dv;
CREATE TABLE ba_dv (id int primary key, a int, b text, c boolean, d date,
                    f numeric(5,2), g int[], h jsonb);
ALTER TABLE ba_dv ALTER COLUMN a SET DEFAULT 7,
                  ALTER COLUMN b SET DEFAULT 'x'::text,
                  ALTER COLUMN c SET DEFAULT true,
                  ALTER COLUMN d SET DEFAULT DATE '2020-01-01',
                  ALTER COLUMN f SET DEFAULT 10::numeric(5,2),
                  ALTER COLUMN g SET DEFAULT ARRAY[1,2],
                  ALTER COLUMN h SET DEFAULT '{}'::jsonb;
INSERT INTO ba_dv (id) VALUES (1);

-- begin-expected
-- columns: a | b | c | d | f | g | h
-- row: 7 | x | true | 2020-01-01 | 10.00 | {1,2} | {}
-- end-expected
SELECT a, b, c::text, d::text, f::text, g::text, h::text FROM ba_dv;

-- An ARRAY[...] default used to fall through the default writer and leave the
-- column with no default at all, so every insert quietly got a null.
CREATE TABLE ba_da (id int primary key, g int[] DEFAULT ARRAY[1,2]);
INSERT INTO ba_da (id) VALUES (1);

-- begin-expected
-- columns: g | column_default
-- row: {1,2} | ARRAY[1, 2]
-- end-expected
SELECT (SELECT g::text FROM ba_da) AS g,
       (SELECT column_default FROM information_schema.columns
        WHERE table_name = 'ba_da' AND column_name = 'g') AS column_default;

-- DEFAULT NULL names no value at all; reading one as a number threw out of the
-- wire handler rather than creating the table.
CREATE TABLE ba_dn (id int primary key, c1 int DEFAULT NULL);
INSERT INTO ba_dn (id) VALUES (1);

-- begin-expected
-- columns: c1
-- row: NULL
-- end-expected
SELECT c1 FROM ba_dn;

-- Parse analysis turns a cast of a literal into a constant, and the catalog
-- reports the constant. It prints bare only where reading the printed text back
-- gives the same type again, which is true of integer and boolean; everything
-- else keeps its label, under the type's canonical name.
DROP TABLE ba_dv;
CREATE TABLE ba_dv (id int primary key, a int, b int[], c boolean, d text, e jsonb);
ALTER TABLE ba_dv ALTER COLUMN a SET DEFAULT '7'::int,
                  ALTER COLUMN b SET DEFAULT '{1,2}'::int[],
                  ALTER COLUMN c SET DEFAULT 'true'::boolean,
                  ALTER COLUMN d SET DEFAULT 'abc'::text,
                  ALTER COLUMN e SET DEFAULT '{}'::jsonb;

-- begin-expected
-- columns: column_name | column_default
-- row: a | 7
-- row: b | '{1,2}'::integer[]
-- row: c | true
-- row: d | 'abc'::text
-- row: e | '{}'::jsonb
-- end-expected
SELECT column_name, column_default FROM information_schema.columns
 WHERE table_name = 'ba_dv' AND column_name <> 'id' ORDER BY ordinal_position;

-- ============================================================================
-- SECTION B: ENABLE / DISABLE in all four firing modes
-- ============================================================================

CREATE TABLE ba_rt (a int primary key, b int);
CREATE RULE ba_rt_r AS ON UPDATE TO ba_rt DO ALSO NOTHING;

-- begin-expected
-- columns: ev_enabled
-- row: O
-- end-expected
SELECT ev_enabled FROM pg_rewrite WHERE rulename = 'ba_rt_r';

ALTER TABLE ba_rt ENABLE REPLICA RULE ba_rt_r;

-- begin-expected
-- columns: ev_enabled
-- row: R
-- end-expected
SELECT ev_enabled FROM pg_rewrite WHERE rulename = 'ba_rt_r';

ALTER TABLE ba_rt ENABLE ALWAYS RULE ba_rt_r;

-- begin-expected
-- columns: ev_enabled
-- row: A
-- end-expected
SELECT ev_enabled FROM pg_rewrite WHERE rulename = 'ba_rt_r';

ALTER TABLE ba_rt DISABLE RULE ba_rt_r;

-- begin-expected
-- columns: ev_enabled
-- row: D
-- end-expected
SELECT ev_enabled FROM pg_rewrite WHERE rulename = 'ba_rt_r';

ALTER TABLE ba_rt ENABLE RULE ba_rt_r;

-- begin-expected
-- columns: ev_enabled
-- row: O
-- end-expected
SELECT ev_enabled FROM pg_rewrite WHERE rulename = 'ba_rt_r';

CREATE TABLE ba_tt (a int primary key);
CREATE FUNCTION ba_trgf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE TRIGGER ba_tt_t BEFORE INSERT ON ba_tt FOR EACH ROW EXECUTE FUNCTION ba_trgf();

ALTER TABLE ba_tt ENABLE REPLICA TRIGGER ba_tt_t;

-- begin-expected
-- columns: tgenabled
-- row: R
-- end-expected
SELECT tgenabled FROM pg_trigger WHERE tgname = 'ba_tt_t';

ALTER TABLE ba_tt ENABLE ALWAYS TRIGGER ba_tt_t;

-- begin-expected
-- columns: tgenabled
-- row: A
-- end-expected
SELECT tgenabled FROM pg_trigger WHERE tgname = 'ba_tt_t';

ALTER TABLE ba_tt DISABLE TRIGGER ba_tt_t;

-- begin-expected
-- columns: tgenabled
-- row: D
-- end-expected
SELECT tgenabled FROM pg_trigger WHERE tgname = 'ba_tt_t';

-- The group selectors reach it too.
ALTER TABLE ba_tt ENABLE TRIGGER ALL;

-- begin-expected
-- columns: tgenabled
-- row: O
-- end-expected
SELECT tgenabled FROM pg_trigger WHERE tgname = 'ba_tt_t';

ALTER TABLE ba_tt DISABLE TRIGGER USER;

-- begin-expected
-- columns: tgenabled
-- row: D
-- end-expected
SELECT tgenabled FROM pg_trigger WHERE tgname = 'ba_tt_t';

ALTER TABLE ba_tt ENABLE TRIGGER USER;

-- A rule or a trigger that is not there is still reported, in every mode.
-- begin-expected-error
-- sqlstate: 42704
-- message-like: rule "ba_nosuch" for relation "ba_rt" does not exist
-- end-expected-error
ALTER TABLE ba_rt ENABLE REPLICA RULE ba_nosuch;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: rule "ba_nosuch" for relation "ba_rt" does not exist
-- end-expected-error
ALTER TABLE ba_rt DISABLE RULE ba_nosuch;

-- The wording differs (PostgreSQL says "for table", memgres names the relation
-- the same way it does elsewhere), so only the SQLSTATE is asserted here.
-- begin-expected-error
-- sqlstate: 42704
-- end-expected-error
ALTER TABLE ba_tt ENABLE ALWAYS TRIGGER ba_nosuch;

-- The neighbouring ENABLE / DISABLE forms must keep parsing.
ALTER TABLE ba_rt ENABLE ROW LEVEL SECURITY;
ALTER TABLE ba_rt DISABLE ROW LEVEL SECURITY;
ALTER TABLE ba_rt SET WITHOUT CLUSTER;

-- ============================================================================
-- SECTION C: an inherited column belongs to its parent
-- ============================================================================

CREATE TABLE ba_par (a int, b int);
CREATE TABLE ba_ch (c int) INHERITS (ba_par);

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot alter inherited column "a"
-- end-expected-error
ALTER TABLE ba_ch ALTER COLUMN a TYPE bigint;

-- The child's own column is still its own.
ALTER TABLE ba_ch ALTER COLUMN c TYPE bigint;

-- begin-expected
-- columns: column_name | data_type
-- row: a | integer
-- row: b | integer
-- row: c | bigint
-- end-expected
SELECT column_name, data_type FROM information_schema.columns
 WHERE table_name = 'ba_ch' ORDER BY ordinal_position;

-- A retype on the parent reaches the child, values and all.
INSERT INTO ba_ch VALUES (1, 2, 3);
ALTER TABLE ba_par ALTER COLUMN b TYPE bigint;

-- begin-expected
-- columns: parent_type | child_type
-- row: bigint | bigint
-- end-expected
SELECT (SELECT data_type FROM information_schema.columns
        WHERE table_name = 'ba_par' AND column_name = 'b') AS parent_type,
       (SELECT data_type FROM information_schema.columns
        WHERE table_name = 'ba_ch' AND column_name = 'b') AS child_type;

ALTER TABLE ba_par ALTER COLUMN a TYPE text;

-- begin-expected
-- columns: a | t
-- row: 1 | text
-- end-expected
SELECT a, pg_typeof(a)::text AS t FROM ba_ch;

-- ONLY on a parent that has children is refused for a retype: the child would
-- be left disagreeing with the parent about the same column.
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: type of inherited column "b" must be changed in child tables too
-- end-expected-error
ALTER TABLE ONLY ba_par ALTER COLUMN b TYPE numeric;

-- The parent renaming the column does not make it the child's.
ALTER TABLE ba_par RENAME COLUMN a TO pa;

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot alter inherited column "pa"
-- end-expected-error
ALTER TABLE ba_ch ALTER COLUMN pa TYPE varchar(30);

-- Nor does attaching the child afterwards rather than declaring it.
CREATE TABLE ba_ip (a int, b int);
CREATE TABLE ba_ic (a int, b int);
ALTER TABLE ba_ic INHERIT ba_ip;

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot alter inherited column "a"
-- end-expected-error
ALTER TABLE ba_ic ALTER COLUMN a TYPE text;

ALTER TABLE ba_ip ALTER COLUMN b TYPE bigint;

-- begin-expected
-- columns: data_type
-- row: bigint
-- end-expected
SELECT data_type FROM information_schema.columns
 WHERE table_name = 'ba_ic' AND column_name = 'b';

-- The rule holds down several levels, in both directions.
CREATE TABLE ba_g1 (a int, b int);
CREATE TABLE ba_g2 () INHERITS (ba_g1);
CREATE TABLE ba_g3 () INHERITS (ba_g2);

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot alter inherited column "a"
-- end-expected-error
ALTER TABLE ba_g3 ALTER COLUMN a TYPE bigint;

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot alter inherited column "a"
-- end-expected-error
ALTER TABLE ba_g2 ALTER COLUMN a TYPE bigint;

ALTER TABLE ba_g1 ALTER COLUMN a TYPE bigint;

-- begin-expected
-- columns: table_name | data_type
-- row: ba_g1 | bigint
-- row: ba_g2 | bigint
-- row: ba_g3 | bigint
-- end-expected
SELECT table_name, data_type FROM information_schema.columns
 WHERE table_name IN ('ba_g1','ba_g2','ba_g3') AND column_name = 'a' ORDER BY 1;

-- Both parents count when a table inherits from two.
CREATE TABLE ba_m1 (a int);
CREATE TABLE ba_m2 (b int);
CREATE TABLE ba_m3 (c int) INHERITS (ba_m1, ba_m2);

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot alter inherited column "a"
-- end-expected-error
ALTER TABLE ba_m3 ALTER COLUMN a TYPE bigint;

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot alter inherited column "b"
-- end-expected-error
ALTER TABLE ba_m3 ALTER COLUMN b TYPE bigint;

ALTER TABLE ba_m1 ALTER COLUMN a TYPE bigint;

-- begin-expected
-- columns: data_type
-- row: bigint
-- end-expected
SELECT data_type FROM information_schema.columns
 WHERE table_name = 'ba_m3' AND column_name = 'a';

-- A partition is an inheritance child too.
CREATE TABLE ba_pt (id int, v text) PARTITION BY RANGE (id);
CREATE TABLE ba_pt1 PARTITION OF ba_pt FOR VALUES FROM (1) TO (100);

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot alter inherited column "v"
-- end-expected-error
ALTER TABLE ba_pt1 ALTER COLUMN v TYPE varchar(20);

ALTER TABLE ba_pt ALTER COLUMN v TYPE varchar(20);

-- begin-expected
-- columns: data_type | character_maximum_length
-- row: character varying | 20
-- end-expected
SELECT data_type, character_maximum_length FROM information_schema.columns
 WHERE table_name = 'ba_pt1' AND column_name = 'v';

-- A table with no hierarchy around it retypes as it always did, ONLY or not,
-- and a child detached from its parent owns its columns again.
CREATE TABLE ba_pu (a int, b int);
ALTER TABLE ba_pu ALTER COLUMN a TYPE bigint;
ALTER TABLE ONLY ba_pu ALTER COLUMN b TYPE bigint;
CREATE TABLE ba_pv (c int) INHERITS (ba_pu);
ALTER TABLE ba_pv NO INHERIT ba_pu;
ALTER TABLE ba_pv ALTER COLUMN a TYPE text;

-- begin-expected
-- columns: table_name | data_type
-- row: ba_pu | bigint
-- row: ba_pv | text
-- end-expected
SELECT table_name, data_type FROM information_schema.columns
 WHERE table_name IN ('ba_pu','ba_pv') AND column_name = 'a' ORDER BY 1;

-- The other column actions are the child's to make on an inherited column.
ALTER TABLE ba_ic ALTER COLUMN a SET NOT NULL;
ALTER TABLE ba_ic ALTER COLUMN a DROP NOT NULL;
ALTER TABLE ba_ic ALTER COLUMN a SET DEFAULT 3;
ALTER TABLE ba_ic ALTER COLUMN a DROP DEFAULT;
ALTER TABLE ba_ic ALTER COLUMN a SET STATISTICS 10;
ALTER TABLE ba_ic ALTER COLUMN a SET STORAGE PLAIN;

-- ============================================================================
-- SECTION D: ONLY on a table whose children have to look like it
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: constraint must be added to child tables too
-- hint-like: Do not specify the ONLY keyword.
-- end-expected-error
ALTER TABLE ONLY ba_pt ALTER COLUMN v SET NOT NULL;

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: constraint must be added to child tables too
-- end-expected-error
ALTER TABLE ONLY ba_pt ADD CONSTRAINT ba_pt_ck CHECK (id > 0);

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: constraint must be added to child tables too
-- end-expected-error
ALTER TABLE ONLY ba_pt ADD CHECK (id > 0);

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: constraint must be added to child tables too
-- end-expected-error
ALTER TABLE ONLY ba_pt ADD CONSTRAINT ba_pt_ck CHECK (id > 0) NOT VALID;

-- ... and without ONLY the same statements are ordinary.
ALTER TABLE ba_pt ALTER COLUMN v SET NOT NULL;
ALTER TABLE ba_pt ADD CONSTRAINT ba_pt_ck CHECK (id > 0);
ALTER TABLE ba_pt ADD COLUMN w int;

-- A CHECK on an inheritance parent belongs to the hierarchy as well ...
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: constraint must be added to child tables too
-- end-expected-error
ALTER TABLE ONLY ba_ip ADD CONSTRAINT ba_ip_ck CHECK (a > 0);

-- ... unless NO INHERIT says it was never going to travel.
ALTER TABLE ONLY ba_ip ADD CONSTRAINT ba_ip_ni CHECK (a > 0) NO INHERIT;

-- SET NOT NULL is looser on an inheritance parent than on a partitioned one:
-- a child there is a table in its own right.
ALTER TABLE ONLY ba_ip ALTER COLUMN b SET NOT NULL;

-- A partition always carries its parent's constraints, so NO INHERIT asks for
-- something the hierarchy cannot express.
-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot add NO INHERIT constraint to partitioned table "ba_pt"
-- end-expected-error
ALTER TABLE ONLY ba_pt ADD CONSTRAINT ba_pt_ni CHECK (id > 0) NO INHERIT;

-- begin-expected-error
-- sqlstate: 42P16
-- message-like: cannot drop column from only the partitioned table when partitions exist
-- end-expected-error
ALTER TABLE ONLY ba_pt DROP COLUMN w;

-- An inheritance parent may drop a column alone, and so may a partitioned table
-- with nothing under it yet.
ALTER TABLE ONLY ba_ip DROP COLUMN b;
CREATE TABLE ba_ref (id int primary key);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot use ONLY for foreign key on partitioned table "ba_pt" referencing relation "ba_ref"
-- end-expected-error
ALTER TABLE ONLY ba_pt ADD CONSTRAINT ba_pt_fk FOREIGN KEY (id) REFERENCES ba_ref(id);

ALTER TABLE ba_pt ADD CONSTRAINT ba_pt_fk FOREIGN KEY (id) REFERENCES ba_ref(id);

-- A partitioned table holds no rows of its own, so there is no storage for a
-- storage parameter to describe.
-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot specify storage parameters for a partitioned table
-- end-expected-error
ALTER TABLE ba_pt SET (fillfactor = 50);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: cannot specify storage parameters for a partitioned table
-- end-expected-error
ALTER TABLE ONLY ba_pt SET (fillfactor = 50);

-- The other SET forms are not storage parameters, and an ordinary table takes
-- them all.
ALTER TABLE ba_pt SET WITHOUT CLUSTER;
ALTER TABLE ba_pt RESET (fillfactor);
ALTER TABLE ba_pu SET (fillfactor = 50);
ALTER TABLE ba_pu RESET (fillfactor);

-- ONLY on something with nothing under it is left alone, whether it is a plain
-- table, a childless partitioned table, or a leaf partition.
ALTER TABLE ONLY ba_pu ALTER COLUMN b SET NOT NULL;
ALTER TABLE ONLY ba_pu ADD CONSTRAINT ba_pu_ck CHECK (a > 0);
ALTER TABLE ONLY ba_pt1 ALTER COLUMN id SET NOT NULL;
ALTER TABLE ONLY ba_pt1 ADD CONSTRAINT ba_pt1_ck CHECK (id > 0);

DROP TABLE IF EXISTS ba_dv, ba_dc, ba_dn, ba_da CASCADE;
DROP TABLE IF EXISTS ba_ch, ba_par CASCADE;
DROP TABLE IF EXISTS ba_g3, ba_g2, ba_g1 CASCADE;
DROP TABLE IF EXISTS ba_m3, ba_m2, ba_m1 CASCADE;
DROP TABLE IF EXISTS ba_pt1, ba_pt, ba_pv, ba_pu CASCADE;
DROP TABLE IF EXISTS ba_ic, ba_ip, ba_ref CASCADE;
DROP TABLE IF EXISTS ba_rt, ba_tt CASCADE;
DROP FUNCTION IF EXISTS ba_trgf() CASCADE;
