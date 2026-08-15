-- ============================================================================
-- The type a default has is the type of the whole expression
--
-- PostgreSQL coerces a DEFAULT to the column's type in assignment context: a
-- cast pg_cast records as implicit or assignment, or -- with no cast row at all
-- -- a read through the value's own text form, which it allows only into a
-- string type. So an operator's result, a cast's target and a call's return
-- type each decide the answer, and not only a literal's.
-- ============================================================================

-- The assignment casts PostgreSQL does have, and the general rule that a string
-- type takes anything at all through its own input function.
CREATE TABLE zze6gd_ok (a bigint DEFAULT 1, b text DEFAULT 1, c numeric DEFAULT 1,
                        d text DEFAULT now(), e varchar(4) DEFAULT 1,
                        f timestamp DEFAULT current_date, g date DEFAULT now());
INSERT INTO zze6gd_ok DEFAULT VALUES;

-- begin-expected
-- columns: a | b | c
-- row: 1 | 1 | 1
-- end-expected
SELECT a, b, c FROM zze6gd_ok;

-- begin-expected
-- columns: n
-- row: 7
-- end-expected
SELECT count(*) AS n FROM information_schema.columns WHERE table_name = 'zze6gd_ok';

DROP TABLE zze6gd_ok;

CREATE TABLE zze6gd_ok3 (a int DEFAULT 1.7, b smallint DEFAULT 3, c real DEFAULT 1,
                         d char(3) DEFAULT 42, e text DEFAULT current_date,
                         f bigint DEFAULT 2::smallint, g numeric(4,1) DEFAULT 2,
                         h text DEFAULT 1+1, i varchar(30) DEFAULT now()::date,
                         j double precision DEFAULT 1);
INSERT INTO zze6gd_ok3 DEFAULT VALUES;

-- A numeric default is rounded into an integer column, and a number reaches a
-- character type through its own text form.
-- begin-expected
-- columns: a | b | c | d | f | g | h | j
-- row: 2 | 3 | 1 | 42  | 2 | 2.0 | 2 | 1
-- end-expected
SELECT a, b, c, d, f, g, h, j FROM zze6gd_ok3;

DROP TABLE zze6gd_ok3;

-- A literal written with no type of its own is read by the column's own input
-- function, whatever the column's type is.
CREATE TABLE zze6gd_v (a int DEFAULT 1::bigint, b date DEFAULT '2020-01-01',
                       c interval DEFAULT '1 day', d int DEFAULT '5',
                       e boolean DEFAULT 'yes', f int[] DEFAULT '{1,2}',
                       g int DEFAULT NULL, h numeric DEFAULT 1.5, i text DEFAULT 3.5);
INSERT INTO zze6gd_v DEFAULT VALUES;

-- begin-expected
-- columns: a | b | c | d | e | f | g | h | i
-- row: 1 | 2020-01-01 | 1 day | 5 | t | {1,2} | NULL | 1.5 | 3.5
-- end-expected
SELECT a, b, c, d, e, f, g, h, i FROM zze6gd_v;

DROP TABLE zze6gd_v;

-- A default too long for the column is a question about the value, not about
-- the type, so it is asked when the row is written.
CREATE TABLE zze6gd_vv (j varchar(5) DEFAULT 1000000);

-- begin-expected-error
-- sqlstate: 22001
-- message-like: value too long for type character varying(5)
-- end-expected-error
INSERT INTO zze6gd_vv DEFAULT VALUES;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*) AS n FROM zze6gd_vv;

DROP TABLE zze6gd_vv;

-- ============================================================================
-- What a default is refused for is the type the expression as a whole has
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type integer but default expression is of type text
-- hint-like: You will need to rewrite or cast the expression.
-- end-expected-error
CREATE TABLE zze6gd_c1 (b int DEFAULT 'a'||'b');

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type integer but default expression is of type integer[]
-- hint-like: You will need to rewrite or cast the expression.
-- end-expected-error
CREATE TABLE zze6gd_c2 (b int DEFAULT '{1,2}'::int[]);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type integer but default expression is of type text
-- end-expected-error
CREATE TABLE zze6gd_c3 (b int DEFAULT coalesce('a','b'));

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type interval but default expression is of type timestamp with time zone
-- end-expected-error
CREATE TABLE zze6gd_c4 (b interval DEFAULT now());

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type boolean but default expression is of type text
-- end-expected-error
CREATE TABLE zze6gd_c5 (b boolean DEFAULT greatest('a','b'));

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b" is of type date but default expression is of type integer
-- end-expected-error
CREATE TABLE zze6gd_c6 (b date DEFAULT 1);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "a" is of type integer but default expression is of type boolean
-- end-expected-error
CREATE TABLE zze6gd_v3 (a int DEFAULT true);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "a" is of type boolean but default expression is of type integer
-- end-expected-error
CREATE TABLE zze6gd_v4 (a boolean DEFAULT 1);

-- A literal of no type of its own is read by the column's input function, so
-- what it is refused for is the value it holds.
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "x"
-- end-expected-error
CREATE TABLE zze6gd_v2 (a int DEFAULT 'x');

-- ============================================================================
-- The same rule where the column is added, and where the default is set later
-- ============================================================================
CREATE TABLE zze6gd_a (k int, b1 int, b2 interval);

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "a1" is of type integer but default expression is of type text
-- hint-like: You will need to rewrite or cast the expression.
-- end-expected-error
ALTER TABLE zze6gd_a ADD COLUMN a1 int DEFAULT 'a'||'b';

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "a2" is of type integer but default expression is of type integer[]
-- end-expected-error
ALTER TABLE zze6gd_a ADD COLUMN a2 int DEFAULT '{1,2}'::int[];

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "a3" is of type integer but default expression is of type text
-- end-expected-error
ALTER TABLE zze6gd_a ADD COLUMN a3 int DEFAULT coalesce('a','b');

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "a4" is of type interval but default expression is of type timestamp with time zone
-- end-expected-error
ALTER TABLE zze6gd_a ADD COLUMN a4 interval DEFAULT now();

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b1" is of type integer but default expression is of type text
-- end-expected-error
ALTER TABLE zze6gd_a ALTER COLUMN b1 SET DEFAULT 'a'||'b';

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b1" is of type integer but default expression is of type integer[]
-- end-expected-error
ALTER TABLE zze6gd_a ALTER COLUMN b1 SET DEFAULT '{1,2}'::int[];

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "b2" is of type interval but default expression is of type timestamp with time zone
-- end-expected-error
ALTER TABLE zze6gd_a ALTER COLUMN b2 SET DEFAULT now();

-- A generation expression is judged by the same rule, and PostgreSQL words it
-- the same way, because it is the same code that stores both.
-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "g1" is of type integer but default expression is of type text
-- end-expected-error
ALTER TABLE zze6gd_a ADD COLUMN g1 int GENERATED ALWAYS AS ('a'||'b') STORED;

-- begin-expected-error
-- sqlstate: 42804
-- message-like: column "g" is of type integer but default expression is of type text
-- end-expected-error
CREATE TABLE zze6gd_g (a int, g int GENERATED ALWAYS AS ('a'||'b') STORED);

-- Nothing the refusals turned away was added, and what PostgreSQL takes stands.
ALTER TABLE zze6gd_a ALTER COLUMN b1 SET DEFAULT 7;
ALTER TABLE zze6gd_a ADD COLUMN a5 bigint DEFAULT 1;
ALTER TABLE zze6gd_a ADD COLUMN a6 text DEFAULT 1;
INSERT INTO zze6gd_a (k) VALUES (1);

-- begin-expected
-- columns: k | b1 | a5 | a6
-- row: 1 | 7 | 1 | 1
-- end-expected
SELECT k, b1, a5, a6 FROM zze6gd_a;

-- begin-expected
-- columns: attname
-- row: k
-- row: b1
-- row: b2
-- row: a5
-- row: a6
-- end-expected
SELECT attname FROM pg_attribute WHERE attrelid = 'zze6gd_a'::regclass
  AND attnum > 0 AND NOT attisdropped ORDER BY attnum;

DROP TABLE zze6gd_a;

-- A generation expression PostgreSQL can coerce is taken, and the column holds
-- what the coercion made of it.
CREATE TABLE zze6gd_gv (a int, g bigint GENERATED ALWAYS AS (a * 2) STORED,
                        h text GENERATED ALWAYS AS (a) STORED,
                        i numeric GENERATED ALWAYS AS (a) STORED);
INSERT INTO zze6gd_gv (a) VALUES (4);

-- begin-expected
-- columns: a | g | h | i
-- row: 4 | 8 | 4 | 4
-- end-expected
SELECT a, g, h, i FROM zze6gd_gv;

DROP TABLE zze6gd_gv;
