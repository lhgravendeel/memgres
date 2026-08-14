-- ============================================================================
-- What an arbiter predicate implies
--
-- A predicate written beside a conflict target names a partial index, and
-- PostgreSQL takes the index only when the index's own predicate follows from
-- the one written. It asks that of the two predicates as the planner leaves
-- them, so the proof reaches through the forms one clause can be written in: a
-- NOT is the comparison it negates, a test against a boolean constant is the
-- operand it tests, BETWEEN and IN are the conjunction and the disjunction they
-- stand for, and a strict operator or function proves its argument has a value.
--
-- What it will not do is guess. A predicate that admits a row the index does
-- not hold reaches nothing, and neither does no predicate at all.
-- ============================================================================

-- ============================================================================
-- The same predicate written another way round reaches the same index
-- ============================================================================
CREATE TABLE arbz_a (i int, f boolean, s text);
CREATE UNIQUE INDEX arbz_a_u ON arbz_a (i) WHERE i > 0;
INSERT INTO arbz_a VALUES (1, true, 'a');

INSERT INTO arbz_a VALUES (1, true, 'b') ON CONFLICT (i) WHERE NOT (i <= 0) DO NOTHING;
INSERT INTO arbz_a VALUES (1, true, 'b') ON CONFLICT (i) WHERE i BETWEEN 1 AND 10 DO NOTHING;
INSERT INTO arbz_a VALUES (1, true, 'b') ON CONFLICT (i) WHERE i IN (1,5) DO NOTHING;
INSERT INTO arbz_a VALUES (1, true, 'b') ON CONFLICT (i) WHERE i = 1 DO NOTHING;
INSERT INTO arbz_a VALUES (1, true, 'b') ON CONFLICT (i) WHERE i >= 1 DO NOTHING;
INSERT INTO arbz_a VALUES (1, true, 'b') ON CONFLICT (i) WHERE i > 5 OR i > 2 DO NOTHING;
INSERT INTO arbz_a VALUES (1, true, 'b') ON CONFLICT (i) WHERE i > 0 AND s = 'x' DO NOTHING;
INSERT INTO arbz_a VALUES (1, true, 'b') ON CONFLICT (i) WHERE i > 0 AND i BETWEEN 1 AND 10 DO NOTHING;

-- a negation that rules out less than the index does proves nothing
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbz_a VALUES (1, true, 'b') ON CONFLICT (i) WHERE NOT (i < 0) DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbz_a VALUES (1, true, 'b') ON CONFLICT (i) WHERE NOT (i BETWEEN -5 AND 0) DO NOTHING;

-- a range whose lower bound admits rows the index does not hold
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbz_a VALUES (1, true, 'b') ON CONFLICT (i) WHERE i BETWEEN -5 AND 10 DO NOTHING;

-- a list holding one value the index does not
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbz_a VALUES (1, true, 'b') ON CONFLICT (i) WHERE i IN (0,5) DO NOTHING;

-- a list of what is ruled out is no list of what is admitted
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbz_a VALUES (1, true, 'b') ON CONFLICT (i) WHERE i NOT IN (0,-1) DO NOTHING;

-- one branch of a disjunction reaching the index is not the disjunction
-- reaching it
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbz_a VALUES (1, true, 'b') ON CONFLICT (i) WHERE i > 0 OR s = 'x' DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbz_a VALUES (1, true, 'b') ON CONFLICT (i) WHERE i > 0 AND false DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbz_a VALUES (1, true, 'b') ON CONFLICT (i) WHERE true DO NOTHING;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM arbz_a;

DROP TABLE arbz_a;

-- ============================================================================
-- A test against a boolean constant is the operand it tests
-- ============================================================================
CREATE TABLE arbz_b (i int, f boolean);
CREATE UNIQUE INDEX arbz_b_u ON arbz_b (i) WHERE f;
INSERT INTO arbz_b VALUES (1, true);

INSERT INTO arbz_b VALUES (1, true) ON CONFLICT (i) WHERE f DO NOTHING;
INSERT INTO arbz_b VALUES (1, true) ON CONFLICT (i) WHERE f = true DO NOTHING;
INSERT INTO arbz_b VALUES (1, true) ON CONFLICT (i) WHERE true = f DO NOTHING;
INSERT INTO arbz_b VALUES (1, true) ON CONFLICT (i) WHERE f <> false DO NOTHING;
INSERT INTO arbz_b VALUES (1, true) ON CONFLICT (i) WHERE NOT (f = false) DO NOTHING;
INSERT INTO arbz_b VALUES (1, true) ON CONFLICT (i) WHERE NOT NOT f DO NOTHING;
INSERT INTO arbz_b VALUES (1, true) ON CONFLICT (i) WHERE f AND i > 0 DO NOTHING;

-- a boolean test is not the operand: it answers where the operand is null
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbz_b VALUES (1, true) ON CONFLICT (i) WHERE f IS TRUE DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbz_b VALUES (1, true) ON CONFLICT (i) WHERE f IS NOT NULL DO NOTHING;

-- and the negation of the operand is not the operand either
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbz_b VALUES (1, true) ON CONFLICT (i) WHERE NOT f DO NOTHING;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM arbz_b;

DROP TABLE arbz_b;

-- ============================================================================
-- A bound proves an inequality where it rules the value out
-- ============================================================================
CREATE TABLE arbz_c (i int);
CREATE UNIQUE INDEX arbz_c_u ON arbz_c (i) WHERE i <> 0;
INSERT INTO arbz_c VALUES (1);

INSERT INTO arbz_c VALUES (1) ON CONFLICT (i) WHERE i <> 0 DO NOTHING;
INSERT INTO arbz_c VALUES (1) ON CONFLICT (i) WHERE i > 0 DO NOTHING;
INSERT INTO arbz_c VALUES (1) ON CONFLICT (i) WHERE i >= 1 DO NOTHING;
INSERT INTO arbz_c VALUES (1) ON CONFLICT (i) WHERE i < 0 DO NOTHING;
INSERT INTO arbz_c VALUES (1) ON CONFLICT (i) WHERE i = 1 DO NOTHING;

-- a bound that admits the value it would have to rule out proves nothing
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbz_c VALUES (1) ON CONFLICT (i) WHERE i > -1 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbz_c VALUES (1) ON CONFLICT (i) WHERE i >= 0 DO NOTHING;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM arbz_c;

DROP TABLE arbz_c;

-- ============================================================================
-- A strict operator or function answers nothing where its argument is null,
-- which is what proves the argument is not null
-- ============================================================================
CREATE TABLE arbz_d (i int, s text);
CREATE UNIQUE INDEX arbz_d_u ON arbz_d (i) WHERE s IS NOT NULL;
INSERT INTO arbz_d VALUES (1,'x');

INSERT INTO arbz_d VALUES (1,'y') ON CONFLICT (i) WHERE length(s) > 0 DO NOTHING;
INSERT INTO arbz_d VALUES (1,'y') ON CONFLICT (i) WHERE upper(s) = 'X' DO NOTHING;
INSERT INTO arbz_d VALUES (1,'y') ON CONFLICT (i) WHERE substr(s,1,1) = 'x' DO NOTHING;
INSERT INTO arbz_d VALUES (1,'y') ON CONFLICT (i) WHERE s || 'y' = 'xy' DO NOTHING;
INSERT INTO arbz_d VALUES (1,'y') ON CONFLICT (i) WHERE s LIKE 'a%' DO NOTHING;
INSERT INTO arbz_d VALUES (1,'y') ON CONFLICT (i) WHERE s::int > 0 DO NOTHING;
INSERT INTO arbz_d VALUES (1,'y') ON CONFLICT (i) WHERE NOT (s = 'q') DO NOTHING;
INSERT INTO arbz_d VALUES (1,'y') ON CONFLICT (i) WHERE NOT (s IS NULL) DO NOTHING;
INSERT INTO arbz_d VALUES (1,'y') ON CONFLICT (i) WHERE s IN ('a','b') DO NOTHING;
INSERT INTO arbz_d VALUES (1,'y') ON CONFLICT (i) WHERE s BETWEEN 'a' AND 'z' DO NOTHING;
INSERT INTO arbz_d VALUES (1,'y') ON CONFLICT (i) WHERE length(s) > 0 OR s = 'q' DO NOTHING;
INSERT INTO arbz_d VALUES (1,'y') ON CONFLICT (i) WHERE length(s) > 0 AND i > 0 DO NOTHING;

-- COALESCE, NULLIF and CASE answer for a row whose column is null, so a clause
-- written over one of them says nothing about the column
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbz_d VALUES (1,'y') ON CONFLICT (i) WHERE coalesce(s,'y') = 'x' DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbz_d VALUES (1,'y') ON CONFLICT (i) WHERE nullif(s,'q') = 'x' DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbz_d VALUES (1,'y') ON CONFLICT (i) WHERE CASE WHEN s = 'x' THEN true ELSE false END DO NOTHING;

-- and a clause about the other column says nothing about this one
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbz_d VALUES (1,'y') ON CONFLICT (i) WHERE i > 0 DO NOTHING;

-- begin-expected
-- columns: i | s
-- row: 1 | x
-- end-expected
SELECT i, s FROM arbz_d ORDER BY i;

DROP TABLE arbz_d;

CREATE TABLE arbz_e (i int, f boolean);
CREATE UNIQUE INDEX arbz_e_u ON arbz_e (i) WHERE f IS NOT NULL;
INSERT INTO arbz_e VALUES (1, true);

-- a row a boolean column holds for is a row where it has a value
INSERT INTO arbz_e VALUES (1, true) ON CONFLICT (i) WHERE f DO NOTHING;

-- but a NOT over it is no strict thing at all
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbz_e VALUES (1, true) ON CONFLICT (i) WHERE NOT f DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbz_e VALUES (1, true) ON CONFLICT (i) WHERE f IS TRUE DO NOTHING;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM arbz_e;

DROP TABLE arbz_e;

-- ============================================================================
-- An index predicate of two branches is reached by what entails either of them
-- ============================================================================
CREATE TABLE arbz_f (i int);
CREATE UNIQUE INDEX arbz_f_u ON arbz_f (i) WHERE i > 0 OR i < -10;
INSERT INTO arbz_f VALUES (1);

INSERT INTO arbz_f VALUES (1) ON CONFLICT (i) WHERE i > 5 DO NOTHING;
INSERT INTO arbz_f VALUES (1) ON CONFLICT (i) WHERE i < -20 DO NOTHING;
INSERT INTO arbz_f VALUES (1) ON CONFLICT (i) WHERE i > 0 OR i < -10 DO NOTHING;
INSERT INTO arbz_f VALUES (1) ON CONFLICT (i) WHERE i BETWEEN 1 AND 10 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbz_f VALUES (1) ON CONFLICT (i) WHERE i > -5 DO NOTHING;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM arbz_f;

DROP TABLE arbz_f;

-- ============================================================================
-- A text bound is a bound like any other
-- ============================================================================
CREATE TABLE arbz_g (i int, s text);
CREATE UNIQUE INDEX arbz_g_u ON arbz_g (i) WHERE s > 'a';
INSERT INTO arbz_g VALUES (1,'x');

INSERT INTO arbz_g VALUES (1,'y') ON CONFLICT (i) WHERE s > 'b' DO NOTHING;
INSERT INTO arbz_g VALUES (1,'y') ON CONFLICT (i) WHERE s >= 'b' DO NOTHING;
INSERT INTO arbz_g VALUES (1,'y') ON CONFLICT (i) WHERE s = 'b' DO NOTHING;
INSERT INTO arbz_g VALUES (1,'y') ON CONFLICT (i) WHERE s > 'aa' DO NOTHING;
INSERT INTO arbz_g VALUES (1,'y') ON CONFLICT (i) WHERE s BETWEEN 'b' AND 'z' DO NOTHING;
INSERT INTO arbz_g VALUES (1,'y') ON CONFLICT (i) WHERE s IN ('b','c') DO NOTHING;
INSERT INTO arbz_g VALUES (1,'y') ON CONFLICT (i) WHERE NOT (s <= 'a') DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbz_g VALUES (1,'y') ON CONFLICT (i) WHERE s < 'b' DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbz_g VALUES (1,'y') ON CONFLICT (i) WHERE s = 'a' DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbz_g VALUES (1,'y') ON CONFLICT (i) WHERE s >= 'a' DO NOTHING;

-- begin-expected
-- columns: i | s
-- row: 1 | x
-- end-expected
SELECT i, s FROM arbz_g ORDER BY i;

DROP TABLE arbz_g;

-- ============================================================================
-- Every part of the index's predicate has to be proved, and the index with no
-- predicate at all has nothing to prove
-- ============================================================================
CREATE TABLE arbz_i (i int, k int, s text);
CREATE UNIQUE INDEX arbz_i_u ON arbz_i (i) WHERE i > 0 AND s IS NOT NULL;
INSERT INTO arbz_i VALUES (1,1,'a');

INSERT INTO arbz_i VALUES (1,2,'b') ON CONFLICT (i) WHERE i > 5 AND s = 'x' DO NOTHING;
INSERT INTO arbz_i VALUES (1,2,'b') ON CONFLICT (i) WHERE length(s) > 0 AND i BETWEEN 2 AND 3 DO NOTHING;

-- half the index's predicate is not the index's predicate
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbz_i VALUES (1,2,'b') ON CONFLICT (i) WHERE i > 5 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbz_i VALUES (1,2,'b') ON CONFLICT (i) WHERE s IS NOT NULL DO NOTHING;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM arbz_i;

DROP TABLE arbz_i;

CREATE TABLE arbz_j (i int PRIMARY KEY, k int);
INSERT INTO arbz_j VALUES (1,1);

INSERT INTO arbz_j VALUES (1,2) ON CONFLICT (i) WHERE false DO NOTHING;
INSERT INTO arbz_j VALUES (1,3) ON CONFLICT (i) WHERE k > 0 DO NOTHING;
INSERT INTO arbz_j VALUES (1,4) ON CONFLICT (i) WHERE i IN (1,5) DO UPDATE SET k = 9;

-- begin-expected
-- columns: i | k
-- row: 1 | 9
-- end-expected
SELECT i, k FROM arbz_j ORDER BY i;

DROP TABLE arbz_j;

-- ============================================================================
-- A partial index is reached by no predicate at all only when there is none to
-- prove, so a conflict target written bare goes on being refused
-- ============================================================================
CREATE TABLE arbz_k (i int, k int);
CREATE UNIQUE INDEX arbz_k_u ON arbz_k (i) WHERE i > 0;
INSERT INTO arbz_k VALUES (1,1);

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbz_k VALUES (1,2) ON CONFLICT (i) DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbz_k VALUES (1,2) ON CONFLICT (i) WHERE false DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO arbz_k VALUES (1,2) ON CONFLICT (i) WHERE k > 0 DO NOTHING;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*) AS n FROM arbz_k;

DROP TABLE arbz_k;

-- ============================================================================
-- The calls the predicate holds are resolved where the statement stands
--
-- The predicate is read, never evaluated, so a call that names nothing is
-- refused and a call that names something stands whatever it would answer.
-- ============================================================================
CREATE TABLE arbz_h (i int PRIMARY KEY, s text);
INSERT INTO arbz_h VALUES (1,'a');

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunc(integer) does not exist
-- end-expected-error
INSERT INTO arbz_h VALUES (1,'b') ON CONFLICT (i) WHERE nosuchfunc(i) > 0 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunc() does not exist
-- end-expected-error
INSERT INTO arbz_h VALUES (1,'b') ON CONFLICT (i) WHERE nosuchfunc() > 0 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunc(unknown) does not exist
-- end-expected-error
INSERT INTO arbz_h VALUES (1,'b') ON CONFLICT (i) WHERE nosuchfunc('lit') > 0 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function pg_catalog.nosuchfunc(integer) does not exist
-- end-expected-error
INSERT INTO arbz_h VALUES (1,'b') ON CONFLICT (i) WHERE pg_catalog.nosuchfunc(i) > 0 DO NOTHING;

-- a name that is a schema's to answer for is answered by the schema
-- begin-expected-error
-- sqlstate: 3F000
-- message-like: schema "nosuchschema" does not exist
-- end-expected-error
INSERT INTO arbz_h VALUES (1,'b') ON CONFLICT (i) WHERE nosuchschema.nosuchfunc(i) > 0 DO NOTHING;

-- a call whose name exists but not for these arguments is refused the same way
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function length(integer) does not exist
-- end-expected-error
INSERT INTO arbz_h VALUES (1,'b') ON CONFLICT (i) WHERE length(i) > 0 DO NOTHING;

-- the arguments are read before the name is looked for, and the predicate left
-- to right, so which fault is reported follows from where it was written
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
INSERT INTO arbz_h VALUES (1,'b') ON CONFLICT (i) WHERE nosuchfunc(nosuchcol) > 0 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
INSERT INTO arbz_h VALUES (1,'b') ON CONFLICT (i) WHERE nosuchcol > 0 AND nosuchfunc(i) > 0 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunc(integer) does not exist
-- end-expected-error
INSERT INTO arbz_h VALUES (1,'b') ON CONFLICT (i) WHERE nosuchfunc(i) > 0 AND nosuchcol > 0 DO NOTHING;

-- the target's own columns are read first, and the action after the predicate
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" does not exist
-- end-expected-error
INSERT INTO arbz_h VALUES (1,'b') ON CONFLICT (nosuchcol) WHERE nosuchfunc(i) > 0 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunc(integer) does not exist
-- end-expected-error
INSERT INTO arbz_h VALUES (1,'b') ON CONFLICT (i) WHERE nosuchfunc(i) > 0 DO UPDATE SET s = nosuchcol;

-- and the call is refused whether or not any index would have arbitrated
-- begin-expected-error
-- sqlstate: 42883
-- message-like: function nosuchfunc(integer) does not exist
-- end-expected-error
INSERT INTO arbz_h VALUES (1,'b') ON CONFLICT (s) WHERE nosuchfunc(i) > 0 DO NOTHING;

-- a call that does name a function stands, because the predicate is read and
-- never evaluated
INSERT INTO arbz_h VALUES (1,'b') ON CONFLICT (i) WHERE random() > 0.5 DO NOTHING;

-- begin-expected
-- columns: i | s
-- row: 1 | a
-- end-expected
SELECT i, s FROM arbz_h ORDER BY i;

DROP TABLE arbz_h;
