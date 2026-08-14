-- ============================================================================
-- What a constant in an arbiter predicate is worth
--
-- Two things settle whether one bound proves another: the value the constant
-- works out to, and the type its spelling gives it.
--
-- The value is worked out. PostgreSQL asks the two predicates of each other
-- after the planner has folded them, so 1 - 1 and 5 % 5 and (1 + 1) * 3 - 6
-- are all the constant 0, and an index written WHERE i > 1 - 1 -- which the
-- catalogue reads back unfolded, as (i > (1 - 1)) -- is reached by a statement
-- that writes the bound as 0. Integer arithmetic stays integer, so 5 / 2 is 2
-- and not 2.5.
--
-- The type is not thrown away. Over an int column, WHERE i > 0.5 is stored as
-- ((i)::numeric > 0.5) -- the column is what the cast lands on -- while over a
-- numeric column WHERE n > 0 is stored as (n > (0)::numeric), where the
-- constant is what moves. Two comparisons are only about the same thing when
-- the same cast lands on the column in each, so over an int column a bound
-- written 1 proves one written 0 and a bound written 1.0 proves nothing at
-- all, while over a numeric column the two are alike.
--
-- Every value below was read off PostgreSQL 18 before it was written down.
-- ============================================================================

-- ============================================================================
-- Arithmetic over constants is worked out, in both directions
-- ============================================================================
CREATE TABLE zzt4a_a (i int, k int);
CREATE UNIQUE INDEX zzt4a_a_u ON zzt4a_a (i) WHERE i > 0;
INSERT INTO zzt4a_a VALUES (1, 1);

INSERT INTO zzt4a_a VALUES (1, 2) ON CONFLICT (i) WHERE i > 1 - 1 DO NOTHING;
INSERT INTO zzt4a_a VALUES (1, 2) ON CONFLICT (i) WHERE i > 2 - 1 DO NOTHING;
INSERT INTO zzt4a_a VALUES (1, 2) ON CONFLICT (i) WHERE i > 1 * 0 DO NOTHING;
INSERT INTO zzt4a_a VALUES (1, 2) ON CONFLICT (i) WHERE i > 0 + 0 DO NOTHING;
INSERT INTO zzt4a_a VALUES (1, 2) ON CONFLICT (i) WHERE i > -1 + 1 DO NOTHING;
INSERT INTO zzt4a_a VALUES (1, 2) ON CONFLICT (i) WHERE i > (1 + 1) * 3 - 6 DO NOTHING;
INSERT INTO zzt4a_a VALUES (1, 2) ON CONFLICT (i) WHERE i > 3 - 1 - 2 DO NOTHING;
INSERT INTO zzt4a_a VALUES (1, 2) ON CONFLICT (i) WHERE i > - (1 - 1) DO NOTHING;

-- Division between integers truncates toward zero, so 5 / 2 is 2 and 4 / 3
-- is 1, both above the index's bound.
INSERT INTO zzt4a_a VALUES (1, 2) ON CONFLICT (i) WHERE i > 5 / 2 DO NOTHING;
INSERT INTO zzt4a_a VALUES (1, 2) ON CONFLICT (i) WHERE i > 4 / 3 DO NOTHING;
INSERT INTO zzt4a_a VALUES (1, 2) ON CONFLICT (i) WHERE i > 6 / 2 / 3 DO NOTHING;
INSERT INTO zzt4a_a VALUES (1, 2) ON CONFLICT (i) WHERE i > 2 / 2 DO NOTHING;

-- A remainder is an integer too.
INSERT INTO zzt4a_a VALUES (1, 2) ON CONFLICT (i) WHERE i > 5 % 5 DO NOTHING;
INSERT INTO zzt4a_a VALUES (1, 2) ON CONFLICT (i) WHERE i > 7 % 3 DO NOTHING;

-- Working the arithmetic out is what refuses these: each comes to a bound
-- below the index's, which admits rows the index does not hold.
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_a VALUES (1, 2) ON CONFLICT (i) WHERE i > 0 - 1 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_a VALUES (1, 2) ON CONFLICT (i) WHERE i > 2 - 3 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_a VALUES (1, 2) ON CONFLICT (i) WHERE i > 2 * 3 - 7 DO NOTHING;

-- An operand that is not a constant is not worked out at all, whichever side
-- of the operator it stands on.
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_a VALUES (1, 2) ON CONFLICT (i) WHERE i > k - 1 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_a VALUES (1, 2) ON CONFLICT (i) WHERE i > 1 - k DO NOTHING;

-- begin-expected
-- columns: i | k
-- row: 1 | 1
-- end-expected
SELECT i, k FROM zzt4a_a ORDER BY i;

DROP TABLE zzt4a_a;

-- ============================================================================
-- The index's own predicate is worked out the same way
--
-- The catalogue keeps the predicate as it was written, and the proof folds
-- both sides when it is asked, so the two spellings meet in the middle.
-- ============================================================================
CREATE TABLE zzt4a_b (i int, k int);
CREATE UNIQUE INDEX zzt4a_b_u ON zzt4a_b (i) WHERE i > 1 - 1;

-- begin-expected
-- columns: pred
-- row: (i > (1 - 1))
-- end-expected
SELECT pg_get_expr(indpred, indrelid) AS pred FROM pg_index WHERE indexrelid = 'zzt4a_b_u'::regclass;

INSERT INTO zzt4a_b VALUES (1, 1);

INSERT INTO zzt4a_b VALUES (1, 2) ON CONFLICT (i) WHERE i > 0 DO NOTHING;
INSERT INTO zzt4a_b VALUES (1, 2) ON CONFLICT (i) WHERE i > 2 - 2 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_b VALUES (1, 2) ON CONFLICT (i) WHERE i > 0 - 2 DO NOTHING;

-- begin-expected
-- columns: i | k
-- row: 1 | 1
-- end-expected
SELECT i, k FROM zzt4a_b ORDER BY i;

DROP TABLE zzt4a_b;

-- ============================================================================
-- Over an int column, a constant spelled as a number with a point is not an
-- integer, and the cast that lands on the column is a different one
-- ============================================================================
CREATE TABLE zzt4a_c (i int, k int);
CREATE UNIQUE INDEX zzt4a_c_u ON zzt4a_c (i) WHERE i > 0;

-- Nothing is cast here: both sides are already integers.
-- begin-expected
-- columns: pred
-- row: (i > 0)
-- end-expected
SELECT pg_get_expr(indpred, indrelid) AS pred FROM pg_index WHERE indexrelid = 'zzt4a_c_u'::regclass;

INSERT INTO zzt4a_c VALUES (1, 1);

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_c VALUES (1, 2) ON CONFLICT (i) WHERE i > 0.5 DO NOTHING;

-- 1.0 is above the index's bound, and still proves nothing: the comparison it
-- writes is over a widened column, which is not the comparison the index made.
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_c VALUES (1, 2) ON CONFLICT (i) WHERE i > 1.0 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_c VALUES (1, 2) ON CONFLICT (i) WHERE i > 0.0 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_c VALUES (1, 2) ON CONFLICT (i) WHERE i > 1e0 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_c VALUES (1, 2) ON CONFLICT (i) WHERE i > 1::numeric DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_c VALUES (1, 2) ON CONFLICT (i) WHERE i > 1::float8 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_c VALUES (1, 2) ON CONFLICT (i) WHERE i > 1::real DO NOTHING;

-- The arithmetic is worked out first and the type of what it comes to is what
-- counts, so a subtraction that lands on a numeric proves nothing either.
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_c VALUES (1, 2) ON CONFLICT (i) WHERE i > 1 - 0.5 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_c VALUES (1, 2) ON CONFLICT (i) WHERE i > 1 - 1.0 DO NOTHING;

-- An integer type no wider than the column leaves the column alone, so these
-- do reach the index.
INSERT INTO zzt4a_c VALUES (1, 2) ON CONFLICT (i) WHERE i > 1::bigint DO NOTHING;
INSERT INTO zzt4a_c VALUES (1, 2) ON CONFLICT (i) WHERE i > 1::smallint DO NOTHING;
INSERT INTO zzt4a_c VALUES (1, 2) ON CONFLICT (i) WHERE i > 1.0::int DO NOTHING;
INSERT INTO zzt4a_c VALUES (1, 2) ON CONFLICT (i) WHERE i > (0) DO NOTHING;

-- A bare string has no type of its own; it takes the column's.
INSERT INTO zzt4a_c VALUES (1, 2) ON CONFLICT (i) WHERE i > '1' DO NOTHING;

-- begin-expected
-- columns: i | k
-- row: 1 | 1
-- end-expected
SELECT i, k FROM zzt4a_c ORDER BY i;

DROP TABLE zzt4a_c;

-- ============================================================================
-- An index built over the widened column is reached by the widened bounds and
-- by nothing else
-- ============================================================================
CREATE TABLE zzt4a_d (i int, k int);
CREATE UNIQUE INDEX zzt4a_d_u ON zzt4a_d (i) WHERE i > 0.5;

-- The column is the side that moves.
-- begin-expected
-- columns: pred
-- row: ((i)::numeric > 0.5)
-- end-expected
SELECT pg_get_expr(indpred, indrelid) AS pred FROM pg_index WHERE indexrelid = 'zzt4a_d_u'::regclass;

INSERT INTO zzt4a_d VALUES (1, 1);

INSERT INTO zzt4a_d VALUES (1, 2) ON CONFLICT (i) WHERE i > 0.6 DO NOTHING;

-- 1 is above 0.5 by any reading, and still proves nothing: it compares the
-- column unwidened.
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_d VALUES (1, 2) ON CONFLICT (i) WHERE i > 1 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_d VALUES (1, 2) ON CONFLICT (i) WHERE i > 0.4 DO NOTHING;

-- begin-expected
-- columns: i | k
-- row: 1 | 1
-- end-expected
SELECT i, k FROM zzt4a_d ORDER BY i;

DROP TABLE zzt4a_d;

-- ============================================================================
-- A cast down to an integer type rounds away from zero
-- ============================================================================
CREATE TABLE zzt4a_e (i int, k int);
CREATE UNIQUE INDEX zzt4a_e_u ON zzt4a_e (i) WHERE i > 1;
INSERT INTO zzt4a_e VALUES (2, 1);

-- 0.5::int is 1 and 1.5::int is 2, so both bounds are the index's or above it.
INSERT INTO zzt4a_e VALUES (2, 2) ON CONFLICT (i) WHERE i > 0.5::int DO NOTHING;
INSERT INTO zzt4a_e VALUES (2, 2) ON CONFLICT (i) WHERE i > 1.5::int DO NOTHING;

-- 0.4::int is 0, which is below it.
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_e VALUES (2, 2) ON CONFLICT (i) WHERE i > 0.4::int DO NOTHING;

-- Away from zero, so (-0.5)::int is -1 and not 0.
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_e VALUES (2, 2) ON CONFLICT (i) WHERE i > (-0.5)::int DO NOTHING;

-- begin-expected
-- columns: i | k
-- row: 2 | 1
-- end-expected
SELECT i, k FROM zzt4a_e ORDER BY i;

DROP TABLE zzt4a_e;

-- ============================================================================
-- Over a numeric column the constant is what moves, so every spelling of the
-- number meets the index
-- ============================================================================
CREATE TABLE zzt4a_f (i int, n numeric);
CREATE UNIQUE INDEX zzt4a_f_u ON zzt4a_f (n) WHERE n > 0;

-- begin-expected
-- columns: pred
-- row: (n > (0)::numeric)
-- end-expected
SELECT pg_get_expr(indpred, indrelid) AS pred FROM pg_index WHERE indexrelid = 'zzt4a_f_u'::regclass;

INSERT INTO zzt4a_f VALUES (1, 5);

INSERT INTO zzt4a_f VALUES (2, 5) ON CONFLICT (n) WHERE n > 0.5 DO NOTHING;
INSERT INTO zzt4a_f VALUES (2, 5) ON CONFLICT (n) WHERE n > 1 DO NOTHING;
INSERT INTO zzt4a_f VALUES (2, 5) ON CONFLICT (n) WHERE n > 1e0 DO NOTHING;
INSERT INTO zzt4a_f VALUES (2, 5) ON CONFLICT (n) WHERE n > 1::int DO NOTHING;
INSERT INTO zzt4a_f VALUES (2, 5) ON CONFLICT (n) WHERE n > 1 - 1 DO NOTHING;
INSERT INTO zzt4a_f VALUES (2, 5) ON CONFLICT (n) WHERE n > 0.5 + 0.5 DO NOTHING;
INSERT INTO zzt4a_f VALUES (2, 5) ON CONFLICT (n) WHERE n > 1.0 / 2 DO NOTHING;
INSERT INTO zzt4a_f VALUES (2, 5) ON CONFLICT (n) WHERE n > 1.5 - 0.5 DO NOTHING;
INSERT INTO zzt4a_f VALUES (2, 5) ON CONFLICT (n) WHERE n > 0.5 - 0.5 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_f VALUES (2, 5) ON CONFLICT (n) WHERE n > -1 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_f VALUES (2, 5) ON CONFLICT (n) WHERE n > 2 - 3 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_f VALUES (2, 5) ON CONFLICT (n) WHERE n > 0.5 - 1.0 DO NOTHING;

-- begin-expected
-- columns: i | n
-- row: 1 | 5
-- end-expected
SELECT i, n FROM zzt4a_f ORDER BY i;

DROP TABLE zzt4a_f;

-- ============================================================================
-- Over a bigint column, and over a float8 one
--
-- A bigint column takes an integer constant of any width, and a float8 column
-- takes every numeric spelling, because in each the constant is what the cast
-- lands on. A number with a point is wider than bigint, so it does not.
-- ============================================================================
CREATE TABLE zzt4a_p (i int, b bigint);
CREATE UNIQUE INDEX zzt4a_p_u ON zzt4a_p (b) WHERE b > 0;
INSERT INTO zzt4a_p VALUES (1, 5);

INSERT INTO zzt4a_p VALUES (2, 5) ON CONFLICT (b) WHERE b > 1 DO NOTHING;
INSERT INTO zzt4a_p VALUES (2, 5) ON CONFLICT (b) WHERE b > 1::int DO NOTHING;
INSERT INTO zzt4a_p VALUES (2, 5) ON CONFLICT (b) WHERE b > 1::bigint DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_p VALUES (2, 5) ON CONFLICT (b) WHERE b > 1.0 DO NOTHING;

-- begin-expected
-- columns: i | b
-- row: 1 | 5
-- end-expected
SELECT i, b FROM zzt4a_p ORDER BY i;

DROP TABLE zzt4a_p;

CREATE TABLE zzt4a_q (i int, f float8);
CREATE UNIQUE INDEX zzt4a_q_u ON zzt4a_q (f) WHERE f > 0;

-- begin-expected
-- columns: pred
-- row: (f > (0)::double precision)
-- end-expected
SELECT pg_get_expr(indpred, indrelid) AS pred FROM pg_index WHERE indexrelid = 'zzt4a_q_u'::regclass;

INSERT INTO zzt4a_q VALUES (1, 5);

INSERT INTO zzt4a_q VALUES (2, 5) ON CONFLICT (f) WHERE f > 1 DO NOTHING;
INSERT INTO zzt4a_q VALUES (2, 5) ON CONFLICT (f) WHERE f > 1.0 DO NOTHING;
INSERT INTO zzt4a_q VALUES (2, 5) ON CONFLICT (f) WHERE f > 1e0 DO NOTHING;
INSERT INTO zzt4a_q VALUES (2, 5) ON CONFLICT (f) WHERE f > 0.5 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_q VALUES (2, 5) ON CONFLICT (f) WHERE f > -1 DO NOTHING;

-- begin-expected
-- columns: i | f
-- row: 1 | 5
-- end-expected
SELECT i, f FROM zzt4a_q ORDER BY i;

DROP TABLE zzt4a_q;

-- ============================================================================
-- A bound the arithmetic reaches is still only a bound
--
-- An index WHERE i >= 1 is not reached by i > 0, however the 0 was written:
-- the proof is over the values the operators admit, and does not count the
-- integers between them.
-- ============================================================================
CREATE TABLE zzt4a_r (i int);
CREATE UNIQUE INDEX zzt4a_r_u ON zzt4a_r (i) WHERE i >= 1;
INSERT INTO zzt4a_r VALUES (1);

INSERT INTO zzt4a_r VALUES (1) ON CONFLICT (i) WHERE i >= 1 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_r VALUES (1) ON CONFLICT (i) WHERE i > 0 DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_r VALUES (1) ON CONFLICT (i) WHERE i > 1 - 1 DO NOTHING;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM zzt4a_r;

DROP TABLE zzt4a_r;

-- Nothing above was left behind.
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_class WHERE relname LIKE 'zzt4a\_%';
