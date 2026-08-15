-- ============================================================================
-- The order a text bound in an arbiter predicate is read in
--
-- A predicate beside a conflict target reaches a partial index only when the
-- index's predicate follows from it, and for a bound over text that question
-- is the collation's to answer, not the codepoints'. Under the reference
-- server's collation a lowercase letter sorts before its uppercase spelling
-- and both sort before the next letter, so 'a' < 'A' < 'b' < 'B'; the
-- codepoints put every uppercase letter first and would answer the other way
-- round.
--
-- So an index WHERE s > 'a' is reached by s > 'A', which the codepoints call a
-- weaker bound, and is not reached by s >= 'a', which they call a stronger one.
-- The proof has to be the collation's the whole way, in the accepting
-- direction and in the refusing one alike -- accepting a bound that admits
-- rows the index does not hold would take the wrong index.
--
-- Every value below was read off PostgreSQL 18 before it was written down.
-- ============================================================================

-- ============================================================================
-- Above a lower bound: the letters the collation puts after 'a'
-- ============================================================================
CREATE TABLE zzt4a_g (i int, s text);
CREATE UNIQUE INDEX zzt4a_g_u ON zzt4a_g (i) WHERE s > 'a';
INSERT INTO zzt4a_g VALUES (1, 'x');

-- 'A' comes after 'a' in the collation, so this bound rules out everything the
-- index rules out. The codepoints say the opposite.
INSERT INTO zzt4a_g VALUES (1, 'y') ON CONFLICT (i) WHERE s > 'A' DO NOTHING;
INSERT INTO zzt4a_g VALUES (1, 'y') ON CONFLICT (i) WHERE s > 'B' DO NOTHING;
INSERT INTO zzt4a_g VALUES (1, 'y') ON CONFLICT (i) WHERE s > 'Z' DO NOTHING;
INSERT INTO zzt4a_g VALUES (1, 'y') ON CONFLICT (i) WHERE s >= 'B' DO NOTHING;
INSERT INTO zzt4a_g VALUES (1, 'y') ON CONFLICT (i) WHERE s > 'b' DO NOTHING;
INSERT INTO zzt4a_g VALUES (1, 'y') ON CONFLICT (i) WHERE s > 'z' DO NOTHING;
INSERT INTO zzt4a_g VALUES (1, 'y') ON CONFLICT (i) WHERE s > 'aa' DO NOTHING;
INSERT INTO zzt4a_g VALUES (1, 'y') ON CONFLICT (i) WHERE s >= 'aa' DO NOTHING;
INSERT INTO zzt4a_g VALUES (1, 'y') ON CONFLICT (i) WHERE s > 'A0' DO NOTHING;

-- An equality is a bound too: the one value it admits has to be above 'a'.
INSERT INTO zzt4a_g VALUES (1, 'y') ON CONFLICT (i) WHERE s = 'A' DO NOTHING;
INSERT INTO zzt4a_g VALUES (1, 'y') ON CONFLICT (i) WHERE s = 'zz' DO NOTHING;

-- And so are the forms a bound can be written in.
INSERT INTO zzt4a_g VALUES (1, 'y') ON CONFLICT (i) WHERE s BETWEEN 'B' AND 'z' DO NOTHING;
INSERT INTO zzt4a_g VALUES (1, 'y') ON CONFLICT (i) WHERE s IN ('A', 'B') DO NOTHING;

-- The empty string is below every other, so this bound admits rows the index
-- does not hold.
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_g VALUES (1, 'y') ON CONFLICT (i) WHERE s > '' DO NOTHING;

-- The same bound made inclusive admits 'a' itself, which the index excludes.
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_g VALUES (1, 'y') ON CONFLICT (i) WHERE s >= 'a' DO NOTHING;

-- A bound from the other side says nothing about the one the index holds.
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_g VALUES (1, 'y') ON CONFLICT (i) WHERE s < 'b' DO NOTHING;

-- begin-expected
-- columns: i | s
-- row: 1 | x
-- end-expected
SELECT i, s FROM zzt4a_g ORDER BY i;

DROP TABLE zzt4a_g;

-- ============================================================================
-- Above an uppercase lower bound, where the codepoints would over-accept
--
-- 'B' sorts after 'a', 'A' and 'b', so of those four spellings only a letter
-- past 'B' proves the index's bound. The codepoints, which put 'B' below all
-- three, would accept every one of them.
-- ============================================================================
CREATE TABLE zzt4a_h (i int, s text);
CREATE UNIQUE INDEX zzt4a_h_u ON zzt4a_h (i) WHERE s > 'B';
INSERT INTO zzt4a_h VALUES (1, 'x');

INSERT INTO zzt4a_h VALUES (1, 'y') ON CONFLICT (i) WHERE s > 'c' DO NOTHING;
INSERT INTO zzt4a_h VALUES (1, 'y') ON CONFLICT (i) WHERE s > 'C' DO NOTHING;
INSERT INTO zzt4a_h VALUES (1, 'y') ON CONFLICT (i) WHERE s = 'c' DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_h VALUES (1, 'y') ON CONFLICT (i) WHERE s > 'a' DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_h VALUES (1, 'y') ON CONFLICT (i) WHERE s > 'A' DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_h VALUES (1, 'y') ON CONFLICT (i) WHERE s > 'b' DO NOTHING;

-- begin-expected
-- columns: i | s
-- row: 1 | x
-- end-expected
SELECT i, s FROM zzt4a_h ORDER BY i;

DROP TABLE zzt4a_h;

-- ============================================================================
-- Below an upper bound, where the same ordering runs the other way
--
-- 'A' and 'B' both sort above 'a', so a bound below either of them admits
-- rows the index WHERE s < 'a' does not hold, and only the index's own bound
-- proves it.
-- ============================================================================
CREATE TABLE zzt4a_i (i int, s text);
CREATE UNIQUE INDEX zzt4a_i_u ON zzt4a_i (i) WHERE s < 'a';
INSERT INTO zzt4a_i VALUES (1, 'x');

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_i VALUES (2, 'y') ON CONFLICT (i) WHERE s < 'A' DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_i VALUES (2, 'y') ON CONFLICT (i) WHERE s < 'B' DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_i VALUES (2, 'y') ON CONFLICT (i) WHERE s <= 'a' DO NOTHING;

-- The index's own bound reaches it, and 'x' is not a row the index holds, so
-- there is nothing to conflict with and the row goes in.
INSERT INTO zzt4a_i VALUES (2, 'y') ON CONFLICT (i) WHERE s < 'a' DO NOTHING;

-- begin-expected
-- columns: i | s
-- row: 1 | x
-- row: 2 | y
-- end-expected
SELECT i, s FROM zzt4a_i ORDER BY i;

DROP TABLE zzt4a_i;

-- ============================================================================
-- Two spellings of one letter are two values
--
-- The collation calls 'a' and 'A' different values, so an equality on one is
-- no equality on the other.
-- ============================================================================
CREATE TABLE zzt4a_j (i int, s text);
CREATE UNIQUE INDEX zzt4a_j_u ON zzt4a_j (i) WHERE s = 'a';
INSERT INTO zzt4a_j VALUES (1, 'x');

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_j VALUES (2, 'y') ON CONFLICT (i) WHERE s = 'A' DO NOTHING;

INSERT INTO zzt4a_j VALUES (2, 'y') ON CONFLICT (i) WHERE s = 'a' DO NOTHING;

-- begin-expected
-- columns: i | s
-- row: 1 | x
-- row: 2 | y
-- end-expected
SELECT i, s FROM zzt4a_j ORDER BY i;

DROP TABLE zzt4a_j;

-- ============================================================================
-- Where the two spellings only decide a tie
--
-- The collation compares the letters first and the case of them last, so
-- 'a1' sorts below 'A1' but 'a2' sorts above it. A bound at 'a1' therefore
-- admits 'A1', which an index WHERE s > 'A1' does not hold, and a bound at
-- 'a2' does not.
-- ============================================================================
CREATE TABLE zzt4a_k (i int, s text);
CREATE UNIQUE INDEX zzt4a_k_u ON zzt4a_k (i) WHERE s > 'A1';
INSERT INTO zzt4a_k VALUES (1, 'x');

INSERT INTO zzt4a_k VALUES (1, 'y') ON CONFLICT (i) WHERE s > 'a2' DO NOTHING;
INSERT INTO zzt4a_k VALUES (1, 'y') ON CONFLICT (i) WHERE s > 'B0' DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_k VALUES (1, 'y') ON CONFLICT (i) WHERE s > 'a1' DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_k VALUES (1, 'y') ON CONFLICT (i) WHERE s > 'a0' DO NOTHING;

-- begin-expected
-- columns: i | s
-- row: 1 | x
-- end-expected
SELECT i, s FROM zzt4a_k ORDER BY i;

DROP TABLE zzt4a_k;

-- ============================================================================
-- Digits are not letters
--
-- Every digit sorts below every letter, so no bound at a digit proves a bound
-- at a letter -- here the codepoints happen to agree, and the answer is the
-- same either way.
-- ============================================================================
CREATE TABLE zzt4a_l (i int, s text);
CREATE UNIQUE INDEX zzt4a_l_u ON zzt4a_l (i) WHERE s > 'b';
INSERT INTO zzt4a_l VALUES (1, 'x');

INSERT INTO zzt4a_l VALUES (1, 'y') ON CONFLICT (i) WHERE s > 'e' DO NOTHING;
INSERT INTO zzt4a_l VALUES (1, 'y') ON CONFLICT (i) WHERE s > 'E' DO NOTHING;
INSERT INTO zzt4a_l VALUES (1, 'y') ON CONFLICT (i) WHERE s > 'Z' DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_l VALUES (1, 'y') ON CONFLICT (i) WHERE s > '0' DO NOTHING;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_l VALUES (1, 'y') ON CONFLICT (i) WHERE s > '9' DO NOTHING;

-- begin-expected
-- columns: i | s
-- row: 1 | x
-- end-expected
SELECT i, s FROM zzt4a_l ORDER BY i;

DROP TABLE zzt4a_l;

-- ============================================================================
-- An inclusive bound is reached by an inclusive bound at the same place
-- ============================================================================
CREATE TABLE zzt4a_m (i int, s text);
CREATE UNIQUE INDEX zzt4a_m_u ON zzt4a_m (i) WHERE s >= 'b';
INSERT INTO zzt4a_m VALUES (1, 'x');

INSERT INTO zzt4a_m VALUES (1, 'y') ON CONFLICT (i) WHERE s >= 'b' DO NOTHING;
INSERT INTO zzt4a_m VALUES (1, 'y') ON CONFLICT (i) WHERE s > 'b' DO NOTHING;
INSERT INTO zzt4a_m VALUES (1, 'y') ON CONFLICT (i) WHERE s > 'B' DO NOTHING;
INSERT INTO zzt4a_m VALUES (1, 'y') ON CONFLICT (i) WHERE s >= 'B' DO NOTHING;
INSERT INTO zzt4a_m VALUES (1, 'y') ON CONFLICT (i) WHERE s = 'B' DO NOTHING;

-- 'a' sorts below 'b', so this bound admits rows the index does not hold.
-- begin-expected-error
-- sqlstate: 42P10
-- message-like: there is no unique or exclusion constraint matching the ON CONFLICT specification
-- end-expected-error
INSERT INTO zzt4a_m VALUES (1, 'y') ON CONFLICT (i) WHERE s >= 'a' DO NOTHING;

-- begin-expected
-- columns: i | s
-- row: 1 | x
-- end-expected
SELECT i, s FROM zzt4a_m ORDER BY i;

DROP TABLE zzt4a_m;

-- Nothing above was left behind.
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_class WHERE relname LIKE 'zzt4a\_%';
