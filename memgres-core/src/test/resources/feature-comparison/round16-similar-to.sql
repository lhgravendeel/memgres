-- ============================================================================
-- Feature Comparison: Round 16 — SIMILAR TO grammar + ESCAPE clause
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- ============================================================================
-- SECTION C1: ESCAPE clause
-- ============================================================================

-- 1. ESCAPE '!' — '!%' matches literal '%'
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT ('a%b' SIMILAR TO 'a!%b' ESCAPE '!') AS ok;

-- 2. Default ESCAPE is '\'
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT ('a%b' SIMILAR TO 'a\%b') AS ok;

-- 3. ESCAPE '' disables escape handling (backslash is literal)
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT ('a\b' SIMILAR TO 'a\b' ESCAPE '') AS ok;

-- ============================================================================
-- SECTION C2: Top-level alternation
-- ============================================================================

-- 4. 'abc' matches alternation branch
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT ('abc' SIMILAR TO 'a|abc|xyz') AS ok;

-- 5. 'q' does not match any branch
-- begin-expected
-- columns: ok
-- row: f
-- end-expected
SELECT ('q' SIMILAR TO 'a|abc|xyz') AS ok;

-- ============================================================================
-- SECTION C3: Repetition operators
-- ============================================================================

-- 6. + matches one-or-more
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT ('aaa' SIMILAR TO 'a+') AS ok;

-- 7. * matches zero-or-more (empty matches)
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT ('' SIMILAR TO 'a*') AS ok;

-- 8. + does not match empty
-- begin-expected
-- columns: ok
-- row: f
-- end-expected
SELECT ('' SIMILAR TO 'a+') AS ok;

-- ============================================================================
-- SECTION C4: Character classes
-- ============================================================================

-- 9. Bracket class
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT ('bat' SIMILAR TO '[bc]at') AS ok;

-- 10. POSIX [[:alpha:]]
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT ('x' SIMILAR TO '[[:alpha:]]') AS ok;

-- 11. [[:alpha:]] must not match digit
-- begin-expected
-- columns: ok
-- row: f
-- end-expected
SELECT ('1' SIMILAR TO '[[:alpha:]]') AS ok;

-- ============================================================================
-- SECTION C5: Grouping
-- ============================================================================

-- 12. Grouped alternation
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT ('abc' SIMILAR TO '(b|a)bc') AS ok;

-- ============================================================================
-- SECTION C6: SIMILAR TO is implicitly anchored at both ends
-- ============================================================================

-- 13. 'abcdef' is NOT similar to 'abc' (anchored)
-- begin-expected
-- columns: ok
-- row: f
-- end-expected
SELECT ('abcdef' SIMILAR TO 'abc') AS ok;

-- 14. Trailing % matches the rest
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT ('abcdef' SIMILAR TO 'abc%') AS ok;
