-- ============================================================
-- 13: Regex fidelity and string function fixes
-- Tests for H25, H26, M23
-- ============================================================

-- === H25: POSIX regex ===

-- POSIX bracket classes
SELECT 'abc123' ~ '[[:digit:]]{3}';
-- expect: true

SELECT 'hello' ~ '[[:alpha:]]+';
-- expect: true

-- Word boundaries
SELECT 'hello world' ~ '\mworld';
-- expect: true

SELECT 'helloworld' ~ '\mworld';
-- expect: false

-- $ does NOT match before trailing newline (PG behavior)
SELECT E'abc\n' ~ 'c$';
-- expect: false

-- regexp_like with flags
SELECT regexp_like('ABC', 'abc', 'i');
-- expect: true

-- === H26: regexp_* functions ===

-- regexp_replace with start position (first match only by default)
SELECT regexp_replace('banana', 'a', 'X', 3);
-- expect: banXna

-- regexp_replace start=0 error
-- SELECT regexp_replace('banana', 'a', 'X', 0);
-- expect: ERROR 22023

-- \& whole match in replacement
SELECT regexp_replace('hello', '(ll)', '[\&]');
-- expect: he[ll]o

-- substring with regex returns group 1
SELECT substring('foobar' from 'o(.)b');
-- expect: o

-- SIMILAR TO bounded quantifiers
SELECT 'aab' SIMILAR TO 'a{2}%';
-- expect: true

SELECT 'ab' SIMILAR TO 'a{2}%';
-- expect: false

-- === M23: format/quoting/string one-offs ===

-- format with width
SELECT format('%10s', 'hello');
-- expect:      hello

SELECT format('%-10s|', 'hello');
-- expect: hello     |

-- format missing arg error
-- SELECT format('%s %s', 'only_one');
-- expect: ERROR 22023

-- quote_literal with backslash
SELECT quote_literal(E'a\\b');
-- expect: E'a\\b'

-- concat(true) prints "t"
SELECT concat(true);
-- expect: t

SELECT concat(false);
-- expect: f

-- concat_ws with zero value args
-- SELECT concat_ws(',');
-- expect: ERROR 42883

-- split_part empty delimiter
SELECT split_part('abc', '', 1);
-- expect: abc

-- lpad negative length
SELECT lpad('hi', -1);
-- expect: (empty string)

-- unistr 6-digit
SELECT unistr('\+01F600');
-- expect: (grinning face emoji U+1F600)

-- unistr 4-digit
SELECT unistr('\00E9');
-- expect: e with accent
