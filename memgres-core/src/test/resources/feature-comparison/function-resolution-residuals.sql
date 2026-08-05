-- ============================================================================
-- Feature Comparison: which function was written decides what the answer means
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- A tsquery literal names lexemes and nothing else: 'Cats'::tsquery is the
-- lexeme Cats, with no case folding, no stemming and no stop-word list, while
-- to_tsquery('Cats') runs the word through a dictionary and yields 'cat'.
-- Reading a literal through the dictionary turned 'a & b' into a query with a
-- hole where the stop word had been.
--
-- PostgreSQL declares four @@ operators and they disagree about bare strings.
-- text @@ tsquery runs the text through to_tsvector; text @@ text also runs the
-- right side through plainto_tsquery, so 'cat dog' there is two words to find
-- rather than a tsquery with a syntax error in it. An untyped literal takes
-- whichever type the resolution leaves it, which is why 'a b' @@ 'a'::tsquery
-- and the commuted form give different answers.
--
-- An empty query names nothing, so it matches nothing and folds away when
-- queries are combined.
-- ============================================================================

SET search_path = public;

-- ============================================================================
-- A tsquery literal is taken as written
-- ============================================================================

-- begin-expected
-- columns: q
-- row: 'a'
-- end-expected
SELECT 'a'::tsquery::text AS q;

-- begin-expected
-- columns: q
-- row: 'the'
-- end-expected
SELECT 'the'::tsquery::text AS q;

-- begin-expected
-- columns: q
-- row: 'cats'
-- end-expected
SELECT 'cats'::tsquery::text AS q;

-- begin-expected
-- columns: q
-- row: 'running'
-- end-expected
SELECT 'running'::tsquery::text AS q;

-- case is part of the lexeme, so it survives too
-- begin-expected
-- columns: q
-- row: 'Cats'
-- end-expected
SELECT 'Cats'::tsquery::text AS q;

-- begin-expected
-- columns: q
-- row: 'CATS' & 'Dogs'
-- end-expected
SELECT 'CATS & Dogs'::tsquery::text AS q;

-- the operators and modifiers are still read as operators and modifiers
-- begin-expected
-- columns: q
-- row: 'a' & 'b'
-- end-expected
SELECT 'a & b'::tsquery::text AS q;

-- begin-expected
-- columns: q
-- row: 'a' | 'b'
-- end-expected
SELECT 'a | b'::tsquery::text AS q;

-- begin-expected
-- columns: q
-- row: !'a'
-- end-expected
SELECT '!a'::tsquery::text AS q;

-- begin-expected
-- columns: q
-- row: 'a' <-> 'b'
-- end-expected
SELECT 'a <-> b'::tsquery::text AS q;

-- begin-expected
-- columns: q
-- row: 'a':*
-- end-expected
SELECT 'a:*'::tsquery::text AS q;

-- begin-expected
-- columns: q
-- row: 'a':A
-- end-expected
SELECT 'a:A'::tsquery::text AS q;

-- to_tsquery runs the word through its configuration instead
-- begin-expected
-- columns: q
-- row: 'cat'
-- end-expected
SELECT to_tsquery('Cats')::text AS q;

-- begin-expected
-- columns: q
-- row: 'run'
-- end-expected
SELECT to_tsquery('running')::text AS q;

-- begin-expected
-- columns: q
-- row: 'cats'
-- end-expected
SELECT to_tsquery('simple','Cats')::text AS q;

-- begin-expected
-- columns: q
-- row: 'a'
-- end-expected
SELECT to_tsquery('simple','a')::text AS q;

-- begin-expected
-- columns: q
-- row: 
-- end-expected
SELECT to_tsquery('a')::text AS q;

-- begin-expected
-- columns: q
-- row: 'cat'
-- end-expected
SELECT plainto_tsquery('Cats')::text AS q;

-- and the default configuration is english
-- begin-expected
-- columns: v
-- row: 'b':2
-- end-expected
SELECT to_tsvector('a b')::text AS v;

-- begin-expected
-- columns: v
-- row: 'cat':1
-- end-expected
SELECT to_tsvector('cats')::text AS v;

-- begin-expected
-- columns: v
-- row: 'a':1 'b':2
-- end-expected
SELECT to_tsvector('simple','a b')::text AS v;

-- ============================================================================
-- Four @@ operators, four ways to read a bare string
-- ============================================================================

-- begin-expected
-- columns: r
-- row: false
-- end-expected
SELECT ('a b'::text @@ 'a'::tsquery)::text AS r;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT ('a b'::text @@ 'b'::tsquery)::text AS r;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT ('cats'::text @@ 'cat'::tsquery)::text AS r;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT ('The Cats'::text @@ 'cat'::tsquery)::text AS r;

-- begin-expected
-- columns: r
-- row: false
-- end-expected
SELECT ('cats'::text @@ 'cats'::tsquery)::text AS r;

-- text on both sides searches for the words
-- begin-expected
-- columns: r
-- row: false
-- end-expected
SELECT ('cat sat'::text @@ 'cat dog'::text)::text AS r;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT ('cat dog'::text @@ 'cat dog'::text)::text AS r;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT ('cat sat'::text @@ 'cats'::text)::text AS r;

-- begin-expected
-- columns: r
-- row: false
-- end-expected
SELECT ('cat sat'::text @@ 'cat | zzz'::text)::text AS r;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT ('a b'::text @@ 'a & b'::text)::text AS r;

-- a vector on either side reads the other as a query
-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT ('a b'::tsvector @@ 'a')::text AS r;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT ('a b'::tsvector @@ 'a & b')::text AS r;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT ('cats'::tsvector @@ 'cats')::text AS r;

-- begin-expected
-- columns: r
-- row: false
-- end-expected
SELECT ('cats'::tsvector @@ 'cat')::text AS r;

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error in tsquery: "cat dog"
-- end-expected-error
SELECT 'a b'::tsvector @@ 'cat dog';

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT ('a'::tsquery @@ 'a b'::tsvector)::text AS r;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT ('a'::tsquery @@ to_tsvector('simple','a b'))::text AS r;

-- an untyped literal takes the type the operator resolution leaves it
-- begin-expected
-- columns: r
-- row: false
-- end-expected
SELECT ('a b' @@ 'a'::tsquery)::text AS r;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT ('a'::tsquery @@ 'a b')::text AS r;

-- begin-expected
-- columns: r
-- row: true
-- end-expected
SELECT ('a b' @@ 'b'::tsquery)::text AS r;

-- ============================================================================
-- An empty query names nothing
-- ============================================================================

-- begin-expected
-- columns: r
-- row: false
-- end-expected
SELECT (''::tsquery @@ 'a'::tsvector)::text AS r;

-- begin-expected
-- columns: r
-- row: false
-- end-expected
SELECT ('a'::tsvector @@ ''::tsquery)::text AS r;

-- begin-expected
-- columns: r
-- row: false
-- end-expected
SELECT (to_tsvector('b') @@ to_tsquery('a'))::text AS r;

-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT numnode(''::tsquery)::text AS n;

-- and folds away when queries are combined
-- begin-expected
-- columns: q
-- row: 'b'
-- end-expected
SELECT (to_tsquery('a') && to_tsquery('b'))::text AS q;

-- begin-expected
-- columns: q
-- row: 'b'
-- end-expected
SELECT (to_tsquery('a') || to_tsquery('b'))::text AS q;

-- begin-expected
-- columns: q
-- row: 'b'
-- end-expected
SELECT (to_tsquery('a') <-> to_tsquery('b'))::text AS q;

-- begin-expected
-- columns: q
-- row: 'cat'
-- end-expected
SELECT (to_tsquery('cat') <-> to_tsquery('a'))::text AS q;

-- begin-expected
-- columns: q
-- row: !'z'
-- end-expected
SELECT (to_tsquery('a') && !!to_tsquery('z'))::text AS q;

-- begin-expected
-- columns: q
-- row: 
-- end-expected
SELECT (to_tsquery('a') && to_tsquery('the'))::text AS q;

-- begin-expected
-- columns: q
-- row: 
-- end-expected
SELECT (!!to_tsquery('a'))::text AS q;

-- begin-expected
-- columns: q
-- row: 'cat' & 'dog'
-- end-expected
SELECT (to_tsquery('cat') && to_tsquery('dog'))::text AS q;

-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT numnode(to_tsquery('a') && to_tsquery('b'))::text AS n;

-- begin-expected
-- columns: n
-- row: 3
-- end-expected
SELECT numnode('a & b'::tsquery)::text AS n;

-- ============================================================================
-- The residuals around them
-- ============================================================================

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT btrim('abc', NULL) IS NULL AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT ltrim('abc', NULL) IS NULL AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT rtrim('abc', NULL) IS NULL AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT trim(both NULL from 'abc') IS NULL AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT trim(leading NULL from 'abc') IS NULL AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT trim(trailing NULL from 'abc') IS NULL AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT btrim(NULL, 'a') IS NULL AS r;

-- with characters to trim they still trim them
-- begin-expected
-- columns: s
-- row: abc
-- end-expected
SELECT btrim('xxabcxx', 'x') AS s;

-- begin-expected
-- columns: s
-- row: ab
-- end-expected
SELECT btrim('  ab  ') AS s;

-- begin-expected
-- columns: s
-- row: bcx
-- end-expected
SELECT ltrim('xxbcx', 'x') AS s;

-- begin-expected
-- columns: s
-- row: xxbc
-- end-expected
SELECT rtrim('xxbcxx', 'x') AS s;

-- to_ascii converts only from the single-byte encodings it can. Only ASCII
-- input is pinned here: a non-ASCII string reaches the server as UTF-8 bytes
-- reinterpreted in the named encoding, so what comes back depends on the
-- client's encoding rather than on either engine.
-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT to_ascii(NULL) IS NULL AS r;

-- begin-expected
-- columns: r
-- row: t
-- end-expected
SELECT to_ascii('abc', NULL) IS NULL AS r;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: encoding conversion from UTF8 to ASCII not supported
-- end-expected-error
SELECT to_ascii('abc');

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: encoding conversion from UTF8 to ASCII not supported
-- end-expected-error
SELECT to_ascii('abc', 'UTF8');

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: encoding conversion from UTF8 to ASCII not supported
-- end-expected-error
SELECT to_ascii('abc', 6);

-- begin-expected
-- columns: s
-- row: abc
-- end-expected
SELECT to_ascii('abc', 'LATIN1') AS s;

-- begin-expected
-- columns: s
-- row: abc
-- end-expected
SELECT to_ascii('abc', 'LATIN2') AS s;

-- begin-expected
-- columns: s
-- row: abc
-- end-expected
SELECT to_ascii('abc', 'LATIN9') AS s;

-- begin-expected
-- columns: s
-- row: abc
-- end-expected
SELECT to_ascii('abc', 'WIN1250') AS s;

-- begin-expected
-- columns: s
-- row: abc
-- end-expected
SELECT to_ascii('abc', 8) AS s;

-- begin-expected-error
-- sqlstate: 42704
-- message-like: NOSUCHENC is not a valid encoding name
-- end-expected-error
SELECT to_ascii('abc', 'NOSUCHENC');

-- begin-expected-error
-- sqlstate: 42704
-- message-like: 999 is not a valid encoding code
-- end-expected-error
SELECT to_ascii('abc', 999);

-- a range constructor refuses NULL flags, but a NULL bound means unbounded
-- begin-expected-error
-- sqlstate: 22000
-- message-like: range constructor flags argument must not be null
-- end-expected-error
SELECT int4range(1, 5, NULL);

-- begin-expected-error
-- sqlstate: 22000
-- message-like: range constructor flags argument must not be null
-- end-expected-error
SELECT numrange(1, 5, NULL);

-- begin-expected-error
-- sqlstate: 22000
-- message-like: range constructor flags argument must not be null
-- end-expected-error
SELECT daterange(DATE '2020-01-01', DATE '2020-02-01', NULL);

-- begin-expected
-- columns: r
-- row: [1,)
-- end-expected
SELECT int4range(1, NULL)::text AS r;

-- begin-expected
-- columns: r
-- row: (,5)
-- end-expected
SELECT int4range(NULL, 5)::text AS r;

-- begin-expected
-- columns: r
-- row: [1,6)
-- end-expected
SELECT int4range(1, 5, '[]')::text AS r;

-- ============================================================================
-- The pretty form of an index definition drops a schema the path reaches
-- ============================================================================

CREATE TABLE b8_t (a int, b int);

CREATE INDEX b8_i ON b8_t (b);

CREATE SCHEMA b8_s;

CREATE TABLE b8_s.t (a int);

CREATE INDEX b8_si ON b8_s.t (a);

-- begin-expected
-- columns: d
-- row: CREATE INDEX b8_i ON public.b8_t USING btree (b)
-- end-expected
SELECT pg_get_indexdef('b8_i'::regclass) AS d;

-- begin-expected
-- columns: d
-- row: CREATE INDEX b8_i ON public.b8_t USING btree (b)
-- end-expected
SELECT pg_get_indexdef('b8_i'::regclass, 0, false) AS d;

-- begin-expected
-- columns: d
-- row: CREATE INDEX b8_i ON b8_t USING btree (b)
-- end-expected
SELECT pg_get_indexdef('b8_i'::regclass, 0, true) AS d;

-- begin-expected
-- columns: d
-- row: CREATE INDEX b8_si ON b8_s.t USING btree (a)
-- end-expected
SELECT pg_get_indexdef('b8_s.b8_si'::regclass, 0, true) AS d;

-- begin-expected
-- columns: d
-- row: b
-- end-expected
SELECT pg_get_indexdef('b8_i'::regclass, 1, true) AS d;

DROP TABLE b8_s.t;

DROP SCHEMA b8_s;

DROP TABLE b8_t;

