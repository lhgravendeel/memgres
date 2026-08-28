-- ============================================================================
-- -- A document and a query are made of the same lexemes, or they never match.
-- --
-- -- Both sides go through the configuration's own parser: an e-mail address is one lexeme, a
-- -- decimal is one lexeme, and a word outside ASCII is a word. A tsvector's text form is the
-- -- value's own -- the same lexeme written twice is one lexeme holding both position lists, a
-- -- quote inside one is doubled so it can be read back, and the lexemes are ordered as bytes.
-- -- A phrase is about where the lexemes sit, so an operand that is not a bare lexeme is still
-- -- answered by position; and a stop word between two lexemes leaves them further apart rather
-- -- than next to each other.
--
-- ============================================================================

-- ============================================================================
-- 1. The text form is the value's own, and reads back as the value
-- ============================================================================
-- begin-expected
-- columns: a
-- row: 'a':1,2
-- end-expected
SELECT 'a:1 a:2'::tsvector::text AS a;
-- begin-expected
-- columns: a
-- row: 'a':1,2,3
-- end-expected
SELECT 'a:3,1,2'::tsvector::text AS a;
-- begin-expected
-- columns: a
-- row: 'a':1
-- end-expected
SELECT 'a:1,1'::tsvector::text AS a;
-- begin-expected
-- columns: a
-- row: 'a':1A
-- end-expected
SELECT 'a:1A,1'::tsvector::text AS a;
-- begin-expected
-- columns: a
-- row: 'a':1A
-- end-expected
SELECT 'a:1,1A'::tsvector::text AS a;
-- begin-expected
-- columns: a
-- row: 'it''s'
-- end-expected
SELECT '''it''''s'''::tsvector::text AS a;
-- begin-expected
-- columns: a
-- row: 'a' 'b'
-- end-expected
SELECT 'a b a'::tsvector::text AS a;
-- begin-expected
-- columns: a
-- row: 'a':1
-- end-expected
SELECT 'a:1 a'::tsvector::text AS a;
-- begin-expected
-- columns: a
-- row: ':1'
-- end-expected
SELECT ':1'::tsvector::text AS a;
-- begin-expected
-- columns: a
-- row: 'a b'
-- end-expected
SELECT 'a\ b'::tsvector::text AS a;
-- begin-expected
-- columns: a
-- row: 'ab'
-- end-expected
SELECT E'a\\b'::tsvector::text AS a;
-- begin-expected
-- columns: a
-- row: 'a':16383
-- end-expected
SELECT 'a:16384'::tsvector::text AS a;

-- expected-divergence: the documented rule is that a position past the last representable one
-- is brought back to it, and that is what this answers. PostgreSQL reads the digits into a
-- fixed-width accumulator first, so a spelling that happens to wrap to zero -- 2147483648 and
-- 4294967296 are the two -- comes out as no position at all rather than as the largest one.
-- Which spellings those are is a property of the accumulator and not of the document.
SELECT 'a:2147483648'::tsvector::text AS a;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: wrong position info in tsvector: "a:0"
-- end-expected-error
SELECT 'a:0'::tsvector::text AS a;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error in tsvector: "a:"
-- end-expected-error
SELECT 'a:'::tsvector::text AS a;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error in tsvector: "a:1AB"
-- end-expected-error
SELECT 'a:1AB'::tsvector::text AS a;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error in tsvector: "''"
-- end-expected-error
SELECT ''''''::tsvector::text AS a;
-- begin-expected
-- columns: a
-- row: 'a' 'b'
-- end-expected
SELECT setweight('a b'::tsvector, 'A')::text AS a;
-- begin-expected
-- columns: a
-- row: 'a'
-- end-expected
SELECT setweight(strip('a:1'::tsvector), 'A')::text AS a;
-- expected-divergence: a weight outside the four is refused either way, and how it is reported
-- moved between PostgreSQL 18.0 and the minors after it: the older build raised an internal
-- error naming the character's code, and the newer one raises 22023 naming the character, the
-- way every other weight-taking function always has. This engine answers as the newer build
-- does, so what is written here would pin one PostgreSQL build's answer and not the other's.
SELECT setweight('a:1'::tsvector, '')::text AS a;

-- expected-divergence: the same, for a weight that is a letter outside A to D.
SELECT setweight('a:1'::tsvector, 'X')::text AS a;
-- begin-expected
-- columns: a
-- row: 'a':16383
-- end-expected
SELECT ('a:16383'::tsvector || 'a:1'::tsvector)::text AS a;
-- begin-expected-error
-- sqlstate: 2200F
-- message-like: lexeme array may not contain empty strings
-- end-expected-error
SELECT array_to_tsvector(ARRAY['']::text[])::text AS a;
-- begin-expected-error
-- sqlstate: 22004
-- message-like: lexeme array may not contain nulls
-- end-expected-error
SELECT array_to_tsvector(ARRAY[NULL]::text[])::text AS a;
-- begin-expected
-- columns: a
-- row: 'é' 'Ａ' '😀'
-- end-expected
SELECT array_to_tsvector(ARRAY[chr(65313), chr(128512), chr(233)])::text AS a;
-- begin-expected
-- columns: a
-- row: false
-- end-expected
SELECT (array_to_tsvector(ARRAY[chr(128512)]) < array_to_tsvector(ARRAY[chr(57344)]))::text AS a;
-- begin-expected
-- columns: a
-- row: -1
-- end-expected
SELECT tsvector_cmp('a'::tsvector,'b'::tsvector) AS a;

-- ============================================================================
-- 2. A query is read as a query, and a '<' with no '>' is not one
-- ============================================================================
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error in tsquery: "a<b"
-- end-expected-error
SELECT 'a<b'::tsquery::text AS a;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error in tsquery: "a <2 b"
-- end-expected-error
SELECT 'a <2 b'::tsquery::text AS a;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error in tsquery: "a<"
-- end-expected-error
SELECT 'a<'::tsquery::text AS a;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error in tsquery: "<->"
-- end-expected-error
SELECT '<->'::tsquery::text AS a;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: no operand in tsquery: "a &"
-- end-expected-error
SELECT 'a &'::tsquery::text AS a;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error in tsquery: "& a"
-- end-expected-error
SELECT '& a'::tsquery::text AS a;
-- begin-expected-error
-- sqlstate: 22023
-- message-like: distance in phrase operator must be an integer value between zero and 16384 inclusive
-- end-expected-error
SELECT 'a <16385> b'::tsquery::text AS a;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error in tsquery: "a <a> b"
-- end-expected-error
SELECT 'a <a> b'::tsquery::text AS a;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error in tsquery: "a:E"
-- end-expected-error
SELECT 'a:E'::tsquery::text AS a;
-- begin-expected
-- columns: a
-- row: 'a':*A
-- end-expected
SELECT 'a:A*'::tsquery::text AS a;
-- begin-expected
-- columns: a
-- row: 'a<b'
-- end-expected
SELECT '''a<b'''::tsquery::text AS a;
-- begin-expected
-- columns: a
-- row: 'a' <-> ( 'b' <-> 'c' )
-- end-expected
SELECT 'a <-> (b <-> c)'::tsquery::text AS a;
-- begin-expected
-- columns: a
-- row: 'a' <-> ( 'b' <-> 'c' )
-- end-expected
SELECT ('a <-> (b <-> c)'::tsquery::text)::tsquery::text AS a;
-- begin-expected
-- columns: a
-- row: false
-- end-expected
SELECT ('a <-> (b <-> c)'::tsquery = 'a <-> b <-> c'::tsquery)::text AS a;
-- begin-expected
-- columns: a
-- row: 'a' <2> ( 'b' <-> 'c' )
-- end-expected
SELECT 'a <2> (b <-> c)'::tsquery::text AS a;
-- begin-expected
-- columns: a
-- row: 'a' & 'b'
-- end-expected
SELECT querytree('a & b'::tsquery) AS a;
-- begin-expected
-- columns: a
-- row: 'a' <-> 'b'
-- end-expected
SELECT querytree('a <-> b'::tsquery) AS a;
-- begin-expected
-- columns: a
-- row: 'a' & ( 'b' | 'c' )
-- end-expected
SELECT querytree('a & (b | c)'::tsquery) AS a;
-- begin-expected
-- columns: a
-- row: T
-- end-expected
SELECT querytree('!a'::tsquery) AS a;
-- begin-expected
-- columns: a
-- row: 3
-- end-expected
SELECT numnode('a & b'::tsquery) AS a;

-- ============================================================================
-- 3. Matching is by position and by weight
-- ============================================================================
-- begin-expected
-- columns: a
-- row: false
-- end-expected
SELECT (to_tsvector('simple','a c b') @@ to_tsquery('simple','a <-> (b & c)'))::text AS a;
-- begin-expected
-- columns: a
-- row: false
-- end-expected
SELECT (to_tsvector('simple','a b c') @@ to_tsquery('simple','a <-> (b & c)'))::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('cat dog'::tsvector @@ 'cat:A'::tsquery)::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT (strip('a:1A'::tsvector) @@ 'a:A'::tsquery)::text AS a;
-- begin-expected
-- columns: a
-- row: false
-- end-expected
SELECT ('a:1B'::tsvector @@ 'a:A'::tsquery)::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('a'::tsquery @> 'a'::tsquery)::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('a & b'::tsquery @> 'a'::tsquery)::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('a'::tsquery <@ 'a & b'::tsquery)::text AS a;
-- begin-expected
-- columns: a
-- row: false
-- end-expected
SELECT ('a'::tsquery @> 'a & b'::tsquery)::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ('cat'::tsvector @@@ 'cat'::tsquery)::text AS a;
-- begin-expected
-- columns: a
-- row: true
-- end-expected
SELECT ts_match_vq('a'::tsvector, 'a'::tsquery)::text AS a;
-- begin-expected
-- columns: a
-- row: !'a'
-- end-expected
SELECT tsquery_not('a'::tsquery)::text AS a;
-- begin-expected
-- columns: a
-- row: 'cat' <2> 'dog'
-- end-expected
SELECT to_tsquery('english', 'the <-> cat <-> the <-> dog')::text AS a;
-- begin-expected
-- columns: a
-- row: 'cat' <4> 'dog'
-- end-expected
SELECT to_tsquery('english', 'cat <2> the <2> dog')::text AS a;

-- ============================================================================
-- 4. Ranking counts the lexemes the query names, NOT branches included
-- ============================================================================
-- begin-expected
-- columns: a
-- row: 1e-20
-- end-expected
SELECT ts_rank('a:1'::tsvector, 'a & !b'::tsquery)::text AS a;
-- begin-expected
-- columns: a
-- row: 0.09910322
-- end-expected
SELECT ts_rank('a:1 b:2'::tsvector, 'a & !b'::tsquery)::text AS a;
-- begin-expected
-- columns: a
-- row: 0.06079271
-- end-expected
SELECT ts_rank(to_tsvector('simple','a b c d e'), to_tsquery('simple','!a'))::text AS a;
-- begin-expected
-- columns: a
-- row: 1e-16
-- end-expected
SELECT ts_rank(strip('a:1 b:2'::tsvector), 'a & b'::tsquery)::text AS a;
-- begin-expected
-- columns: a
-- row: 0.2
-- end-expected
SELECT ts_rank_cd('a:1,10,20 b:2,11'::tsvector, 'a <-> b'::tsquery, 0)::text AS a;
-- begin-expected
-- columns: a
-- row: 0
-- end-expected
SELECT ts_rank_cd('a:1,10,20 b:2,11'::tsvector, 'a <2> b'::tsquery, 0)::text AS a;
-- begin-expected
-- columns: a
-- row: 'cat':1 'dog':3
-- end-expected
SELECT ts_delete(to_tsvector('english','cats and dogs'), 'cats')::text AS a;

-- expected-divergence: what a rewrite produces is the set of lexemes and the operators over
-- them, and both engines agree on those. The order the operands of a commutative operator are
-- printed in is not: PostgreSQL rebuilds the query from its own internal form and sorts each
-- node's children by a hash of the lexeme, so 'a' & 'b' comes back 'b' & 'a' whether or not
-- anything was rewritten.
SELECT ts_rewrite('a & b'::tsquery, 'a'::tsquery, 'z'::tsquery)::text AS a;

-- ============================================================================
-- 5. The query builders read the same text the document side reads
-- ============================================================================
-- begin-expected
-- columns: a
-- row: 'café'
-- end-expected
SELECT plainto_tsquery('english', 'café')::text AS a;
-- begin-expected
-- columns: a
-- row: 'café':1
-- end-expected
SELECT to_tsvector('english','café')::text AS a;
-- begin-expected
-- columns: a
-- row: 'foo@bar.com'
-- end-expected
SELECT plainto_tsquery('english', 'foo@bar.com')::text AS a;
-- begin-expected
-- columns: a
-- row: 'a.b.c'
-- end-expected
SELECT plainto_tsquery('english', 'a.b.c')::text AS a;
-- begin-expected
-- columns: a
-- row: 'well-known' <-> 'well' <-> 'known' <-> 'thing'
-- end-expected
SELECT phraseto_tsquery('english', 'well-known thing')::text AS a;
-- begin-expected
-- columns: a
-- row: 'foo@bar.com'
-- end-expected
SELECT websearch_to_tsquery('english', 'foo@bar.com')::text AS a;
-- begin-expected
-- columns: a
-- row: 'b' | 'c' & 'd'
-- end-expected
SELECT websearch_to_tsquery('english', 'a b OR c d')::text AS a;
-- begin-expected
-- columns: a
-- row: 'x' | 'y'
-- end-expected
SELECT websearch_to_tsquery('english', 'x OR OR y')::text AS a;
-- begin-expected
-- columns: a
-- row: !'cat'
-- end-expected
SELECT websearch_to_tsquery('english', '- cat')::text AS a;
-- begin-expected
-- columns: a
-- row: !!'cat'
-- end-expected
SELECT websearch_to_tsquery('english', '--cat')::text AS a;
-- begin-expected
-- columns: a
-- row: 'cat' | 'dog'
-- end-expected
SELECT websearch_to_tsquery('english', 'cat or dog')::text AS a;

-- ============================================================================
-- 6. The configuration and the dictionary are the ones that were named
-- ============================================================================
-- begin-expected
-- columns: a
-- row: {cats}
-- end-expected
SELECT ts_lexize('simple', 'Cats')::text AS a;
-- begin-expected
-- columns: a
-- row: {}
-- end-expected
SELECT ts_lexize('english_stem', 'the')::text AS a;
-- begin-expected
-- columns: a
-- row: {cat}
-- end-expected
SELECT ts_lexize('english_stem', 'cats')::text AS a;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: text search dictionary "no_such_dict" does not exist
-- end-expected-error
SELECT ts_lexize('no_such_dict', 'x')::text AS a;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: text search configuration "nonexistent_cfg" does not exist
-- end-expected-error
SELECT plainto_tsquery('nonexistent_cfg', 'hello')::text AS a;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: text search configuration "nonexistent_cfg" does not exist
-- end-expected-error
SELECT ts_headline('nonexistent_cfg', 'hello', 'x'::tsquery) AS a;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: text search parser "no_such_parser" does not exist
-- end-expected-error
SELECT count(*) FROM ts_parse('no_such_parser', 'a b');
-- begin-expected-error
-- sqlstate: 22023
-- message-like: ts_stat query must return one tsvector column
-- end-expected-error
SELECT count(*) FROM ts_stat('SELECT 1');
-- begin-expected
-- columns: a
-- row: <b>Cats</b> and Dogs
-- end-expected
SELECT ts_headline('simple', 'Cats and Dogs', to_tsquery('simple','cats')) AS a;
-- begin-expected
-- columns: a
-- row: <b>supernovae</b> stars
-- end-expected
SELECT ts_headline('english', 'supernovae stars', to_tsquery('english','sup:*')) AS a;
-- begin-expected
-- columns: a
-- row: The cat. The <b>dog</b>. The bird.
-- end-expected
SELECT ts_headline('english', 'The cat. The dog. The bird.', to_tsquery('english','dog')) AS a;
-- begin-expected
-- columns: a
-- row: 32
-- end-expected
SELECT length(ts_headline('english', 'Multi  spaced    text fox', to_tsquery('english','fox'))) AS a;
-- begin-expected
-- columns: a
-- row: line1 <b>fox</b><NL>line2
-- end-expected
SELECT replace(ts_headline('english', E'line1 fox\nline2', to_tsquery('english','fox')), E'\n', '<NL>') AS a;
-- begin-expected
-- columns: a
-- row: <b>six</b> seven
-- end-expected
SELECT ts_headline('english','one two three four five six seven eight nine ten eleven twelve', to_tsquery('english','six'),'MaxWords=5, MinWords=2') AS a;
-- begin-expected
-- columns: a
-- row: quick brown <b>fox</b>
-- end-expected
SELECT ts_headline('english','the quick brown fox',to_tsquery('english','fox'),'MaxFragments=1') AS a;
-- begin-expected
-- columns: a
-- row: the big <b>cat</b> sat
-- end-expected
SELECT ts_headline('english','the big<br>cat sat', to_tsquery('cat')) AS a;

-- ============================================================================
-- 7. A text search value is of a text search type
-- ============================================================================
-- begin-expected
-- columns: a
-- row: tsvector
-- end-expected
SELECT pg_typeof(to_tsvector('english','a'))::text AS a;
-- begin-expected
-- columns: a
-- row: tsquery
-- end-expected
SELECT pg_typeof(to_tsquery('english','a'))::text AS a;
-- begin-expected
-- columns: a
-- row: tsvector
-- end-expected
SELECT pg_typeof(strip('a'::tsvector))::text AS a;
-- begin-expected
-- columns: a
-- row: tsquery
-- end-expected
SELECT pg_typeof(plainto_tsquery('english','a'))::text AS a;
-- begin-expected
-- columns: a
-- row: tsvector
-- end-expected
SELECT pg_typeof(setweight('a:1'::tsvector,'A'))::text AS a;
-- begin-expected
-- columns: a
-- row: regconfig
-- end-expected
SELECT pg_typeof(get_current_ts_config())::text AS a;
-- begin-expected
-- columns: a
-- row: 'i':1
-- end-expected
SELECT to_tsvector('simple', U&'\0130')::text AS a;
-- begin-expected
-- columns: a
-- row: {i}
-- end-expected
SELECT ts_lexize('simple', U&'\0130')::text AS a;
