-- ============================================================================
-- Feature Comparison: full-text search exactness
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- to_tsvector('english', ...) only matches PG when all three of its pieces do:
-- the default parser's token types (which keep emails and URLs whole and split
-- hyphenated compounds), PG's exact 127-word english.stop list, and the bundled
-- Snowball (Porter2) stemmer. Ranks additionally need PG's cover-density
-- algorithm and its float4 output function.
-- ============================================================================

-- ============================================================================
-- 1. Stemming and stop words
-- ============================================================================

-- begin-expected
-- columns: v
-- row: 'brown':3 'dog':9 'fox':4 'jump':5 'lazili':7 'quick':2 'run':8
-- end-expected
SELECT to_tsvector('english', 'The Quick brown foxes jumped over lazily running dogs')::text AS v;

-- begin-expected
-- columns: v
-- row: 'easili':4 'fair':5 'general':7 'happi':6 'ran':3 'run':1,2
-- end-expected
SELECT to_tsvector('english', 'running runs ran easily fairly happiness generalization')::text AS v;

-- Snowball leaves the double in a three-letter stem alone
-- begin-expected
-- columns: v
-- row: 'add':1,2 'bed':6 'egg':3 'hop':5 'pad':4
-- end-expected
SELECT to_tsvector('english', 'added adding egged padded hopped bedded')::text AS v;

-- Stop words take a position but leave no lexeme
-- begin-expected
-- columns: v
-- row: 'cat':2 'mat':5
-- end-expected
SELECT to_tsvector('english', 'the cat on the mat')::text AS v;

-- Words the stock list does not contain
-- begin-expected
-- columns: v
-- row: 'could':2 'may':3 'shall':4 'would':1
-- end-expected
SELECT to_tsvector('english', 'would could may shall')::text AS v;

-- ============================================================================
-- 2. Token typing: emails, hosts, paths and numbers
-- ============================================================================

-- begin-expected
-- columns: v
-- row: 'contact':1 'john.doe@example.com':2
-- end-expected
SELECT to_tsvector('english', 'Contact john.doe@example.com')::text AS v;

-- begin-expected
-- columns: v
-- row: '/path?x=1':4 'visit':1 'www.example.com':3 'www.example.com/path?x=1':2
-- end-expected
SELECT to_tsvector('english', 'visit https://www.example.com/path?x=1')::text AS v;

-- A host needs a dot, so a bare name after a protocol is just a word
-- begin-expected
-- columns: v
-- row: 'localhost':2 'visit':1
-- end-expected
SELECT to_tsvector('english', 'visit localhost')::text AS v;

-- Underscore is not a word character
-- begin-expected
-- columns: v
-- row: 'bar':2 'baz':3 'foo':1 'qux':4
-- end-expected
SELECT to_tsvector('english', 'foo_bar baz_qux')::text AS v;

-- begin-expected
-- columns: v
-- row: '-01':2 '-15':3 '192.168.1.1':6 '2024':1 '3.14':5 'v1.2.3':4
-- end-expected
SELECT to_tsvector('english', '2024-01-15 v1.2.3 3.14 192.168.1.1')::text AS v;

-- begin-expected
-- columns: v
-- row: '+2.5':4 '-1.5e+10':3 '1.5e-3':2 '1e10':1
-- end-expected
SELECT to_tsvector('english', '1e10 1.5e-3 -1.5E+10 +2.5')::text AS v;

-- A hyphenated compound yields the whole word and each part
-- begin-expected
-- columns: v
-- row: 'art':8 'co':10 'co-oper':9 'known':3 'oper':11 'state':5 'state-of-the-art':4 'well':2 'well-known':1
-- end-expected
SELECT to_tsvector('english', 'well-known state-of-the-art co-operate')::text AS v;

-- ...but a numeric part after a hyphen is a signed number
-- begin-expected
-- columns: v
-- row: '-3':2 'row':1
-- end-expected
SELECT to_tsvector('english', 'row-3')::text AS v;

-- ============================================================================
-- 3. Ranking
-- ============================================================================

-- begin-expected
-- columns: r
-- row: 0.06079271
-- end-expected
SELECT ts_rank(to_tsvector('english','the quick brown fox jumps over the lazy dog'),
               to_tsquery('english','fox'))::text AS r;

-- Cover density over a single-lexeme cover at weight D
-- begin-expected
-- columns: r
-- row: 0.1
-- end-expected
SELECT ts_rank_cd(to_tsvector('english','a b c d e f g'), to_tsquery('english','a & g'))::text AS r;

-- begin-expected
-- columns: r
-- row: 0.033333335
-- end-expected
SELECT ts_rank_cd(to_tsvector('english','alpha beta gamma delta'),
                  to_tsquery('english','alpha & delta'))::text AS r;

-- Normalisation bits, including bit 4 (mean harmonic distance between covers)
-- begin-expected
-- columns: n0 | n2 | n4
-- row: 0.18333334, 0.026190476, 0.0712963
-- end-expected
SELECT ts_rank_cd(to_tsvector('english','alpha beta gamma delta alpha epsilon delta'),
                  to_tsquery('english','alpha & delta'), 0)::text AS n0,
       ts_rank_cd(to_tsvector('english','alpha beta gamma delta alpha epsilon delta'),
                  to_tsquery('english','alpha & delta'), 2)::text AS n2,
       ts_rank_cd(to_tsvector('english','alpha beta gamma delta alpha epsilon delta'),
                  to_tsquery('english','alpha & delta'), 4)::text AS n4;

-- ts_rank's bit 4 is documented as not applicable and is a no-op
-- begin-expected
-- columns: n0 | n4
-- row: 0.098500855, 0.098500855
-- end-expected
SELECT ts_rank(to_tsvector('english','alpha beta gamma'), to_tsquery('english','alpha & gamma'), 0)::text AS n0,
       ts_rank(to_tsvector('english','alpha beta gamma'), to_tsquery('english','alpha & gamma'), 4)::text AS n4;

-- ============================================================================
-- 4. float4 / float8 output
-- ============================================================================

-- begin-expected
-- columns: a | b | c | d | e
-- row: 0.1, 100000000000000, 1e+15, 0.0001, 1e-05
-- end-expected
SELECT (0.1::float8)::text AS a, (1e14::float8)::text AS b, (1e15::float8)::text AS c,
       (0.0001::float8)::text AS d, (0.00001::float8)::text AS e;

-- real switches to scientific notation nine orders of magnitude earlier than double
-- begin-expected
-- columns: a | b | c | d
-- row: 100000, 1e+06, 0.33333334, 1e-05
-- end-expected
SELECT (100000::float4)::text AS a, (1000000::float4)::text AS b,
       ('0.33333334'::float4)::text AS c, (0.00001::float4)::text AS d;

-- begin-expected
-- columns: a | b | c
-- row: NaN, Infinity, -Infinity
-- end-expected
SELECT ('NaN'::float8)::text AS a, ('Infinity'::float8)::text AS b, ('-Infinity'::float8)::text AS c;

-- The rank functions return real, not text
-- begin-expected
-- columns: t
-- row: real
-- end-expected
SELECT pg_typeof(ts_rank(to_tsvector('english','a fox'), to_tsquery('english','fox')))::text AS t;
