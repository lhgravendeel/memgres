-- Text search correctness: H27, H28, H29, H30, M18, L14

-- H27: tsvector cast preserves case
-- begin-expected
-- columns: v
-- row: 'Cat':3 'Fat':2
-- end-expected
SELECT '''Fat'':2 ''Cat'':3'::tsvector AS v;

-- H27: empty tsvector
-- begin-expected
-- columns: v
-- row:
-- end-expected
SELECT ''::tsvector AS v;

-- H28: simple config keeps all words, no stemming
-- begin-expected
-- columns: v
-- row: 'dogs':3 'running':2 'the':1
-- end-expected
SELECT to_tsvector('simple', 'the Running Dogs') AS v;

-- H30: phraseto_tsquery stopword distances
-- begin-expected
-- columns: q
-- row: 'cat' <3> 'hat'
-- end-expected
SELECT phraseto_tsquery('english', 'the cats in the hat') AS q;

-- H30: plainto_tsquery strips punctuation
-- begin-expected
-- columns: q
-- row: 'cat' & 'dog'
-- end-expected
SELECT plainto_tsquery('english', 'cats & dogs') AS q;

-- M18: querytree strips NOT
-- begin-expected
-- columns: qt
-- row: T
-- end-expected
SELECT querytree('!cat'::tsquery) AS qt;

-- M18: array_to_tsvector no positions
-- begin-expected
-- columns: v
-- row: 'cat' 'dog'
-- end-expected
SELECT array_to_tsvector(ARRAY['cat','dog']) AS v;

-- L14: empty tsquery display
-- begin-expected
-- columns: q
-- row:
-- end-expected
SELECT ''::tsquery AS q;

-- M18: strip removes positions
-- begin-expected
-- columns: v
-- row: 'cat' 'fat'
-- end-expected
SELECT strip('fat:1 cat:2'::tsvector) AS v;

-- ts_rank returns non-zero for match
-- begin-expected
-- columns: positive
-- row: t
-- end-expected
SELECT ts_rank('cat:1 dog:2'::tsvector, 'cat'::tsquery) > 0 AS positive;
