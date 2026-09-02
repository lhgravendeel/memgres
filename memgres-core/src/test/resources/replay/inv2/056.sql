-- source: investigation-2026-08.md
-- finding: 56
-- title: tsvector and tsquery text I/O is not a faithful reader/writer: parseLiteral overwrites a repeated lexeme instead of merging position lists, never normalises the
-- begin-expected
-- columns: text:text
-- row: 'a':1,2
-- rowcount: 1
-- end-expected
SELECT 'a:1 a:2'::tsvector::text;
-- begin-expected
-- columns: text:text
-- row: 'it''s'
-- rowcount: 1
-- end-expected
SELECT '''it''''s'''::tsvector::text;
-- begin-expected
-- columns: setweight:text
-- row: 'a' 'b'
-- rowcount: 1
-- end-expected
SELECT setweight('a b'::tsvector, 'A')::text;
-- begin-expected
-- columns: setweight:text
-- row: 'a'
-- rowcount: 1
-- end-expected
SELECT setweight(strip('a:1'::tsvector), 'A')::text;
-- begin-expected
-- columns: text:text
-- row: 'a':16383
-- rowcount: 1
-- end-expected
SELECT ('a:16383'::tsvector || 'a:1'::tsvector)::text;
-- begin-expected
-- columns: text:text
-- row: 'a':1,2,3
-- rowcount: 1
-- end-expected
SELECT 'a:3,1,2'::tsvector::text;
-- begin-expected
-- columns: text:text
-- row: 'a':1
-- rowcount: 1
-- end-expected
SELECT 'a:1,1'::tsvector::text;
-- begin-expected
-- columns: text:text
-- row: 'a b'
-- rowcount: 1
-- end-expected
SELECT 'a\ b'::tsvector::text;
-- begin-expected
-- columns: text:text
-- row: 'ab'
-- rowcount: 1
-- end-expected
SELECT E'a\\b'::tsvector::text;
-- begin-expected
-- columns: text:text
-- row: ':1'
-- rowcount: 1
-- end-expected
SELECT ':1'::tsvector::text;
