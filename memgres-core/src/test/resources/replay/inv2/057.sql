-- source: investigation-2026-08.md
-- finding: 57
-- title: The text-search query builders pre-clean their input with replaceAll("[^a-zA-Z0-9\\s]"," ") instead of running the configuration's parser, and ts_headline split
-- begin-expected
-- columns: plainto_tsquery:text
-- row: 'café'
-- rowcount: 1
-- end-expected
SELECT plainto_tsquery('english', 'café')::text;
-- begin-expected
-- columns: to_tsvector:text
-- row: 'café':1
-- rowcount: 1
-- end-expected
SELECT to_tsvector('english','café')::text;
-- begin-expected
-- columns: length:int4
-- row: 32
-- rowcount: 1
-- end-expected
SELECT length(ts_headline('english', 'Multi  spaced    text fox', to_tsquery('english','fox')));
-- begin-expected
-- columns: replace:text
-- row: line1 <b>fox</b><NL>line2
-- rowcount: 1
-- end-expected
SELECT replace(ts_headline('english', E'line1 fox\nline2', to_tsquery('english','fox')), E'\n', '<NL>');
-- begin-expected
-- columns: plainto_tsquery:text
-- row: 'foo@bar.com'
-- rowcount: 1
-- end-expected
SELECT plainto_tsquery('english', 'foo@bar.com')::text;
-- begin-expected
-- columns: plainto_tsquery:text
-- row: 'a.b.c'
-- rowcount: 1
-- end-expected
SELECT plainto_tsquery('english', 'a.b.c')::text;
-- begin-expected
-- columns: plainto_tsquery:text
-- row: 'example.com/x' & 'example.com' & '/x'
-- rowcount: 1
-- end-expected
SELECT plainto_tsquery('english', 'http://example.com/x')::text;
-- begin-expected
-- columns: phraseto_tsquery:text
-- row: 'well-known' <-> 'well' <-> 'known' <-> 'thing'
-- rowcount: 1
-- end-expected
SELECT phraseto_tsquery('english', 'well-known thing')::text;
-- begin-expected
-- columns: websearch_to_tsquery:text
-- row: 'foo@bar.com'
-- rowcount: 1
-- end-expected
SELECT websearch_to_tsquery('english', 'foo@bar.com')::text;
-- begin-expected
-- columns: websearch_to_tsquery:text
-- row: 'b' | 'c' & 'd'
-- rowcount: 1
-- end-expected
SELECT websearch_to_tsquery('english', 'a b OR c d')::text;
-- begin-expected
-- columns: websearch_to_tsquery:text
-- row: 'x' | 'y'
-- rowcount: 1
-- end-expected
SELECT websearch_to_tsquery('english', 'x OR OR y')::text;
-- begin-expected
-- columns: websearch_to_tsquery:text
-- row: !'cat'
-- rowcount: 1
-- end-expected
SELECT websearch_to_tsquery('english', '- cat')::text;
-- begin-expected
-- columns: websearch_to_tsquery:text
-- row: !!'cat'
-- rowcount: 1
-- end-expected
SELECT websearch_to_tsquery('english', '--cat')::text;
-- begin-expected
-- columns: ts_headline:text
-- row: The cat. The <b>dog</b>. The bird.
-- rowcount: 1
-- end-expected
SELECT ts_headline('english', 'The cat. The dog. The bird.', to_tsquery('english','dog'));
-- begin-expected
-- columns: ts_headline:text
-- row: <b>six</b> seven
-- rowcount: 1
-- end-expected
SELECT ts_headline('english','one two three four five six seven eight nine ten eleven twelve', to_tsquery('english','six'),'MaxWords=5, MinWords=2');
-- begin-expected
-- columns: ts_headline:text
-- row: quick brown <b>fox</b>
-- rowcount: 1
-- end-expected
SELECT ts_headline('english','the quick brown fox',to_tsquery('english','fox'),'MaxFragments=1');
