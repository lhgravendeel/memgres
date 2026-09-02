-- source: review-2026-08.md
-- finding: Root cause 10: the text-search configuration or dictionary argument is parsed and then ignored
-- area: Strings, pattern matching, text search, encodings and the exotic types
-- title: Root cause 10: the text-search configuration or dictionary argument is parsed and then ignored
-- begin-expected
-- columns: ts_lexize:_text
-- row: {cats}
-- rowcount: 1
-- end-expected
SELECT ts_lexize('simple', 'Cats');
-- begin-expected
-- columns: ts_lexize:_text
-- row: {}
-- rowcount: 1
-- end-expected
SELECT ts_lexize('english_stem', 'the');
-- begin-expected-error
-- sqlstate: 42704
-- message-like: text search dictionary "no_such_dict" does not exist
-- end-expected-error
SELECT ts_lexize('no_such_dict', 'x');
-- begin-expected
-- columns: ts_headline:text
-- row: <b>Cats</b> and Dogs
-- rowcount: 1
-- end-expected
SELECT ts_headline('simple', 'Cats and Dogs', to_tsquery('simple','cats'));
-- begin-expected
-- columns: ts_headline:text
-- row: <b>supernovae</b> stars
-- rowcount: 1
-- end-expected
SELECT ts_headline('english', 'supernovae stars', to_tsquery('english','sup:*'));
-- begin-expected
-- ok: 0
-- end-expected
SET default_text_search_config = 'pg_catalog.simple';
-- begin-expected
-- columns: to_tsvector:text
-- row: 'cats':2 'the':1
-- rowcount: 1
-- end-expected
SELECT to_tsvector('The Cats')::text;
-- begin-expected
-- columns: get_current_ts_config:regconfig
-- row: simple
-- rowcount: 1
-- end-expected
SELECT get_current_ts_config();
-- begin-expected-error
-- sqlstate: 22023
-- message-like: invalid value for parameter "default_text_search_config": "no_such_config"
-- end-expected-error
SELECT set_config('default_text_search_config','no_such_config',false);
-- begin-expected
-- ok: 0
-- end-expected
CREATE TEXT SEARCH CONFIGURATION zz_vf_ts_cfg (COPY = simple);
-- begin-expected
-- columns: plainto_tsquery:text
-- row: 'the' & 'cats'
-- rowcount: 1
-- end-expected
SELECT plainto_tsquery('zz_vf_ts_cfg', 'The Cats')::text;
-- begin-expected
-- ok: 0
-- end-expected
ALTER TEXT SEARCH CONFIGURATION zz_vf_ts_cfg ALTER MAPPING FOR asciiword WITH english_stem;
-- begin-expected
-- columns: to_tsvector:text
-- row: 'cat':2
-- rowcount: 1
-- end-expected
SELECT to_tsvector('zz_vf_ts_cfg', 'The Cats')::text;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: text search configuration "nonexistent_cfg" does not exist
-- end-expected-error
SELECT plainto_tsquery('nonexistent_cfg', 'hello');
-- begin-expected-error
-- sqlstate: 42704
-- message-like: text search parser "no_such_parser" does not exist
-- end-expected-error
SELECT count(*) FROM ts_parse('no_such_parser', 'a b');
