-- source: investigation-2026-08.md
-- finding: 65
-- title: The text-search configuration or dictionary argument is parsed out of argv and then never used: plainto/phraseto/websearch_to_tsquery never call resolveTsConfig
-- begin-expected
-- columns: ts_lexize:text
-- row: {cats}
-- rowcount: 1
-- end-expected
SELECT ts_lexize('simple', 'Cats')::text;
-- begin-expected
-- columns: ts_lexize:text
-- row: {}
-- rowcount: 1
-- end-expected
SELECT ts_lexize('english_stem', 'the')::text;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: text search dictionary "no_such_dict" does not exist
-- end-expected-error
SELECT ts_lexize('no_such_dict', 'x')::text;
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
-- columns: get_current_ts_config:text
-- row: simple
-- rowcount: 1
-- end-expected
SELECT get_current_ts_config()::text;
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
-- begin-expected
-- ok: 0
-- end-expected
DROP TEXT SEARCH CONFIGURATION zz_vf_ts_cfg;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: text search configuration "nonexistent_cfg" does not exist
-- end-expected-error
SELECT plainto_tsquery('nonexistent_cfg', 'hello')::text;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: text search configuration "nonexistent_cfg" does not exist
-- end-expected-error
SELECT phraseto_tsquery('nonexistent_cfg', 'hello')::text;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: text search configuration "nonexistent_cfg" does not exist
-- end-expected-error
SELECT websearch_to_tsquery('nonexistent_cfg', 'hello')::text;
-- begin-expected-error
-- sqlstate: 42704
-- message-like: text search configuration "nonexistent_cfg" does not exist
-- end-expected-error
SELECT ts_headline('nonexistent_cfg', 'hello', 'x'::tsquery);
-- begin-expected-error
-- sqlstate: 42704
-- message-like: text search configuration "no_such_cfg" does not exist
-- end-expected-error
SELECT count(*) FROM ts_debug('no_such_cfg', 'a b');
-- begin-expected-error
-- sqlstate: 42704
-- message-like: text search parser "no_such_parser" does not exist
-- end-expected-error
SELECT count(*) FROM ts_parse('no_such_parser', 'a b');
