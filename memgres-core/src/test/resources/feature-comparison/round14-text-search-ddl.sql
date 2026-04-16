-- ============================================================================
-- Feature Comparison: Round 14 — Text Search DDL + utilities
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

DROP SCHEMA IF EXISTS r14_ts CASCADE;
CREATE SCHEMA r14_ts;
SET search_path = r14_ts, public;

-- ============================================================================
-- SECTION A: pg_ts_* catalogs populated
-- ============================================================================

-- 1. pg_ts_config has defaults
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (count(*) >= 2)::text AS ok FROM pg_ts_config;

-- 2. pg_ts_dict has defaults
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (count(*) >= 1)::text AS ok FROM pg_ts_dict;

-- 3. pg_ts_parser has defaults
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (count(*) >= 1)::text AS ok FROM pg_ts_parser;

-- 4. pg_ts_template has defaults
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (count(*) >= 1)::text AS ok FROM pg_ts_template;

-- 5. pg_ts_config_map has defaults
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (count(*) >= 1)::text AS ok FROM pg_ts_config_map;

-- ============================================================================
-- SECTION B: CREATE TEXT SEARCH CONFIGURATION
-- ============================================================================

-- 6. COPY-based configuration
CREATE TEXT SEARCH CONFIGURATION r14_ts_cfg (COPY = english);

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM pg_ts_config WHERE cfgname = 'r14_ts_cfg';

-- 7. ALTER ... ALTER MAPPING
ALTER TEXT SEARCH CONFIGURATION r14_ts_cfg
  ALTER MAPPING FOR word, asciiword WITH simple;

-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (count(*) >= 1)::text AS ok FROM pg_ts_config_map
  WHERE mapcfg = (SELECT oid FROM pg_ts_config WHERE cfgname = 'r14_ts_cfg');

-- 8. DROP
DROP TEXT SEARCH CONFIGURATION r14_ts_cfg;

-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::text AS c FROM pg_ts_config WHERE cfgname = 'r14_ts_cfg';

-- ============================================================================
-- SECTION C: CREATE TEXT SEARCH DICTIONARY
-- ============================================================================

-- 9. simple template
CREATE TEXT SEARCH DICTIONARY r14_ts_dict (TEMPLATE = pg_catalog.simple);

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::text AS c FROM pg_ts_dict WHERE dictname = 'r14_ts_dict';

-- 10. DROP dictionary
DROP TEXT SEARCH DICTIONARY r14_ts_dict;

-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::text AS c FROM pg_ts_dict WHERE dictname = 'r14_ts_dict';

-- ============================================================================
-- SECTION D: ts_* utility functions
-- ============================================================================

-- 11. ts_lexize with simple dictionary
-- begin-expected
-- columns: v
-- row: {word}
-- end-expected
SELECT ts_lexize('simple', 'Word')::text AS v;

-- 12. ts_parse default parser yields tokens
-- begin-expected
-- columns: n
-- row: t
-- end-expected
SELECT (count(*) >= 2)::text AS n FROM ts_parse('default', 'hello world');

-- 13. ts_token_type enumerates token types
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (count(*) >= 1)::text AS ok FROM ts_token_type('default');

-- 14. ts_debug returns rows
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (count(*) >= 1)::text AS ok FROM ts_debug('hello world');

-- ============================================================================
-- SECTION E: ts_headline advanced options
-- ============================================================================

-- 15. StartSel/StopSel honored
-- begin-expected
-- columns: hit
-- row: t
-- end-expected
SELECT (ts_headline('english', 'the quick brown fox',
                    to_tsquery('english', 'fox'),
                    'StartSel=<em>, StopSel=</em>') LIKE '%<em>%</em>%')::text AS hit;

-- ============================================================================
-- SECTION F: websearch_to_tsquery
-- ============================================================================

-- 16. AND default
-- begin-expected
-- columns: v
-- row: 'quick' & 'fox'
-- end-expected
SELECT websearch_to_tsquery('english', 'quick fox')::text AS v;

-- 17. OR
-- begin-expected
-- columns: hit
-- row: t
-- end-expected
SELECT (websearch_to_tsquery('english', 'quick OR fox')::text LIKE '%|%')::text AS hit;

-- 18. Phrase
-- begin-expected
-- columns: hit
-- row: t
-- end-expected
SELECT (websearch_to_tsquery('english', '"quick fox"')::text LIKE '%<->%')::text AS hit;

-- 19. Negation
-- begin-expected
-- columns: hit
-- row: t
-- end-expected
SELECT (websearch_to_tsquery('english', 'fox -lazy')::text LIKE '%!%')::text AS hit;
