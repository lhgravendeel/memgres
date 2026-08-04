-- ============================================================================
-- Feature Comparison: array-typed columns of the pg_catalog stub relations
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- The pg_catalog relations memgres builds as stubs said a column was a scalar
-- where PostgreSQL says it is an array.
--
-- Every foreign-data catalog carries a text[] of key=value options, an event
-- trigger a text[] of command tags, a subscription a text[] of publications, an
-- hba line a text[] of databases and of roles, and a partitioned table three
-- vectors: an int2vector of key columns and two oidvectors beside it. memgres
-- wrote exactly the right value into each and then declared the column text, so
-- unnest() and subscripting worked while pg_attribute, format_type, pg_typeof
-- and the wire type all reported a string -- the same shape of defect a
-- domain[] column has, where the data is there and the advertised type makes it
-- unreadable to a client that reads by type.
--
-- Two further claims the same relations made and could not back: pg_am.amhandler
-- is a regproc and printed its raw OID rather than the handler's name, and
-- pg_ts_config_map listed two of the nineteen token types the shipped
-- configurations really lexize.
--
-- Nothing here compares a count over a whole catalog or an OID minted at
-- runtime: the reference server carries contrib extensions and a pg_hba.conf CI
-- does not have, and initdb-assigned OIDs are not the same number twice. Only
-- the pinned OIDs PostgreSQL writes into its own .dat files are compared.
-- ============================================================================

-- ============================================================================
-- 1. The four option columns of the foreign-data catalogs are text[]
-- ============================================================================
-- begin-expected
-- columns: relname, attname, format_type
-- row: pg_foreign_data_wrapper, fdwoptions, text[]
-- row: pg_foreign_server, srvoptions, text[]
-- row: pg_foreign_table, ftoptions, text[]
-- row: pg_user_mapping, umoptions, text[]
-- row: pg_user_mappings, umoptions, text[]
-- end-expected
SELECT c.relname::text AS relname, a.attname::text AS attname,
       format_type(a.atttypid, a.atttypmod) AS format_type
  FROM pg_attribute a
  JOIN pg_class c ON c.oid = a.attrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'pg_catalog'
   AND a.attname IN ('fdwoptions', 'srvoptions', 'ftoptions', 'umoptions')
 ORDER BY c.relname, a.attname;

-- ============================================================================
-- 2. A written option list reads back as an array
-- ============================================================================
DROP FOREIGN DATA WRAPPER IF EXISTS csa_fdw CASCADE;
CREATE FOREIGN DATA WRAPPER csa_fdw OPTIONS (k1 'v1', k2 'v2');
CREATE SERVER csa_srv FOREIGN DATA WRAPPER csa_fdw OPTIONS (host 'h', port '5');

-- begin-expected
-- columns: fdwoptions
-- row: {k1=v1,k2=v2}
-- end-expected
SELECT fdwoptions::text AS fdwoptions
  FROM pg_foreign_data_wrapper WHERE fdwname = 'csa_fdw';

-- begin-expected
-- columns: unnest
-- row: k1=v1
-- row: k2=v2
-- end-expected
SELECT unnest(fdwoptions)::text AS unnest
  FROM pg_foreign_data_wrapper WHERE fdwname = 'csa_fdw';

-- begin-expected
-- columns: first, len, typ
-- row: host=h, 2, text[]
-- end-expected
SELECT srvoptions[1]::text AS first, array_length(srvoptions, 1)::text AS len,
       pg_typeof(srvoptions)::text AS typ
  FROM pg_foreign_server WHERE srvname = 'csa_srv';

-- ============================================================================
-- 3. PostgreSQL orders pg_foreign_server srvacl before srvoptions
-- ============================================================================
-- Reading the row positionally with the two swapped took the ACL out of the
-- options column.
-- begin-expected
-- columns: attname
-- row: oid
-- row: srvname
-- row: srvowner
-- row: srvfdw
-- row: srvtype
-- row: srvversion
-- row: srvacl
-- row: srvoptions
-- end-expected
SELECT a.attname::text AS attname
  FROM pg_attribute a
  JOIN pg_class c ON c.oid = a.attrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'pg_catalog' AND c.relname = 'pg_foreign_server'
   AND a.attnum > 0 AND a.attname <> 'xmin'
 ORDER BY a.attnum;

DROP FOREIGN DATA WRAPPER IF EXISTS csa_fdw CASCADE;

-- ============================================================================
-- 4. An event trigger's command tags are a text[]
-- ============================================================================
DROP EVENT TRIGGER IF EXISTS csa_et;
DROP FUNCTION IF EXISTS csa_etf();
CREATE FUNCTION csa_etf() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN END $$;
CREATE EVENT TRIGGER csa_et ON ddl_command_start
    WHEN TAG IN ('CREATE TABLE', 'DROP TABLE') EXECUTE FUNCTION csa_etf();

-- begin-expected
-- columns: evttags, len, typ
-- row: {"CREATE TABLE","DROP TABLE"}, 2, text[]
-- end-expected
SELECT evttags::text AS evttags, array_length(evttags, 1)::text AS len,
       pg_typeof(evttags)::text AS typ
  FROM pg_event_trigger WHERE evtname = 'csa_et';

DROP EVENT TRIGGER IF EXISTS csa_et;
DROP FUNCTION IF EXISTS csa_etf();

-- ============================================================================
-- 5. The remaining list-valued stub columns
-- ============================================================================
-- begin-expected
-- columns: relname, attname, format_type
-- row: pg_available_extension_versions, requires, name[]
-- row: pg_event_trigger, evttags, text[]
-- row: pg_hba_file_rules, database, text[]
-- row: pg_hba_file_rules, options, text[]
-- row: pg_hba_file_rules, user_name, text[]
-- row: pg_subscription, subpublications, text[]
-- row: pg_tablespace, spcoptions, text[]
-- end-expected
SELECT c.relname::text AS relname, a.attname::text AS attname,
       format_type(a.atttypid, a.atttypmod) AS format_type
  FROM pg_attribute a
  JOIN pg_class c ON c.oid = a.attrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'pg_catalog'
   AND (c.relname, a.attname) IN (
        ('pg_tablespace', 'spcoptions'), ('pg_event_trigger', 'evttags'),
        ('pg_subscription', 'subpublications'), ('pg_hba_file_rules', 'database'),
        ('pg_hba_file_rules', 'user_name'), ('pg_hba_file_rules', 'options'),
        ('pg_available_extension_versions', 'requires'))
 ORDER BY c.relname, a.attname;

-- ============================================================================
-- 6. An hba line's database and role lists are array values, not bare words
-- ============================================================================
-- A rule for two databases is two elements; flattened to a string it cannot be
-- told from one database literally named "a,b". Only the first rule's shape is
-- compared: how many rules there are is whatever pg_hba.conf says.
-- begin-expected
-- columns: database, user_name, typ
-- row: {all}, all, text[]
-- end-expected
SELECT database::text AS database, user_name[1]::text AS user_name,
       pg_typeof(database)::text AS typ
  FROM pg_hba_file_rules ORDER BY rule_number LIMIT 1;

-- ============================================================================
-- 7. A partition key is an int2vector and two oidvectors, one entry per column
-- ============================================================================
-- begin-expected
-- columns: attname, format_type
-- row: partattrs, int2vector
-- row: partclass, oidvector
-- row: partcollation, oidvector
-- row: partexprs, pg_node_tree
-- end-expected
SELECT a.attname::text AS attname, format_type(a.atttypid, a.atttypmod) AS format_type
  FROM pg_attribute a
  JOIN pg_class c ON c.oid = a.attrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'pg_catalog' AND c.relname = 'pg_partitioned_table'
   AND a.attname IN ('partattrs', 'partclass', 'partcollation', 'partexprs')
 ORDER BY a.attname;

DROP TABLE IF EXISTS csa_p CASCADE;
CREATE TABLE csa_p (a int, b int) PARTITION BY RANGE (a, b);

-- The two key columns are 1 and 2, and partcollation has an entry for each of
-- them. partclass names the operator class each key was resolved through, which
-- memgres does not record, so it is not compared here.
-- begin-expected
-- columns: partnatts, partattrs, partcollation
-- row: 2, 1 2, 0 0
-- end-expected
SELECT partnatts::text AS partnatts, partattrs::text AS partattrs,
       partcollation::text AS partcollation
  FROM pg_partitioned_table WHERE partrelid = 'csa_p'::regclass;

DROP TABLE IF EXISTS csa_p CASCADE;

-- ============================================================================
-- 8. Extended statistics: stxkeys is an int2vector, stxstattarget comes first
-- ============================================================================
-- begin-expected
-- columns: attname
-- row: oid
-- row: stxrelid
-- row: stxname
-- row: stxnamespace
-- row: stxowner
-- row: stxkeys
-- row: stxstattarget
-- row: stxkind
-- row: stxexprs
-- end-expected
SELECT a.attname::text AS attname
  FROM pg_attribute a
  JOIN pg_class c ON c.oid = a.attrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'pg_catalog' AND c.relname = 'pg_statistic_ext'
   AND a.attnum > 0
 ORDER BY a.attnum;

DROP TABLE IF EXISTS csa_s CASCADE;
CREATE TABLE csa_s (a int, b int);
CREATE STATISTICS csa_st (ndistinct) ON a, b FROM csa_s;

-- begin-expected
-- columns: stxkeys, stxkind, keytype
-- row: 1 2, {d}, int2vector
-- end-expected
SELECT s.stxkeys::text AS stxkeys, s.stxkind::text AS stxkind,
       format_type(a.atttypid, a.atttypmod) AS keytype
  FROM pg_statistic_ext s
  JOIN pg_class c ON c.relname = 'pg_statistic_ext'
  JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'pg_catalog'
  JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = 'stxkeys'
 WHERE s.stxname = 'csa_st';

DROP TABLE IF EXISTS csa_s CASCADE;

-- ============================================================================
-- 9. A publication's column list is an int2vector, its filter a pg_node_tree
-- ============================================================================
-- begin-expected
-- columns: attname, format_type
-- row: prattrs, int2vector
-- row: prqual, pg_node_tree
-- end-expected
SELECT a.attname::text AS attname, format_type(a.atttypid, a.atttypmod) AS format_type
  FROM pg_attribute a
  JOIN pg_class c ON c.oid = a.attrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'pg_catalog' AND c.relname = 'pg_publication_rel'
   AND a.attname IN ('prattrs', 'prqual')
 ORDER BY a.attname;

-- ============================================================================
-- 10. pg_am.amhandler is a regproc and prints the handler's name
-- ============================================================================
-- begin-expected
-- columns: amname, amhandler
-- row: brin, brinhandler
-- row: btree, bthandler
-- row: gin, ginhandler
-- row: gist, gisthandler
-- row: hash, hashhandler
-- row: heap, heap_tableam_handler
-- row: spgist, spghandler
-- end-expected
SELECT amname::text AS amname, amhandler::text AS amhandler FROM pg_am ORDER BY amname;

-- begin-expected
-- columns: format_type
-- row: regproc
-- end-expected
SELECT format_type(a.atttypid, a.atttypmod) AS format_type
  FROM pg_attribute a
  JOIN pg_class c ON c.oid = a.attrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'pg_catalog' AND c.relname = 'pg_am' AND a.attname = 'amhandler';

-- The name it prints is a row pg_proc really has.
-- begin-expected
-- columns: count
-- row: 7
-- end-expected
SELECT count(*)::text AS count FROM pg_am a JOIN pg_proc p ON p.oid = a.amhandler;

-- ============================================================================
-- 11. A shipped text-search configuration maps every token type it lexizes
-- ============================================================================
-- PostgreSQL maps nineteen of the parser's twenty-three: everything except
-- blank, tag, protocol and entity, which carry no lexeme. memgres listed two
-- and lexized all nineteen anyway, so the catalog said an email address or a
-- URL would be dropped from a tsvector it in fact indexes.
-- begin-expected
-- columns: cfgname, count
-- row: english, 19
-- row: simple, 19
-- end-expected
SELECT c.cfgname::text AS cfgname, count(*)::text AS count
  FROM pg_ts_config_map m JOIN pg_ts_config c ON c.oid = m.mapcfg
 WHERE c.cfgname IN ('simple', 'english')
 GROUP BY c.cfgname ORDER BY c.cfgname;

-- begin-expected
-- columns: cfgname, aliases
-- row: english, asciihword,asciiword,email,file,float,host,hword,hword_asciipart,hword_numpart,hword_part,int,numhword,numword,sfloat,uint,url,url_path,version,word
-- row: simple, asciihword,asciiword,email,file,float,host,hword,hword_asciipart,hword_numpart,hword_part,int,numhword,numword,sfloat,uint,url,url_path,version,word
-- end-expected
SELECT c.cfgname::text AS cfgname,
       string_agg(DISTINCT t.alias, ',' ORDER BY t.alias) AS aliases
  FROM pg_ts_config_map m
  JOIN pg_ts_config c ON c.oid = m.mapcfg
  JOIN ts_token_type(c.cfgparser) t ON t.tokid = m.maptokentype
 WHERE c.cfgname IN ('simple', 'english')
 GROUP BY c.cfgname ORDER BY c.cfgname;

-- ...and those token types really do reach the tsvector.
-- begin-expected
-- columns: email, flt, uint, urlpath
-- row: 'a@b.com':1, '3.5':1, '42':1, 'a.b/c':1
-- end-expected
SELECT to_tsvector('simple', 'a@b.com')::text AS email,
       to_tsvector('english', '3.5')::text AS flt,
       to_tsvector('english', '42')::text AS uint,
       to_tsvector('simple', 'http://a.b/c')::text AS urlpath;

-- ...through the dictionary each token type is really lexized by. A snowball
-- stemmer is for words: PostgreSQL's english configuration sends the six
-- word-shaped token types through english_stem and every other one through
-- simple, because stemming an address or a version string would change a value
-- that has to come back out as it went in. Naming english_stem for all
-- nineteen contradicted what to_tsvector here actually does.
-- begin-expected
-- columns: dictname, aliases
-- row: english_stem, asciihword,asciiword,hword,hword_asciipart,hword_part,word
-- row: simple, email,file,float,host,hword_numpart,int,numhword,numword,sfloat,uint,url,url_path,version
-- end-expected
SELECT d.dictname::text AS dictname,
       string_agg(DISTINCT t.alias, ',' ORDER BY t.alias) AS aliases
  FROM pg_ts_config_map m
  JOIN pg_ts_config c ON c.oid = m.mapcfg
  JOIN pg_ts_dict d ON d.oid = m.mapdict
  JOIN ts_token_type(c.cfgparser) t ON t.tokid = m.maptokentype
 WHERE c.cfgname = 'english'
 GROUP BY d.dictname ORDER BY d.dictname;

-- The simple configuration stems nothing, so all nineteen go through simple.
-- begin-expected
-- columns: dictname, count
-- row: simple, 19
-- end-expected
SELECT d.dictname::text AS dictname, count(*)::text AS count
  FROM pg_ts_config_map m
  JOIN pg_ts_config c ON c.oid = m.mapcfg
  JOIN pg_ts_dict d ON d.oid = m.mapdict
 WHERE c.cfgname = 'simple'
 GROUP BY d.dictname ORDER BY d.dictname;

-- And the engine agrees with the catalog: the word is stemmed, the address and
-- the number are not.
-- begin-expected
-- columns: word, email, num
-- row: 'run':1, 'user@example.com':1, '12345':1
-- end-expected
SELECT to_tsvector('english', 'running')::text AS word,
       to_tsvector('english', 'user@example.com')::text AS email,
       to_tsvector('english', '12345')::text AS num;

-- ============================================================================
-- 12. The text-search OIDs PostgreSQL pins are the ones memgres reports
-- ============================================================================
-- Only the four written into PostgreSQL's own .dat files are compared; snowball
-- and english_stem are created at initdb and have no fixed number. memgres used
-- to give snowball 3726, which PostgreSQL hands to the function dsimple_lexize
-- -- an OID that resolves to something else entirely on a real server.
-- begin-expected
-- columns: parser, dict, tmpl, syn, cfg, collides
-- row: 3722, 3765, 3727, 3730, 3748, 0
-- end-expected
SELECT (SELECT oid FROM pg_ts_parser WHERE prsname = 'default')::text AS parser,
       (SELECT oid FROM pg_ts_dict WHERE dictname = 'simple')::text AS dict,
       (SELECT oid FROM pg_ts_template WHERE tmplname = 'simple')::text AS tmpl,
       (SELECT oid FROM pg_ts_template WHERE tmplname = 'synonym')::text AS syn,
       (SELECT oid FROM pg_ts_config WHERE cfgname = 'simple')::text AS cfg,
       (SELECT count(*) FROM pg_ts_template WHERE oid = 3726)::text AS collides;
