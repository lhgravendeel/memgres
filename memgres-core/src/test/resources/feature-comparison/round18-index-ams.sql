-- ============================================================================
-- Feature Comparison: Round 18 — Index access methods beyond btree
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

-- ============================================================================
-- SECTION AB1: btree vs hash indam
-- ============================================================================

DROP TABLE IF EXISTS r18_iam CASCADE;
CREATE TABLE r18_iam(a int, b text);
CREATE INDEX r18_iam_bt ON r18_iam USING btree(a);
CREATE INDEX r18_iam_hs ON r18_iam USING hash(a);

-- 1. btree index relam = btree
-- begin-expected
-- columns: amname
-- row: btree
-- end-expected
SELECT am.amname
  FROM pg_class c JOIN pg_am am ON am.oid=c.relam
 WHERE c.relname='r18_iam_bt';

-- 2. hash index relam = hash
-- begin-expected
-- columns: amname
-- row: hash
-- end-expected
SELECT am.amname
  FROM pg_class c JOIN pg_am am ON am.oid=c.relam
 WHERE c.relname='r18_iam_hs';

-- ============================================================================
-- SECTION AB2: GIN on tsvector
-- ============================================================================

DROP TABLE IF EXISTS r18_gin CASCADE;
CREATE TABLE r18_gin(doc tsvector);
CREATE INDEX r18_gin_ix ON r18_gin USING gin(doc);

-- 3. gin index relam = gin
-- begin-expected
-- columns: amname
-- row: gin
-- end-expected
SELECT am.amname
  FROM pg_class c JOIN pg_am am ON am.oid=c.relam
 WHERE c.relname='r18_gin_ix';

-- ============================================================================
-- SECTION AB3: GiST on point
-- ============================================================================

DROP TABLE IF EXISTS r18_gist CASCADE;
CREATE TABLE r18_gist(p point);
CREATE INDEX r18_gist_ix ON r18_gist USING gist(p);

-- 4. gist index relam = gist
-- begin-expected
-- columns: amname
-- row: gist
-- end-expected
SELECT am.amname
  FROM pg_class c JOIN pg_am am ON am.oid=c.relam
 WHERE c.relname='r18_gist_ix';

-- ============================================================================
-- SECTION AB4: pg_opclass has non-btree entries
-- ============================================================================

-- 5. hash opclasses present
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (count(*) > 0) AS ok FROM pg_opclass oc
JOIN pg_am a ON a.oid=oc.opcmethod WHERE a.amname='hash';

-- 6. gin opclasses present
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (count(*) > 0) AS ok FROM pg_opclass oc
JOIN pg_am a ON a.oid=oc.opcmethod WHERE a.amname='gin';

-- 7. gist opclasses present
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (count(*) > 0) AS ok FROM pg_opclass oc
JOIN pg_am a ON a.oid=oc.opcmethod WHERE a.amname='gist';

-- ============================================================================
-- SECTION AB5: indoption bits
-- ============================================================================

DROP TABLE IF EXISTS r18_idop CASCADE;
CREATE TABLE r18_idop(a int);
CREATE INDEX r18_idop_ix ON r18_idop(a DESC NULLS FIRST);

-- 8. indoption[1] non-zero for DESC/NULLS FIRST
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (indoption[1]::int <> 0) AS ok
  FROM pg_index ix JOIN pg_class ci ON ci.oid=ix.indexrelid
 WHERE ci.relname='r18_idop_ix';

-- ============================================================================
-- SECTION AB6: indclass per-type opclass
-- ============================================================================

DROP TABLE IF EXISTS r18_icls CASCADE;
CREATE TABLE r18_icls(a int, b text);
CREATE INDEX r18_icls_a ON r18_icls(a);
CREATE INDEX r18_icls_b ON r18_icls(b);

-- 9. indclass differs for int vs text columns
-- begin-expected
-- columns: ok
-- row: t
-- end-expected
SELECT (
  (SELECT indclass[1]::int FROM pg_index ix
     JOIN pg_class ci ON ci.oid=ix.indexrelid WHERE ci.relname='r18_icls_a')
  <>
  (SELECT indclass[1]::int FROM pg_index ix
     JOIN pg_class ci ON ci.oid=ix.indexrelid WHERE ci.relname='r18_icls_b')
) AS ok;
