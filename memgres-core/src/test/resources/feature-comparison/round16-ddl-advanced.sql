-- ============================================================================
-- Feature Comparison: Round 16 — Advanced DDL / tables / sequences
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

DROP SCHEMA IF EXISTS r16_ddl CASCADE;
CREATE SCHEMA r16_ddl;
SET search_path = r16_ddl, public;

-- ============================================================================
-- SECTION I1: CREATE TABLE LIKE INCLUDING INDEXES
-- ============================================================================

CREATE TABLE li_src (id int, email text);
CREATE UNIQUE INDEX li_src_email_idx ON li_src(email);
CREATE TABLE li_dst (LIKE li_src INCLUDING INDEXES);

-- 1. Destination has a UNIQUE index cloned from source
-- begin-expected
-- columns: n
-- row: 1
-- end-expected
SELECT count(*)::int AS n FROM pg_indexes
WHERE schemaname='r16_ddl' AND tablename='li_dst' AND indexdef LIKE '%UNIQUE%';

-- ============================================================================
-- SECTION I2: UNLOGGED table
-- ============================================================================

CREATE UNLOGGED TABLE ul (id int);

-- 2. pg_class.relpersistence = 'u' for UNLOGGED
-- begin-expected
-- columns: p
-- row: u
-- end-expected
SELECT relpersistence::text AS p FROM pg_class
WHERE relname='ul' AND relnamespace = 'r16_ddl'::regnamespace;

-- ============================================================================
-- SECTION I3: Generated column STORED default
-- ============================================================================

CREATE TABLE gc (a int, b int GENERATED ALWAYS AS (a*2) STORED);

-- 3. attgenerated = 's' for STORED generated column
-- begin-expected
-- columns: g
-- row: s
-- end-expected
SELECT attgenerated::text AS g FROM pg_attribute
WHERE attrelid='r16_ddl.gc'::regclass AND attname='b';

-- ============================================================================
-- SECTION I4: CHECK NO INHERIT
-- ============================================================================

CREATE TABLE ni (x int, CONSTRAINT chk_pos CHECK (x > 0) NO INHERIT);

-- 4. pg_constraint.connoinherit = true for NO INHERIT check
-- begin-expected
-- columns: c
-- row: t
-- end-expected
SELECT connoinherit AS c FROM pg_constraint
WHERE conrelid='r16_ddl.ni'::regclass AND conname='chk_pos';

-- ============================================================================
-- SECTION I5: FK SET NULL (col, col)
-- ============================================================================

CREATE TABLE fk_p (a int, b int, PRIMARY KEY (a, b));
CREATE TABLE fk_c (a int, b int, extra int,
    FOREIGN KEY (a, b) REFERENCES fk_p(a, b) ON DELETE SET NULL (a));
INSERT INTO fk_p VALUES (1, 2);
INSERT INTO fk_c VALUES (1, 2, 99);
DELETE FROM fk_p WHERE a=1;

-- 5. After parent delete, only column a nullified; b stays 2
-- begin-expected
-- columns: a,b
-- row: 1 | 2
-- end-expected
SELECT a, b FROM fk_c;

-- ============================================================================
-- SECTION I6: ALTER TABLE SET UNLOGGED / SET LOGGED
-- ============================================================================

CREATE TABLE su (id int);
ALTER TABLE su SET UNLOGGED;

-- 6. relpersistence = 'u' after SET UNLOGGED
-- begin-expected
-- columns: p
-- row: u
-- end-expected
SELECT relpersistence::text AS p FROM pg_class WHERE relname='su'
  AND relnamespace='r16_ddl'::regnamespace;

CREATE UNLOGGED TABLE sl (id int);
ALTER TABLE sl SET LOGGED;

-- 7. relpersistence = 'p' after SET LOGGED
-- begin-expected
-- columns: p
-- row: p
-- end-expected
SELECT relpersistence::text AS p FROM pg_class WHERE relname='sl'
  AND relnamespace='r16_ddl'::regnamespace;

-- ============================================================================
-- SECTION I7: ALTER COLUMN SET STORAGE / STATISTICS / COMPRESSION
-- ============================================================================

CREATE TABLE cs (t text);
ALTER TABLE cs ALTER COLUMN t SET STORAGE EXTERNAL;

-- 8. attstorage = 'e' for EXTERNAL
-- begin-expected
-- columns: s
-- row: e
-- end-expected
SELECT attstorage::text AS s FROM pg_attribute
WHERE attrelid='r16_ddl.cs'::regclass AND attname='t';

CREATE TABLE cst (v int);
ALTER TABLE cst ALTER COLUMN v SET STATISTICS 321;

-- 9. attstattarget = 321
-- begin-expected
-- columns: s
-- row: 321
-- end-expected
SELECT attstattarget AS s FROM pg_attribute
WHERE attrelid='r16_ddl.cst'::regclass AND attname='v';

CREATE TABLE cc (t text);
ALTER TABLE cc ALTER COLUMN t SET COMPRESSION pglz;

-- 10. attcompression = 'p' for pglz
-- begin-expected
-- columns: c
-- row: p
-- end-expected
SELECT attcompression::text AS c FROM pg_attribute
WHERE attrelid='r16_ddl.cc'::regclass AND attname='t';

-- ============================================================================
-- SECTION I8: pg_partition_tree
-- ============================================================================

CREATE TABLE pt (id int) PARTITION BY RANGE (id);
CREATE TABLE pt_1 PARTITION OF pt FOR VALUES FROM (1) TO (100);

-- 11. pg_partition_tree returns root + child
-- begin-expected
-- columns: n
-- row: 2
-- end-expected
SELECT count(*)::int AS n FROM pg_partition_tree('r16_ddl.pt'::regclass);

-- ============================================================================
-- SECTION I9: ALTER SEQUENCE ... OWNED BY
-- ============================================================================

CREATE SEQUENCE seq_ob;
CREATE TABLE sq (id int DEFAULT nextval('seq_ob'));

-- 12. ALTER SEQUENCE OWNED BY parses
-- begin-expected-noop
-- end-expected-noop
ALTER SEQUENCE seq_ob OWNED BY sq.id;

-- ============================================================================
-- SECTION I10: pg_sequences.last_value
-- ============================================================================

CREATE SEQUENCE seq_lv;
SELECT nextval('seq_lv');

-- 13. last_value column present and = 1 after one nextval
-- begin-expected
-- columns: v
-- row: 1
-- end-expected
SELECT last_value AS v FROM pg_sequences WHERE sequencename='seq_lv';

-- ============================================================================
-- SECTION I11: information_schema.sequences metadata
-- ============================================================================

CREATE SEQUENCE seq_i AS integer;

-- 14. data_type reflects AS integer, not hardcoded bigint
-- begin-expected
-- columns: dt
-- row: integer
-- end-expected
SELECT data_type AS dt FROM information_schema.sequences
WHERE sequence_name='seq_i';

CREATE SEQUENCE seq_c CYCLE;

-- 15. cycle_option reflects CYCLE
-- begin-expected
-- columns: co
-- row: YES
-- end-expected
SELECT cycle_option AS co FROM information_schema.sequences
WHERE sequence_name='seq_c';
