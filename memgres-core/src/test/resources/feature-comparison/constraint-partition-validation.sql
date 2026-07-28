-- ============================================================================
-- Feature Comparison: FOREIGN KEY, ALTER CONSTRAINT and partition validation
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================
-- A foreign key whose referenced columns are not unique, or whose two sides
-- hold values that can never compare equal, cannot enforce anything. Storing it
-- is worse than refusing it: the constraint appears in the catalog and a reader
-- of the schema concludes the data is protected. The same applies to a
-- partition attached with bounds nothing can satisfy.
--
-- Covers: definition-time checks on FOREIGN KEY (uniqueness of the referenced
-- key, column counts, duplicates, missing/system columns, referenced relation
-- kind, type comparability, MATCH PARTIAL, ON DELETE SET NULL/DEFAULT column
-- lists), multi-table TRUNCATE of a reference pair, ALTER CONSTRAINT attribute
-- validation, and ATTACH/DETACH PARTITION bound and relation-kind validation.
-- ============================================================================

-- ============================================================================
-- Setup
-- ============================================================================

DROP TABLE IF EXISTS dcp_att, dcp_badcols, dcp_plain2, dcp_plain, dcp_rp2, dcp_hp CASCADE;
DROP TABLE IF EXISTS dcp_rp, dcp_lp CASCADE;
DROP TABLE IF EXISTS dcp_ac, dcp_ac_p CASCADE;
DROP TABLE IF EXISTS dcp_tc, dcp_tp, dcp_tself CASCADE;
DROP VIEW IF EXISTS dcp_v CASCADE;
DROP TABLE IF EXISTS dcp_fk, dcp_pk, dcp_pk1, dcp_nopk, dcp_bigpk, dcp_vpk CASCADE;
DROP TABLE IF EXISTS dcp_ok1, dcp_ok2, dcp_ok3, dcp_ok4, dcp_ok5, dcp_ok6 CASCADE;
DROP TABLE IF EXISTS dcp_self1, dcp_self2, dcp_self3 CASCADE;
DROP TABLE IF EXISTS dcp_n_c, dcp_n_p, dcp_n_compc, dcp_n_comp CASCADE;
DROP TABLE IF EXISTS dcp_n_att, dcp_n_rp, dcp_n_hp, dcp_n_lp CASCADE;

CREATE TABLE dcp_pk (ptest1 int, ptest2 text, primary key(ptest1, ptest2));
CREATE TABLE dcp_fk (ftest1 int, ftest2 text);
CREATE TABLE dcp_pk1 (id int primary key, u int unique, plain int);
CREATE TABLE dcp_nopk (a int, b text);
CREATE VIEW dcp_v AS SELECT id FROM dcp_pk1;

-- ============================================================================
-- 1. The referenced columns must carry a unique or primary key
-- ============================================================================

ALTER TABLE dcp_fk ADD FOREIGN KEY (ftest1) REFERENCES dcp_pk(ptest2);
CREATE TABLE dcp_c1 (a int REFERENCES dcp_pk1(plain));
CREATE TABLE dcp_c2 (a int, FOREIGN KEY (a) REFERENCES dcp_pk1(plain));
ALTER TABLE dcp_fk ADD FOREIGN KEY (ftest1, ftest2) REFERENCES dcp_pk(ptest1);
-- a self-reference is resolved against the table being created, and checked
CREATE TABLE dcp_self2 (a int primary key, b int REFERENCES dcp_self2(b));
CREATE TABLE dcp_self3 (a int, b int, FOREIGN KEY (b) REFERENCES dcp_self3(a));
-- a unique (not primary) key is enough
CREATE TABLE dcp_ok1 (a int REFERENCES dcp_pk1(u));
DROP TABLE dcp_ok1;
-- a self-reference to a real key is accepted
CREATE TABLE dcp_self1 (a int primary key, b int REFERENCES dcp_self1);
DROP TABLE dcp_self1;

-- ============================================================================
-- 2. The two column lists must agree, and hold no duplicates
-- ============================================================================

CREATE TABLE dcp_c3 (a int, b text, FOREIGN KEY (a, b) REFERENCES dcp_pk1(id));
CREATE TABLE dcp_c4 (a int, FOREIGN KEY (a) REFERENCES dcp_pk(ptest1, ptest2));
CREATE TABLE dcp_c5 (a int, b text, FOREIGN KEY (a, b) REFERENCES dcp_pk(ptest1, ptest1));

-- ============================================================================
-- 3. Every key column must exist and must not be a system column
-- ============================================================================

CREATE TABLE dcp_c7 (a int, FOREIGN KEY (a) REFERENCES dcp_pk1(nosuchcol));
CREATE TABLE dcp_c8 (a int REFERENCES dcp_pk1(nosuchcol));
CREATE TABLE dcp_c9 (a int, FOREIGN KEY (nosuchcol) REFERENCES dcp_pk1(id));
ALTER TABLE dcp_fk ADD FOREIGN KEY (nosuchcol) REFERENCES dcp_pk1(id);
CREATE TABLE dcp_c10 (a int, FOREIGN KEY (tableoid) REFERENCES dcp_pk1(id));
ALTER TABLE dcp_fk ADD FOREIGN KEY (ftest1) REFERENCES dcp_pk1(tableoid);

-- ============================================================================
-- 4. The referenced relation must exist, be a table, and have a primary key
-- ============================================================================

CREATE TABLE dcp_c11 (a int REFERENCES dcp_nopk);
CREATE TABLE dcp_c12 (a int, FOREIGN KEY (a) REFERENCES dcp_nopk);
CREATE TABLE dcp_c13 (a int REFERENCES dcp_nosuchtable);
CREATE TABLE dcp_c14 (a int, FOREIGN KEY (a) REFERENCES dcp_nosuchtable(x));
CREATE TABLE dcp_c15 (a int REFERENCES dcp_v);
CREATE TABLE dcp_c16 (a int, FOREIGN KEY (a) REFERENCES dcp_v(id));

-- ============================================================================
-- 5. The key columns must hold values that can compare equal
-- ============================================================================

CREATE TABLE dcp_c17 (a inet REFERENCES dcp_pk1);
CREATE TABLE dcp_c18 (a inet, FOREIGN KEY (a) REFERENCES dcp_pk1(id));
CREATE TABLE dcp_c19 (a cidr, b timestamp, FOREIGN KEY (a, b) REFERENCES dcp_pk(ptest1, ptest2));
CREATE TABLE dcp_c20 (a text, b int, FOREIGN KEY (a, b) REFERENCES dcp_pk(ptest1, ptest2));
CREATE TABLE dcp_c6 (a int, b text, FOREIGN KEY (a, a) REFERENCES dcp_pk(ptest1, ptest2));
-- widening within a family, and an implicit cast, are both fine
CREATE TABLE dcp_bigpk (id bigint primary key);
CREATE TABLE dcp_ok2 (a int REFERENCES dcp_bigpk);
DROP TABLE dcp_ok2;
CREATE TABLE dcp_vpk (id varchar(10) primary key);
CREATE TABLE dcp_ok3 (a text REFERENCES dcp_vpk);
DROP TABLE dcp_ok3;

-- ============================================================================
-- 6. MATCH PARTIAL, which PostgreSQL has never implemented
-- ============================================================================

CREATE TABLE dcp_c21 (a int, b text, FOREIGN KEY (a, b) REFERENCES dcp_pk MATCH PARTIAL);
ALTER TABLE dcp_fk ADD FOREIGN KEY (ftest1, ftest2) REFERENCES dcp_pk MATCH PARTIAL;
CREATE TABLE dcp_ok4 (a int, b text, FOREIGN KEY (a, b) REFERENCES dcp_pk MATCH FULL);
DROP TABLE dcp_ok4;

-- ============================================================================
-- 7. The ON DELETE SET NULL / SET DEFAULT column list
-- ============================================================================

CREATE TABLE dcp_c22 (a int, b int, FOREIGN KEY (a) REFERENCES dcp_pk1(id) ON DELETE SET NULL (bar));
CREATE TABLE dcp_c23 (a int, b int, FOREIGN KEY (a) REFERENCES dcp_pk1(id) ON DELETE SET NULL (b));
CREATE TABLE dcp_c24 (a int, b int, FOREIGN KEY (a) REFERENCES dcp_pk1(id) ON UPDATE SET NULL (a));
CREATE TABLE dcp_c25 (a int, b int, FOREIGN KEY (a) REFERENCES dcp_pk1(id) ON UPDATE SET DEFAULT (a));
CREATE TABLE dcp_c26 (a int, b int, FOREIGN KEY (a) REFERENCES dcp_pk1(id) ON DELETE SET DEFAULT (bar));
CREATE TABLE dcp_c27 (a int, b int, FOREIGN KEY (a) REFERENCES dcp_pk1(id) ON DELETE SET DEFAULT (b));
CREATE TABLE dcp_ok5 (a int, b int, FOREIGN KEY (a) REFERENCES dcp_pk1(id) ON DELETE SET NULL (a));
DROP TABLE dcp_ok5;
CREATE TABLE dcp_ok6 (a int, b int, FOREIGN KEY (a) REFERENCES dcp_pk1(id) ON DELETE SET DEFAULT (a));
DROP TABLE dcp_ok6;

-- ============================================================================
-- 8. TRUNCATE naming both halves of a reference pair
-- ============================================================================

CREATE TABLE dcp_tp (id int primary key);
CREATE TABLE dcp_tc (id int primary key, p int REFERENCES dcp_tp);
INSERT INTO dcp_tp VALUES (1);
INSERT INTO dcp_tc VALUES (1, 1);
TRUNCATE dcp_tp;
TRUNCATE dcp_tp, dcp_tc;
SELECT count(*) FROM dcp_tp;
SELECT count(*) FROM dcp_tc;
INSERT INTO dcp_tp VALUES (2);
INSERT INTO dcp_tc VALUES (2, 2);
TRUNCATE dcp_tc, dcp_tp;
TRUNCATE dcp_tc;
INSERT INTO dcp_tp VALUES (3);
INSERT INTO dcp_tc VALUES (3, 3);
TRUNCATE dcp_tp CASCADE;
SELECT count(*) FROM dcp_tc;
CREATE TABLE dcp_tself (id int primary key, p int REFERENCES dcp_tself);
TRUNCATE dcp_tself;

-- ============================================================================
-- 9. ALTER CONSTRAINT: only the attributes the constraint kind accepts
-- ============================================================================

CREATE TABLE dcp_ac_p (id int primary key);
CREATE TABLE dcp_ac (id int primary key, q int, p int,
    CONSTRAINT dcp_ac_fk FOREIGN KEY (p) REFERENCES dcp_ac_p,
    CONSTRAINT dcp_ac_ck CHECK (id > 0),
    CONSTRAINT dcp_ac_uq UNIQUE (q));
ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_ac_fk NOT DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_ac_fk NO INHERIT;
ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_ac_fk INHERIT;
ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_ac_fk NOT VALID;
ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_ac_fk ENFORCED NOT ENFORCED;
ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_ac_ck DEFERRABLE;
ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_ac_uq DEFERRABLE;
ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_ac_pkey DEFERRABLE;
ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_nosuch DEFERRABLE;
ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_ac_ck NOT ENFORCED;
-- the spellings a foreign key does accept
ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_ac_fk DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_ac_fk NOT DEFERRABLE;
ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_ac_fk INITIALLY DEFERRED;
SELECT conname, condeferrable, condeferred FROM pg_constraint
    WHERE conrelid = 'dcp_ac'::regclass AND conname = 'dcp_ac_fk';
ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_ac_fk NOT DEFERRABLE;
SELECT conname, condeferrable, condeferred FROM pg_constraint
    WHERE conrelid = 'dcp_ac'::regclass AND conname = 'dcp_ac_fk';
ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_ac_fk NOT ENFORCED;
ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_ac_fk ENFORCED;

-- ============================================================================
-- 10. ATTACH PARTITION: bounds that no row could satisfy
-- ============================================================================

CREATE TABLE dcp_rp (a int, b text) PARTITION BY RANGE (a);
CREATE TABLE dcp_rp1 PARTITION OF dcp_rp FOR VALUES FROM (0) TO (10);
CREATE TABLE dcp_att (a int, b text);
ALTER TABLE dcp_rp ATTACH PARTITION dcp_att FOR VALUES FROM (30) TO (20);
CREATE TABLE dcp_rev PARTITION OF dcp_rp FOR VALUES FROM (30) TO (20);
CREATE TABLE dcp_eq PARTITION OF dcp_rp FOR VALUES FROM (30) TO (30);

-- ============================================================================
-- 11. ATTACH PARTITION: bounds of the wrong kind, arity or type
-- ============================================================================

ALTER TABLE dcp_rp ATTACH PARTITION dcp_att FOR VALUES IN (100);
ALTER TABLE dcp_rp ATTACH PARTITION dcp_att FOR VALUES WITH (MODULUS 4, REMAINDER 0);
ALTER TABLE dcp_rp ATTACH PARTITION dcp_att FOR VALUES FROM (1, 2) TO (5, 6);
ALTER TABLE dcp_rp ATTACH PARTITION dcp_att FOR VALUES FROM ('abc') TO ('def');
CREATE TABLE dcp_lp (a int, b text) PARTITION BY LIST (a);
CREATE TABLE dcp_lp1 PARTITION OF dcp_lp FOR VALUES IN (1, 2);
ALTER TABLE dcp_lp ATTACH PARTITION dcp_att FOR VALUES FROM (0) TO (10);
ALTER TABLE dcp_lp ATTACH PARTITION dcp_att FOR VALUES IN ('x');
CREATE TABLE dcp_hp (a int, b text) PARTITION BY HASH (a);
ALTER TABLE dcp_hp ATTACH PARTITION dcp_att FOR VALUES FROM (0) TO (10);
ALTER TABLE dcp_hp ATTACH PARTITION dcp_att FOR VALUES IN (1);

-- ============================================================================
-- 12. ATTACH PARTITION: overlapping bounds and duplicate default partitions
-- ============================================================================

ALTER TABLE dcp_rp ATTACH PARTITION dcp_att FOR VALUES FROM (5) TO (15);
CREATE TABLE dcp_ov PARTITION OF dcp_rp FOR VALUES FROM (5) TO (15);
ALTER TABLE dcp_lp ATTACH PARTITION dcp_att FOR VALUES IN (2, 3);
CREATE TABLE dcp_lov PARTITION OF dcp_lp FOR VALUES IN (2);

-- ============================================================================
-- 13. ATTACH/DETACH PARTITION: the relations both sides must be
-- ============================================================================

CREATE TABLE dcp_plain (a int, b text);
CREATE TABLE dcp_plain2 (a int, b text);
ALTER TABLE dcp_plain ATTACH PARTITION dcp_plain2 FOR VALUES FROM (0) TO (10);
ALTER TABLE dcp_plain DETACH PARTITION dcp_plain2;
ALTER TABLE dcp_rp ATTACH PARTITION dcp_rp1 FOR VALUES FROM (20) TO (30);
CREATE TABLE dcp_rp2 (a int, b text) PARTITION BY RANGE (a);
ALTER TABLE dcp_rp2 ATTACH PARTITION dcp_rp1 FOR VALUES FROM (20) TO (30);
ALTER TABLE dcp_rp ATTACH PARTITION dcp_rp FOR VALUES FROM (20) TO (30);
ALTER TABLE dcp_rp ATTACH PARTITION dcp_v FOR VALUES FROM (20) TO (30);
CREATE TABLE dcp_badcols (a int, c text);
ALTER TABLE dcp_rp ATTACH PARTITION dcp_badcols FOR VALUES FROM (20) TO (30);
ALTER TABLE dcp_rp DETACH PARTITION dcp_att;
ALTER TABLE dcp_rp DETACH PARTITION dcp_plain;
ALTER TABLE dcp_rp DETACH PARTITION dcp_nosuch;

-- ============================================================================
-- 14. A second default partition, and a default holding a claimed row
-- ============================================================================

CREATE TABLE dcp_d1 PARTITION OF dcp_rp DEFAULT;
CREATE TABLE dcp_d2 PARTITION OF dcp_rp DEFAULT;
ALTER TABLE dcp_rp ATTACH PARTITION dcp_att DEFAULT;
INSERT INTO dcp_rp VALUES (500, 'x');
ALTER TABLE dcp_rp ATTACH PARTITION dcp_att FOR VALUES FROM (400) TO (600);
ALTER TABLE dcp_rp ATTACH PARTITION dcp_att FOR VALUES FROM (600) TO (700);
ALTER TABLE dcp_rp DETACH PARTITION dcp_att;

-- ============================================================================
-- 15. Neighbouring behaviour that must not change
-- ============================================================================

CREATE TABLE dcp_n_p (id int primary key, code text unique);
INSERT INTO dcp_n_p VALUES (1, 'a'), (2, 'b');
CREATE TABLE dcp_n_c (id int primary key, pid int REFERENCES dcp_n_p,
    code text REFERENCES dcp_n_p(code) ON DELETE CASCADE);
INSERT INTO dcp_n_c VALUES (10, 1, 'a');
INSERT INTO dcp_n_c VALUES (11, 99, 'a');
INSERT INTO dcp_n_c VALUES (12, 1, 'zz');
DELETE FROM dcp_n_p WHERE id = 1;
ALTER TABLE dcp_n_c ADD CONSTRAINT dcp_n_fk2 FOREIGN KEY (pid) REFERENCES dcp_n_p (id) ON DELETE SET NULL;
ALTER TABLE dcp_n_c ADD CONSTRAINT dcp_n_fk2 FOREIGN KEY (pid) REFERENCES dcp_n_p (id);
SELECT count(*) FROM dcp_n_c;
CREATE TABLE dcp_n_comp (a int, b text, primary key (b, a));
CREATE TABLE dcp_n_compc (x text, y int, FOREIGN KEY (x, y) REFERENCES dcp_n_comp (b, a));

CREATE TABLE dcp_n_rp (a int, b text) PARTITION BY RANGE (a);
CREATE TABLE dcp_n_rp1 PARTITION OF dcp_n_rp FOR VALUES FROM (MINVALUE) TO (10);
CREATE TABLE dcp_n_att (a int, b text);
INSERT INTO dcp_n_att VALUES (25, 'x');
ALTER TABLE dcp_n_rp ATTACH PARTITION dcp_n_att FOR VALUES FROM (20) TO (30);
INSERT INTO dcp_n_rp VALUES (5, 'lo'), (22, 'hi');
SELECT a, b FROM dcp_n_rp ORDER BY a;
ALTER TABLE dcp_n_rp DETACH PARTITION dcp_n_att;
SELECT a, b FROM dcp_n_rp ORDER BY a;
SELECT a, b FROM dcp_n_att ORDER BY a;
ALTER TABLE dcp_n_rp ATTACH PARTITION dcp_n_att FOR VALUES FROM (20) TO (30);
CREATE TABLE dcp_n_def PARTITION OF dcp_n_rp DEFAULT;
INSERT INTO dcp_n_rp VALUES (100, 'def');
SELECT a FROM dcp_n_def ORDER BY a;

CREATE TABLE dcp_n_hp (a int, b text) PARTITION BY HASH (a);
CREATE TABLE dcp_n_hp0 PARTITION OF dcp_n_hp FOR VALUES WITH (MODULUS 2, REMAINDER 0);
CREATE TABLE dcp_n_hp1 PARTITION OF dcp_n_hp FOR VALUES WITH (MODULUS 2, REMAINDER 1);
INSERT INTO dcp_n_hp VALUES (1, 'a'), (2, 'b'), (3, 'c');
SELECT count(*) FROM dcp_n_hp;

CREATE TABLE dcp_n_lp (a int, b text) PARTITION BY LIST (a);
CREATE TABLE dcp_n_lp1 PARTITION OF dcp_n_lp FOR VALUES IN (1, 2);
CREATE TABLE dcp_n_lp2 PARTITION OF dcp_n_lp FOR VALUES IN (3);
INSERT INTO dcp_n_lp VALUES (1, 'a'), (3, 'c');
INSERT INTO dcp_n_lp VALUES (9, 'z');
SELECT a FROM dcp_n_lp ORDER BY a;
TRUNCATE dcp_n_lp;
TRUNCATE dcp_n_p CASCADE;
SELECT count(*) FROM dcp_n_c;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP TABLE IF EXISTS dcp_att, dcp_badcols, dcp_plain2, dcp_plain, dcp_rp2, dcp_hp CASCADE;
DROP TABLE IF EXISTS dcp_rp, dcp_lp CASCADE;
DROP TABLE IF EXISTS dcp_ac, dcp_ac_p CASCADE;
DROP TABLE IF EXISTS dcp_tc, dcp_tp, dcp_tself CASCADE;
DROP VIEW IF EXISTS dcp_v CASCADE;
DROP TABLE IF EXISTS dcp_fk, dcp_pk, dcp_pk1, dcp_nopk, dcp_bigpk, dcp_vpk CASCADE;
DROP TABLE IF EXISTS dcp_self2, dcp_self3 CASCADE;
DROP TABLE IF EXISTS dcp_n_c, dcp_n_p, dcp_n_compc, dcp_n_comp CASCADE;
DROP TABLE IF EXISTS dcp_n_att, dcp_n_rp, dcp_n_hp, dcp_n_lp CASCADE;
