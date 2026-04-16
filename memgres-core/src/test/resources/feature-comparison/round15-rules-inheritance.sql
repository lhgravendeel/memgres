-- ============================================================================
-- Feature Comparison: Round 15 — Rules, inheritance, ONLY, CHECK NO INHERIT
-- Target: PostgreSQL 18 vs Memgres
-- ============================================================================

DROP SCHEMA IF EXISTS r15_ri CASCADE;
CREATE SCHEMA r15_ri;
SET search_path = r15_ri, public;

-- ============================================================================
-- SECTION A: CREATE RULE DO INSTEAD / DO ALSO
-- ============================================================================

CREATE TABLE r15_r_a (id int);
CREATE TABLE r15_r_b (id int);
CREATE RULE r15_rule_redir AS ON INSERT TO r15_r_a
  DO INSTEAD INSERT INTO r15_r_b VALUES (NEW.id);

INSERT INTO r15_r_a VALUES (1);

-- 1. DO INSTEAD redirects
-- begin-expected
-- columns: a, b
-- row: 0, 1
-- end-expected
SELECT (SELECT count(*)::int FROM r15_r_a) AS a,
       (SELECT count(*)::int FROM r15_r_b) AS b;

CREATE TABLE r15_ra (id int);
CREATE TABLE r15_rb (id int);
CREATE RULE r15_rule_also AS ON INSERT TO r15_ra
  DO ALSO INSERT INTO r15_rb VALUES (NEW.id);

INSERT INTO r15_ra VALUES (1);

-- 2. DO ALSO writes both
-- begin-expected
-- columns: a, b
-- row: 1, 1
-- end-expected
SELECT (SELECT count(*)::int FROM r15_ra) AS a,
       (SELECT count(*)::int FROM r15_rb) AS b;

-- ============================================================================
-- SECTION B: pg_rules view
-- ============================================================================

CREATE TABLE r15_pr (id int);
CREATE TABLE r15_pr_dest (id int);
CREATE RULE r15_rule_pr AS ON INSERT TO r15_pr
  DO INSTEAD INSERT INTO r15_pr_dest VALUES (NEW.id);

-- 3. pg_rules shows the rule
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM pg_rules WHERE rulename='r15_rule_pr';

-- 4. pg_rules standard columns
SELECT schemaname, tablename, rulename, definition
  FROM pg_rules WHERE rulename='r15_rule_pr';

-- ============================================================================
-- SECTION C: INHERITS and pg_inherits
-- ============================================================================

CREATE TABLE r15_parent (a int);
CREATE TABLE r15_child () INHERITS (r15_parent);

-- 5. pg_inherits populated
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM pg_inherits
  WHERE inhrelid = 'r15_child'::regclass::oid
    AND inhparent = 'r15_parent'::regclass::oid;

-- ============================================================================
-- SECTION D: ONLY qualifier
-- ============================================================================

CREATE TABLE r15_pr_o (a int);
CREATE TABLE r15_ch_o () INHERITS (r15_pr_o);
INSERT INTO r15_pr_o VALUES (1);
INSERT INTO r15_ch_o VALUES (2);

-- 6. All rows
-- begin-expected
-- columns: c
-- row: 2
-- end-expected
SELECT count(*)::int AS c FROM r15_pr_o;

-- 7. ONLY excludes child
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM ONLY r15_pr_o;

-- 8. UPDATE ONLY skips child
CREATE TABLE r15_pu_o (a int, tag text);
CREATE TABLE r15_cu_o () INHERITS (r15_pu_o);
INSERT INTO r15_pu_o VALUES (1, 'p');
INSERT INTO r15_cu_o VALUES (1, 'c');

UPDATE ONLY r15_pu_o SET tag='TAGGED' WHERE a=1;

-- begin-expected
-- columns: parent_tagged, child_tagged
-- row: 1, 0
-- end-expected
SELECT (SELECT count(*)::int FROM ONLY r15_pu_o WHERE tag='TAGGED') AS parent_tagged,
       (SELECT count(*)::int FROM r15_cu_o WHERE tag='TAGGED') AS child_tagged;

-- ============================================================================
-- SECTION E: ALTER TABLE INHERIT / NO INHERIT
-- ============================================================================

CREATE TABLE r15_ai_par (a int);
CREATE TABLE r15_ai_ch (a int);
ALTER TABLE r15_ai_ch INHERIT r15_ai_par;

-- 9. ALTER adds inheritance
-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM pg_inherits
  WHERE inhrelid = 'r15_ai_ch'::regclass::oid
    AND inhparent = 'r15_ai_par'::regclass::oid;

CREATE TABLE r15_ni_par (a int);
CREATE TABLE r15_ni_ch () INHERITS (r15_ni_par);
ALTER TABLE r15_ni_ch NO INHERIT r15_ni_par;

-- 10. NO INHERIT removes inheritance
-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::int AS c FROM pg_inherits
  WHERE inhrelid = 'r15_ni_ch'::regclass::oid
    AND inhparent = 'r15_ni_par'::regclass::oid;

-- ============================================================================
-- SECTION F: CHECK NO INHERIT
-- ============================================================================

CREATE TABLE r15_cn_par (a int CHECK (a > 0) NO INHERIT);
CREATE TABLE r15_cn_ch () INHERITS (r15_cn_par);

-- 11. Parent rejects negative
-- begin-expected-error
-- message-like: check
-- end-expected-error
INSERT INTO r15_cn_par VALUES (-1);

-- 12. Child accepts negative (no inherit)
INSERT INTO r15_cn_ch VALUES (-1);

-- 13. pg_constraint.connoinherit flag
CREATE TABLE r15_noin (a int CHECK (a > 0) NO INHERIT);

-- begin-expected
-- columns: c
-- row: 1
-- end-expected
SELECT count(*)::int AS c FROM pg_constraint c
  JOIN pg_class r ON c.conrelid = r.oid
  WHERE r.relname='r15_noin' AND c.connoinherit;

-- ============================================================================
-- SECTION G: DROP RULE
-- ============================================================================

CREATE TABLE r15_dr (id int);
CREATE TABLE r15_dr_dest (id int);
CREATE RULE r15_rule_dr AS ON INSERT TO r15_dr
  DO INSTEAD INSERT INTO r15_dr_dest VALUES (NEW.id);
DROP RULE r15_rule_dr ON r15_dr;

-- 14. Dropped
-- begin-expected
-- columns: c
-- row: 0
-- end-expected
SELECT count(*)::int AS c FROM pg_rules WHERE rulename='r15_rule_dr';
