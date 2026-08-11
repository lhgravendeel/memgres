-- ============================================================================
-- An index declared on a partitioned table reaches every relation beneath it
-- ============================================================================

CREATE TABLE zzt9x_h (i int, s text) PARTITION BY RANGE (i);
CREATE TABLE zzt9x_h_0 PARTITION OF zzt9x_h FOR VALUES FROM (0) TO (100) PARTITION BY RANGE (i);
CREATE TABLE zzt9x_h_0_0 PARTITION OF zzt9x_h_0 FOR VALUES FROM (0) TO (10) PARTITION BY RANGE (i);
CREATE TABLE zzt9x_h_0_0_0 PARTITION OF zzt9x_h_0_0 FOR VALUES FROM (0) TO (5);
CREATE INDEX zzt9x_h_idx ON zzt9x_h (s);

-- stmt: the leaf three levels down carries a copy, named after the relation it indexes
-- begin-expected
-- columns: tablename | indexname
-- row: zzt9x_h | zzt9x_h_idx
-- row: zzt9x_h_0 | zzt9x_h_0_s_idx
-- row: zzt9x_h_0_0 | zzt9x_h_0_0_s_idx
-- row: zzt9x_h_0_0_0 | zzt9x_h_0_0_0_s_idx
-- end-expected
SELECT tablename, indexname FROM pg_indexes
 WHERE tablename LIKE 'zzt9x_h%' ORDER BY 1, 2;

-- stmt: only the leaf's copy is an ordinary index; every index over a partitioned relation is 'I'
-- begin-expected
-- columns: relname | relkind
-- row: zzt9x_h | p
-- row: zzt9x_h_0 | p
-- row: zzt9x_h_0_0 | p
-- row: zzt9x_h_0_0_0 | r
-- row: zzt9x_h_0_0_0_s_idx | i
-- row: zzt9x_h_0_0_s_idx | I
-- row: zzt9x_h_0_s_idx | I
-- row: zzt9x_h_idx | I
-- end-expected
SELECT c.relname, c.relkind FROM pg_class c
 WHERE c.relname LIKE 'zzt9x_h%' ORDER BY 1;

-- stmt: each copy is recorded as a child of the copy one level up
-- begin-expected
-- columns: parent | child
-- row: zzt9x_h | zzt9x_h_0
-- row: zzt9x_h_0 | zzt9x_h_0_0
-- row: zzt9x_h_0_0 | zzt9x_h_0_0_0
-- row: zzt9x_h_0_0_s_idx | zzt9x_h_0_0_0_s_idx
-- row: zzt9x_h_0_s_idx | zzt9x_h_0_0_s_idx
-- row: zzt9x_h_idx | zzt9x_h_0_s_idx
-- end-expected
SELECT pc.relname AS parent, cc.relname AS child FROM pg_inherits inh
 JOIN pg_class pc ON pc.oid = inh.inhparent
 JOIN pg_class cc ON cc.oid = inh.inhrelid
 WHERE cc.relname LIKE 'zzt9x_h%' ORDER BY 1, 2;

DROP TABLE zzt9x_h;

-- ============================================================================
-- ATTACH indexes the incoming table's own leaves; DETACH withdraws only the link
-- ============================================================================

CREATE TABLE zzt9x_k (i int, s text) PARTITION BY RANGE (i);
CREATE INDEX zzt9x_k_idx ON zzt9x_k (s);
CREATE TABLE zzt9x_k_0 (i int, s text) PARTITION BY RANGE (i);
CREATE TABLE zzt9x_k_0_0 PARTITION OF zzt9x_k_0 FOR VALUES FROM (0) TO (10);
ALTER TABLE zzt9x_k ATTACH PARTITION zzt9x_k_0 FOR VALUES FROM (0) TO (100);

-- stmt: the attached table's existing leaf is indexed too
-- begin-expected
-- columns: tablename | indexname
-- row: zzt9x_k | zzt9x_k_idx
-- row: zzt9x_k_0 | zzt9x_k_0_s_idx
-- row: zzt9x_k_0_0 | zzt9x_k_0_0_s_idx
-- end-expected
SELECT tablename, indexname FROM pg_indexes
 WHERE tablename LIKE 'zzt9x_k%' ORDER BY 1, 2;

-- begin-expected
-- columns: parent | child
-- row: zzt9x_k | zzt9x_k_0
-- row: zzt9x_k_0 | zzt9x_k_0_0
-- row: zzt9x_k_0_s_idx | zzt9x_k_0_0_s_idx
-- row: zzt9x_k_idx | zzt9x_k_0_s_idx
-- end-expected
SELECT pc.relname AS parent, cc.relname AS child FROM pg_inherits inh
 JOIN pg_class pc ON pc.oid = inh.inhparent
 JOIN pg_class cc ON cc.oid = inh.inhrelid
 WHERE cc.relname LIKE 'zzt9x_k%' ORDER BY 1, 2;

ALTER TABLE zzt9x_k DETACH PARTITION zzt9x_k_0;

-- stmt: the detached table keeps every index it was given
-- begin-expected
-- columns: tablename | indexname
-- row: zzt9x_k | zzt9x_k_idx
-- row: zzt9x_k_0 | zzt9x_k_0_s_idx
-- row: zzt9x_k_0_0 | zzt9x_k_0_0_s_idx
-- end-expected
SELECT tablename, indexname FROM pg_indexes
 WHERE tablename LIKE 'zzt9x_k%' ORDER BY 1, 2;

-- stmt: but the link to the index it was detached from is gone, and only that one
-- begin-expected
-- columns: parent | child
-- row: zzt9x_k_0 | zzt9x_k_0_0
-- row: zzt9x_k_0_s_idx | zzt9x_k_0_0_s_idx
-- end-expected
SELECT pc.relname AS parent, cc.relname AS child FROM pg_inherits inh
 JOIN pg_class pc ON pc.oid = inh.inhparent
 JOIN pg_class cc ON cc.oid = inh.inhrelid
 WHERE cc.relname LIKE 'zzt9x_k%' ORDER BY 1, 2;

DROP TABLE zzt9x_k;
DROP TABLE zzt9x_k_0;

-- ============================================================================
-- Every copy is named for the relation it indexes, and carries the whole key
-- ============================================================================

CREATE TABLE zzt9x_p (i int, s text, t text) PARTITION BY RANGE (i);
CREATE TABLE zzt9x_p_0 PARTITION OF zzt9x_p FOR VALUES FROM (0) TO (10) PARTITION BY RANGE (i);
CREATE TABLE zzt9x_p_0_0 PARTITION OF zzt9x_p_0 FOR VALUES FROM (0) TO (5);
CREATE INDEX ON zzt9x_p (s) INCLUDE (t);
CREATE INDEX ON zzt9x_p (s) WHERE i > 1;
CREATE INDEX ON zzt9x_p ((s || t));

-- stmt: an INCLUDE column takes part in the derived name at every level, and an
-- expression key contributes "expr" rather than its own text
-- begin-expected
-- columns: tablename | indexname
-- row: zzt9x_p | zzt9x_p_expr_idx
-- row: zzt9x_p | zzt9x_p_s_idx
-- row: zzt9x_p | zzt9x_p_s_t_idx
-- row: zzt9x_p_0 | zzt9x_p_0_expr_idx
-- row: zzt9x_p_0 | zzt9x_p_0_s_idx
-- row: zzt9x_p_0 | zzt9x_p_0_s_t_idx
-- row: zzt9x_p_0_0 | zzt9x_p_0_0_expr_idx
-- row: zzt9x_p_0_0 | zzt9x_p_0_0_s_idx
-- row: zzt9x_p_0_0 | zzt9x_p_0_0_s_t_idx
-- end-expected
SELECT tablename, indexname FROM pg_indexes
 WHERE tablename LIKE 'zzt9x_p%' ORDER BY 1, 2;

DROP TABLE zzt9x_p;

-- stmt: and the copies land in the schema of the relation they index
CREATE SCHEMA zzt9x_sc;
SET search_path = zzt9x_sc;
CREATE TABLE zzt9x_w (i int, s text) PARTITION BY RANGE (i);
CREATE TABLE zzt9x_w_0 PARTITION OF zzt9x_w FOR VALUES FROM (0) TO (100) PARTITION BY RANGE (i);
CREATE TABLE zzt9x_w_0_0 PARTITION OF zzt9x_w_0 FOR VALUES FROM (0) TO (10);
CREATE INDEX ON zzt9x_w (s);

-- begin-expected
-- columns: schemaname | tablename | indexname
-- row: zzt9x_sc | zzt9x_w | zzt9x_w_s_idx
-- row: zzt9x_sc | zzt9x_w_0 | zzt9x_w_0_s_idx
-- row: zzt9x_sc | zzt9x_w_0_0 | zzt9x_w_0_0_s_idx
-- end-expected
SELECT schemaname, tablename, indexname FROM pg_indexes
 WHERE schemaname = 'zzt9x_sc' ORDER BY 1, 2, 3;

RESET search_path;
DROP SCHEMA zzt9x_sc CASCADE;

-- ============================================================================
-- CREATE INDEX with no name derives the name PostgreSQL derives
-- ============================================================================

CREATE TABLE zzt9x_n (a int, b int, c text);
CREATE INDEX ON zzt9x_n (a);
CREATE INDEX ON zzt9x_n (a);
CREATE INDEX ON zzt9x_n ((a+b));
CREATE INDEX ON zzt9x_n ((a+b));
CREATE INDEX ON zzt9x_n ((a*2), (b*3));
CREATE INDEX ON zzt9x_n (a, (b+1));
CREATE INDEX ON zzt9x_n (lower(c));
CREATE INDEX ON zzt9x_n (a, b);
CREATE UNIQUE INDEX ON zzt9x_n (c);
CREATE INDEX ON zzt9x_n (a) WHERE b > 0;
CREATE INDEX ON zzt9x_n (a, a);
CREATE INDEX ON zzt9x_n (b) INCLUDE (c);
CREATE INDEX ON zzt9x_n (a DESC NULLS FIRST);

-- stmt: <table>_<key name>..._idx, numbered from 1 on the second relation of that name,
-- and a repeated key name numbered from 1 within one index
-- begin-expected
-- columns: indexname
-- row: zzt9x_n_a_a1_idx
-- row: zzt9x_n_a_b_idx
-- row: zzt9x_n_a_expr_idx
-- row: zzt9x_n_a_idx
-- row: zzt9x_n_a_idx1
-- row: zzt9x_n_a_idx2
-- row: zzt9x_n_a_idx3
-- row: zzt9x_n_b_c_idx
-- row: zzt9x_n_c_idx
-- row: zzt9x_n_expr_expr1_idx
-- row: zzt9x_n_expr_idx
-- row: zzt9x_n_expr_idx1
-- row: zzt9x_n_lower_idx
-- end-expected
SELECT indexname FROM pg_indexes WHERE tablename = 'zzt9x_n' ORDER BY indexname;

-- stmt: every one of them is a relation of its own
-- begin-expected
-- columns: cnt
-- row: 13
-- end-expected
SELECT count(*)::int AS cnt FROM pg_class WHERE relname LIKE 'zzt9x_n%' AND relkind = 'i';

-- stmt: and the unnamed UNIQUE one is unique, under the derived name rather than <table>_unique
-- begin-expected
-- columns: cnt
-- row: 1
-- end-expected
SELECT count(*)::int AS cnt FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
 WHERE c.relname = 'zzt9x_n_c_idx' AND i.indisunique;

DROP TABLE zzt9x_n;

-- begin-expected
-- columns: cnt
-- row: 0
-- end-expected
SELECT count(*)::int AS cnt FROM pg_class WHERE relname LIKE 'zzt9x_n%' AND relkind = 'i';

-- stmt: a table, a view and a sequence each hold the name against the index
CREATE TABLE zzt9x_q (a int, b int, c int);
CREATE TABLE zzt9x_q_a_idx (x int);
CREATE VIEW zzt9x_q_b_idx AS SELECT 1 AS x;
CREATE SEQUENCE zzt9x_q_c_idx;
CREATE INDEX ON zzt9x_q (a);
CREATE INDEX ON zzt9x_q (b);
CREATE INDEX ON zzt9x_q (c);

-- begin-expected
-- columns: indexname
-- row: zzt9x_q_a_idx1
-- row: zzt9x_q_b_idx1
-- row: zzt9x_q_c_idx1
-- end-expected
SELECT indexname FROM pg_indexes WHERE tablename = 'zzt9x_q' ORDER BY indexname;

DROP TABLE zzt9x_q;
DROP TABLE zzt9x_q_a_idx;
DROP VIEW zzt9x_q_b_idx;
DROP SEQUENCE zzt9x_q_c_idx;

-- ============================================================================
-- What each kind of index key contributes to the name
-- ============================================================================

CREATE TABLE zzt9x_e (a int, b int, c text, d text[]);
CREATE INDEX ON zzt9x_e ((a));
CREATE INDEX ON zzt9x_e (greatest(a,b));
CREATE INDEX ON zzt9x_e (least(a,b));
CREATE INDEX ON zzt9x_e (nullif(a,b));
CREATE INDEX ON zzt9x_e ((ARRAY[a,b]));
CREATE INDEX ON zzt9x_e ((-a));
CREATE INDEX ON zzt9x_e ((a IS NULL));
CREATE INDEX ON zzt9x_e ((c LIKE 'x%'));
CREATE INDEX ON zzt9x_e ((case when a>0 then lower(c) else upper(c) end));
CREATE INDEX ON zzt9x_e (((a)::text));
CREATE INDEX ON zzt9x_e ((abs(a)::text));
CREATE INDEX ON zzt9x_e (c COLLATE "C");
CREATE INDEX ON zzt9x_e (coalesce(a,0));

-- stmt: a column contributes its own name, a function call its function name, a cast the
-- name of what it casts, a COLLATE clause the name of what it collates, anything else "expr"
-- begin-expected
-- columns: indexname
-- row: zzt9x_e_a_idx
-- row: zzt9x_e_a_idx1
-- row: zzt9x_e_abs_idx
-- row: zzt9x_e_array_idx
-- row: zzt9x_e_c_idx
-- row: zzt9x_e_coalesce_idx
-- row: zzt9x_e_expr_idx
-- row: zzt9x_e_expr_idx1
-- row: zzt9x_e_expr_idx2
-- row: zzt9x_e_greatest_idx
-- row: zzt9x_e_least_idx
-- row: zzt9x_e_nullif_idx
-- row: zzt9x_e_upper_idx
-- end-expected
SELECT indexname FROM pg_indexes WHERE tablename = 'zzt9x_e' ORDER BY indexname;

DROP TABLE zzt9x_e;

CREATE TABLE zzt9x_cc (a int, b int, c text);
CREATE INDEX ON zzt9x_cc (((a+b)::text));
CREATE INDEX ON zzt9x_cc (((a+b)::bigint));
CREATE INDEX ON zzt9x_cc ((CASE WHEN a>0 THEN 1 END));
CREATE INDEX ON zzt9x_cc ((CASE WHEN a>0 THEN c::text ELSE c END));
CREATE INDEX ON zzt9x_cc (((a+b)::text::int));
CREATE INDEX ON zzt9x_cc ((('x')::text));
CREATE INDEX ON zzt9x_cc ((c::varchar(10)));

-- stmt: a cast over something with no name of its own is named after the type, spelled the
-- way the catalogue spells it
-- begin-expected
-- columns: indexname
-- row: zzt9x_cc_c_idx
-- row: zzt9x_cc_c_idx1
-- row: zzt9x_cc_case_idx
-- row: zzt9x_cc_int4_idx
-- row: zzt9x_cc_int8_idx
-- row: zzt9x_cc_text_idx
-- row: zzt9x_cc_text_idx1
-- end-expected
SELECT indexname FROM pg_indexes WHERE tablename = 'zzt9x_cc' ORDER BY indexname;

DROP TABLE zzt9x_cc;

CREATE TABLE zzt9x_ty (a int, b int);
CREATE INDEX ON zzt9x_ty (((a+b)::smallint));
CREATE INDEX ON zzt9x_ty (((a+b)::real));
CREATE INDEX ON zzt9x_ty (((a+b)::double precision));
CREATE INDEX ON zzt9x_ty (((a+b)::decimal));
CREATE INDEX ON zzt9x_ty (((a+b)::boolean));
CREATE INDEX ON zzt9x_ty (((a+b)::char(3)));
CREATE INDEX ON zzt9x_ty (((a+b)::varchar));

-- begin-expected
-- columns: indexname
-- row: zzt9x_ty_bool_idx
-- row: zzt9x_ty_bpchar_idx
-- row: zzt9x_ty_float4_idx
-- row: zzt9x_ty_float8_idx
-- row: zzt9x_ty_int2_idx
-- row: zzt9x_ty_numeric_idx
-- row: zzt9x_ty_varchar_idx
-- end-expected
SELECT indexname FROM pg_indexes WHERE tablename = 'zzt9x_ty' ORDER BY indexname;

DROP TABLE zzt9x_ty;

-- stmt: a quoted column keeps its own spelling, spaces and all
CREATE TABLE zzt9x_ix ("MixEd" int, "with space" int);
CREATE INDEX ON zzt9x_ix ("MixEd");
CREATE INDEX ON zzt9x_ix ("with space");

-- begin-expected
-- columns: indexname
-- row: zzt9x_ix_MixEd_idx
-- row: zzt9x_ix_with space_idx
-- end-expected
SELECT indexname FROM pg_indexes WHERE tablename = 'zzt9x_ix' ORDER BY indexname;

DROP TABLE zzt9x_ix;

-- ============================================================================
-- A derived name is cut back to fit a relation name
-- ============================================================================

CREATE TABLE zzt9x_txxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx (a int, cyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy int, dzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz int);
CREATE INDEX ON zzt9x_txxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx (a);
CREATE INDEX ON zzt9x_txxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx (a);
CREATE INDEX ON zzt9x_txxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx (a);
CREATE INDEX ON zzt9x_txxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx (cyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy);
CREATE INDEX ON zzt9x_txxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx (cyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy, dzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz);

-- stmt: the parts are cut back until the whole name fits in 63 bytes, and the number that
-- separates it from an existing relation of that name is added after the cut
-- begin-expected
-- columns: indexname | len
-- row: zzt9x_txxxxxxxxxxxxxxxxxxxxxx_cyyyyyyyyyyyyyyyyyyyyyyyyyyy_idx1 | 63
-- row: zzt9x_txxxxxxxxxxxxxxxxxxxxxx_cyyyyyyyyyyyyyyyyyyyyyyyyyyyy_idx | 63
-- row: zzt9x_txxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx_a_idx1 | 63
-- row: zzt9x_txxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx_a_idx2 | 63
-- row: zzt9x_txxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx_a_idx | 63
-- end-expected
SELECT indexname, length(indexname) AS len FROM pg_indexes
 WHERE tablename = 'zzt9x_txxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' ORDER BY indexname;

DROP TABLE zzt9x_txxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx;

CREATE TABLE zzt9x_s (a int, cyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy int, dzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz int);
CREATE INDEX ON zzt9x_s (cyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy);
CREATE INDEX ON zzt9x_s (cyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy);
CREATE INDEX ON zzt9x_s (cyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy, dzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz);

-- stmt: the two-column name is cut on the underscore between the columns, so it carries a
-- double underscore
-- begin-expected
-- columns: indexname | len
-- row: zzt9x_s_cyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy__idx | 63
-- row: zzt9x_s_cyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy_idx | 62
-- row: zzt9x_s_cyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy_idx1 | 63
-- end-expected
SELECT indexname, length(indexname) AS len FROM pg_indexes
 WHERE tablename = 'zzt9x_s' ORDER BY indexname;

DROP TABLE zzt9x_s;

-- ============================================================================
-- The derived name is the name the errors report
-- ============================================================================

CREATE TABLE zzt9x_u (a int, b int);
INSERT INTO zzt9x_u VALUES (1,1),(1,2);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: could not create unique index "zzt9x_u_a_idx"
-- end-expected-error
CREATE UNIQUE INDEX ON zzt9x_u (a);

CREATE UNIQUE INDEX ON zzt9x_u (b);

-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "zzt9x_u_b_idx"
-- end-expected-error
INSERT INTO zzt9x_u VALUES (3,2);

-- stmt: the index the refused statement named was not left behind
-- begin-expected
-- columns: indexname
-- row: zzt9x_u_b_idx
-- end-expected
SELECT indexname FROM pg_indexes WHERE tablename = 'zzt9x_u' ORDER BY indexname;

DROP TABLE zzt9x_u;
