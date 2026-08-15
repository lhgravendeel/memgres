-- ============================================================================
-- A whole row and a parenthesised column in a partition key
--
-- A key element written in parentheses is an expression, and a bare name in an
-- expression is resolved the way any expression's is: a column of the relation
-- first, and failing that the relation itself, which stands for the whole row.
-- So PARTITION BY RANGE ((t)) over a table t is the whole row as the key, and
-- the catalogue reads it back with the .* PostgreSQL stores it as. Where a
-- column of the relation happens to carry the relation's name, the column
-- wins. Written bare, outside parentheses, the same name is only ever a
-- column name, and a relation name there is 42703.
--
-- The other way about, a parenthesised name that does resolve to a column is
-- read back as that column and nothing more -- pg_get_partkeydef prints a
-- plain column reference as the column. Which is why a generated column, which
-- PostgreSQL refuses in a partition key because its value is worked out after
-- the row has been routed, is refused only where the key element is read as a
-- name: ((k)) is accepted and reads back as RANGE (k), while the bare k and
-- any real expression over it are 42P17.
--
-- And VALUES is a word PostgreSQL's grammar reads as a column name, so it is
-- the parenthesis after it that has nowhere to go: both ((VALUES (1))) and
-- (VALUES (1)) are a syntax error at the parenthesis, not a refusal of a
-- sub-query.
--
-- Every value below was read off PostgreSQL 18 before it was written down.
-- ============================================================================

-- ============================================================================
-- The whole row, under each of the three strategies
-- ============================================================================
CREATE TABLE zzt4a_r (i int, k int) PARTITION BY RANGE ((zzt4a_r));

-- begin-expected
-- columns: d
-- row: RANGE ((zzt4a_r.*))
-- end-expected
SELECT pg_get_partkeydef('zzt4a_r'::regclass) AS d;

-- Attribute number 0 is how PostgreSQL records a key element that is not a
-- column of the relation.
-- begin-expected
-- columns: strat | natts | attrs
-- row: r | 1 | 0
-- end-expected
SELECT partstrat AS strat, partnatts AS natts, partattrs::text AS attrs
  FROM pg_partitioned_table WHERE partrelid = 'zzt4a_r'::regclass;

DROP TABLE zzt4a_r;

CREATE TABLE zzt4a_l (i int, k int) PARTITION BY LIST ((zzt4a_l));

-- begin-expected
-- columns: d
-- row: LIST ((zzt4a_l.*))
-- end-expected
SELECT pg_get_partkeydef('zzt4a_l'::regclass) AS d;

-- begin-expected
-- columns: strat | natts | attrs
-- row: l | 1 | 0
-- end-expected
SELECT partstrat AS strat, partnatts AS natts, partattrs::text AS attrs
  FROM pg_partitioned_table WHERE partrelid = 'zzt4a_l'::regclass;

DROP TABLE zzt4a_l;

CREATE TABLE zzt4a_hh (i int, k int) PARTITION BY HASH ((zzt4a_hh));

-- begin-expected
-- columns: d
-- row: HASH ((zzt4a_hh.*))
-- end-expected
SELECT pg_get_partkeydef('zzt4a_hh'::regclass) AS d;

-- begin-expected
-- columns: strat | natts | attrs
-- row: h | 1 | 0
-- end-expected
SELECT partstrat AS strat, partnatts AS natts, partattrs::text AS attrs
  FROM pg_partitioned_table WHERE partrelid = 'zzt4a_hh'::regclass;

DROP TABLE zzt4a_hh;

-- ----------------------------------------------------------------------------
-- In company with a column of its own
-- ----------------------------------------------------------------------------
CREATE TABLE zzt4a_m (i int, k int) PARTITION BY RANGE ((zzt4a_m), i);

-- begin-expected
-- columns: d
-- row: RANGE ((zzt4a_m.*), i)
-- end-expected
SELECT pg_get_partkeydef('zzt4a_m'::regclass) AS d;

-- begin-expected
-- columns: strat | natts | attrs
-- row: r | 2 | 0 1
-- end-expected
SELECT partstrat AS strat, partnatts AS natts, partattrs::text AS attrs
  FROM pg_partitioned_table WHERE partrelid = 'zzt4a_m'::regclass;

DROP TABLE zzt4a_m;

-- ----------------------------------------------------------------------------
-- A column of the relation carrying the relation's name takes the name back
-- ----------------------------------------------------------------------------
CREATE TABLE zzt4a_s (zzt4a_s int, k int) PARTITION BY RANGE ((zzt4a_s));

-- begin-expected
-- columns: d
-- row: RANGE (zzt4a_s)
-- end-expected
SELECT pg_get_partkeydef('zzt4a_s'::regclass) AS d;

-- The column's own attribute number, not 0.
-- begin-expected
-- columns: strat | natts | attrs
-- row: r | 1 | 1
-- end-expected
SELECT partstrat AS strat, partnatts AS natts, partattrs::text AS attrs
  FROM pg_partitioned_table WHERE partrelid = 'zzt4a_s'::regclass;

DROP TABLE zzt4a_s;

-- ----------------------------------------------------------------------------
-- A partitioned table keyed on its whole row takes partitions like any other
-- ----------------------------------------------------------------------------
CREATE TABLE zzt4a_w (i int, k int) PARTITION BY LIST ((zzt4a_w));
CREATE TABLE zzt4a_w_1 PARTITION OF zzt4a_w FOR VALUES IN ('(1,1)');

-- begin-expected
-- columns: relname | rk
-- row: zzt4a_w | p
-- row: zzt4a_w_1 | r
-- end-expected
SELECT relname, relkind::text AS rk FROM pg_class
  WHERE relname LIKE 'zzt4a\_w%' ORDER BY relname;

DROP TABLE zzt4a_w;

-- ----------------------------------------------------------------------------
-- Written bare, the relation's name is only ever a column name
-- ----------------------------------------------------------------------------

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "zzt4a_p" named in partition key does not exist
-- end-expected-error
CREATE TABLE zzt4a_p (i int, k int) PARTITION BY RANGE (zzt4a_p);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "zzt4a_q" named in partition key does not exist
-- end-expected-error
CREATE TABLE zzt4a_q (i int, k int) PARTITION BY LIST (zzt4a_q);

-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "zzt4a_hb" named in partition key does not exist
-- end-expected-error
CREATE TABLE zzt4a_hb (i int, k int) PARTITION BY HASH (zzt4a_hb);

-- And a parenthesised name that is neither a column nor the relation is
-- reported the way any expression's unknown name is.
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "zzt4a_nosuch" does not exist
-- end-expected-error
CREATE TABLE zzt4a_n (i int, k int) PARTITION BY RANGE ((zzt4a_nosuch));

-- ============================================================================
-- A parenthesised column reduces to the column
-- ============================================================================
CREATE TABLE zzt4a_gh (i int, k int) PARTITION BY RANGE ((i));

-- begin-expected
-- columns: d
-- row: RANGE (i)
-- end-expected
SELECT pg_get_partkeydef('zzt4a_gh'::regclass) AS d;

-- begin-expected
-- columns: strat | natts | attrs
-- row: r | 1 | 1
-- end-expected
SELECT partstrat AS strat, partnatts AS natts, partattrs::text AS attrs
  FROM pg_partitioned_table WHERE partrelid = 'zzt4a_gh'::regclass;

DROP TABLE zzt4a_gh;

CREATE TABLE zzt4a_gi (i int, k int) PARTITION BY RANGE (i, (k));

-- begin-expected
-- columns: d
-- row: RANGE (i, k)
-- end-expected
SELECT pg_get_partkeydef('zzt4a_gi'::regclass) AS d;

DROP TABLE zzt4a_gi;

-- ----------------------------------------------------------------------------
-- Written bare, or inside a real expression, a generated column is refused
-- ----------------------------------------------------------------------------

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: cannot use generated column in partition key
-- end-expected-error
CREATE TABLE zzt4a_gd (i int, k int GENERATED ALWAYS AS (i * 2) STORED)
  PARTITION BY RANGE (k);

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: cannot use generated column in partition key
-- end-expected-error
CREATE TABLE zzt4a_ge (i int, k int GENERATED ALWAYS AS (i * 2) VIRTUAL)
  PARTITION BY RANGE (k);

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: cannot use generated column in partition key
-- end-expected-error
CREATE TABLE zzt4a_gf (i int, k int GENERATED ALWAYS AS (i * 2) STORED)
  PARTITION BY RANGE ((k + 1));

-- The parentheses decide nothing. An element that comes back to a plain column
-- is that column, and a generated one is refused whichever way it was written,
-- under every strategy, and stored or virtual alike.
-- begin-expected-error
-- sqlstate: 42P17
-- message-like: cannot use generated column in partition key
-- end-expected-error
CREATE TABLE zzt4a_ga (i int, k int GENERATED ALWAYS AS (i * 2) STORED)
  PARTITION BY RANGE ((k));

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: cannot use generated column in partition key
-- end-expected-error
CREATE TABLE zzt4a_gb (i int, k int GENERATED ALWAYS AS (i * 2) VIRTUAL)
  PARTITION BY LIST ((k));

-- begin-expected-error
-- sqlstate: 42P17
-- message-like: cannot use generated column in partition key
-- end-expected-error
CREATE TABLE zzt4a_gc (i int, k int GENERATED ALWAYS AS (i * 2) STORED)
  PARTITION BY HASH ((k));

-- One ordinary column beside it does not save it either.
-- begin-expected-error
-- sqlstate: 42P17
-- message-like: cannot use generated column in partition key
-- end-expected-error
CREATE TABLE zzt4a_gg (i int, k int GENERATED ALWAYS AS (i * 2) STORED)
  PARTITION BY RANGE (i, (k));

-- ============================================================================
-- VALUES reads as a column name, so the parenthesis after it is the fault
-- ============================================================================

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "("
-- end-expected-error
CREATE TABLE zzt4a_v1 (i int, k int) PARTITION BY RANGE ((VALUES (1)));

-- One pair of parentheses, and it is the same fault.
-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "("
-- end-expected-error
CREATE TABLE zzt4a_v2 (i int, k int) PARTITION BY RANGE (VALUES (1));

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "("
-- end-expected-error
CREATE TABLE zzt4a_v3 (i int, k int) PARTITION BY LIST ((VALUES (1)));

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "("
-- end-expected-error
CREATE TABLE zzt4a_v4 (i int, k int) PARTITION BY HASH ((VALUES (1)));

-- Standing inside an expression, where the element's own parentheses have
-- already been spent, it is refused for the sub-query it is.
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in partition key expression
-- end-expected-error
CREATE TABLE zzt4a_v5 (i int, k int) PARTITION BY RANGE ((i + (VALUES (1))));

-- The other sub-query spellings are unchanged by any of this.
-- begin-expected-error
-- sqlstate: 0A000
-- message-like: cannot use subquery in partition key expression
-- end-expected-error
CREATE TABLE zzt4a_v6 (i int, k int) PARTITION BY RANGE (((SELECT 1)));

-- begin-expected-error
-- sqlstate: 42601
-- message-like: syntax error at or near "SELECT"
-- end-expected-error
CREATE TABLE zzt4a_v7 (i int, k int) PARTITION BY RANGE ((SELECT 1));

-- Nothing above was left behind.
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_class WHERE relname LIKE 'zzt4a\_%';
