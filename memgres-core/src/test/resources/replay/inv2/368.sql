-- source: investigation-2026-08.md
-- finding: 368
-- title: DISTINCT, GROUP BY, UNION and the unique-index pre-check build their key with RowKey, which special-cases BigDecimal, byte[] and lists and then ends in Java's a
-- begin-expected
-- columns: eq:bool | n:int8
-- row: t | 1
-- rowcount: 1
-- end-expected
SELECT '1 day'::interval = '24 hours'::interval AS eq,
       count(DISTINCT x) AS n
  FROM (SELECT '1 day'::interval AS x UNION ALL SELECT '24 hours'::interval) t;
-- begin-expected
-- columns: eq:bool | n:int8
-- row: t | 1
-- rowcount: 1
-- end-expected
SELECT '2020-01-01 12:00:00+00'::timestamptz = '2020-01-01 13:00:00+01'::timestamptz AS eq,
       count(DISTINCT x) AS n
  FROM (SELECT '2020-01-01 12:00:00+00'::timestamptz AS x
        UNION ALL SELECT '2020-01-01 13:00:00+01'::timestamptz) t;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf2_rt_iv (v interval UNIQUE);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf2_rt_iv VALUES ('1 day');
-- begin-expected-error
-- sqlstate: 23505
-- message-like: duplicate key value violates unique constraint "zz_vf2_rt_iv_v_key"
-- end-expected-error
INSERT INTO zz_vf2_rt_iv VALUES ('24 hours');
-- begin-expected
-- columns: eq:bool | gt:bool
-- row: t | f
-- rowcount: 1
-- end-expected
SELECT 0.0::float8 = -0.0::float8 AS eq, 0.0::float8 > -0.0::float8 AS gt;
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(*) FROM (SELECT x FROM
  (SELECT 0.0::float8 AS x UNION ALL SELECT -0.0::float8) t GROUP BY x) g;
-- begin-expected
-- columns: count:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT count(DISTINCT x) FROM
  (SELECT 0.0::float8 AS x UNION ALL SELECT -0.0::float8) t;
