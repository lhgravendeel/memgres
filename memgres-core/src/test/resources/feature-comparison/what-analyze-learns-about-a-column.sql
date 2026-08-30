CREATE TABLE zg_a (id int, v text, f float8, b bool, n int);
INSERT INTO zg_a SELECT g, 'x'||(g%7), g/2.0, g%2=0, CASE WHEN g%10=0 THEN NULL ELSE g%3 END FROM generate_series(1,300) g;
ANALYZE zg_a;
-- begin-expected
-- columns: a | null_frac | avg_width | n_distinct | correlation
-- row: b | 0 | 1 | 2 | 0.50498337
-- row: f | 0 | 8 | -1 | 1
-- row: id | 0 | 4 | -1 | 1
-- row: n | 0.1 | 4 | 3 | 0.33329675
-- row: v | 0 | 3 | 7 | 0.15719064
-- end-expected
SELECT attname::text AS a, null_frac, avg_width, n_distinct, correlation FROM pg_stats WHERE tablename='zg_a' ORDER BY 1;
-- begin-expected
-- columns: a | mcv | f
-- row: b | {f,t} | {0.5,0.5}
-- row: f | NULL | NULL
-- row: id | NULL | NULL
-- row: n | {0,1,2} | {0.3,0.3,0.3}
-- row: v | {x1,x2,x3,x4,x5,x6,x0} | {0.14333333,0.14333333,0.14333333,0.14333333,0.14333333,0.14333333,0.14}
-- end-expected
SELECT attname::text AS a, most_common_vals::text AS mcv, most_common_freqs::text AS f FROM pg_stats WHERE tablename='zg_a' ORDER BY 1;
-- begin-expected
-- columns: a | hb
-- row: b | f
-- row: f | t
-- row: id | t
-- row: n | f
-- row: v | f
-- end-expected
SELECT attname::text AS a, histogram_bounds IS NOT NULL AS hb FROM pg_stats WHERE tablename='zg_a' ORDER BY 1;
-- begin-expected
-- columns: staattnum | stanullfrac | stawidth | stadistinct | stakind1 | stakind2 | stakind3
-- row: 1 | 0 | 4 | -1 | 2 | 3 | 0
-- row: 2 | 0 | 3 | 7 | 1 | 3 | 0
-- row: 3 | 0 | 8 | -1 | 2 | 3 | 0
-- row: 4 | 0 | 1 | 2 | 1 | 3 | 0
-- row: 5 | 0.1 | 4 | 3 | 1 | 3 | 0
-- end-expected
SELECT staattnum, stanullfrac, stawidth, stadistinct, stakind1, stakind2, stakind3 FROM pg_statistic WHERE starelid='zg_a'::regclass ORDER BY staattnum;
CREATE TABLE zg_b (t text);
INSERT INTO zg_b VALUES ('short'), (repeat('y', 200));
ANALYZE zg_b;
-- begin-expected
-- columns: avg_width
-- row: 105
-- end-expected
SELECT avg_width FROM pg_stats WHERE tablename='zg_b';
CREATE TABLE zg_c (a int);
ANALYZE zg_c;
-- begin-expected
-- columns: n
-- row: 0
-- end-expected
SELECT count(*)::int AS n FROM pg_stats WHERE tablename='zg_c';
CREATE TABLE zg_d (id int);
INSERT INTO zg_d VALUES (1),(1),(2),(3),(4),(5),(6),(7),(8),(9);
ANALYZE zg_d;
-- begin-expected
-- columns: n_distinct | mcv | f | hb
-- row: -0.9 | {1} | {0.2} | {2,3,4,5,6,7,8,9}
-- end-expected
SELECT n_distinct, most_common_vals::text AS mcv, most_common_freqs::text AS f, histogram_bounds::text AS hb FROM pg_stats WHERE tablename='zg_d';
CREATE TABLE zg_e (id int);
INSERT INTO zg_e SELECT g%40 FROM generate_series(1,300) g;
ANALYZE zg_e;
-- begin-expected
-- columns: n_distinct | mcv | hn
-- row: -0.13333334 | {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,0,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39} | NULL
-- end-expected
SELECT n_distinct, most_common_vals::text AS mcv, array_length(histogram_bounds,1) AS hn FROM pg_stats WHERE tablename='zg_e';
CREATE TABLE zg_f (id int);
INSERT INTO zg_f SELECT g FROM generate_series(1,10) g;
ANALYZE zg_f;
-- begin-expected
-- columns: hb | n_distinct | correlation
-- row: {1,2,3,4,5,6,7,8,9,10} | -1 | 1
-- end-expected
SELECT histogram_bounds::text AS hb, n_distinct, correlation FROM pg_stats WHERE tablename='zg_f';
CREATE TABLE zg_g (id int);
INSERT INTO zg_g SELECT g FROM generate_series(1,300) g;
ANALYZE zg_g;
-- begin-expected
-- columns: n
-- row: 101
-- end-expected
SELECT array_length(histogram_bounds,1) AS n FROM pg_stats WHERE tablename='zg_g';
CREATE TABLE zg_v (id int);
INSERT INTO zg_v VALUES (1);
-- begin-expected
-- columns: vacuum_count | analyze_count
-- row: 0 | 0
-- end-expected
SELECT vacuum_count, analyze_count FROM pg_stat_user_tables WHERE relname='zg_v';
ANALYZE zg_v;
-- begin-expected
-- columns: vacuum_count | analyze_count
-- row: 0 | 1
-- end-expected
SELECT vacuum_count, analyze_count FROM pg_stat_user_tables WHERE relname='zg_v';
VACUUM zg_v;
-- begin-expected
-- columns: vacuum_count | analyze_count
-- row: 1 | 1
-- end-expected
SELECT vacuum_count, analyze_count FROM pg_stat_user_tables WHERE relname='zg_v';
VACUUM ANALYZE zg_v;
-- begin-expected
-- columns: vacuum_count | analyze_count | autovacuum_count | autoanalyze_count
-- row: 2 | 2 | 0 | 0
-- end-expected
SELECT vacuum_count, analyze_count, autovacuum_count, autoanalyze_count FROM pg_stat_user_tables WHERE relname='zg_v';
CREATE TABLE zg_p (a int, b int);
INSERT INTO zg_p SELECT g, g%5 FROM generate_series(1,50) g;
ANALYZE zg_p (a);
-- begin-expected
-- columns: a
-- row: a
-- end-expected
SELECT attname::text AS a FROM pg_stats WHERE tablename='zg_p' ORDER BY 1;
ANALYZE zg_p (b);
-- begin-expected
-- columns: a
-- row: a
-- row: b
-- end-expected
SELECT attname::text AS a FROM pg_stats WHERE tablename='zg_p' ORDER BY 1;
-- begin-expected-error
-- sqlstate: 42703
-- message-like: column "nosuchcol" of relation "zg_p" does not exist
-- end-expected-error
ANALYZE zg_p (nosuchcol);
DROP TABLE zg_a, zg_b, zg_c, zg_d, zg_e, zg_f, zg_g, zg_v, zg_p;
