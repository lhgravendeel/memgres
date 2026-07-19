-- Window frame bound resolution:
-- 1. RANGE frames with an offset under ORDER BY ... DESC
-- 2. first_value/last_value/nth_value honoring explicit frames (incl. empty frames)
-- 3. GROUPS frames with offsets beyond the partition edge

-- setup
CREATE TABLE wfb_t5 (x int);
INSERT INTO wfb_t5 VALUES (1),(2),(3),(4),(5);
CREATE TABLE wfb_t3 (x int);
INSERT INTO wfb_t3 VALUES (1),(2),(3);
CREATE TABLE wfb_tg (g int);
INSERT INTO wfb_tg VALUES (1),(1),(2),(2),(3);

-- stmt 1: DESC RANGE offset frame, N PRECEDING .. CURRENT ROW
-- begin-expected
-- columns: x | s
-- row: 1, 3
-- row: 2, 5
-- row: 3, 7
-- row: 4, 9
-- row: 5, 5
-- end-expected
SELECT x, sum(x) OVER (ORDER BY x DESC RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS s
FROM wfb_t5 ORDER BY x;

-- stmt 2: DESC RANGE offset frame, CURRENT ROW .. N FOLLOWING
-- begin-expected
-- columns: x | s
-- row: 1, 1
-- row: 2, 3
-- row: 3, 5
-- row: 4, 7
-- row: 5, 9
-- end-expected
SELECT x, sum(x) OVER (ORDER BY x DESC RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS s
FROM wfb_t5 ORDER BY x;

-- stmt 3: DESC RANGE with both bounds PRECEDING (empty frame for x=5)
-- begin-expected
-- columns: x | s
-- row: 1, 9
-- row: 2, 12
-- row: 3, 9
-- row: 4, 5
-- row: 5, NULL
-- end-expected
SELECT x, sum(x) OVER (ORDER BY x DESC RANGE BETWEEN 3 PRECEDING AND 1 PRECEDING) AS s
FROM wfb_t5 ORDER BY x;

-- stmt 4: DESC RANGE with both bounds FOLLOWING (empty frame for x=1)
-- begin-expected
-- columns: x | s
-- row: 1, NULL
-- row: 2, 1
-- row: 3, 3
-- row: 4, 5
-- row: 5, 7
-- end-expected
SELECT x, sum(x) OVER (ORDER BY x DESC RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS s
FROM wfb_t5 ORDER BY x;

-- stmt 5: DESC RANGE PRECEDING .. FOLLOWING
-- begin-expected
-- columns: x | s
-- row: 1, 3
-- row: 2, 6
-- row: 3, 10
-- row: 4, 14
-- row: 5, 12
-- end-expected
SELECT x, sum(x) OVER (ORDER BY x DESC RANGE BETWEEN 1 PRECEDING AND 2 FOLLOWING) AS s
FROM wfb_t5 ORDER BY x;

-- stmt 6: count over empty DESC RANGE frame is 0, not NULL
-- begin-expected
-- columns: x | c
-- row: 1, 3
-- row: 2, 3
-- row: 3, 2
-- row: 4, 1
-- row: 5, 0
-- end-expected
SELECT x, count(*) OVER (ORDER BY x DESC RANGE BETWEEN 3 PRECEDING AND 1 PRECEDING) AS c
FROM wfb_t5 ORDER BY x;

-- stmt 7: ASC RANGE offset frames still correct
-- begin-expected
-- columns: x | s
-- row: 1, 1
-- row: 2, 3
-- row: 3, 5
-- row: 4, 7
-- row: 5, 9
-- end-expected
SELECT x, sum(x) OVER (ORDER BY x RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS s
FROM wfb_t5 ORDER BY x;

-- stmt 8: ASC RANGE with both bounds PRECEDING (empty frame for x=1)
-- begin-expected
-- columns: x | s
-- row: 1, NULL
-- row: 2, 1
-- row: 3, 3
-- row: 4, 6
-- row: 5, 9
-- end-expected
SELECT x, sum(x) OVER (ORDER BY x RANGE BETWEEN 3 PRECEDING AND 1 PRECEDING) AS s
FROM wfb_t5 ORDER BY x;

-- stmt 9: DESC RANGE with EXCLUDE CURRENT ROW
-- begin-expected
-- columns: x | s
-- row: 1, 2
-- row: 2, 3
-- row: 3, 4
-- row: 4, 5
-- row: 5, NULL
-- end-expected
SELECT x, sum(x) OVER (ORDER BY x DESC RANGE BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) AS s
FROM wfb_t5 ORDER BY x;

-- stmt 10: first_value with ROWS frame entirely FOLLOWING (empty frame -> NULL)
-- begin-expected
-- columns: x | fv
-- row: 1, 2
-- row: 2, 3
-- row: 3, NULL
-- end-expected
SELECT x, first_value(x) OVER (ORDER BY x ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS fv
FROM wfb_t3 ORDER BY x;

-- stmt 11: last_value with ROWS frame entirely FOLLOWING (empty frame -> NULL)
-- begin-expected
-- columns: x | lv
-- row: 1, 3
-- row: 2, 3
-- row: 3, NULL
-- end-expected
SELECT x, last_value(x) OVER (ORDER BY x ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS lv
FROM wfb_t3 ORDER BY x;

-- stmt 12: nth_value with ROWS frame entirely FOLLOWING
-- begin-expected
-- columns: x | nv
-- row: 1, 3
-- row: 2, NULL
-- row: 3, NULL
-- end-expected
SELECT x, nth_value(x, 2) OVER (ORDER BY x ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS nv
FROM wfb_t3 ORDER BY x;

-- stmt 13: first_value/last_value with ROWS frame entirely PRECEDING
-- begin-expected
-- columns: x | fv | lv
-- row: 1, NULL, NULL
-- row: 2, 1, 1
-- row: 3, 1, 2
-- end-expected
SELECT x,
       first_value(x) OVER w AS fv,
       last_value(x) OVER w AS lv
FROM wfb_t3
WINDOW w AS (ORDER BY x ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
ORDER BY x;

-- stmt 14: first_value/last_value with RANGE offset frame
-- begin-expected
-- columns: x | fv | lv
-- row: 1, 2, 3
-- row: 2, 3, 3
-- row: 3, NULL, NULL
-- end-expected
SELECT x,
       first_value(x) OVER w AS fv,
       last_value(x) OVER w AS lv
FROM wfb_t3
WINDOW w AS (ORDER BY x RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING)
ORDER BY x;

-- stmt 15: first_value with DESC RANGE offset frame
-- begin-expected
-- columns: x | fv
-- row: 1, 2
-- row: 2, 3
-- row: 3, 4
-- row: 4, 5
-- row: 5, 5
-- end-expected
SELECT x, first_value(x) OVER (ORDER BY x DESC RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS fv
FROM wfb_t5 ORDER BY x;

-- stmt 16: GROUPS frame with FOLLOWING offsets beyond the partition edge (no error, NULL)
-- begin-expected
-- columns: x | s
-- row: 1, NULL
-- row: 2, NULL
-- row: 3, NULL
-- end-expected
SELECT x, sum(x) OVER (ORDER BY x GROUPS BETWEEN 3 FOLLOWING AND 4 FOLLOWING) AS s
FROM wfb_t3 ORDER BY x;

-- stmt 17: GROUPS frame with PRECEDING offsets beyond the partition edge (no error, NULL)
-- begin-expected
-- columns: x | s
-- row: 1, NULL
-- row: 2, NULL
-- row: 3, NULL
-- end-expected
SELECT x, sum(x) OVER (ORDER BY x GROUPS BETWEEN 5 PRECEDING AND 3 PRECEDING) AS s
FROM wfb_t3 ORDER BY x;

-- stmt 18: count over empty GROUPS frame is 0
-- begin-expected
-- columns: x | c
-- row: 1, 0
-- row: 2, 0
-- row: 3, 0
-- end-expected
SELECT x, count(*) OVER (ORDER BY x GROUPS BETWEEN 3 FOLLOWING AND 4 FOLLOWING) AS c
FROM wfb_t3 ORDER BY x;

-- stmt 19: GROUPS frame partially out of range (clamped end, empty once start passes the last group)
-- begin-expected
-- columns: g | s
-- row: 1, 3
-- row: 1, 3
-- row: 2, NULL
-- row: 2, NULL
-- row: 3, NULL
-- end-expected
SELECT g, sum(g) OVER (ORDER BY g GROUPS BETWEEN 2 FOLLOWING AND 3 FOLLOWING) AS s
FROM wfb_tg ORDER BY g;

-- stmt 20: GROUPS frames in range still correct
-- begin-expected
-- columns: g | s1 | s2
-- row: 1, 2, 6
-- row: 1, 2, 6
-- row: 2, 6, 7
-- row: 2, 6, 7
-- row: 3, 7, 3
-- end-expected
SELECT g,
       sum(g) OVER (ORDER BY g GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s1,
       sum(g) OVER (ORDER BY g GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS s2
FROM wfb_tg ORDER BY g;

-- cleanup
DROP TABLE wfb_t5;
DROP TABLE wfb_t3;
DROP TABLE wfb_tg;
