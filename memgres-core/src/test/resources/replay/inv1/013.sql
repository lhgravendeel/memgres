-- source: investigation.md
-- finding: 13
-- title: `GROUP BY` rejects valid SQL ⚠️
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE t (id int PRIMARY KEY, n int NOT NULL);
-- begin-expected
-- columns: id:int4 | n:int4
-- rowcount: 0
-- end-expected
SELECT id, n FROM t GROUP BY id;
--   PG: works — grouping by the primary key functionally determines every other column
--   mg: 42803 column must appear in the GROUP BY clause;
