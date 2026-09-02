-- source: investigation.md
-- finding: 43
-- title: MERGE clause validation
-- unrunnable: the report wrote this reproducer abbreviated
-- An unconditional WHEN makes any later WHEN of the same kind unreachable.
MERGE … WHEN MATCHED THEN UPDATE … WHEN MATCHED AND t.v > 0 THEN …
--   PG: 42601 unreachable WHEN clause specified after unconditional WHEN clause | mg: accepted
--   (confirmed for MATCHED, NOT MATCHED, and NOT MATCHED BY SOURCE)

MERGE … WHEN NOT MATCHED THEN INSERT (id, id) VALUES (s.id, s.v);
--   PG: 42701 column "id" specified more than once | mg: accepted

MERGE … WHEN MATCHED THEN UPDATE SET v = 5, v = 6;
--   PG: 42601 multiple assignments to same column "v" | mg: accepted

WITH RECURSIVE c(n) AS (…) MERGE INTO t USING c …
--   PG: 42601 WITH RECURSIVE is not supported for MERGE statement | mg: accepted
MERGE … WHEN MATCHED THEN UPDATE SET (v, w) = (SELECT 10, 20);
--   PG: works | mg: 42601 syntax error at or near "(";;
