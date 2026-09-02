-- source: investigation.md
-- finding: 94
-- title: `information_schema.columns.datetime_precision` ignores the typmod
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE t (e timestamp(3));
-- begin-expected
-- columns: datetime_precision:int4
-- row: 3
-- rowcount: 1
-- end-expected
SELECT datetime_precision FROM information_schema.columns WHERE column_name='e';
--   PG: 3 | memgres: 6;
