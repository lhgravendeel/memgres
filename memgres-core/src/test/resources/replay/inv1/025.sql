-- source: investigation.md
-- finding: 25
-- title: Sequence caching consumes values before they are handed out ⚠️
-- begin-expected
-- ok: 0
-- end-expected
CREATE SEQUENCE s MAXVALUE 3 CACHE 10;
-- begin-expected
-- columns: nextval:int8
-- row: 1
-- rowcount: 1
-- end-expected
SELECT nextval('s');
-- PG: 1   memgres: 42000 reached maximum value of sequence "s" (3);
