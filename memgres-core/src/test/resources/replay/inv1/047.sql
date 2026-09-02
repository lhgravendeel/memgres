-- source: investigation.md
-- finding: 47
-- title: Views: non-updatable views accept DML (5 cases)
-- unrunnable: the report wrote this reproducer abbreviated
CREATE VIEW v AS WITH c AS (SELECT …) SELECT … FROM c;
INSERT INTO v VALUES (3, 'c');
-- PG: 55000 cannot insert into view "v" | mg: 1 row affected
UPDATE v SET val = 'z' …;
-- PG: 55000 | mg: 1 row
DELETE FROM v …;
-- PG: 55000 | mg: 1 row;;
