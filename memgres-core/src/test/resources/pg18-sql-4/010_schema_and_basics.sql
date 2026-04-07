DROP SCHEMA IF EXISTS test_010 CASCADE;
CREATE SCHEMA test_010;
SET search_path TO test_010;

CREATE TABLE people (
    person_id integer PRIMARY KEY,
    full_name text NOT NULL,
    nickname text DEFAULT 'n/a',
    created_flag boolean DEFAULT true
);

ALTER TABLE people ADD COLUMN city text DEFAULT 'unknown';
ALTER TABLE people RENAME COLUMN city TO hometown;
ALTER TABLE people RENAME TO persons;

CREATE TABLE "QuotedTable" (
    "MixedCaseId" integer PRIMARY KEY,
    "select" text NOT NULL
);

INSERT INTO persons(person_id, full_name, hometown) VALUES
    (1, 'Alice Adams', 'Amsterdam'),
    (2, 'Bob Brown', 'Berlin');

INSERT INTO "QuotedTable"("MixedCaseId", "select") VALUES
    (10, 'reserved-ok');

-- begin-expected
-- columns: person_id|full_name|nickname|created_flag|hometown
-- row: 1|Alice Adams|n/a|true|Amsterdam
-- row: 2|Bob Brown|n/a|true|Berlin
-- end-expected
SELECT person_id, full_name, nickname, created_flag, hometown
FROM persons
ORDER BY person_id;

-- begin-expected
-- columns: MixedCaseId|select
-- row: 10|reserved-ok
-- end-expected
SELECT "MixedCaseId", "select"
FROM "QuotedTable";

DROP SCHEMA test_010 CASCADE;
