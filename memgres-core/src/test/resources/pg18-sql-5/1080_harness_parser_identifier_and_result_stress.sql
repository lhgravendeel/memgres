DROP SCHEMA IF EXISTS test_1080 CASCADE;
CREATE SCHEMA test_1080;
SET search_path TO test_1080;

CREATE TABLE "Mixed Case Table" (
    "Id" integer PRIMARY KEY,
    "Value With Space" text NOT NULL
);

INSERT INTO "Mixed Case Table" VALUES
(1, 'alpha'),
(2, 'beta');

/* block comment that should not terminate statement */

-- begin-expected
-- columns: Id,Value With Space
-- row: 1|alpha
-- row: 2|beta
-- end-expected
SELECT "Id", "Value With Space"
FROM "Mixed Case Table"
ORDER BY "Id";

CREATE FUNCTION quoted_body()
RETURNS text
LANGUAGE plpgsql
AS $outer$
BEGIN
  RETURN $$inner quoted value$$;
END
$outer$;

-- begin-expected
-- columns: quoted_body
-- row: inner quoted value
-- end-expected
SELECT quoted_body();

-- begin-expected
-- columns: c1,c2,c3,c4,c5,c6,c7,c8,c9,c10
-- row: 1|2|3|4|5|6|7|8|9|10
-- end-expected
SELECT 1 AS c1, 2 AS c2, 3 AS c3, 4 AS c4, 5 AS c5,
       6 AS c6, 7 AS c7, 8 AS c8, 9 AS c9, 10 AS c10;

