CREATE TABLE zzw2_c1 (a int, b text);
INSERT INTO zzw2_c1 VALUES (1, 'x');

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: option "nosuchoption" not recognized
-- end-expected-error
COPY zzw2_c1 TO STDOUT WITH (NOSUCHOPTION true);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: COPY format "bogus" not recognized
-- end-expected-error
COPY zzw2_c1 TO STDOUT WITH (FORMAT bogus);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: COPY delimiter must be a single one-byte character
-- end-expected-error
COPY zzw2_c1 TO STDOUT WITH (DELIMITER 'ab');

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: COPY delimiter cannot be "a"
-- end-expected-error
COPY zzw2_c1 TO STDOUT WITH (DELIMITER 'a');

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: COPY delimiter cannot be newline or carriage return
-- end-expected-error
COPY zzw2_c1 TO STDOUT WITH (DELIMITER E'\n');

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: COPY null representation cannot use newline or carriage return
-- end-expected-error
COPY zzw2_c1 TO STDOUT WITH (NULL E'a\nb');

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: COPY QUOTE requires CSV mode
-- end-expected-error
COPY zzw2_c1 TO STDOUT WITH (QUOTE '"');

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: COPY ESCAPE requires CSV mode
-- end-expected-error
COPY zzw2_c1 TO STDOUT WITH (ESCAPE '#');

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: COPY quote must be a single one-byte character
-- end-expected-error
COPY zzw2_c1 TO STDOUT WITH (FORMAT csv, QUOTE 'ab');

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: COPY delimiter and quote must be different
-- end-expected-error
COPY zzw2_c1 TO STDOUT WITH (FORMAT csv, DELIMITER '"');

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: COPY delimiter character must not appear in the NULL specification
-- end-expected-error
COPY zzw2_c1 TO STDOUT WITH (FORMAT csv, DELIMITER ',', NULL ',');

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: CSV quote character must not appear in the NULL specification
-- end-expected-error
COPY zzw2_c1 TO STDOUT WITH (FORMAT csv, NULL '"');

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: cannot specify DELIMITER in BINARY mode
-- end-expected-error
COPY zzw2_c1 TO STDOUT WITH (FORMAT binary, DELIMITER ',');

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: cannot specify NULL in BINARY mode
-- end-expected-error
COPY zzw2_c1 TO STDOUT WITH (FORMAT binary, NULL 'x');

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: cannot specify HEADER in BINARY mode
-- end-expected-error
COPY zzw2_c1 TO STDOUT WITH (FORMAT binary, HEADER);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: cannot use "match" with HEADER in COPY TO
-- end-expected-error
COPY zzw2_c1 TO STDOUT WITH (HEADER MATCH);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: header requires a Boolean value or "match"
-- end-expected-error
COPY zzw2_c1 TO STDOUT WITH (HEADER 'bogus');

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: COPY FREEZE cannot be used with COPY TO
-- end-expected-error
COPY zzw2_c1 TO STDOUT WITH (FREEZE);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: COPY ON_ERROR cannot be used with COPY TO
-- end-expected-error
COPY zzw2_c1 TO STDOUT WITH (ON_ERROR ignore);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: COPY DEFAULT cannot be used with COPY TO
-- end-expected-error
COPY zzw2_c1 TO STDOUT WITH (DEFAULT '\D');

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: COPY FORCE_NOT_NULL cannot be used with COPY TO
-- end-expected-error
COPY zzw2_c1 TO STDOUT WITH (FORMAT csv, FORCE_NOT_NULL (a));

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: COPY FORCE_NULL cannot be used with COPY TO
-- end-expected-error
COPY zzw2_c1 TO STDOUT WITH (FORMAT csv, FORCE_NULL (a));

-- begin-expected-error
-- sqlstate: 42703
-- message-like: ERROR: column "nosuchcol" of relation "zzw2_c1" does not exist
-- end-expected-error
COPY zzw2_c1 TO STDOUT WITH (FORMAT csv, FORCE_QUOTE (nosuchcol));

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: COPY FORCE_QUOTE cannot be used with COPY FROM
-- end-expected-error
COPY zzw2_c1 FROM STDIN WITH (FORMAT csv, FORCE_QUOTE (a));

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: COPY FORCE_NULL requires CSV mode
-- end-expected-error
COPY zzw2_c1 FROM STDIN WITH (FORCE_NULL (a));

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: COPY ON_ERROR "bogus" not recognized
-- end-expected-error
COPY zzw2_c1 FROM STDIN WITH (ON_ERROR bogus);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: COPY LOG_VERBOSITY "bogus" not recognized
-- end-expected-error
COPY zzw2_c1 FROM STDIN WITH (LOG_VERBOSITY bogus);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: argument to option "encoding" must be a valid encoding name
-- end-expected-error
COPY zzw2_c1 FROM STDIN WITH (ENCODING 'NOSUCHENC');

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: REJECT_LIMIT (0) must be greater than zero
-- end-expected-error
COPY zzw2_c1 FROM STDIN WITH (FORMAT csv, ON_ERROR ignore, REJECT_LIMIT 0);

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: NULL specification and DEFAULT specification cannot be the same
-- end-expected-error
COPY zzw2_c1 FROM STDIN WITH (FORMAT csv, DEFAULT '', NULL '');

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: WHERE clause not allowed with COPY TO
-- end-expected-error
COPY zzw2_c1 TO STDOUT WHERE a > 0;

-- begin-expected-error
-- sqlstate: 0A000
-- message-like: ERROR: cannot use subquery in COPY FROM WHERE condition
-- end-expected-error
COPY zzw2_c1 FROM STDIN WHERE (SELECT true);

-- begin-expected-error
-- sqlstate: 42803
-- message-like: ERROR: aggregate functions are not allowed in COPY FROM WHERE conditions
-- end-expected-error
COPY zzw2_c1 FROM STDIN WHERE count(*) > 0;

-- begin-expected-error
-- sqlstate: 42P20
-- message-like: ERROR: window functions are not allowed in COPY FROM WHERE conditions
-- end-expected-error
COPY zzw2_c1 FROM STDIN WHERE row_number() OVER () > 0;

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: COPY format "bogus" not recognized
-- end-expected-error
COPY (SELECT 1) TO STDOUT WITH (FORMAT bogus);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: option "nosuchopt" not recognized
-- end-expected-error
COPY (SELECT 1) TO STDOUT WITH (NOSUCHOPT true);

-- begin-expected-error
-- sqlstate: 22023
-- message-like: ERROR: COPY FREEZE cannot be used with COPY TO
-- end-expected-error
COPY (SELECT 1) TO STDOUT WITH (FREEZE);

-- begin-expected-error
-- sqlstate: 42601
-- message-like: ERROR: syntax error at or near "WHERE"
-- end-expected-error
COPY (SELECT 1) TO STDOUT WHERE true;

CREATE VIEW zzw2_c1v AS SELECT a, b FROM zzw2_c1;
CREATE MATERIALIZED VIEW zzw2_c1m AS SELECT a, b FROM zzw2_c1;
CREATE SEQUENCE zzw2_c1s;
CREATE TABLE zzw2_c1p (a int, b text) PARTITION BY RANGE (a);
CREATE TABLE zzw2_c1p1 PARTITION OF zzw2_c1p FOR VALUES FROM (0) TO (10);

-- begin-expected-error
-- sqlstate: 42809
-- message-like: ERROR: cannot copy from view "zzw2_c1v"
-- end-expected-error
COPY zzw2_c1v TO STDOUT;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: ERROR: cannot copy to view "zzw2_c1v"
-- end-expected-error
COPY zzw2_c1v FROM STDIN;

-- PostgreSQL does refuse this with 42809, but only after it has already asked
-- the client for the data: the CopyInResponse goes out first and the
-- ErrorResponse follows it, so a driver that never enters copy mode sees the
-- statement complete. The refusal for a view or a sequence is raised before the
-- CopyInResponse and so is visible; this one is not.
COPY zzw2_c1m FROM STDIN;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: ERROR: cannot copy from sequence "zzw2_c1s"
-- end-expected-error
COPY zzw2_c1s TO STDOUT;

-- begin-expected-error
-- sqlstate: 42809
-- message-like: ERROR: cannot copy to sequence "zzw2_c1s"
-- end-expected-error
COPY zzw2_c1s FROM STDIN;

-- PostgreSQL does refuse this with 42809, but only after the CopyOutResponse
-- has gone out, so a driver that never enters copy mode sees the statement
-- complete. The refusal for a view or a sequence is raised before the
-- CopyOutResponse and so is visible; this one is not.
COPY zzw2_c1p TO STDOUT;

CREATE TABLE zzw2_g1 (a int, g int GENERATED ALWAYS AS (a*2) STORED);
INSERT INTO zzw2_g1 (a) VALUES (5);

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: ERROR: column "g" is a generated column
-- end-expected-error
COPY zzw2_g1 (a,g) TO STDOUT;

-- begin-expected-error
-- sqlstate: 42P10
-- message-like: ERROR: column "g" is a generated column
-- end-expected-error
COPY zzw2_g1 (a,g) FROM STDIN;

DROP TABLE zzw2_g1;
DROP TABLE zzw2_c1p;
DROP SEQUENCE zzw2_c1s;
DROP MATERIALIZED VIEW zzw2_c1m;
DROP VIEW zzw2_c1v;
DROP TABLE zzw2_c1;