-- source: review-2026-08.md
-- finding: Root cause 3: COPY validates nothing
-- area: Dates, times, intervals — and the DML/MERGE/COPY findings filed with them
-- title: Root cause 3: COPY validates nothing
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_c1 (a int, b text);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_c1 VALUES (1,'x');
-- COPY zz_c1 TO STDOUT WITH (FORMAT bogus) | (NOSUCHOPTION true) | (FORMAT csv, QUOTE 'ab')
-- | (FORMAT binary, HEADER) | (FREEZE) | (ENCODING 'NOSUCHENC') | (DELIMITER 'ab') | (QUOTE '"')
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_c5 (a int, b text);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_c5 VALUES (1,'x');
-- begin-expected
-- ok: 0
-- end-expected
CREATE VIEW zz_c5v AS SELECT a, b FROM zz_c5;
-- COPY zz_c5v TO STDOUT ;  COPY zz_c5v FROM STDIN  with  2\ty
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_c7 (a int, g int GENERATED ALWAYS AS (a*2) STORED);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_c7 (a) VALUES (5);
-- COPY zz_c7 TO STDOUT WITH (FORMAT csv, HEADER)
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_c4 (a int);
-- COPY zz_c4 FROM STDIN WITH (FORMAT binary)  with the 8-byte payload  garbage\n
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_c8 (a int);
-- COPY zz_c8 FROM STDIN WITH (FORMAT csv, ON_ERROR ignore, REJECT_LIMIT 1)  with  1 / bad / worse;
