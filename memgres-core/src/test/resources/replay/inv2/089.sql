-- source: investigation-2026-08.md
-- finding: 89
-- title: COPY validates nothing: parseCopy skips unknown options and never cross-checks them, executeCopy checks only the direction, and the binary reader seeks past the
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_c4 (a int);
-- over the wire: COPY zz_c4 FROM STDIN WITH (FORMAT binary)  with the 8-byte payload  garbage\n
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_c7 (a int, g int GENERATED ALWAYS AS (a*2) STORED);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_c7 (a) VALUES (5);
-- over the wire: COPY zz_c7 TO STDOUT WITH (FORMAT csv, HEADER)
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_c2 (a int, b text);
-- over the wire: COPY zz_c2 FROM STDIN WITH (FORMAT csv) WHERE a > 1  with  1,x  and  5,y
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_c1 (a int, b text);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_c1 VALUES (1,'x');
-- over the wire: COPY zz_c1 TO STDOUT WITH (FORMAT bogus) / (NOSUCHOPTION true) / (FORMAT csv, QUOTE 'ab') / (FORMAT binary, HEADER) / (FREEZE) / (ENCODING 'NOSUCHENC') / (DELIMITER 'ab') / (QUOTE '"')
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
-- over the wire: COPY zz_c5v TO STDOUT ; COPY zz_c5v FROM STDIN with 2\ty
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_c3 (a int);
-- over the wire: COPY zz_c3 FROM STDIN WITH (FORMAT csv) WHERE (SELECT true)  with  1;
