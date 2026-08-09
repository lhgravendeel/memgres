CREATE TABLE zz_st_brace (id int, d date, i int, u uuid);

-- begin-expected-error
-- sqlstate: 22007
-- message-like: ERROR: invalid input syntax for type date: "{foo}"
-- end-expected-error
INSERT INTO zz_st_brace (id, d) VALUES (1, '{foo}');

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: ERROR: invalid input syntax for type integer: "{1,2}"
-- end-expected-error
INSERT INTO zz_st_brace (id, i) VALUES (2, '{1,2}');

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: ERROR: invalid input syntax for type uuid: "{zzz}"
-- end-expected-error
INSERT INTO zz_st_brace (id, u) VALUES (3, '{zzz}');

-- begin-expected
-- columns: a
-- row: 0
-- end-expected
SELECT count(*) AS a FROM zz_st_brace;

CREATE TABLE zz_st_num (id int, n numeric(5));

-- begin-expected-error
-- sqlstate: 22003
-- message-like: ERROR: numeric field overflow
-- end-expected-error
INSERT INTO zz_st_num VALUES (1, 123456789);

INSERT INTO zz_st_num VALUES (2, 1.7);

-- begin-expected
-- columns: id|n
-- row: 2|2
-- end-expected
SELECT id, n FROM zz_st_num ORDER BY id;

CREATE TABLE zz_st_nn (id int, n numeric(5,2));

-- begin-expected-error
-- sqlstate: 22003
-- message-like: ERROR: numeric field overflow
-- end-expected-error
INSERT INTO zz_st_nn VALUES (1, 'Infinity');

CREATE TABLE zz_st_tp (id int, ts timestamp(0), tm time(0));

INSERT INTO zz_st_tp VALUES (1, '2024-01-01 01:02:03.987', '01:02:03.987');

-- begin-expected
-- columns: id|ts|tm
-- row: 1|2024-01-01 01:02:04|01:02:04
-- end-expected
SELECT id, ts, tm FROM zz_st_tp;

CREATE TABLE zz_st_b (a bit(4), b varbit(4));

-- begin-expected-error
-- sqlstate: 22026
-- message-like: ERROR: bit string length 3 does not match type bit(4)
-- end-expected-error
INSERT INTO zz_st_b VALUES ('101', '101');

-- begin-expected-error
-- sqlstate: 22026
-- message-like: ERROR: bit string length 5 does not match type bit(4)
-- end-expected-error
INSERT INTO zz_st_b VALUES ('10101', '10101');

INSERT INTO zz_st_b VALUES ('1010', '1010');

-- begin-expected
-- columns: a
-- row: 1
-- end-expected
SELECT count(*) AS a FROM zz_st_b;

CREATE TABLE zz_st_lsn (id int, l pg_lsn);

INSERT INTO zz_st_lsn VALUES (1, '0/16B374D');

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: ERROR: invalid input syntax for type pg_lsn: "garbage"
-- end-expected-error
INSERT INTO zz_st_lsn VALUES (2, 'garbage');

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: ERROR: invalid input syntax for type pg_lsn: "16B374D"
-- end-expected-error
SELECT '16B374D'::pg_lsn;

-- begin-expected-error
-- sqlstate: 22P02
-- message-like: ERROR: invalid input syntax for type pg_lsn: "zz/1"
-- end-expected-error
SELECT 'zz/1'::pg_lsn;

-- begin-expected
-- columns: pg_lsn
-- row: 0/16B374D
-- end-expected
SELECT '0/16b374d'::pg_lsn;

-- begin-expected
-- columns: a
-- row: 63
-- end-expected
SELECT length(repeat('a',100)::name) AS a;

-- begin-expected-error
-- sqlstate: 22007
-- message-like: ERROR: invalid input syntax for type date: "not a date"
-- end-expected-error
INSERT INTO zz_st_brace (id, d) VALUES (4, 'not a date');

DROP TABLE zz_st_brace, zz_st_num, zz_st_nn, zz_st_tp, zz_st_b, zz_st_lsn;

