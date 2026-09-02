-- source: investigation-2026-08.md
-- finding: 2
-- title: Storage is an I/O conversion, not an assignment cast, and the typmod pass that follows it is incomplete. coerceForStorage runs the target type's input function 
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_brace (id int, d date, i int, ts timestamp, u uuid);
-- begin-expected-error
-- sqlstate: 22007
-- message-like: invalid input syntax for type date: "{foo}"
-- end-expected-error
INSERT INTO zz_vf_brace (id, d) VALUES (1, '{foo}');
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "{1,2}"
-- end-expected-error
INSERT INTO zz_vf_brace (id, i) VALUES (2, '{1,2}');
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type uuid: "{zzz}"
-- end-expected-error
INSERT INTO zz_vf_brace (id, u) VALUES (4, '{zzz}');
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_vf_brace;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_num (id int, n numeric(5));
-- begin-expected-error
-- sqlstate: 22003
-- message-like: numeric field overflow
-- end-expected-error
INSERT INTO zz_vf_num VALUES (1, 123456789);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_num VALUES (2, 1.7);
-- begin-expected
-- columns: id:int4 | n:numeric
-- row: 2 | 2
-- rowcount: 1
-- end-expected
SELECT id, n FROM zz_vf_num ORDER BY id;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_nn (id int, n numeric(5,2));
-- begin-expected-error
-- sqlstate: 22003
-- message-like: numeric field overflow
-- end-expected-error
INSERT INTO zz_vf_nn VALUES (1, 'Infinity');
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_tp (id int, ts timestamp(0), tm time(0));
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_tp VALUES (1, '2024-01-01 01:02:03.987', '01:02:03.987');
-- begin-expected
-- columns: id:int4 | ts:timestamp | tm:time
-- row: 1 | 2024-01-01 01:02:04 | 01:02:04
-- rowcount: 1
-- end-expected
SELECT id, ts, tm FROM zz_vf_tp;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_b (a bit(4), b varbit(4));
-- begin-expected-error
-- sqlstate: 22026
-- message-like: bit string length 3 does not match type bit(4)
-- end-expected-error
INSERT INTO zz_vf_b VALUES ('101', '101');
-- begin-expected-error
-- sqlstate: 22026
-- message-like: bit string length 5 does not match type bit(4)
-- end-expected-error
INSERT INTO zz_vf_b VALUES ('10101', '10101');
-- begin-expected
-- columns: count:int8
-- row: 0
-- rowcount: 1
-- end-expected
SELECT count(*) FROM zz_vf_b;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_lsn (id int, l pg_lsn);
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type pg_lsn: "garbage"
-- end-expected-error
INSERT INTO zz_vf_lsn VALUES (2, 'garbage');
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type pg_lsn: "16B374D"
-- end-expected-error
SELECT '16B374D'::pg_lsn;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type pg_lsn: "zz/1"
-- end-expected-error
SELECT 'zz/1'::pg_lsn;
-- begin-expected
-- columns: pg_lsn:pg_lsn
-- row: 0/16B374D
-- rowcount: 1
-- end-expected
SELECT '0/16b374d'::pg_lsn;
-- begin-expected
-- columns: length:int4
-- row: 63
-- rowcount: 1
-- end-expected
SELECT length(repeat('a',100)::name);
