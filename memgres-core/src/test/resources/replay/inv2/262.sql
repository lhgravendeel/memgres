-- source: investigation-2026-08.md
-- finding: 262
-- title: Return-type enforcement exists on the RETURN NEXT and composite-SETOF paths but not on their siblings. executeReturn never casts to __return_type__ although exe
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_w1() RETURNS int AS $$ begin return 'abc'; end $$ LANGUAGE plpgsql;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "abc"
-- end-expected-error
SELECT zz_vf_w1();
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_rt(id int, nm text);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf_rt VALUES (1,'a'),(2,'b');
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_n7() RETURNS SETOF int AS $$ begin return query select id, nm from zz_vf_rt; end $$ LANGUAGE plpgsql;
-- begin-expected-error
-- sqlstate: 42804
-- message-like: structure of query does not match function result type
-- end-expected-error
SELECT * FROM zz_vf_n7();
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_n8() RETURNS SETOF int AS $$ begin return query select nm from zz_vf_rt order by nm; end $$ LANGUAGE plpgsql;
-- begin-expected-error
-- sqlstate: 42804
-- message-like: structure of query does not match function result type
-- end-expected-error
SELECT * FROM zz_vf_n8();
