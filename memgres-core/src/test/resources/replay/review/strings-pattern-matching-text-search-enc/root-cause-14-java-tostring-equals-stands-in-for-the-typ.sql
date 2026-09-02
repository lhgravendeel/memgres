-- source: review-2026-08.md
-- finding: Root cause 14: Java toString()/equals() stands in for the type's output function or its = operator
-- area: Strings, pattern matching, text search, encodings and the exotic types
-- title: Root cause 14: Java toString()/equals() stands in for the type's output function or its = operator
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_dn (id int, n numeric, b bytea);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf_dn VALUES (1, 1.0, '\x0102'), (2, 1.00, '\x0102'), (3, 2.0, '\x03');
-- begin-expected
-- columns: id:int4 | n:numeric
-- row: 1 | 1.0
-- row: 3 | 2.0
-- rowcount: 2
-- end-expected
SELECT DISTINCT ON (n) id, n FROM zz_vf_dn ORDER BY n, id;
-- begin-expected
-- columns: id:int4
-- row: 1
-- row: 3
-- rowcount: 2
-- end-expected
SELECT DISTINCT ON (b) id FROM zz_vf_dn ORDER BY b, id;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_n1 (v numeric);
-- begin-expected
-- ok: 2
-- end-expected
INSERT INTO zz_vf_n1 VALUES (1.0), (1.00);
-- begin-expected-error
-- sqlstate: 23505
-- message-like: could not create unique index "zz_vf_i1"
-- end-expected-error
CREATE UNIQUE INDEX zz_vf_i1 ON zz_vf_n1 (v);
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_case() RETURNS text AS $$
DECLARE x int := 1;
BEGIN CASE x WHEN 1.0 THEN RETURN 'one'; ELSE RETURN 'other'; END CASE; END $$ LANGUAGE plpgsql;
-- begin-expected
-- columns: zz_vf_case:text
-- row: one
-- rowcount: 1
-- end-expected
SELECT zz_vf_case();
-- begin-expected
-- ok: 0
-- end-expected
CREATE FUNCTION zz_vf_a() RETURNS void AS $$ BEGIN ASSERT false, ARRAY[1,2]; END $$ LANGUAGE plpgsql;
-- begin-expected-error
-- sqlstate: P0004
-- message-like: {1,2}
-- end-expected-error
SELECT zz_vf_a();
