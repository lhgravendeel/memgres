-- source: investigation-2026-08.md
-- finding: 20
-- title: Java objects reach storage and the wire through toString(): INSERT ... SELECT rebuilds each source value as a literal from val.toString(), and format(), LIKE an
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_sel (b bytea);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_sel SELECT '\x00010203de'::bytea;
-- begin-expected
-- columns: b:bytea
-- row: \x00010203de
-- rowcount: 1
-- end-expected
SELECT b FROM zz_vf_sel;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_sel2 (a int[]);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_sel2 SELECT ARRAY[1,2,3];
-- begin-expected
-- columns: a:_int4
-- row: {1,2,3}
-- rowcount: 1
-- end-expected
SELECT a FROM zz_vf_sel2;
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_by (b bytea);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_by VALUES ('1');
-- begin-expected-error
-- sqlstate: 42846
-- message-like: cannot cast type bytea to xml
-- end-expected-error
SELECT b::xml FROM zz_vf_by;
-- begin-expected
-- columns: format:text | format:text | format:text
-- row: \x61 | {1,2} | t
-- rowcount: 1
-- end-expected
SELECT format('%s', '\x61'::bytea), format('%s', ARRAY[1,2]), format('%s', true);
-- begin-expected
-- columns: text:text
-- row: true
-- rowcount: 1
-- end-expected
SELECT ('\x61'::bytea LIKE 'a')::text;
-- begin-expected
-- columns: xmlelement:xml
-- row: <a><element>1</element><element>2</element></a>
-- rowcount: 1
-- end-expected
SELECT xmlelement(name a, ARRAY[1,2]);
