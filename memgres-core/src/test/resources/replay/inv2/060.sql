-- source: investigation-2026-08.md
-- finding: 60
-- title: The XML implementation is a thin wrapper over the JDK parser: it has no DOCUMENT/CONTENT gate on the ::xml input path, no xml-typed special case in xmlforest, n
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_xd (id int, x xml);
-- begin-expected
-- ok: 3
-- end-expected
INSERT INTO zz_vf_xd VALUES (1,'<root><row><a>1</a></row><row><a>2</a></row></root>'),(2,'<root><row/></root>'),(4,'<root><row><a>zz</a></row></root>');
-- begin-expected
-- columns: n:int4 | a:int4
-- row: 1 | 1
-- row: 2 | 2
-- rowcount: 2
-- end-expected
SELECT t.* FROM zz_vf_xd d, xmltable('/root/row' PASSING d.x COLUMNS n FOR ORDINALITY, a int PATH 'a') t WHERE d.id=1 ORDER BY n;
-- begin-expected
-- columns: a:int4
-- row: 42
-- rowcount: 1
-- end-expected
SELECT t.* FROM zz_vf_xd d, xmltable('/root/row' PASSING d.x COLUMNS a int PATH 'a' DEFAULT 42) t WHERE d.id=2;
-- begin-expected-error
-- sqlstate: 22004
-- message-like: null is not allowed in column "a"
-- end-expected-error
SELECT t.* FROM zz_vf_xd d, xmltable('/root/row' PASSING d.x COLUMNS a int PATH 'a' NOT NULL) t WHERE d.id=2;
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "zz"
-- end-expected-error
SELECT t.* FROM zz_vf_xd d, xmltable('/root/row' PASSING d.x COLUMNS a int PATH 'a') t WHERE d.id=4;
-- begin-expected
-- ok: 0
-- end-expected
DROP TABLE zz_vf_xd;
-- begin-expected
-- columns: length:int4
-- row: 4
-- rowcount: 1
-- end-expected
SELECT length(xmlroot('<a/>'::xml, version '1.0')::text);
-- begin-expected
-- columns: ?column?:bool
-- row: NULL
-- rowcount: 1
-- end-expected
SELECT NULL::xml IS DOCUMENT;
-- begin-expected
-- columns: length:int4
-- row: 19
-- rowcount: 1
-- end-expected
SELECT length(XMLSERIALIZE(DOCUMENT '<a><b>x</b></a>'::xml AS text INDENT));
-- begin-expected
-- columns: ?column?:text
-- row: [<a/> ]
-- rowcount: 1
-- end-expected
SELECT '[' || XMLSERIALIZE(CONTENT ' <a/> '::xml AS text INDENT) || ']';
-- begin-expected
-- columns: xmlforest:xml
-- row: <f><root><row/></root></f>
-- rowcount: 1
-- end-expected
SELECT xmlforest('<root><row/></root>'::xml AS f);
-- begin-expected
-- columns: xmlelement:xml
-- row: <foo att_x003C_r="a&amp;b"/>
-- rowcount: 1
-- end-expected
SELECT xmlelement(name foo, xmlattributes('a&b' as "att<r"));
