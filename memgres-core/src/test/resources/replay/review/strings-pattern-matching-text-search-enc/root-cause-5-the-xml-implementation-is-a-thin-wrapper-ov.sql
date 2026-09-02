-- source: review-2026-08.md
-- finding: Root cause 5: the XML implementation is a thin wrapper over the JDK parser
-- area: Strings, pattern matching, text search, encodings and the exotic types
-- title: Root cause 5: the XML implementation is a thin wrapper over the JDK parser
-- begin-expected
-- ok: 0
-- end-expected
CREATE TABLE zz_vf_xd (id int, x xml);
-- begin-expected
-- ok: 1
-- end-expected
INSERT INTO zz_vf_xd VALUES (4, '<root><row><a>zz</a></row></root>');
-- begin-expected-error
-- sqlstate: 22P02
-- message-like: invalid input syntax for type integer: "zz"
-- end-expected-error
SELECT t.* FROM zz_vf_xd d, xmltable('/root/row' PASSING d.x COLUMNS a int PATH 'a') t;
-- begin-expected
-- columns: a:int4
-- rowcount: 0
-- end-expected
SELECT t.* FROM zz_vf_xd d,
  xmltable(XMLNAMESPACES('http://ex.com' AS x), '/x:root/x:row' PASSING d.x COLUMNS a int PATH 'x:a') t;
-- begin-expected
-- columns: length:int4
-- row: 4
-- rowcount: 1
-- end-expected
SELECT length(xmlroot('<a/>'::xml, version '1.0')::text);
-- begin-expected
-- columns: xmlforest:xml
-- row: <f><root><row/></root></f>
-- rowcount: 1
-- end-expected
SELECT xmlforest('<root><row/></root>'::xml AS f);
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
-- columns: xmlelement:xml
-- row: <foo att_x003C_r="a&amp;b"/>
-- rowcount: 1
-- end-expected
SELECT xmlelement(name foo, xmlattributes('a&b' as "att<r"));
-- begin-expected
-- columns: query_to_xml:xml
-- row: <table xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">\n\n<row>\n  <a>1</a>\n</row>\n\n</table>\n
-- rowcount: 1
-- end-expected
SELECT query_to_xml('SELECT 1 AS a', false, false, '');
-- begin-expected
-- columns: ?column?:bool
-- row: t
-- rowcount: 1
-- end-expected
SELECT table_to_xml('pg_class', false, false, '') IS NOT NULL;
-- begin-expected
-- columns: xmlserialize:varchar
-- row: <a>x</a>
-- rowcount: 1
-- end-expected
SELECT XMLSERIALIZE(CONTENT '<a>x</a>'::xml AS varchar(20));
-- begin-expected
-- columns: xmlserialize:text
-- row: <a><b>x</b></a>
-- rowcount: 1
-- end-expected
SELECT XMLSERIALIZE(DOCUMENT '<a><b>x</b></a>'::xml AS text NO INDENT);
-- begin-expected
-- ok: 0
-- end-expected
SET xmloption = 'document';
-- begin-expected-error
-- sqlstate: 2200M
-- message-like: invalid XML document
-- end-expected-error
SELECT 'text'::xml;
-- begin-expected-error
-- sqlstate: 42601
-- message-like: XML attribute name "a" appears more than once
-- end-expected-error
SELECT xmlelement(name a, xmlattributes(1 as a, 2 as a));
-- begin-expected-error
-- sqlstate: 2200M
-- message-like: invalid XML document
-- end-expected-error
SELECT '<a>'::xml;
-- begin-expected-error
-- sqlstate: 2200M
-- message-like: invalid XML document
-- end-expected-error
SELECT XMLPARSE(DOCUMENT 'abc');
-- begin-expected-error
-- sqlstate: 2200S
-- message-like: invalid XML comment
-- end-expected-error
SELECT xmlcomment('bad -- comment');
