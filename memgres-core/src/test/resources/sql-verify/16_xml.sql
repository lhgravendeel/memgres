-- XML correctness: H34, L1

-- H34.1: nested xmlelement should not re-escape
-- begin-expected
-- columns: x
-- row: <foo><bar/></foo>
-- end-expected
SELECT xmlelement(name foo, xmlelement(name bar)) AS x;

-- H34.1: text content should still be escaped
-- begin-expected
-- columns: x
-- row: <p>x &lt; y &amp; z</p>
-- end-expected
SELECT xmlelement(name p, 'x < y & z') AS x;

-- H34.2: invalid element name escaping
-- begin-expected
-- columns: x
-- row: <Foo_x0024_Bar>val</Foo_x0024_Bar>
-- end-expected
SELECT xmlelement(name "Foo$Bar", 'val') AS x;

-- H34.6: valid xml cast should work
-- begin-expected
-- columns: x
-- row: <root/>
-- end-expected
SELECT '<root/>'::xml AS x;

-- H34.6: invalid xml cast should error
-- begin-expected-error
-- sqlstate: 2200N
-- end-expected-error
SELECT '<unclosed>'::xml AS x;

-- L1: xmlcomment SQLSTATE 2200S
-- begin-expected-error
-- sqlstate: 2200S
-- end-expected-error
SELECT xmlcomment('bad--comment');

-- L1: xmlpi SQLSTATE 2200T
-- begin-expected-error
-- sqlstate: 2200T
-- end-expected-error
SELECT XMLPI(NAME xml);

-- L1: XMLPARSE CONTENT SQLSTATE 2200N
-- begin-expected-error
-- sqlstate: 2200N
-- end-expected-error
SELECT XMLPARSE(CONTENT '<unclosed>');

-- H34.1: nested with content
-- begin-expected
-- columns: x
-- row: <outer><inner>hello</inner></outer>
-- end-expected
SELECT xmlelement(name outer, xmlelement(name inner, 'hello')) AS x;

-- H34: xmlelement with xmlforest inside
-- begin-expected
-- columns: x
-- row: <person><name>John</name><age>30</age></person>
-- end-expected
SELECT xmlelement(name person, xmlforest('John' AS name, '30' AS age)) AS x;
