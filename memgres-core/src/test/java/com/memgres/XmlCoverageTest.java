package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Coverage tests for checklist item 117: XML type & functions.
 * Tests XMLPARSE, XMLSERIALIZE, xml cast, IS DOCUMENT, SET XML OPTION,
 * xmltext, xmlcomment, xmlconcat, xmlelement (with xmlattributes), xmlforest,
 * xmlpi, xmlroot, xmlagg, xmlexists, xml_is_well_formed, xpath, xpath_exists,
 * XMLTABLE, table_to_xml, query_to_xml, schema_to_xml, database_to_xml.
 */
class XmlCoverageTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setup() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE xml_data (id serial PRIMARY KEY, doc text, name text)");
            st.execute("INSERT INTO xml_data (doc, name) VALUES ('<root><a>1</a></root>', 'doc1')");
            st.execute("INSERT INTO xml_data (doc, name) VALUES ('<root><a>2</a><b>3</b></root>', 'doc2')");
            st.execute("INSERT INTO xml_data (doc, name) VALUES ('<item>hello</item>', 'doc3')");
        }
    }

    @AfterAll
    static void teardown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ---- XML Type & Cast ----

    @Test void testXmlCast() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '<root/>'::xml")) {
            assertTrue(rs.next());
            assertEquals("<root/>", rs.getString(1));
        }
    }

    @Test void testCastXmlToText() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT CAST('<root/>' AS xml)")) {
            assertTrue(rs.next());
            assertEquals("<root/>", rs.getString(1));
        }
    }

    @Test void testXmlColumnType() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE xml_typed (id int, payload xml)");
            st.execute("INSERT INTO xml_typed VALUES (1, '<data>test</data>')");
            try (ResultSet rs = st.executeQuery("SELECT payload FROM xml_typed WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals("<data>test</data>", rs.getString(1));
            }
            st.execute("DROP TABLE xml_typed");
        }
    }

    // ---- XMLPARSE ----

    @Test void testXmlparseDocument() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLPARSE(DOCUMENT '<root><a>1</a></root>')")) {
            assertTrue(rs.next());
            assertEquals("<root><a>1</a></root>", rs.getString(1));
        }
    }

    @Test void testXmlparseContent() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLPARSE(CONTENT '<a>1</a><b>2</b>')")) {
            assertTrue(rs.next());
            assertEquals("<a>1</a><b>2</b>", rs.getString(1));
        }
    }

    @Test void testXmlparseDocumentInvalid() throws Exception {
        try (Statement st = conn.createStatement()) {
            assertThrows(Exception.class, () ->
                    st.executeQuery("SELECT XMLPARSE(DOCUMENT '<unclosed>')"));
        }
    }

    @Test void testXmlparseContentEmpty() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLPARSE(CONTENT '')")) {
            assertTrue(rs.next());
            assertEquals("", rs.getString(1));
        }
    }

    // ---- XMLSERIALIZE ----

    @Test void testXmlserializeContent() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLSERIALIZE(CONTENT '<a>1</a>' AS text)")) {
            assertTrue(rs.next());
            assertEquals("<a>1</a>", rs.getString(1));
        }
    }

    @Test void testXmlserializeDocument() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLSERIALIZE(DOCUMENT '<root/>' AS text)")) {
            assertTrue(rs.next());
            assertEquals("<root/>", rs.getString(1));
        }
    }

    // ---- IS DOCUMENT / IS NOT DOCUMENT ----

    @Test void testIsDocumentTrue() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '<root><a/></root>' IS DOCUMENT")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test void testIsDocumentFalseFragment() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '<a/><b/>' IS DOCUMENT")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
        }
    }

    @Test void testIsNotDocument() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '<a/><b/>' IS NOT DOCUMENT")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test void testIsDocumentWellFormed() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '<html><body>text</body></html>' IS DOCUMENT")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test void testIsNotDocumentWellFormed() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '<root/>' IS NOT DOCUMENT")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
        }
    }

    // ---- SET XML OPTION ----

    @Test void testSetXmlOptionDocument() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("SET XML OPTION DOCUMENT");
            try (ResultSet rs = st.executeQuery("SHOW xmloption")) {
                assertTrue(rs.next());
                assertEquals("document", rs.getString(1));
            }
        }
    }

    @Test void testSetXmlOptionContent() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("SET XML OPTION CONTENT");
            try (ResultSet rs = st.executeQuery("SHOW xmloption")) {
                assertTrue(rs.next());
                assertEquals("content", rs.getString(1));
            }
        }
    }

    // ---- xmltext ----

    @Test void testXmltext() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xmltext('hello <world> & \"stuff\"')")) {
            assertTrue(rs.next());
            assertEquals("hello &lt;world&gt; &amp; \"stuff\"", rs.getString(1));
        }
    }

    @Test void testXmltextPlain() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xmltext('plain text')")) {
            assertTrue(rs.next());
            assertEquals("plain text", rs.getString(1));
        }
    }

    // ---- xmlcomment ----

    @Test void testXmlcomment() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xmlcomment('hello')")) {
            assertTrue(rs.next());
            assertEquals("<!--hello-->", rs.getString(1));
        }
    }

    @Test void testXmlcommentWithSpaces() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xmlcomment(' a comment ')")) {
            assertTrue(rs.next());
            assertEquals("<!-- a comment -->", rs.getString(1));
        }
    }

    @Test void testXmlcommentInvalidDoubleDash() throws Exception {
        try (Statement st = conn.createStatement()) {
            assertThrows(Exception.class, () ->
                    st.executeQuery("SELECT xmlcomment('bad--comment')"));
        }
    }

    @Test void testXmlcommentInvalidTrailingDash() throws Exception {
        try (Statement st = conn.createStatement()) {
            assertThrows(Exception.class, () ->
                    st.executeQuery("SELECT xmlcomment('bad-')"));
        }
    }

    // ---- xmlconcat ----

    @Test void testXmlconcat() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xmlconcat('<a/>', '<b/>')")) {
            assertTrue(rs.next());
            assertEquals("<a/><b/>", rs.getString(1));
        }
    }

    @Test void testXmlconcatThree() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xmlconcat('<a/>', '<b/>', '<c/>')")) {
            assertTrue(rs.next());
            assertEquals("<a/><b/><c/>", rs.getString(1));
        }
    }

    @Test void testXmlconcatWithDeclaration() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xmlconcat('<?xml version=\"1.0\"?><a/>', '<b/>')")) {
            assertTrue(rs.next());
            assertEquals("<a/><b/>", rs.getString(1));
        }
    }

    @Test void testXmlconcatWithNull() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xmlconcat('<a/>', NULL, '<b/>')")) {
            assertTrue(rs.next());
            assertEquals("<a/><b/>", rs.getString(1));
        }
    }

    // ---- xmlelement ----

    @Test void testXmlelementSimple() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLELEMENT(NAME foo)")) {
            assertTrue(rs.next());
            assertEquals("<foo/>", rs.getString(1));
        }
    }

    @Test void testXmlelementWithContent() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLELEMENT(NAME foo, 'bar')")) {
            assertTrue(rs.next());
            assertEquals("<foo>bar</foo>", rs.getString(1));
        }
    }

    @Test void testXmlelementWithAttributes() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLELEMENT(NAME foo, XMLATTRIBUTES('v1' AS a1, 'v2' AS a2))")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("a1=\"v1\""));
            assertTrue(result.contains("a2=\"v2\""));
            assertTrue(result.startsWith("<foo ") && result.endsWith("/>"));
        }
    }

    @Test void testXmlelementWithAttrsAndContent() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLELEMENT(NAME div, XMLATTRIBUTES('myclass' AS class), 'hello')")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("class=\"myclass\""));
            assertTrue(result.contains("hello"));
            assertTrue(result.endsWith("</div>"));
        }
    }

    @Test void testXmlelementWithExpression() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLELEMENT(NAME val, 1 + 2)")) {
            assertTrue(rs.next());
            assertEquals("<val>3</val>", rs.getString(1));
        }
    }

    @Test void testXmlelementMultipleContents() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLELEMENT(NAME p, 'hello', ' ', 'world')")) {
            assertTrue(rs.next());
            assertEquals("<p>hello world</p>", rs.getString(1));
        }
    }

    @Test void testXmlelementFromColumn() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLELEMENT(NAME item, name) FROM xml_data WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals("<item>doc1</item>", rs.getString(1));
        }
    }

    // ---- xmlforest ----

    @Test void testXmlforest() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLFOREST('hello' AS greeting, 'world' AS target)")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("<greeting>hello</greeting>"));
            assertTrue(result.contains("<target>world</target>"));
        }
    }

    @Test void testXmlforestWithNull() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLFOREST('hello' AS greeting, NULL AS missing)")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("<greeting>hello</greeting>"));
            assertFalse(result.contains("missing")); // NULL produces no element
        }
    }

    @Test void testXmlforestFromColumns() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLFOREST(id AS id, name AS name) FROM xml_data WHERE id = 1")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("<id>1</id>"));
            assertTrue(result.contains("<name>doc1</name>"));
        }
    }

    @Test void testXmlforestSingleElement() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLFOREST(42 AS answer)")) {
            assertTrue(rs.next());
            assertEquals("<answer>42</answer>", rs.getString(1));
        }
    }

    // ---- xmlpi ----

    @Test void testXmlpiNoContent() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLPI(NAME php)")) {
            assertTrue(rs.next());
            assertEquals("<?php?>", rs.getString(1));
        }
    }

    @Test void testXmlpiWithContent() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLPI(NAME php, 'echo \"hello\";')")) {
            assertTrue(rs.next());
            assertEquals("<?php echo \"hello\";?>", rs.getString(1));
        }
    }

    @Test void testXmlpiInvalidTarget() throws Exception {
        try (Statement st = conn.createStatement()) {
            // 'xml' as target is not allowed
            assertThrows(Exception.class, () ->
                    st.executeQuery("SELECT XMLPI(NAME xml)"));
        }
    }

    // ---- xmlroot ----

    @Test void testXmlrootVersion() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLROOT('<a/>', VERSION '1.0')")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("<?xml"));
            assertTrue(result.contains("version=\"1.0\""));
            assertTrue(result.contains("<a/>"));
        }
    }

    @Test void testXmlrootNoVersion() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLROOT('<a/>', VERSION NO VALUE)")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("<?xml"));
            assertTrue(result.contains("<a/>"));
        }
    }

    @Test void testXmlrootStandaloneYes() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLROOT('<a/>', VERSION '1.0', STANDALONE YES)")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("standalone=\"yes\""));
        }
    }

    @Test void testXmlrootStandaloneNo() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLROOT('<a/>', VERSION '1.0', STANDALONE NO)")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("standalone=\"no\""));
        }
    }

    @Test void testXmlrootStandaloneNoValue() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLROOT('<a/>', VERSION '1.0', STANDALONE NO VALUE)")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("version=\"1.0\""));
            assertFalse(result.contains("standalone"));
        }
    }

    // ---- xmlagg ----

    @Test void testXmlagg() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT xmlagg(XMLELEMENT(NAME item, name)) FROM xml_data")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("<item>doc1</item>"));
            assertTrue(result.contains("<item>doc2</item>"));
            assertTrue(result.contains("<item>doc3</item>"));
        }
    }

    @Test void testXmlaggEmpty() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT xmlagg(XMLELEMENT(NAME item, name)) FROM xml_data WHERE id = -1")) {
            assertTrue(rs.next());
            assertNull(rs.getString(1));
        }
    }

    // ---- xmlexists ----

    @Test void testXmlexistsTrue() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLEXISTS('//a' PASSING '<root><a>1</a></root>')")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test void testXmlexistsFalse() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLEXISTS('//b' PASSING '<root><a>1</a></root>')")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
        }
    }

    @Test void testXmlexistsByRef() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLEXISTS('//a' PASSING BY REF '<root><a/></root>')")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    // ---- xml_is_well_formed ----

    @Test void testXmlIsWellFormedTrue() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xml_is_well_formed('<root/>')")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test void testXmlIsWellFormedFalse() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xml_is_well_formed('<unclosed>')")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
        }
    }

    @Test void testXmlIsWellFormedDocument() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xml_is_well_formed_document('<root><a/></root>')")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test void testXmlIsWellFormedDocumentFalse() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xml_is_well_formed_document('<a/><b/>')")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
        }
    }

    @Test void testXmlIsWellFormedContent() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xml_is_well_formed_content('<a/><b/>')")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test void testXmlIsWellFormedContentFalse() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xml_is_well_formed_content('<unclosed>')")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
        }
    }

    @Test void testXmlIsWellFormedPlainText() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xml_is_well_formed_content('hello world')")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1)); // plain text is valid content
        }
    }

    // ---- xpath ----

    @Test void testXpath() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xpath('//a', '<root><a>1</a><a>2</a></root>')")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("<a>1</a>"));
            assertTrue(result.contains("<a>2</a>"));
        }
    }

    @Test void testXpathSingle() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xpath('//b', '<root><a>1</a><b>2</b></root>')")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("<b>2</b>"));
        }
    }

    @Test void testXpathNoMatch() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xpath('//z', '<root><a>1</a></root>')")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertEquals("{}", result);
        }
    }

    @Test void testXpathText() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xpath('//a/text()', '<root><a>hello</a></root>')")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("hello"));
        }
    }

    @Test void testXpathAttribute() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xpath('/root/@id', '<root id=\"42\"/>')")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("42"));
        }
    }

    // ---- xpath_exists ----

    @Test void testXpathExistsTrue() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xpath_exists('//a', '<root><a>1</a></root>')")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test void testXpathExistsFalse() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xpath_exists('//z', '<root><a>1</a></root>')")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
        }
    }

    // ---- table_to_xml ----

    @Test void testTableToXml() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT table_to_xml('xml_data', true, false, '')")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("<xml_data>"));
            assertTrue(result.contains("<row>"));
            assertTrue(result.contains("<name>doc1</name>"));
            assertTrue(result.contains("</xml_data>"));
        }
    }

    @Test void testTableToXmlNoNulls() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE xml_null_test (id int, val text)");
            st.execute("INSERT INTO xml_null_test VALUES (1, NULL)");
            try (ResultSet rs = st.executeQuery("SELECT table_to_xml('xml_null_test', false, false, '')")) {
                assertTrue(rs.next());
                String result = rs.getString(1);
                assertFalse(result.contains("xsi:nil"));
            }
            st.execute("DROP TABLE xml_null_test");
        }
    }

    // ---- query_to_xml ----

    @Test void testQueryToXml() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT query_to_xml('SELECT 1 AS num, ''hello'' AS txt', true, false, '')")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("<num>1</num>"));
            assertTrue(result.contains("<txt>hello</txt>"));
        }
    }

    // ---- schema_to_xml & database_to_xml (stub) ----

    @Test void testSchemaToXml() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT schema_to_xml('public', true, false, '')")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    @Test void testDatabaseToXml() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT database_to_xml(true, false, '')")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    // ---- XML with special characters ----

    @Test void testXmltextSpecialChars() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xmltext('a < b & c > d')")) {
            assertTrue(rs.next());
            assertEquals("a &lt; b &amp; c &gt; d", rs.getString(1));
        }
    }

    @Test void testXmlelementEscapesContent() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLELEMENT(NAME p, 'a < b')")) {
            assertTrue(rs.next());
            assertEquals("<p>a &lt; b</p>", rs.getString(1));
        }
    }

    @Test void testXmlelementEscapesAttributes() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLELEMENT(NAME a, XMLATTRIBUTES('he said \"hi\"' AS title))")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("&quot;"));
        }
    }

    // ---- Combining XML functions ----

    @Test void testXmlconcatWithElements() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xmlconcat(XMLELEMENT(NAME a, 1), XMLELEMENT(NAME b, 2))")) {
            assertTrue(rs.next());
            assertEquals("<a>1</a><b>2</b>", rs.getString(1));
        }
    }

    @Test void testXmlelementNested() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT XMLELEMENT(NAME outer, XMLELEMENT(NAME inner, 'text'))")) {
            assertTrue(rs.next());
            // The inner element will be escaped
            String result = rs.getString(1);
            assertTrue(result.startsWith("<outer>"));
            assertTrue(result.endsWith("</outer>"));
        }
    }

    @Test void testXmlforestEscaping() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLFOREST('a & b' AS item)")) {
            assertTrue(rs.next());
            assertEquals("<item>a &amp; b</item>", rs.getString(1));
        }
    }

    // ---- IS DOCUMENT in WHERE clause ----

    @Test void testIsDocumentInWhere() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT doc FROM xml_data WHERE doc IS DOCUMENT ORDER BY id")) {
            // All our test docs are valid documents
            assertTrue(rs.next());
            assertEquals("<root><a>1</a></root>", rs.getString(1));
        }
    }

    // ---- Various edge cases ----

    @Test void testXmlcommentEmpty() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xmlcomment('')")) {
            assertTrue(rs.next());
            assertEquals("<!---->", rs.getString(1));
        }
    }

    @Test void testXmlelementNameKeyword() throws Exception {
        // Element name can be a keyword like "table"
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLELEMENT(NAME data, 'content')")) {
            assertTrue(rs.next());
            assertEquals("<data>content</data>", rs.getString(1));
        }
    }

    @Test void testXpathRootElement() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xpath('/root', '<root><a>1</a></root>')")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("<root>"));
        }
    }

    @Test void testXpathCount() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xpath('count(//a)', '<root><a>1</a><a>2</a></root>')")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("2"));
        }
    }

    @Test void testXmlelementFromQuery() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT XMLELEMENT(NAME row, " +
                     "  XMLELEMENT(NAME id, id), " +
                     "  XMLELEMENT(NAME name, name)) " +
                     "FROM xml_data WHERE id = 1")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.startsWith("<row>"));
            assertTrue(result.endsWith("</row>"));
        }
    }

    @Test void testXmlpiCustom() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLPI(NAME stylesheet, 'type=\"text/xsl\" href=\"style.xsl\"')")) {
            assertTrue(rs.next());
            assertEquals("<?stylesheet type=\"text/xsl\" href=\"style.xsl\"?>", rs.getString(1));
        }
    }

    @Test void testXmlrootStripsOldDeclaration() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLROOT('<?xml version=\"1.0\"?><a/>', VERSION '1.1')")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("version=\"1.1\""));
            // Should not have duplicate declarations
            assertEquals(result.indexOf("<?xml"), result.lastIndexOf("<?xml"));
        }
    }

    // ---- xml_is_well_formed variants ----

    @Test void testXmlIsWellFormedWithDeclaration() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xml_is_well_formed('<?xml version=\"1.0\"?><root/>')")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test void testXmlIsWellFormedMalformed() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xml_is_well_formed('not xml at <all')")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
        }
    }

    // ---- XMLELEMENT with XMLATTRIBUTES from columns ----

    @Test void testXmlelementWithColumnAttributes() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT XMLELEMENT(NAME item, XMLATTRIBUTES(id AS id), name) FROM xml_data WHERE id = 2")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("id=\"2\""));
            assertTrue(result.contains("doc2"));
        }
    }

    // ---- Multiple XMLFOREST expressions ----

    @Test void testXmlforestAllNull() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLFOREST(NULL AS a, NULL AS b)")) {
            assertTrue(rs.next());
            assertNull(rs.getString(1));
        }
    }

    // ---- pg_typeof xml ----

    @Test void testPgTypeofXml() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT pg_typeof(XMLPARSE(DOCUMENT '<a/>'))")) {
            assertTrue(rs.next());
            // May return "xml" or "text" depending on implementation
            assertNotNull(rs.getString(1));
        }
    }

    // ---- XML in subquery ----

    @Test void testXmlInSubquery() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT * FROM (SELECT XMLELEMENT(NAME item, 'hello') AS x) sub")) {
            assertTrue(rs.next());
            assertEquals("<item>hello</item>", rs.getString(1));
        }
    }

    // ---- xmlexists with complex XPath ----

    @Test void testXmlexistsDeepPath() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT XMLEXISTS('//root/a' PASSING '<root><a><b>1</b></a></root>')")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test void testXmlexistsPredicate() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT XMLEXISTS('//item[@id]' PASSING '<root><item id=\"1\"/></root>')")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    // ---- xmlagg with GROUP BY ----

    @Test void testXmlaggGroupBy() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE xml_group (category text, val text)");
            st.execute("INSERT INTO xml_group VALUES ('a', 'x1'), ('a', 'x2'), ('b', 'y1')");
            try (ResultSet rs = st.executeQuery(
                    "SELECT category, xmlagg(XMLELEMENT(NAME v, val)) FROM xml_group GROUP BY category ORDER BY category")) {
                assertTrue(rs.next());
                assertEquals("a", rs.getString(1));
                String agg = rs.getString(2);
                assertTrue(agg.contains("<v>x1</v>"));
                assertTrue(agg.contains("<v>x2</v>"));
                assertTrue(rs.next());
                assertEquals("b", rs.getString(1));
                assertTrue(rs.getString(2).contains("<v>y1</v>"));
            }
            st.execute("DROP TABLE xml_group");
        }
    }

    // ---- table_to_xml with nulls and forest mode ----

    @Test void testTableToXmlForestMode() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE xml_forest_test (id int, val text)");
            st.execute("INSERT INTO xml_forest_test VALUES (1, 'a'), (2, 'b')");
            try (ResultSet rs = st.executeQuery("SELECT table_to_xml('xml_forest_test', true, true, '')")) {
                assertTrue(rs.next());
                String result = rs.getString(1);
                // Forest mode: no wrapping table element
                assertTrue(result.contains("<row>"));
                assertFalse(result.contains("<xml_forest_test>"));
            }
            st.execute("DROP TABLE xml_forest_test");
        }
    }

    // ---- XMLPARSE with complex document ----

    @Test void testXmlparseComplexDocument() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT XMLPARSE(DOCUMENT '<html><head><title>Test</title></head><body><p>Content</p></body></html>')")) {
            assertTrue(rs.next());
            assertTrue(rs.getString(1).contains("<title>Test</title>"));
        }
    }

    // ---- xpath with multiple results ----

    @Test void testXpathMultipleResults() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xpath('//item', '<root><item>a</item><item>b</item><item>c</item></root>')")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("<item>a</item>"));
            assertTrue(result.contains("<item>b</item>"));
            assertTrue(result.contains("<item>c</item>"));
        }
    }

    // ---- XMLELEMENT with numeric/boolean content ----

    @Test void testXmlelementNumericContent() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLELEMENT(NAME num, 42)")) {
            assertTrue(rs.next());
            assertEquals("<num>42</num>", rs.getString(1));
        }
    }

    // ---- xml_is_well_formed edge cases ----

    @Test void testXmlIsWellFormedEmptyString() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xml_is_well_formed('')")) {
            assertTrue(rs.next());
            // Empty string is valid content
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test void testXmlIsWellFormedDocumentEmpty() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xml_is_well_formed_document('')")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1)); // Empty string is NOT a valid document
        }
    }

    // ---- XMLELEMENT with no content or attrs produces self-closing tag ----

    @Test void testXmlelementSelfClosing() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT XMLELEMENT(NAME br)")) {
            assertTrue(rs.next());
            assertEquals("<br/>", rs.getString(1));
        }
    }

    // ---- xmlconcat all nulls ----

    @Test void testXmlconcatAllNull() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT xmlconcat(NULL, NULL)")) {
            assertTrue(rs.next());
            assertNull(rs.getString(1));
        }
    }
}
