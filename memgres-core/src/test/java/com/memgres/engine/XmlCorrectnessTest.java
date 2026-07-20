package com.memgres.engine;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * XML correctness tests for H34, L1.
 */
class XmlCorrectnessTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private String q(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    // ========================================================================
    // H34.1: Nested xmlelement should not re-escape xml args
    // ========================================================================

    @Test
    void h34_nestedXmlelement() throws SQLException {
        String result = q("SELECT xmlelement(name foo, xmlelement(name bar))");
        assertEquals("<foo><bar/></foo>", result);
    }

    @Test
    void h34_nestedXmlelementWithContent() throws SQLException {
        String result = q("SELECT xmlelement(name outer, xmlelement(name inner, 'hello'))");
        assertEquals("<outer><inner>hello</inner></outer>", result);
    }

    @Test
    void h34_xmlelementTextContent() throws SQLException {
        // Text content should still be escaped
        String result = q("SELECT xmlelement(name p, 'x < y & z')");
        assertEquals("<p>x &lt; y &amp; z</p>", result);
    }

    @Test
    void h34_xmlelementMixedContent() throws SQLException {
        // Mix of text and xml — text escapes, xml passes through
        String result = q("SELECT xmlelement(name div, xmlelement(name br))");
        assertEquals("<div><br/></div>", result);
    }

    // ========================================================================
    // H34.2: Invalid XML element names should be escaped
    // ========================================================================

    @Test
    void h34_invalidElementNameDollar() throws SQLException {
        String result = q("SELECT xmlelement(name \"Foo$Bar\", 'val')");
        // PG escapes $ as _x0024_
        assertTrue(result.contains("_x0024_"),
                "Invalid char $ should be escaped to _x0024_, got: " + result);
    }

    @Test
    void h34_invalidElementNameDigitStart() throws SQLException {
        String result = q("SELECT xmlelement(name \"1abc\", 'val')");
        // Starting digit should be escaped
        assertTrue(result.contains("_x0031_"),
                "Leading digit should be escaped, got: " + result);
    }

    @Test
    void h34_validElementNameUnchanged() throws SQLException {
        // Valid names should not be changed
        String result = q("SELECT xmlelement(name foo_bar, 'val')");
        assertEquals("<foo_bar>val</foo_bar>", result);
    }

    // ========================================================================
    // H34.3: xpath results should be properly escaped
    // ========================================================================

    @Test
    void h34_xpathTextEscaping() throws SQLException {
        // Text content with & should be escaped in xpath results
        String result = q("SELECT xpath('/root/text()', '<root>x &amp; y</root>')");
        assertNotNull(result);
        assertTrue(result.contains("&amp;"),
                "xpath text should re-serialize with XML escaping, got: " + result);
    }

    @Test
    void h34_xpathElementResult() throws SQLException {
        String result = q("SELECT xpath('/root/a', '<root><a>1</a><a>2</a></root>')");
        assertNotNull(result);
        assertTrue(result.contains("<a>1</a>") && result.contains("<a>2</a>"),
                "xpath should return element nodes, got: " + result);
    }

    // ========================================================================
    // H34.4: xmlagg ORDER BY
    // ========================================================================

    @Test
    void h34_xmlaggOrderBy() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE xml_test (id integer, val text)");
            s.execute("INSERT INTO xml_test VALUES (3, 'c'), (1, 'a'), (2, 'b')");

            String result = q("SELECT xmlagg(xmlelement(name v, val) ORDER BY id) FROM xml_test");
            assertNotNull(result);
            assertEquals("<v>a</v><v>b</v><v>c</v>", result);
        }
    }

    @Test
    void h34_xmlaggOrderByDesc() throws SQLException {
        String result = q("SELECT xmlagg(xmlelement(name v, val) ORDER BY id DESC) FROM xml_test");
        assertNotNull(result);
        assertEquals("<v>c</v><v>b</v><v>a</v>", result);
    }

    // ========================================================================
    // H34.5: table_to_xml xsi namespace
    // ========================================================================

    @Test
    void h34_tableToXmlXsiNamespace() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE xml_ns_test (id integer, name text)");
            s.execute("INSERT INTO xml_ns_test VALUES (1, NULL)");

            String result = q("SELECT table_to_xml('xml_ns_test', true, false, '')");
            assertNotNull(result);
            assertTrue(result.contains("xmlns:xsi"),
                    "table_to_xml with nulls=true should include xsi namespace, got: " + result);
            assertTrue(result.contains("xsi:nil=\"true\""),
                    "Should have xsi:nil for NULL values, got: " + result);
        }
    }

    // ========================================================================
    // H34.6: ::xml cast validation
    // ========================================================================

    @Test
    void h34_xmlCastValid() throws SQLException {
        // Valid XML should work
        assertEquals("<root/>", q("SELECT '<root/>'::xml"));
    }

    @Test
    void h34_xmlCastValidContent() throws SQLException {
        // Valid XML content (fragment) should work
        String result = q("SELECT '<a/><b/>'::xml");
        assertEquals("<a/><b/>", result);
    }

    @Test
    void h34_xmlCastInvalid() {
        // Invalid XML should throw with SQLSTATE 2200N
        SQLException ex = assertThrows(SQLException.class,
                () -> q("SELECT '<unclosed>'::xml"));
        assertTrue(ex.getSQLState().equals("2200N") || ex.getSQLState().equals("2200M"),
                "Expected SQLSTATE 2200N or 2200M for invalid XML cast, got: " + ex.getSQLState());
    }

    // ========================================================================
    // L1: XML SQLSTATE fixes
    // ========================================================================

    @Test
    void l1_xmlcommentSqlstate() {
        // xmlcomment with -- should return SQLSTATE 2200S
        SQLException ex = assertThrows(SQLException.class,
                () -> q("SELECT xmlcomment('bad--comment')"));
        assertEquals("2200S", ex.getSQLState(),
                "xmlcomment error should use SQLSTATE 2200S");
    }

    @Test
    void l1_xmlcommentTrailingDashSqlstate() {
        // xmlcomment ending with - should return SQLSTATE 2200S
        SQLException ex = assertThrows(SQLException.class,
                () -> q("SELECT xmlcomment('bad-')"));
        assertEquals("2200S", ex.getSQLState(),
                "xmlcomment trailing dash error should use SQLSTATE 2200S");
    }

    @Test
    void l1_xmlpiSqlstate() {
        // xmlpi with target "xml" should return SQLSTATE 2200T
        SQLException ex = assertThrows(SQLException.class,
                () -> q("SELECT XMLPI(NAME xml)"));
        assertEquals("2200T", ex.getSQLState(),
                "xmlpi target 'xml' error should use SQLSTATE 2200T");
    }

    @Test
    void l1_xmlparseContentSqlstate() {
        // XMLPARSE CONTENT with invalid XML should return SQLSTATE 2200N
        SQLException ex = assertThrows(SQLException.class,
                () -> q("SELECT XMLPARSE(CONTENT '<unclosed>')"));
        assertEquals("2200N", ex.getSQLState(),
                "XMLPARSE CONTENT error should use SQLSTATE 2200N");
    }

    // ========================================================================
    // Misc: xmlelement with xmlforest, xmlconcat inside
    // ========================================================================

    @Test
    void h34_xmlelementWithXmlforest() throws SQLException {
        String result = q("SELECT xmlelement(name person, xmlforest('John' AS name, '30' AS age))");
        assertEquals("<person><name>John</name><age>30</age></person>", result);
    }

    @Test
    void h34_xmlelementWithXmlconcat() throws SQLException {
        String result = q("SELECT xmlelement(name wrap, xmlconcat('<a/>'::xml, '<b/>'::xml))");
        assertEquals("<wrap><a/><b/></wrap>", result);
    }

    @Test
    void h34_escapeXmlNameSpecialChars() throws SQLException {
        // xmlforest with special chars in column name
        String result = q("SELECT xmlelement(name \"test-elem\", 'ok')");
        assertEquals("<test-elem>ok</test-elem>", result);
    }
}
