package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * How rows are written out as XML, and how much of a text query an index could answer.
 *
 * <p>The XML layout is PostgreSQL's own and every part of it is fixed: the schema-instance
 * namespace is on the outermost element whether or not any value is null, the row elements sit at
 * the left margin with their fields indented two spaces, and a blank line stands between the
 * outermost element and the rows it holds. As a forest there is no outermost element, so each row
 * carries the namespaces and is named after what it is a row of.
 *
 * <p>{@code querytree} writes the part of a query an index could be searched with. A NOT branch
 * names what must not be there, which no lookup can supply — and what that leaves depends on the
 * operator above it: an AND still has its other branch, where an OR and a phrase need both.
 */
class XmlLayoutAndQueryTreeTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String one(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), sql);
            return rs.getString(1);
        }
    }

    private static String stateOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    private static String messageOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "";
        } catch (SQLException e) {
            return e.getMessage() == null ? "" : e.getMessage();
        }
    }

    /** The namespace is always there, and the blank line and the indentation are fixed. */
    @Test
    void theLayoutRowsAreWrittenOutIn() throws SQLException {
        assertEquals("<table xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">"
                        + "||<row>|  <a>1</a>|</row>||</table>|",
                one("SELECT replace(query_to_xml('SELECT 1 AS a', false, false, '')::text,"
                        + " chr(10), '|')"));
        assertEquals("<table xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
                        + " xmlns=\"ns\">||<row>|  <a>1</a>|  <b xsi:nil=\"true\"/>|</row>"
                        + "||</table>|",
                one("SELECT replace(query_to_xml('SELECT 1 AS a, NULL::int AS b', true, false,"
                        + " 'ns')::text, chr(10), '|')"));
    }

    /** As a forest each row carries the namespaces and is named after what it is a row of. */
    @Test
    void rowsWrittenOutAsAForest() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE zxq (id int)");
            s.execute("INSERT INTO zxq VALUES (1)");
        }
        assertEquals("<zxq xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">"
                        + "|  <id>1</id>|</zxq>||",
                one("SELECT replace(table_to_xml('zxq', true, true, '')::text, chr(10), '|')"));
        // A query's rows are rows of nothing in particular, so they stay rows.
        assertEquals("<row xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">"
                        + "|  <a>1</a>|</row>||",
                one("SELECT replace(query_to_xml('SELECT 1 AS a', false, true, '')::text,"
                        + " chr(10), '|')"));
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE zxq");
        }
    }

    /** The result is a value of the type that was asked for, width and all. */
    @Test
    void whatXmlserializeMayBeAskedFor() throws SQLException {
        assertEquals("<a>x</a>", one("SELECT XMLSERIALIZE(CONTENT '<a>x</a>'::xml AS varchar(20))"));
        assertEquals("character varying",
                one("SELECT pg_typeof(XMLSERIALIZE(CONTENT '<a>x</a>'::xml AS varchar(20)))::text"));
        assertEquals("<a>xyz</a>          ",
                one("SELECT XMLSERIALIZE(CONTENT '<a>xyz</a>'::xml AS char(20))"));
        // Read as the type rather than cast to it: what will not fit is refused, not shortened.
        assertEquals("22001", stateOf("SELECT XMLSERIALIZE(CONTENT '<a>xyz</a>'::xml AS varchar(5))"));
        assertTrue(messageOf("SELECT XMLSERIALIZE(CONTENT '<a>x</a>'::xml AS int)")
                .contains("cannot cast XMLSERIALIZE result to integer"));
    }

    /** PostgreSQL names the thing that is wrong rather than the rule it broke. */
    @Test
    void whatAnInvalidCommentIsCalled() {
        assertTrue(messageOf("SELECT xmlcomment('bad -- comment')")
                .contains("invalid XML comment"));
        assertTrue(messageOf("SELECT xmlcomment('bad-')").contains("invalid XML comment"));
        assertNull(stateOf("SELECT xmlcomment('ok')"));
    }

    /** What a NOT leaves depends on the operator above it. */
    @Test
    void howMuchOfAQueryAnIndexCouldAnswer() throws SQLException {
        assertEquals("'a' & 'b'", one("SELECT querytree('a & b'::tsquery)"));
        assertEquals("'a'", one("SELECT querytree('a & !b'::tsquery)"));
        assertEquals("T", one("SELECT querytree('!a'::tsquery)"));
        // An OR is satisfied by either branch and a phrase needs both, so neither survives one.
        assertEquals("T", one("SELECT querytree('a | !b'::tsquery)"));
        // A lexeme is written with everything that says which lexemes it names.
        assertEquals("'a':*", one("SELECT querytree('a:*'::tsquery)"));
        // A query with nothing in it has nothing to write, not even the word for "anything".
        assertEquals("", one("SELECT querytree(''::tsquery)"));
    }

    /** A quoted lexeme holding a space asks for the words next to each other. */
    @Test
    void aQuotedPhraseInAQuery() throws SQLException {
        assertEquals("'cat' <-> 'dog'", one("SELECT to_tsquery('english', '''cat dog''')::text"));
        assertEquals("'one' <-> 'two' <-> 'three'",
                one("SELECT to_tsquery('english', '''one two three''')::text"));
        assertEquals("'cat' <-> 'dog' & 'bird'",
                one("SELECT to_tsquery('english', '''cat dog'' & bird')::text"));
        // A literal is read as written: there the quotes make one lexeme, space and all.
        assertEquals("'cat dog'", one("SELECT ('''cat dog'''::tsquery)::text"));
    }
}
