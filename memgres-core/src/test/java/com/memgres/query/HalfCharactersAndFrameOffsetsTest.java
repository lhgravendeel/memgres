package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Escapes that name no character, sorts asked for with an operator that does not sort, and
 * distances measured in a type that cannot measure one.
 *
 * <p>Half of a surrogate pair is not a character: it names one only together with its other half,
 * written as the very next escape. Nor is the zero byte a character a string may hold — it is what
 * ends a string rather than something standing in one.
 *
 * <p>An ordering operator has to be an operator at all and then has to order, and which of the two
 * complaints a sort gets depends on the type being sorted. A client that sends its statement whole
 * is never asked about it in advance, so the question is asked again where the sort runs.
 */
class HalfCharactersAndFrameOffsetsTest {

    private static Memgres memgres;
    private static Connection conn;
    private static Connection whole;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        whole = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (whole != null) whole.close();
        if (memgres != null) memgres.close();
    }

    private static String one(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), sql);
            return rs.getString(1);
        }
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private static String stateOf(String sql) {
        return stateOf(conn, sql);
    }

    private static String stateOf(Connection over, String sql) {
        try (Statement s = over.createStatement()) {
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

    /** A surrogate names a character only with its other half beside it. */
    @Test
    void halfOfACharacterIsNotOne() throws SQLException {
        assertEquals("42601", stateOf("SELECT E'\\uD801'"));
        assertEquals("42601", stateOf("SELECT E'\\uDC00'"));
        assertEquals("42601", stateOf("SELECT E'\\uD801x'"));
        assertEquals("42601", stateOf("SELECT E'\\uD801\\u0041'"));
        // The two halves written together are the one character they stand for.
        assertEquals("\uD801\uDC00", one("SELECT E'\\uD801\\uDC00'"));
        assertEquals("A", one("SELECT E'\\u0041'"));
        // Which half was written decides what the complaint is quoted against.
        assertTrue(messageOf("SELECT E'\\uD801'")
                .contains("invalid Unicode surrogate pair at or near \"'\""));
        assertTrue(messageOf("SELECT E'a\\uDC00'")
                .contains("invalid Unicode surrogate pair at or near \"\\uDC00\""));
    }

    /** The same rule holds for a literal written with its escapes spelled out. */
    @Test
    void halfOfACharacterInAUnicodeLiteral() throws SQLException {
        assertEquals("42601", stateOf("SELECT U&'\\d801'"));
        assertEquals("42601", stateOf("SELECT U&'\\dc00'"));
        assertEquals("42601", stateOf("SELECT U&'a\\dc00b'"));
        assertEquals("\uD801\uDC00", one("SELECT U&'\\d801\\dc00'"));
        assertEquals("A", one("SELECT U&'\\0041'"));
        assertTrue(messageOf("SELECT U&'\\d801'").contains("invalid Unicode surrogate pair"));
    }

    /** The zero byte is not a character a string may hold. */
    @Test
    void theByteThatEndsAString() {
        assertEquals("22021", stateOf("SELECT length(E'\\0')"));
        assertEquals("22021", stateOf("SELECT length(E'a\\000b')"));
        assertEquals("22021", stateOf("SELECT length(E'\\x00')"));
        assertTrue(messageOf("SELECT length(E'\\0')")
                .contains("invalid byte sequence for encoding \"UTF8\": 0x00"));
        // Written as a codepoint it is the escape that is at fault rather than the encoding.
        assertEquals("42601", stateOf("SELECT length(E'\\u0000')"));
        assertEquals("42601", stateOf("SELECT length(U&'\\0000')"));
        assertTrue(messageOf("SELECT length(U&'\\0000'))")
                .contains("invalid Unicode escape value"));
    }

    /** A sort is refused however the statement reached the server. */
    @Test
    void anOperatorThatDoesNotOrder() throws SQLException {
        exec("CREATE TABLE zhc_o (a int, t text)");
        exec("INSERT INTO zhc_o VALUES (2,'b'), (1,'a')");
        for (Connection over : new Connection[] {conn, whole}) {
            assertNull(stateOf(over, "SELECT a FROM zhc_o ORDER BY a USING <"));
            assertEquals("42809", stateOf(over, "SELECT a FROM zhc_o ORDER BY a USING +"));
            assertEquals("42809", stateOf(over, "SELECT a FROM zhc_o ORDER BY a USING *"));
            assertEquals("42809", stateOf(over, "SELECT a FROM zhc_o ORDER BY a USING #"));
            assertEquals("42809", stateOf(over, "SELECT t FROM zhc_o ORDER BY t USING ||"));
            // An operator the type has not got at all is the other complaint.
            assertEquals("42883", stateOf(over, "SELECT t FROM zhc_o ORDER BY t USING +"));
            assertEquals("42809", stateOf(over, "SELECT 1 ORDER BY 1 USING +"));
        }
        assertTrue(messageOf("SELECT a FROM zhc_o ORDER BY a USING #")
                .contains("operator # is not a valid ordering operator"));
        assertTrue(messageOf("SELECT t FROM zhc_o ORDER BY t USING +")
                .contains("operator does not exist: text + text"));
        exec("DROP TABLE zhc_o");
    }

    /** What a window falls back to is the type of what it stands in for. */
    @Test
    void theDefaultIsOfTheValuesType() throws SQLException {
        exec("CREATE TABLE zhc_w (v int)");
        exec("INSERT INTO zhc_w VALUES (10), (20)");
        assertEquals("22P02", stateOf("SELECT lag(v,1,'zz') OVER (ORDER BY v) FROM zhc_w"));
        assertEquals("22P02", stateOf("SELECT lead(v,1,'zz') OVER (ORDER BY v) FROM zhc_w"));
        assertTrue(messageOf("SELECT lag(v,1,'zz') OVER (ORDER BY v) FROM zhc_w")
                .contains("invalid input syntax for type integer: \"zz\""));
        // A default that does read as one is that value.
        assertEquals("5", one("SELECT lag(v,1,'5') OVER (ORDER BY v) FROM zhc_w"));
        exec("DROP TABLE zhc_w");
    }

    /** A distance is measured in a type the ordering column can be stepped along in. */
    @Test
    void whatAFrameOffsetMayBe() throws SQLException {
        exec("CREATE TABLE zhc_f (i int, n numeric, d date, t text)");
        exec("INSERT INTO zhc_f VALUES (1, 1, '2020-01-01', 'a')");
        assertNull(stateOf("SELECT sum(i) OVER (ORDER BY i"
                + " RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM zhc_f"));
        assertNull(stateOf("SELECT sum(i) OVER (ORDER BY n"
                + " RANGE BETWEEN 1.5 PRECEDING AND CURRENT ROW) FROM zhc_f"));
        assertNull(stateOf("SELECT sum(i) OVER (ORDER BY d"
                + " RANGE BETWEEN interval '1 day' PRECEDING AND CURRENT ROW) FROM zhc_f"));
        assertEquals("0A000", stateOf("SELECT sum(i) OVER (ORDER BY i"
                + " RANGE BETWEEN '1'::text PRECEDING AND CURRENT ROW) FROM zhc_f"));
        assertEquals("0A000", stateOf("SELECT sum(i) OVER (ORDER BY i"
                + " RANGE BETWEEN 1.5 PRECEDING AND CURRENT ROW) FROM zhc_f"));
        assertEquals("0A000", stateOf("SELECT sum(i) OVER (ORDER BY d"
                + " RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM zhc_f"));
        assertTrue(messageOf("SELECT sum(i) OVER (ORDER BY d"
                + " RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM zhc_f")
                .contains("column type date and offset type integer"));
        // A column nothing measures a distance along is named on its own.
        assertTrue(messageOf("SELECT sum(i) OVER (ORDER BY t"
                + " RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM zhc_f")
                .endsWith("is not supported for column type text"));
        exec("DROP TABLE zhc_f");
    }

    /** A function an extension brings is not there until the extension is. */
    @Test
    void aFunctionAnExtensionBrings() throws SQLException {
        assertEquals("42883", stateOf("SELECT gen_random_bytes(16)"));
        assertTrue(messageOf("SELECT gen_random_bytes(16)")
                .contains("function gen_random_bytes(integer) does not exist"));
        // A uuid of the server's own making needs nothing installed for it.
        assertEquals("true", one("SELECT (gen_random_uuid() IS NOT NULL)::text"));
        exec("CREATE EXTENSION pgcrypto");
        assertEquals("16", one("SELECT length(gen_random_bytes(16))::text"));
        exec("DROP EXTENSION pgcrypto");
    }
}
