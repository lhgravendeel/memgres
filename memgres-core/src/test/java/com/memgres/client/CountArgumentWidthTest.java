package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * These routines take an integer count and PostgreSQL declares no bigint overload, so a wider
 * count is a function that does not exist. Narrowing it instead produced an empty string for a
 * count of four billion — a result nothing downstream could tell apart from a real one.
 */
class CountArgumentWidthTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE caw_t (t text, n int)");
        exec("INSERT INTO caw_t VALUES ('abcde', 3)");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    private static void assertNoSuchFunction(String expectedSignature, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals("42883", e.getSQLState(),
                "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(expectedSignature),
                "expected signature " + expectedSignature + " in: " + e.getMessage());
    }

    @Test
    void aCountWiderThanIntegerHasNoMatchingFunction() {
        assertNoSuchFunction("left(unknown, bigint)", "SELECT left('abcde', 4294967296)");
        assertNoSuchFunction("right(unknown, bigint)", "SELECT right('abcde', 4294967296)");
        assertNoSuchFunction("repeat(unknown, bigint)", "SELECT repeat('ab', 4294967296)");
        assertNoSuchFunction("lpad(unknown, bigint, unknown)",
                "SELECT lpad('abc', 4294967296, 'x')");
        assertNoSuchFunction("rpad(unknown, bigint, unknown)",
                "SELECT rpad('abc', 4294967296, 'x')");
        assertNoSuchFunction("substr(unknown, bigint)", "SELECT substr('abcde', 4294967296)");
        assertNoSuchFunction("substr(unknown, integer, bigint)",
                "SELECT substr('abcde', 1, 4294967296)");
        assertNoSuchFunction("split_part(unknown, unknown, bigint)",
                "SELECT split_part('a,b', ',', 4294967296)");
    }

    @Test
    void theArgumentTypesReportedFollowTheCall() {
        // a column is text where a bare literal is still unknown
        assertNoSuchFunction("left(text, bigint)", "SELECT left(t, 4294967296) FROM caw_t");
        assertNoSuchFunction("repeat(text, bigint)", "SELECT repeat(t, 4294967296) FROM caw_t");
        assertNoSuchFunction("lpad(text, bigint, unknown)",
                "SELECT lpad(t, 4294967296, 'x') FROM caw_t");
    }

    @Test
    void theBoundaryIsTheIntegerRangeNotTheStringLength() throws Exception {
        // one past the top of integer has no overload
        assertNoSuchFunction("left(unknown, bigint)", "SELECT left('abcde', 2147483648)");
        assertNoSuchFunction("left(unknown, bigint)", "SELECT left('abcde', -4294967296)");
        assertNoSuchFunction("left(unknown, bigint)", "SELECT left('abcde', 4294967296::bigint)");
        // the top of integer itself is fine
        assertEquals("abcde", scalar("SELECT left('abcde', 2147483647)"));
    }

    @Test
    void countsThatFitAreUnaffected() throws Exception {
        assertEquals("ab", scalar("SELECT left('abcde', 2)"));
        assertEquals("de", scalar("SELECT right('abcde', 2)"));
        assertEquals("abcd", scalar("SELECT left('abcde', -1)"));
        assertEquals("bcde", scalar("SELECT right('abcde', -1)"));
        assertEquals("", scalar("SELECT left('abcde', 0)"));
        assertEquals("ababab", scalar("SELECT repeat('ab', 3)"));
        assertEquals("", scalar("SELECT repeat('ab', 0)"));
        assertEquals("xyxabc", scalar("SELECT lpad('abc', 6, 'xy')"));
        assertEquals("abcxyx", scalar("SELECT rpad('abc', 6, 'xy')"));
        assertEquals("bcde", scalar("SELECT substr('abcde', 2)"));
        assertEquals("bc", scalar("SELECT substr('abcde', 2, 2)"));
        assertEquals("b", scalar("SELECT split_part('a,b,c', ',', 2)"));
        assertEquals("c", scalar("SELECT split_part('a,b,c', ',', -1)"));
        // a smallint or a column value narrows normally
        assertEquals("abc", scalar("SELECT left('abcde', 3::smallint)"));
        assertEquals("abc", scalar("SELECT left('abcde', n) FROM caw_t"));
        assertEquals("ababab", scalar("SELECT repeat('ab', n) FROM caw_t"));
    }

    @Test
    void nullCountsStayStrict() throws Exception {
        // a NULL count is still NULL rather than a complaint about width
        assertNull(scalar("SELECT left('abcde', NULL)"));
        assertNull(scalar("SELECT right('abcde', NULL)"));
        assertNull(scalar("SELECT repeat('ab', NULL)"));
        assertNull(scalar("SELECT lpad('abc', NULL, 'x')"));
        assertNull(scalar("SELECT rpad('abc', NULL, 'x')"));
        assertNull(scalar("SELECT split_part('a,b', ',', NULL)"));
        assertNull(scalar("SELECT substr(NULL, 1, 2)"));
    }
}
