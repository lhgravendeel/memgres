package com.memgres.compat16;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

class FetchOffsetCompatTest {
    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE fetch_data (id integer PRIMARY KEY, score integer, name text)");
            stmt.execute("INSERT INTO fetch_data VALUES "
                    + "(1, 90, 'alice'), (2, 85, 'bob'), (3, 90, 'carol'), "
                    + "(4, 80, 'dave'), (5, 75, 'eve'), (6, 90, 'frank'), "
                    + "(7, 85, 'grace'), (8, 70, 'heidi'), (9, 95, 'ivan'), (10, 80, 'judy')");
            stmt.execute("CREATE TABLE fetch_same (id integer PRIMARY KEY, val integer)");
            stmt.execute("INSERT INTO fetch_same VALUES (1, 10), (2, 10), (3, 10)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @Test
    @DisplayName("WITH TIES syntax should be supported and return tied rows")
    void testWithTiesSyntaxSupported() throws Exception {
        // Memgres errors with syntax error on WITH TIES
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT name, score FROM fetch_data ORDER BY score DESC, id FETCH FIRST 2 ROWS WITH TIES")) {
            assertTrue(rs.next());
            assertEquals("ivan", rs.getString("name"));
            assertTrue(rs.next());
            assertEquals("alice", rs.getString("name"));
            assertEquals(2, countRemainingRows(rs) + 2);
        }
    }

    @Test
    @DisplayName("WITH TIES singular ROW should be supported")
    void testWithTiesSingularRow() throws Exception {
        // Memgres errors with syntax error on FETCH FIRST 1 ROW WITH TIES
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT name, score FROM fetch_data ORDER BY score DESC, id FETCH FIRST 1 ROW WITH TIES")) {
            assertTrue(rs.next());
            assertEquals("ivan", rs.getString("name"));
            assertFalse(rs.next(), "Expected exactly 1 row");
        }
    }

    @Test
    @DisplayName("WITH TIES in subquery should be supported")
    void testWithTiesInSubquery() throws Exception {
        // Memgres errors with syntax error on WITH TIES inside subquery
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT count(*)::integer AS cnt FROM "
                     + "(SELECT id FROM fetch_same ORDER BY val FETCH FIRST 1 ROW WITH TIES) sub")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("cnt"));
        }
    }

    @Test
    @DisplayName("WITH TIES combined with OFFSET should be supported")
    void testWithTiesWithOffset() throws Exception {
        // Memgres errors with syntax error on OFFSET ... FETCH ... WITH TIES
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT name, score FROM fetch_data ORDER BY score DESC, id "
                     + "OFFSET 1 ROW FETCH FIRST 1 ROW WITH TIES")) {
            assertTrue(rs.next());
            assertEquals("alice", rs.getString("name"));
            assertFalse(rs.next(), "Expected exactly 1 row after offset");
        }
    }

    @Test
    @DisplayName("FETCH in parenthesized subquery with UNION ALL should be supported")
    void testFetchInParenthesizedSubqueryWithUnion() throws Exception {
        // Memgres errors with syntax error on FETCH inside parenthesized UNION subquery
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT count(*)::integer AS cnt FROM ("
                     + "(SELECT id FROM fetch_data ORDER BY id FETCH FIRST 2 ROWS ONLY) "
                     + "UNION ALL "
                     + "(SELECT id FROM fetch_data ORDER BY id DESC FETCH FIRST 2 ROWS ONLY)"
                     + ") sub")) {
            assertTrue(rs.next());
            assertEquals(4, rs.getInt("cnt"));
        }
    }

    private static int countRemainingRows(ResultSet rs) throws SQLException {
        int count = 0;
        while (rs.next()) count++;
        return count;
    }
}
