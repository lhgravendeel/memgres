package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 36c: pgcrypto unknown-type rejection + SEARCH ord record type.
 */
class Round36cFixesTest {
    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ========================================================================
    // pgcrypto: untyped args succeed (PG resolves unknown to text overloads)
    // ========================================================================

    @Test void digest_untyped_args_succeed() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto");
            ResultSet rs = s.executeQuery("SELECT encode(digest('hello', 'sha256'), 'hex') AS v");
            assertTrue(rs.next());
            assertEquals("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", rs.getString(1));
        }
    }

    @Test void hmac_untyped_args_succeed() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto");
            ResultSet rs = s.executeQuery("SELECT length(encode(hmac('msg', 'key', 'sha256'), 'hex')) AS n");
            assertTrue(rs.next());
            assertEquals(64, rs.getInt(1));
        }
    }

    @Test void gen_salt_untyped_arg_succeed() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto");
            ResultSet rs = s.executeQuery("SELECT (gen_salt('bf') LIKE '$2%') AS ok");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    // ========================================================================
    // pgcrypto: typed args succeed (explicit cast resolves signature)
    // ========================================================================

    @Test void digest_with_text_cast_succeeds() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto");
            ResultSet rs = s.executeQuery("SELECT encode(digest('hello'::text, 'sha256'), 'hex') AS v");
            assertTrue(rs.next());
            assertEquals("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", rs.getString(1));
        }
    }

    @Test void digest_with_bytea_cast_succeeds() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto");
            ResultSet rs = s.executeQuery("SELECT encode(digest('hello'::bytea, 'sha256'), 'hex') AS v");
            assertTrue(rs.next());
            assertEquals("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", rs.getString(1));
        }
    }

    @Test void hmac_with_text_cast_succeeds() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto");
            ResultSet rs = s.executeQuery("SELECT length(encode(hmac('msg'::text, 'key', 'sha256'), 'hex')) AS n");
            assertTrue(rs.next());
            assertEquals(64, rs.getInt(1));
        }
    }

    @Test void gen_salt_with_text_cast_succeeds() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto");
            ResultSet rs = s.executeQuery("SELECT (gen_salt('bf'::text) LIKE '$2%') AS ok");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test void digest_with_column_ref_succeeds() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto");
            s.execute("CREATE TABLE r36c_d (data text)");
            s.execute("INSERT INTO r36c_d VALUES ('hello')");
            ResultSet rs = s.executeQuery("SELECT encode(digest(data, 'sha256'), 'hex') AS v FROM r36c_d");
            assertTrue(rs.next());
            assertEquals("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", rs.getString(1));
            s.execute("DROP TABLE r36c_d");
        }
    }

    // ========================================================================
    // SEARCH BREADTH FIRST: ord column is record type, not castable to int
    // ========================================================================

    @Test void search_breadth_first_ord_cast_int_rejected() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE r36c_edges (src int, dst int)");
            s.execute("INSERT INTO r36c_edges VALUES (1,2),(2,3)");
            SQLException ex = assertThrows(SQLException.class,
                    () -> s.executeQuery(
                            "WITH RECURSIVE t(n, p) AS (" +
                                    "  SELECT src, ARRAY[src] FROM r36c_edges WHERE src=1 " +
                                    "  UNION ALL " +
                                    "  SELECT e.dst, t.p || e.dst FROM t JOIN r36c_edges e ON t.n = e.src" +
                                    ") SEARCH BREADTH FIRST BY n SET ord " +
                                    "SELECT n, ord::int FROM t ORDER BY ord"));
            assertTrue(ex.getMessage().contains("cannot cast type record to"),
                    "Expected record-to-int cast rejection, got: " + ex.getMessage());
            assertEquals("42846", ex.getSQLState());
            s.execute("DROP TABLE r36c_edges");
        }
    }

    @Test void search_breadth_first_ord_column_exists_and_orders() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE r36c_edges2 (src int, dst int)");
            s.execute("INSERT INTO r36c_edges2 VALUES (1,2),(2,3)");
            ResultSet rs = s.executeQuery(
                    "WITH RECURSIVE t(n, p) AS (" +
                            "  SELECT src, ARRAY[src] FROM r36c_edges2 WHERE src=1 " +
                            "  UNION ALL " +
                            "  SELECT e.dst, t.p || e.dst FROM t JOIN r36c_edges2 e ON t.n = e.src" +
                            ") SEARCH BREADTH FIRST BY n SET ord " +
                            "SELECT n FROM t ORDER BY ord");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertFalse(rs.next());
            s.execute("DROP TABLE r36c_edges2");
        }
    }

    @Test void search_depth_first_ord_is_record() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE r36c_dfs (src int, dst int)");
            s.execute("INSERT INTO r36c_dfs VALUES (1,2),(1,3),(2,4)");
            ResultSet rs = s.executeQuery(
                    "WITH RECURSIVE t(n) AS (" +
                            "  SELECT src FROM r36c_dfs WHERE src=1 " +
                            "  UNION ALL " +
                            "  SELECT e.dst FROM t JOIN r36c_dfs e ON t.n = e.src" +
                            ") SEARCH DEPTH FIRST BY n SET ord " +
                            "SELECT n FROM t ORDER BY ord");
            assertTrue(rs.next());
            // Just verify it returns rows in some order without error
            assertNotNull(rs.getObject(1));
            s.execute("DROP TABLE r36c_dfs");
        }
    }

    @Test void search_depth_first_ord_cast_int_rejected() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE r36c_dfs2 (src int, dst int)");
            s.execute("INSERT INTO r36c_dfs2 VALUES (1,2),(2,3)");
            SQLException ex = assertThrows(SQLException.class,
                    () -> s.executeQuery(
                            "WITH RECURSIVE t(n) AS (" +
                                    "  SELECT src FROM r36c_dfs2 WHERE src=1 " +
                                    "  UNION ALL " +
                                    "  SELECT e.dst FROM t JOIN r36c_dfs2 e ON t.n = e.src" +
                                    ") SEARCH DEPTH FIRST BY n SET ord " +
                                    "SELECT n, ord::int FROM t ORDER BY ord"));
            assertTrue(ex.getMessage().contains("cannot cast type record to"),
                    "Expected record-to-int cast rejection, got: " + ex.getMessage());
            s.execute("DROP TABLE r36c_dfs2");
        }
    }
}
