package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for automatically updatable views. DML through simple views
 * should be routed to the underlying table, matching PG18 behavior.
 */
class UpdatableViewsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE uv_base (id SERIAL PRIMARY KEY, a INT, b TEXT)");
            s.execute("INSERT INTO uv_base (a, b) VALUES (1, 'x'), (2, 'y')");
            s.execute("CREATE VIEW uv_simple AS SELECT id, a, b FROM uv_base");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP VIEW IF EXISTS uv_simple CASCADE");
                s.execute("DROP VIEW IF EXISTS uv_filtered CASCADE");
                s.execute("DROP TABLE IF EXISTS uv_base CASCADE");
            } catch (SQLException ignored) {}
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    static String querySingle(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    // ========================================================================
    // INSERT through simple view
    // ========================================================================

    @Test
    void insert_through_simple_view() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO uv_simple (a, b) VALUES (3, 'z')");
        }
        // Verify in base table
        String count = querySingle("SELECT COUNT(*) FROM uv_base WHERE a = 3");
        assertEquals("1", count, "INSERT through view should add row to base table");
    }

    // ========================================================================
    // UPDATE through simple view
    // ========================================================================

    @Test
    void update_through_simple_view() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("UPDATE uv_simple SET b = 'updated' WHERE a = 1");
        }
        String val = querySingle("SELECT b FROM uv_base WHERE a = 1");
        assertEquals("updated", val, "UPDATE through view should modify base table");
    }

    // ========================================================================
    // DELETE through simple view
    // ========================================================================

    @Test
    void delete_through_simple_view() throws SQLException {
        // Insert a row we can safely delete
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO uv_base (a, b) VALUES (99, 'delete_me')");
            s.execute("DELETE FROM uv_simple WHERE a = 99");
        }
        String count = querySingle("SELECT COUNT(*) FROM uv_base WHERE a = 99");
        assertEquals("0", count, "DELETE through view should remove row from base table");
    }

    // ========================================================================
    // Non-updatable view should error
    // ========================================================================

    @Test
    void insert_into_aggregate_view_fails() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE VIEW uv_agg AS SELECT COUNT(*) AS cnt FROM uv_base");
            assertThrows(SQLException.class, () ->
                s.execute("INSERT INTO uv_agg (cnt) VALUES (5)"));
            s.execute("DROP VIEW uv_agg");
        }
    }
}
