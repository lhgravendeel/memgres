package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for row value expressions (also called row constructors).
 *
 * PostgreSQL supports:
 *   WHERE (col1, col2) IN (SELECT a, b FROM ...)
 *   WHERE ROW(col1, col2) = ROW(val1, val2)
 *   WHERE (col1, col2) = (val1, val2)
 *   WHERE (col1, col2) > (val1, val2)
 *
 * Also covers string_to_array comparison which is related to row/array comparisons.
 */
class RowValueExpressionTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE memberships (user_id text, group_id text, role text)");
            s.execute("INSERT INTO memberships VALUES ('u1', 'g1', 'admin'), ('u2', 'g1', 'member'), ('u3', 'g2', 'admin')");
            s.execute("CREATE TABLE pairs (a int, b int, label text)");
            s.execute("INSERT INTO pairs VALUES (1, 10, 'first'), (2, 20, 'second'), (3, 30, 'third')");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next()); return rs.getString(1);
        }
    }

    // =========================================================================
    // (col1, col2) IN (subquery)
    // =========================================================================

    @Test
    void testRowValueInSubquery() throws SQLException {
        // The exact pattern from migrations: DELETE WHERE (a, b) IN (SELECT ...)
        String count = query1("""
            SELECT COUNT(*) FROM memberships
            WHERE (user_id, group_id) IN (
                SELECT user_id, group_id FROM memberships WHERE role = 'admin'
            )
        """);
        assertEquals("2", count);
    }

    @Test
    void testDeleteWithRowValueInSubquery() throws SQLException {
        exec("CREATE TABLE del_rv (a text, b text, val text)");
        exec("INSERT INTO del_rv VALUES ('x', '1', 'keep'), ('y', '2', 'remove'), ('z', '3', 'keep')");
        exec("CREATE TABLE del_rv_filter (a text, b text)");
        exec("INSERT INTO del_rv_filter VALUES ('y', '2')");
        exec("""
            DELETE FROM del_rv WHERE (a, b) IN (
                SELECT a, b FROM del_rv_filter
            )
        """);
        assertEquals("2", query1("SELECT COUNT(*) FROM del_rv"));
    }

    @Test
    void testRowValueInSubqueryWithJoin() throws SQLException {
        // Complex real-world pattern: DELETE with JOIN in subquery
        exec("CREATE TABLE rv_main (post_id text, user_id text, active boolean DEFAULT true)");
        exec("CREATE TABLE rv_related (post_id text, deleted boolean DEFAULT false)");
        exec("INSERT INTO rv_main VALUES ('p1', 'u1', true), ('p2', 'u2', true)");
        exec("INSERT INTO rv_related VALUES ('p1', false), ('p2', true)");
        exec("""
            DELETE FROM rv_main WHERE (post_id, user_id) IN (
                SELECT m.post_id, m.user_id
                FROM rv_main m
                JOIN rv_related r ON r.post_id = m.post_id
                WHERE r.deleted = true
            )
        """);
        assertEquals("1", query1("SELECT COUNT(*) FROM rv_main"));
    }

    // =========================================================================
    // (col1, col2) IN (VALUES list)
    // =========================================================================

    @Test
    void testRowValueInValuesList() throws SQLException {
        String count = query1("""
            SELECT COUNT(*) FROM pairs
            WHERE (a, b) IN ((1, 10), (3, 30))
        """);
        assertEquals("2", count);
    }

    // =========================================================================
    // (col1, col2) = (val1, val2)
    // =========================================================================

    @Test
    void testRowValueEquality() throws SQLException {
        assertEquals("first", query1("""
            SELECT label FROM pairs WHERE (a, b) = (1, 10)
        """));
    }

    @Test
    void testRowValueInequality() throws SQLException {
        String count = query1("""
            SELECT COUNT(*) FROM pairs WHERE (a, b) != (1, 10)
        """);
        assertEquals("2", count);
    }

    // =========================================================================
    // (col1, col2) > (val1, val2): row comparison
    // =========================================================================

    @Test
    void testRowValueGreaterThan() throws SQLException {
        // Row comparison is lexicographic: (2,20) > (1,10)
        String count = query1("""
            SELECT COUNT(*) FROM pairs WHERE (a, b) > (1, 10)
        """);
        assertEquals("2", count);
    }

    @Test
    void testRowValueLessThan() throws SQLException {
        assertEquals("first", query1("""
            SELECT label FROM pairs WHERE (a, b) < (2, 20)
        """));
    }

    // =========================================================================
    // ROW() constructor explicit form
    // =========================================================================

    @Test
    void testExplicitRowConstructor() throws SQLException {
        assertEquals("first", query1("""
            SELECT label FROM pairs WHERE ROW(a, b) = ROW(1, 10)
        """));
    }

    @Test
    void testRowConstructorInSelect() throws SQLException {
        assertNotNull(query1("""
            SELECT ROW(a, b, label) FROM pairs ORDER BY a LIMIT 1
        """));
    }

    // =========================================================================
    // NOT IN with row values
    // =========================================================================

    @Test
    void testRowValueNotInSubquery() throws SQLException {
        String count = query1("""
            SELECT COUNT(*) FROM memberships
            WHERE (user_id, group_id) NOT IN (
                SELECT user_id, group_id FROM memberships WHERE role = 'admin'
            )
        """);
        assertEquals("1", count);
    }

    // =========================================================================
    // UPDATE with row value comparison
    // =========================================================================

    @Test
    void testUpdateWithRowValueWhere() throws SQLException {
        exec("CREATE TABLE rv_upd (a int, b int, val text)");
        exec("INSERT INTO rv_upd VALUES (1, 10, 'old'), (2, 20, 'old')");
        exec("UPDATE rv_upd SET val = 'new' WHERE (a, b) = (1, 10)");
        assertEquals("new", query1("SELECT val FROM rv_upd WHERE a = 1"));
    }

    // =========================================================================
    // string_to_array comparisons (related: row size mismatch errors)
    // =========================================================================

    @Test
    void testStringToArrayComparison() throws SQLException {
        exec("CREATE TABLE versions (id serial PRIMARY KEY, version text)");
        exec("INSERT INTO versions (version) VALUES ('5.12.0')");
        assertNotNull(query1("""
            SELECT version FROM versions
            WHERE string_to_array(version, '.')::int[] > ARRAY[5, 0, 0]
        """));
    }

    @Test
    void testStringToArrayElementComparison() throws SQLException {
        exec("CREATE TABLE ver_check (id serial PRIMARY KEY, ver text)");
        exec("INSERT INTO ver_check (ver) VALUES ('3.14.2')");
        assertNotNull(query1("""
            SELECT ver FROM ver_check
            WHERE (string_to_array(ver, '.'))[1]::int >= 3
        """));
    }

    // =========================================================================
    // Composite type comparison (row of different sizes)
    // =========================================================================

    @Test
    void testRowValueWithDifferentColumnCount() throws SQLException {
        // This should fail with a clear error, not an internal error
        assertThrows(SQLException.class, () ->
            query1("SELECT 1 WHERE (1, 2) = (1, 2, 3)")
        );
    }
}
