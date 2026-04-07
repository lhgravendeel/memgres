package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for PreparedStatement reuse patterns.
 *
 * The PG JDBC driver caches parsed statements on the server side.
 * After the first Parse/Bind/Execute cycle, subsequent executions
 * reuse the parsed statement (skipping Parse, just doing Bind/Execute).
 *
 * This is critical for:
 *   - Connection pools that reuse PreparedStatements
 *   - ORMs that execute the same query with different parameters
 *   - Batch operations that insert many rows with the same statement
 *
 * Also covers addBatch/executeBatch for bulk operations.
 */
class PreparedStatementReuseTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
        conn.setAutoCommit(true);
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
    // Parse once, Bind+Execute multiple times (SELECT)
    // =========================================================================

    @Test
    void testReuseSelectWithDifferentStringParams() throws SQLException {
        exec("CREATE TABLE reuse_sel (id serial PRIMARY KEY, category text, val int)");
        exec("INSERT INTO reuse_sel (category, val) VALUES ('a', 1), ('b', 2), ('a', 3), ('c', 4)");

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT SUM(val) FROM reuse_sel WHERE category = ?")) {
            ps.setString(1, "a");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(4, rs.getInt(1));
            }

            ps.setString(1, "b");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }

            ps.setString(1, "c");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(4, rs.getInt(1));
            }

            ps.setString(1, "nonexistent");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
            }
        }
    }

    @Test
    void testReuseSelectWithDifferentIntParams() throws SQLException {
        exec("CREATE TABLE reuse_int (id serial PRIMARY KEY, name text)");
        exec("INSERT INTO reuse_int (name) VALUES ('alpha'), ('beta'), ('gamma')");

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT name FROM reuse_int WHERE id = ?")) {
            for (int i = 1; i <= 3; i++) {
                ps.setInt(1, i);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "Should find row for id=" + i);
                    assertNotNull(rs.getString(1));
                }
            }
        }
    }

    @Test
    void testReuseManyTimes() throws SQLException {
        exec("CREATE TABLE reuse_many (id serial PRIMARY KEY, val int)");
        exec("INSERT INTO reuse_many (val) SELECT generate_series(1, 100)");

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT val FROM reuse_many WHERE id = ?")) {
            for (int i = 1; i <= 50; i++) {
                ps.setInt(1, i);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(i, rs.getInt(1));
                }
            }
        }
    }

    // =========================================================================
    // Parse once, Bind+Execute multiple times (DML)
    // =========================================================================

    @Test
    void testReuseInsert() throws SQLException {
        exec("CREATE TABLE reuse_ins (id serial PRIMARY KEY, name text, value int)");

        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO reuse_ins (name, value) VALUES (?, ?)")) {
            for (int i = 1; i <= 10; i++) {
                ps.setString(1, "item_" + i);
                ps.setInt(2, i * 10);
                assertEquals(1, ps.executeUpdate());
            }
        }
        assertEquals("10", query1("SELECT COUNT(*) FROM reuse_ins"));
        assertEquals("550", query1("SELECT SUM(value) FROM reuse_ins"));
    }

    @Test
    void testReuseUpdate() throws SQLException {
        exec("CREATE TABLE reuse_upd (id serial PRIMARY KEY, status text DEFAULT 'pending')");
        exec("INSERT INTO reuse_upd DEFAULT VALUES");
        exec("INSERT INTO reuse_upd DEFAULT VALUES");
        exec("INSERT INTO reuse_upd DEFAULT VALUES");

        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE reuse_upd SET status = ? WHERE id = ?")) {
            ps.setString(1, "done"); ps.setInt(2, 1); ps.executeUpdate();
            ps.setString(1, "failed"); ps.setInt(2, 2); ps.executeUpdate();
            ps.setString(1, "done"); ps.setInt(2, 3); ps.executeUpdate();
        }
        assertEquals("2", query1("SELECT COUNT(*) FROM reuse_upd WHERE status = 'done'"));
    }

    // =========================================================================
    // addBatch / executeBatch
    // =========================================================================

    @Test
    void testBatchInsert() throws SQLException {
        exec("CREATE TABLE batch_ins (id serial PRIMARY KEY, name text, score int)");

        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO batch_ins (name, score) VALUES (?, ?)")) {
            for (int i = 1; i <= 20; i++) {
                ps.setString(1, "player_" + i);
                ps.setInt(2, i * 5);
                ps.addBatch();
            }
            int[] results = ps.executeBatch();
            assertEquals(20, results.length);
            for (int r : results) {
                assertEquals(1, r); // each INSERT affects 1 row
            }
        }
        assertEquals("20", query1("SELECT COUNT(*) FROM batch_ins"));
    }

    @Test
    void testBatchUpdate() throws SQLException {
        exec("CREATE TABLE batch_upd (id serial PRIMARY KEY, processed boolean DEFAULT false)");
        for (int i = 0; i < 10; i++) exec("INSERT INTO batch_upd DEFAULT VALUES");

        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE batch_upd SET processed = true WHERE id = ?")) {
            for (int i = 1; i <= 5; i++) {
                ps.setInt(1, i);
                ps.addBatch();
            }
            int[] results = ps.executeBatch();
            assertEquals(5, results.length);
        }
        assertEquals("5", query1("SELECT COUNT(*) FROM batch_upd WHERE processed = true"));
    }

    @Test
    void testBatchMixedParameterTypes() throws SQLException {
        exec("CREATE TABLE batch_mixed (id serial PRIMARY KEY, name text, active boolean, score numeric)");

        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO batch_mixed (name, active, score) VALUES (?, ?, ?)")) {
            ps.setString(1, "first"); ps.setBoolean(2, true); ps.setBigDecimal(3, new java.math.BigDecimal("9.5")); ps.addBatch();
            ps.setString(1, "second"); ps.setBoolean(2, false); ps.setBigDecimal(3, new java.math.BigDecimal("7.3")); ps.addBatch();
            ps.setString(1, "third"); ps.setBoolean(2, true); ps.setNull(3, Types.NUMERIC); ps.addBatch();
            int[] results = ps.executeBatch();
            assertEquals(3, results.length);
        }
        assertEquals("3", query1("SELECT COUNT(*) FROM batch_mixed"));
    }

    // =========================================================================
    // Interleaved PreparedStatements on same connection
    // =========================================================================

    @Test
    void testInterleavedPreparedStatements() throws SQLException {
        exec("CREATE TABLE ilv_a (id serial PRIMARY KEY, val text)");
        exec("CREATE TABLE ilv_b (id serial PRIMARY KEY, ref_id int, info text)");

        PreparedStatement psA = conn.prepareStatement("INSERT INTO ilv_a (val) VALUES (?) RETURNING id");
        PreparedStatement psB = conn.prepareStatement("INSERT INTO ilv_b (ref_id, info) VALUES (?, ?)");

        // Interleave: insert into A, get ID, insert into B with that ID
        psA.setString(1, "parent1");
        ResultSet rs = psA.executeQuery();
        assertTrue(rs.next());
        int id1 = rs.getInt(1);
        rs.close();

        psB.setInt(1, id1);
        psB.setString(2, "child_of_1");
        psB.executeUpdate();

        psA.setString(1, "parent2");
        rs = psA.executeQuery();
        assertTrue(rs.next());
        int id2 = rs.getInt(1);
        rs.close();

        psB.setInt(1, id2);
        psB.setString(2, "child_of_2");
        psB.executeUpdate();

        psA.close();
        psB.close();

        assertEquals("2", query1("SELECT COUNT(*) FROM ilv_a"));
        assertEquals("2", query1("SELECT COUNT(*) FROM ilv_b"));
    }
}
