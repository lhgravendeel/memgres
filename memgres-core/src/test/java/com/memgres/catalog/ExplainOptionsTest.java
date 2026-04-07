package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for EXPLAIN statement option parsing and output.
 *
 * Covers:
 * - EXPLAIN (FORMAT text): default format
 * - EXPLAIN (FORMAT json): JSON output
 * - EXPLAIN (ANALYZE false, COSTS false, VERBOSE true): boolean options
 * - EXPLAIN (COSTS OFF): plan without costs
 * - EXPLAIN with invalid option values (FORMAT yamlish, ANALYZE maybe, BUFFERS 123)
 * - COMMENT ON INDEX
 * - Basic EXPLAIN output structure
 */
class ExplainOptionsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE explain_t(id int PRIMARY KEY, val text, score int)");
        exec("INSERT INTO explain_t VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try { exec("DROP TABLE IF EXISTS explain_t"); } catch (SQLException ignored) {}
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static List<String> queryColumn(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            List<String> rows = new ArrayList<>();
            while (rs.next()) rows.add(rs.getString(1));
            return rows;
        }
    }

    // ========================================================================
    // Basic EXPLAIN
    // ========================================================================

    @Test
    void explain_basic_select() throws SQLException {
        List<String> plan = queryColumn("EXPLAIN SELECT * FROM explain_t");
        assertFalse(plan.isEmpty(), "EXPLAIN should produce output");
        // Plan should mention Seq Scan or similar
        String joined = String.join("\n", plan);
        assertTrue(joined.toLowerCase().contains("scan") || joined.toLowerCase().contains("seq"),
                "EXPLAIN plan should mention a scan type");
    }

    @Test
    void explain_select_with_where() throws SQLException {
        List<String> plan = queryColumn("EXPLAIN SELECT * FROM explain_t WHERE id = 1");
        assertFalse(plan.isEmpty(), "EXPLAIN with WHERE should produce output");
    }

    @Test
    void explain_select_with_order_by() throws SQLException {
        List<String> plan = queryColumn("EXPLAIN SELECT * FROM explain_t ORDER BY id");
        assertFalse(plan.isEmpty());
    }

    // ========================================================================
    // EXPLAIN (COSTS OFF): no cost info in output
    // ========================================================================

    @Test
    void explain_costs_off() throws SQLException {
        List<String> plan = queryColumn("EXPLAIN (COSTS OFF) SELECT * FROM explain_t ORDER BY id");
        assertFalse(plan.isEmpty(), "EXPLAIN (COSTS OFF) should produce output");
        String joined = String.join("\n", plan);
        // With COSTS OFF, lines should not contain cost= or rows= or width=
        assertFalse(joined.contains("cost="),
                "EXPLAIN (COSTS OFF) should not show cost= in output");
    }

    @Test
    void explain_costs_false() throws SQLException {
        List<String> plan = queryColumn("EXPLAIN (COSTS false) SELECT * FROM explain_t");
        assertFalse(plan.isEmpty(), "EXPLAIN (COSTS false) should produce output");
        String joined = String.join("\n", plan);
        assertFalse(joined.contains("cost="),
                "EXPLAIN (COSTS false) should not show cost= in output");
    }

    // ========================================================================
    // EXPLAIN (FORMAT text): explicit text format
    // ========================================================================

    @Test
    void explain_format_text() throws SQLException {
        List<String> plan = queryColumn("EXPLAIN (FORMAT text) SELECT * FROM explain_t");
        assertFalse(plan.isEmpty(), "EXPLAIN (FORMAT text) should produce output");
        // Text format: lines are plain text, not JSON/XML/YAML
        String first = plan.get(0);
        assertFalse(first.startsWith("[") || first.startsWith("{"),
                "FORMAT text should not produce JSON-like output");
    }

    // ========================================================================
    // EXPLAIN (FORMAT json): JSON output
    // ========================================================================

    @Test
    void explain_format_json() throws SQLException {
        List<String> plan = queryColumn("EXPLAIN (FORMAT json) SELECT * FROM explain_t");
        assertFalse(plan.isEmpty(), "EXPLAIN (FORMAT json) should produce output");
        String joined = String.join("", plan);
        // JSON format should start with [ and contain Plan
        assertTrue(joined.contains("[") || joined.contains("{"),
                "FORMAT json should produce JSON-like output");
    }

    // ========================================================================
    // EXPLAIN with combined boolean options
    // ========================================================================

    @Test
    void explain_analyze_false_costs_false_verbose_true() throws SQLException {
        // PG accepts boolean option values: true/false, on/off, yes/no, 0/1
        List<String> plan = queryColumn(
                "EXPLAIN (ANALYZE false, COSTS false, VERBOSE true) SELECT * FROM explain_t");
        assertFalse(plan.isEmpty(),
                "EXPLAIN with multiple boolean options should produce output");
    }

    @Test
    void explain_analyze_on() throws SQLException {
        List<String> plan = queryColumn("EXPLAIN (ANALYZE) SELECT * FROM explain_t");
        assertFalse(plan.isEmpty(), "EXPLAIN (ANALYZE) should produce output");
        String joined = String.join("\n", plan);
        // ANALYZE should show actual time and rows
        assertTrue(joined.toLowerCase().contains("actual") || joined.toLowerCase().contains("time") || joined.toLowerCase().contains("rows"),
                "EXPLAIN ANALYZE should show actual execution info");
    }

    @Test
    void explain_analyze_select_with_where() throws SQLException {
        List<String> plan = queryColumn("EXPLAIN ANALYZE SELECT * FROM explain_t WHERE id = 1");
        assertFalse(plan.isEmpty());
        String joined = String.join("\n", plan);
        assertTrue(joined.toLowerCase().contains("actual") || joined.toLowerCase().contains("execution") || joined.toLowerCase().contains("planning"),
                "EXPLAIN ANALYZE should include timing info");
    }

    // ========================================================================
    // EXPLAIN with invalid option values, which must error
    // ========================================================================

    @Test
    void explain_format_yamlish_fails() {
        // PG: "unrecognized value for EXPLAIN option FORMAT: yamlish"
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("EXPLAIN (FORMAT yamlish) SELECT * FROM explain_t"),
                "FORMAT with invalid value 'yamlish' should fail");
        // Could be 22023 (invalid_parameter_value) or similar
        assertNotNull(ex.getSQLState(), "Should have an error code");
    }

    @Test
    void explain_analyze_maybe_fails() {
        // PG: "unrecognized value for EXPLAIN option ANALYZE: maybe"
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("EXPLAIN (ANALYZE maybe) SELECT * FROM explain_t"),
                "ANALYZE with invalid boolean 'maybe' should fail");
        assertNotNull(ex.getSQLState());
    }

    @Test
    void explain_buffers_numeric_fails() {
        // PG: BUFFERS expects a boolean, not a number
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("EXPLAIN (BUFFERS 123) SELECT * FROM explain_t"),
                "BUFFERS with numeric value should fail");
        assertNotNull(ex.getSQLState());
    }

    @Test
    void explain_unknown_option_fails() {
        assertThrows(SQLException.class,
                () -> exec("EXPLAIN (FOOBAR true) SELECT * FROM explain_t"),
                "Unknown EXPLAIN option should fail");
    }

    // ========================================================================
    // EXPLAIN FORMAT yaml, valid in PG
    // ========================================================================

    @Test
    void explain_format_yaml() throws SQLException {
        List<String> plan = queryColumn("EXPLAIN (FORMAT yaml) SELECT * FROM explain_t");
        assertFalse(plan.isEmpty(), "EXPLAIN (FORMAT yaml) should produce output");
    }

    // ========================================================================
    // EXPLAIN FORMAT xml, valid in PG
    // ========================================================================

    @Test
    void explain_format_xml() throws SQLException {
        List<String> plan = queryColumn("EXPLAIN (FORMAT xml) SELECT * FROM explain_t");
        assertFalse(plan.isEmpty(), "EXPLAIN (FORMAT xml) should produce output");
        String joined = String.join("", plan);
        assertTrue(joined.contains("<") && joined.contains(">"),
                "FORMAT xml should produce XML-like output");
    }

    // ========================================================================
    // COMMENT ON INDEX
    // ========================================================================

    @Test
    void comment_on_index() throws SQLException {
        exec("CREATE INDEX idx_explain_val ON explain_t(val)");
        try {
            // PG: COMMENT ON INDEX is valid
            exec("COMMENT ON INDEX idx_explain_val IS 'index comment'");

            // Verify comment was stored
            String comment = null;
            try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(
                    "SELECT obj_description('idx_explain_val'::regclass)")) {
                if (rs.next()) comment = rs.getString(1);
            }
            assertEquals("index comment", comment, "Index comment should be stored");
        } finally {
            try { exec("DROP INDEX idx_explain_val"); } catch (SQLException ignored) {}
        }
    }

    @Test
    void comment_on_table() throws SQLException {
        exec("COMMENT ON TABLE explain_t IS 'test table'");
        String comment = null;
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(
                "SELECT obj_description('explain_t'::regclass)")) {
            if (rs.next()) comment = rs.getString(1);
        }
        assertEquals("test table", comment);
        // Clear comment
        exec("COMMENT ON TABLE explain_t IS NULL");
    }

    // ========================================================================
    // EXPLAIN with subquery, join, CTE
    // ========================================================================

    @Test
    void explain_with_join() throws SQLException {
        exec("CREATE TABLE explain_j(id int PRIMARY KEY, ref_id int)");
        exec("INSERT INTO explain_j VALUES (1, 1), (2, 2)");
        try {
            List<String> plan = queryColumn(
                    "EXPLAIN SELECT * FROM explain_t t JOIN explain_j j ON t.id = j.ref_id");
            assertFalse(plan.isEmpty());
        } finally {
            exec("DROP TABLE explain_j");
        }
    }

    @Test
    void explain_with_cte() throws SQLException {
        List<String> plan = queryColumn("""
            EXPLAIN WITH x AS (SELECT id, val FROM explain_t WHERE score > 10)
            SELECT * FROM x ORDER BY id
            """);
        assertFalse(plan.isEmpty());
    }

    @Test
    void explain_with_aggregate() throws SQLException {
        List<String> plan = queryColumn(
                "EXPLAIN SELECT count(*), sum(score) FROM explain_t");
        assertFalse(plan.isEmpty());
    }
}
