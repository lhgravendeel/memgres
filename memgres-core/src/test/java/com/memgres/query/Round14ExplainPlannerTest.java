package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 14 gaps: EXPLAIN and planner controls.
 *
 * - EXPLAIN BUFFERS / WAL / MEMORY / SERIALIZE / GENERIC_PLAN
 * - plan_cache_mode GUC
 * - enable_* planner toggles
 * - pg_stat_plans / parallel-query nodes
 */
class Round14ExplainPlannerTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE r14_ex (id int PRIMARY KEY, v int)");
        exec("INSERT INTO r14_ex SELECT g, g*2 FROM generate_series(1,100) g");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private static String firstPlanLine(String sql) throws SQLException {
        StringBuilder sb = new StringBuilder();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                if (sb.length() > 0) sb.append("\n");
                sb.append(rs.getString(1));
            }
        }
        return sb.toString();
    }

    // =========================================================================
    // A. EXPLAIN option matrix
    // =========================================================================

    @Test
    void explain_buffers_option() throws SQLException {
        String plan = firstPlanLine("EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM r14_ex WHERE id = 1");
        assertNotNull(plan);
        assertTrue(plan.toLowerCase().contains("buffers") || plan.toLowerCase().contains("shared"),
                "BUFFERS option should add buffer counters; got: " + plan);
    }

    @Test
    void explain_wal_option() throws SQLException {
        String plan = firstPlanLine(
                "EXPLAIN (ANALYZE, WAL) INSERT INTO r14_ex VALUES (1000, 2000)");
        assertNotNull(plan);
        // WAL section typically says "WAL:" with records= / bytes=
        assertTrue(plan.toLowerCase().contains("wal"),
                "WAL option should report WAL counters; got: " + plan);
    }

    @Test
    void explain_memory_option() throws SQLException {
        // PG 17+ MEMORY option
        String plan = firstPlanLine("EXPLAIN (MEMORY) SELECT * FROM r14_ex");
        assertNotNull(plan);
        assertTrue(plan.toLowerCase().contains("memory") || plan.toLowerCase().contains("used"),
                "MEMORY option should report planner memory; got: " + plan);
    }

    @Test
    void explain_serialize_option() throws SQLException {
        // PG 17+ SERIALIZE option
        String plan = firstPlanLine("EXPLAIN (ANALYZE, SERIALIZE TEXT) SELECT * FROM r14_ex");
        assertNotNull(plan);
        assertTrue(plan.toLowerCase().contains("serial") || plan.toLowerCase().contains("bytes"),
                "SERIALIZE option should report serialization; got: " + plan);
    }

    @Test
    void explain_generic_plan_option() throws SQLException {
        // PG 16+ GENERIC_PLAN option: plan with parameter placeholders
        String plan = firstPlanLine("EXPLAIN (GENERIC_PLAN) SELECT * FROM r14_ex WHERE id = $1");
        assertNotNull(plan);
        // Should produce a plan for a parameterized query
        assertTrue(plan.length() > 0, "GENERIC_PLAN should produce output");
    }

    @Test
    void explain_format_json() throws SQLException {
        String plan = firstPlanLine("EXPLAIN (FORMAT JSON) SELECT * FROM r14_ex");
        assertNotNull(plan);
        // JSON output begins with '[' in PG
        String trimmed = plan.trim();
        assertTrue(trimmed.startsWith("[") || trimmed.startsWith("{"),
                "FORMAT JSON must emit JSON; got: " + plan);
    }

    @Test
    void explain_format_yaml() throws SQLException {
        String plan = firstPlanLine("EXPLAIN (FORMAT YAML) SELECT * FROM r14_ex");
        assertNotNull(plan);
        assertTrue(plan.contains("Plan:") || plan.contains("-"),
                "FORMAT YAML should produce YAML output; got: " + plan);
    }

    @Test
    void explain_format_xml() throws SQLException {
        String plan = firstPlanLine("EXPLAIN (FORMAT XML) SELECT * FROM r14_ex");
        assertNotNull(plan);
        assertTrue(plan.contains("<explain") || plan.contains("<?xml"),
                "FORMAT XML should produce XML; got: " + plan);
    }

    @Test
    void explain_costs_off() throws SQLException {
        String plan = firstPlanLine("EXPLAIN (COSTS OFF) SELECT * FROM r14_ex");
        assertNotNull(plan);
        // COSTS OFF → no cost= section
        assertFalse(plan.toLowerCase().contains("cost="),
                "COSTS OFF should suppress costs; got: " + plan);
    }

    @Test
    void explain_verbose_option() throws SQLException {
        String plan = firstPlanLine("EXPLAIN (VERBOSE) SELECT * FROM r14_ex");
        assertNotNull(plan);
        assertTrue(plan.toLowerCase().contains("output"),
                "VERBOSE should list Output columns; got: " + plan);
    }

    @Test
    void explain_settings_option() throws SQLException {
        // PG 12+ SETTINGS option
        String plan = firstPlanLine("EXPLAIN (SETTINGS) SELECT * FROM r14_ex");
        assertNotNull(plan);
        // Just verifying it parses and executes
        assertTrue(plan.length() > 0);
    }

    // =========================================================================
    // B. plan_cache_mode GUC
    // =========================================================================

    @Test
    void plan_cache_mode_force_generic() throws SQLException {
        exec("SET plan_cache_mode = 'force_generic_plan'");
        try (Statement s = conn.createStatement(); ResultSet rs =
                s.executeQuery("SHOW plan_cache_mode")) {
            assertTrue(rs.next());
            assertEquals("force_generic_plan", rs.getString(1));
        }
        exec("RESET plan_cache_mode");
    }

    @Test
    void plan_cache_mode_force_custom() throws SQLException {
        exec("SET plan_cache_mode = 'force_custom_plan'");
        try (Statement s = conn.createStatement(); ResultSet rs =
                s.executeQuery("SHOW plan_cache_mode")) {
            assertTrue(rs.next());
            assertEquals("force_custom_plan", rs.getString(1));
        }
        exec("RESET plan_cache_mode");
    }

    // =========================================================================
    // C. enable_* planner toggles
    // =========================================================================

    @Test
    void enable_seqscan_toggle() throws SQLException {
        exec("SET enable_seqscan = off");
        try (Statement s = conn.createStatement(); ResultSet rs =
                s.executeQuery("SHOW enable_seqscan")) {
            assertTrue(rs.next());
            assertEquals("off", rs.getString(1));
        }
        exec("RESET enable_seqscan");
    }

    @Test
    void enable_hashjoin_toggle() throws SQLException {
        exec("SET enable_hashjoin = off");
        try (Statement s = conn.createStatement(); ResultSet rs =
                s.executeQuery("SHOW enable_hashjoin")) {
            assertTrue(rs.next());
            assertEquals("off", rs.getString(1));
        }
        exec("RESET enable_hashjoin");
    }

    @Test
    void enable_partitionwise_join() throws SQLException {
        exec("SET enable_partitionwise_join = on");
        try (Statement s = conn.createStatement(); ResultSet rs =
                s.executeQuery("SHOW enable_partitionwise_join")) {
            assertTrue(rs.next());
            assertEquals("on", rs.getString(1));
        }
        exec("RESET enable_partitionwise_join");
    }

    @Test
    void enable_incremental_sort() throws SQLException {
        // PG 13+
        try (Statement s = conn.createStatement(); ResultSet rs =
                s.executeQuery("SHOW enable_incremental_sort")) {
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertTrue("on".equals(v) || "off".equals(v),
                    "enable_incremental_sort must be on/off; got " + v);
        }
    }

    @Test
    void enable_presorted_aggregate() throws SQLException {
        // PG 16+
        try (Statement s = conn.createStatement(); ResultSet rs =
                s.executeQuery("SHOW enable_presorted_aggregate")) {
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertTrue("on".equals(v) || "off".equals(v));
        }
    }

    // =========================================================================
    // D. Parallel-query planner GUCs
    // =========================================================================

    @Test
    void max_parallel_workers_per_gather() throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs =
                s.executeQuery("SHOW max_parallel_workers_per_gather")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    @Test
    void min_parallel_table_scan_size() throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs =
                s.executeQuery("SHOW min_parallel_table_scan_size")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    @Test
    void parallel_leader_participation() throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs =
                s.executeQuery("SHOW parallel_leader_participation")) {
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertTrue("on".equals(v) || "off".equals(v));
        }
    }

    // =========================================================================
    // E. Planner hints / cost constants
    // =========================================================================

    @Test
    void random_page_cost_setting() throws SQLException {
        exec("SET random_page_cost = 1.1");
        try (Statement s = conn.createStatement(); ResultSet rs =
                s.executeQuery("SHOW random_page_cost")) {
            assertTrue(rs.next());
            assertTrue(rs.getString(1).startsWith("1"));
        }
        exec("RESET random_page_cost");
    }

    @Test
    void effective_cache_size_setting() throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs =
                s.executeQuery("SHOW effective_cache_size")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }
}
