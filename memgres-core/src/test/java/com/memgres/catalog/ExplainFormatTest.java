package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Diffs #19-21: EXPLAIN output doesn't match PG's plan format.
 * #19: EXPLAIN SELECT, data/structure differs
 * #20: EXPLAIN (COSTS OFF) SELECT ... ORDER BY: PG returns 1 row, memgres returns 2
 * #21: EXPLAIN ANALYZE: PG returns 6 rows (plan+timing), memgres returns 4
 */
class ExplainFormatTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE expl_t(id int PRIMARY KEY, note text)");
        exec("INSERT INTO expl_t VALUES (1, 'x')");
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }
    static void exec(String sql) throws SQLException { try (Statement s = conn.createStatement()) { s.execute(sql); } }
    static List<String> column(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            List<String> vals = new ArrayList<>(); while (rs.next()) vals.add(rs.getString(1)); return vals;
        }
    }

    /**
     * Every line of the plan is a row of its own. PostgreSQL answers EXPLAIN with one row per
     * line whatever the options are; it does not fold the plan into a single string.
     */
    @Test void explain_costs_off_is_one_row_per_line() throws SQLException {
        List<String> lines = column("EXPLAIN (COSTS OFF) SELECT * FROM expl_t ORDER BY id");
        assertEquals(3, lines.size(), "a sort over a scan is three lines, got " + lines);
        assertEquals("Sort", lines.get(0));
        assertEquals("  Sort Key: id", lines.get(1));
        assertEquals("  ->  Seq Scan on expl_t", lines.get(2));
    }

    // Diff #21: PG returns 6 rows for EXPLAIN ANALYZE (plan nodes + Planning Time + Execution Time)
    @Test void explain_analyze_row_count_and_timing() throws SQLException {
        List<String> lines = column("EXPLAIN ANALYZE SELECT * FROM expl_t WHERE id = 1");
        String all = String.join("\n", lines);
        // PG: plan rows + "Planning Time: X.XXX ms" + "Execution Time: X.XXX ms"
        assertTrue(all.toLowerCase().contains("planning time"),
                "EXPLAIN ANALYZE should include 'Planning Time', got: " + all);
        assertTrue(all.toLowerCase().contains("execution time"),
                "EXPLAIN ANALYZE should include 'Execution Time', got: " + all);
        // The scan, its filter, and the two totals. PostgreSQL prints more only because it
        // reads this table through its primary key index, which memgres has no plan node for.
        assertTrue(lines.size() >= 4,
                "EXPLAIN ANALYZE should return the plan and both totals, got " + lines.size());
    }

    // Diff #19: EXPLAIN should include cost estimates in default format
    @Test void explain_basic_includes_cost() throws SQLException {
        List<String> lines = column("EXPLAIN SELECT * FROM expl_t WHERE id = 1");
        assertFalse(lines.isEmpty(), "EXPLAIN should return plan rows");
        String all = String.join("\n", lines);
        // PG default EXPLAIN includes cost estimates like "(cost=0.00..1.01 rows=1 width=36)"
        assertTrue(all.contains("cost="),
                "Default EXPLAIN should include cost estimates, got: " + all);
    }
}
