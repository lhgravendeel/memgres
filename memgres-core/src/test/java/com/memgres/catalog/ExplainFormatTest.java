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

    // Diff #20: PG returns 1 row for EXPLAIN (COSTS OFF) ... ORDER BY
    @Test void explain_costs_off_single_row() throws SQLException {
        List<String> lines = column("EXPLAIN (COSTS OFF) SELECT * FROM expl_t ORDER BY id");
        // PG returns exactly 1 row with multi-line plan text:
        // "Sort\n  Sort Key: id\n  ->  Seq Scan on expl_t"
        assertEquals(1, lines.size(),
                "EXPLAIN (COSTS OFF) should return 1 row (PG collapses plan into single text), got " + lines.size());
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
        // PG returns 6 rows for this simple query
        assertTrue(lines.size() >= 6,
                "EXPLAIN ANALYZE should return >= 6 rows (plan + timing), got " + lines.size());
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
