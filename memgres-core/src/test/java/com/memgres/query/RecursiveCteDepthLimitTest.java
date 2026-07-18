package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that recursive CTEs do not silently truncate results. PG has no
 * hard iteration/row limit — a well-formed recursive CTE runs until the
 * working table is empty. Memgres silently caps at 1000 iterations / 10000 rows.
 */
class RecursiveCteDepthLimitTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
    }

    @Test void recursive_cte_exceeds_1000_iterations() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            // Generate 2000 rows via recursive CTE
            try (ResultSet rs = s.executeQuery(
                    "WITH RECURSIVE r(n) AS ("
                    + "  SELECT 1"
                    + "  UNION ALL"
                    + "  SELECT n + 1 FROM r WHERE n < 2000"
                    + ") SELECT count(*) AS cnt FROM r")) {
                assertTrue(rs.next());
                assertEquals(2000, rs.getInt("cnt"),
                        "Recursive CTE should produce 2000 rows, not be truncated at 1000");
            }
        }
    }

    @Test void recursive_cte_exceeds_10000_rows() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            // Generate 15000 rows
            try (ResultSet rs = s.executeQuery(
                    "WITH RECURSIVE r(n) AS ("
                    + "  SELECT 1"
                    + "  UNION ALL"
                    + "  SELECT n + 1 FROM r WHERE n < 15000"
                    + ") SELECT count(*) AS cnt FROM r")) {
                assertTrue(rs.next());
                assertEquals(15000, rs.getInt("cnt"),
                        "Recursive CTE should produce 15000 rows, not be capped at 10000");
            }
        }
    }

    @Test void recursive_cte_max_value_correct() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            // Verify the max value is correct (not truncated)
            try (ResultSet rs = s.executeQuery(
                    "WITH RECURSIVE r(n) AS ("
                    + "  SELECT 1"
                    + "  UNION ALL"
                    + "  SELECT n + 1 FROM r WHERE n < 5000"
                    + ") SELECT max(n) AS mx FROM r")) {
                assertTrue(rs.next());
                assertEquals(5000, rs.getInt("mx"),
                        "Max value should be 5000, not truncated at 1000");
            }
        }
    }

    @Test void small_recursive_cte_still_works() throws Exception {
        // Sanity check: small CTEs under the limit work fine
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            try (ResultSet rs = s.executeQuery(
                    "WITH RECURSIVE r(n) AS ("
                    + "  SELECT 1"
                    + "  UNION ALL"
                    + "  SELECT n + 1 FROM r WHERE n < 10"
                    + ") SELECT count(*) AS cnt, max(n) AS mx FROM r")) {
                assertTrue(rs.next());
                assertEquals(10, rs.getInt("cnt"));
                assertEquals(10, rs.getInt("mx"));
            }
        }
    }
}
