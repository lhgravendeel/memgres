package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that DISTINCT, UNION/INTERSECT/EXCEPT, and GROUP BY use value-based
 * equality rather than string representation. Using toString()/deepToString()
 * causes false collisions (e.g. 1.0 vs 1.00, or comma-containing strings
 * colliding with multi-column boundaries).
 */
class RowEqualityByStringTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
    }

    // ===== INTERSECT =====

    @Test void intersect_distinguishes_numeric_scale() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            // PG: 1.0 and 1.00 are equal numerics → INTERSECT returns 1 row
            try (ResultSet rs = s.executeQuery("SELECT 1.0 AS v INTERSECT SELECT 1.00 AS v")) {
                assertTrue(rs.next(), "1.0 INTERSECT 1.00 should produce 1 row (equal values)");
                assertFalse(rs.next());
            }
        }
    }

    @Test void union_does_not_collapse_comma_containing_strings() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            // ('a, b', 'c') and ('a', 'b, c') have different column values but
            // deepToString produces the same string: "[a, b, c]"
            try (ResultSet rs = s.executeQuery(
                    "SELECT 'a, b' AS c1, 'c' AS c2 UNION SELECT 'a' AS c1, 'b, c' AS c2")) {
                assertTrue(rs.next());
                assertTrue(rs.next(), "UNION should keep both rows — they have different column values");
            }
        }
    }

    @Test void except_distinguishes_numeric_scale() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            // PG: 1.0 and 1.00 are equal → EXCEPT should return 0 rows
            try (ResultSet rs = s.executeQuery("SELECT 1.0 AS v EXCEPT SELECT 1.00 AS v")) {
                assertFalse(rs.next(), "1.0 EXCEPT 1.00 should produce 0 rows (equal values)");
            }
        }
    }

    // ===== DISTINCT =====

    @Test void distinct_does_not_collapse_comma_strings() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE rse_d1 (a text, b text)");
            s.execute("INSERT INTO rse_d1 VALUES ('a, b', 'c'), ('a', 'b, c')");

            try (ResultSet rs = s.executeQuery("SELECT DISTINCT a, b FROM rse_d1 ORDER BY a")) {
                assertTrue(rs.next());
                assertTrue(rs.next(), "DISTINCT should keep both rows — different column values");
            }

            s.execute("DROP TABLE rse_d1");
        }
    }

    @Test void distinct_treats_equal_numerics_as_same() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE rse_d2 (v numeric)");
            s.execute("INSERT INTO rse_d2 VALUES (1.0), (1.00), (1.000)");

            try (ResultSet rs = s.executeQuery("SELECT DISTINCT v FROM rse_d2")) {
                assertTrue(rs.next());
                assertFalse(rs.next(), "DISTINCT on numerics: 1.0, 1.00, 1.000 are the same value");
            }

            s.execute("DROP TABLE rse_d2");
        }
    }

    // ===== GROUP BY =====

    @Test void group_by_does_not_split_equal_numerics() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE rse_g1 (v numeric, x int)");
            s.execute("INSERT INTO rse_g1 VALUES (1.0, 10), (1.00, 20), (2.0, 30)");

            try (ResultSet rs = s.executeQuery(
                    "SELECT v, sum(x) AS s FROM rse_g1 GROUP BY v ORDER BY v")) {
                assertTrue(rs.next());
                assertEquals(30, rs.getInt("s"), "1.0 and 1.00 should group together → sum=30");
                assertTrue(rs.next());
                assertEquals(30, rs.getInt("s"));
                assertFalse(rs.next(), "Should be exactly 2 groups (1.0/1.00 and 2.0)");
            }

            s.execute("DROP TABLE rse_g1");
        }
    }

    @Test void group_by_does_not_collapse_comma_strings() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE rse_g2 (a text, b text, x int)");
            s.execute("INSERT INTO rse_g2 VALUES ('a, b', 'c', 1), ('a', 'b, c', 2)");

            try (ResultSet rs = s.executeQuery(
                    "SELECT a, b, sum(x) AS s FROM rse_g2 GROUP BY a, b ORDER BY a")) {
                assertTrue(rs.next());
                assertTrue(rs.next(), "GROUP BY should not collapse different column values");
            }

            s.execute("DROP TABLE rse_g2");
        }
    }
}
