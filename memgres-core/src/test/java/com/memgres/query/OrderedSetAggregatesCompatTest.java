package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Compatibility tests for ordered-set aggregate differences between PostgreSQL and Memgres.
 * These tests document 5 known behavioral gaps from ordered-set-aggregates.sql.
 * All tests are INTENDED TO FAIL against Memgres, passing only against real PostgreSQL.
 */
public class OrderedSetAggregatesCompatTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE osa_data (grp text, val integer)");
            stmt.execute("INSERT INTO osa_data VALUES ('a', 10), ('a', 30), ('a', 50), ('b', 20), ('b', 40)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    /**
     * PG returns {10,30,50} for percentile_cont with an array argument.
     * Memgres returns 10 (ignores the array, returns only the first result).
     */
    @Test
    void testPercentileContWithArrayArgument() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT percentile_cont(ARRAY[0.0, 0.5, 1.0]) WITHIN GROUP (ORDER BY val)::integer[] AS percentiles FROM osa_data WHERE grp = 'a'")) {
            assertTrue(rs.next());
            Array arr = rs.getArray("percentiles");
            assertNotNull(arr, "Expected an array result, but got null");
            Integer[] percentiles = (Integer[]) arr.getArray();
            assertArrayEquals(new Integer[]{10, 30, 50}, percentiles,
                    "PG returns {10,30,50} but Memgres returns 10");
        }
    }

    /**
     * PG returns {10,30,50} for percentile_disc with an array argument.
     * Memgres returns 10 (ignores the array, returns only the first result).
     */
    @Test
    void testPercentileDiscWithArrayArgument() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT percentile_disc(ARRAY[0.0, 0.5, 1.0]) WITHIN GROUP (ORDER BY val)::integer[] AS percentiles FROM osa_data WHERE grp = 'a'")) {
            assertTrue(rs.next());
            Array arr = rs.getArray("percentiles");
            assertNotNull(arr, "Expected an array result, but got null");
            Integer[] percentiles = (Integer[]) arr.getArray();
            assertArrayEquals(new Integer[]{10, 30, 50}, percentiles,
                    "PG returns {10,30,50} but Memgres returns 10");
        }
    }

    /**
     * PG raises SQLSTATE 22003 (numeric_value_out_of_range) for out-of-range percentile.
     * Memgres raises SQLSTATE 22023 (invalid_parameter_value) instead.
     */
    @Test
    void testPercentileContOutOfRangeSqlstate() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            SQLException ex = assertThrows(SQLException.class, () ->
                    stmt.executeQuery("SELECT percentile_cont(1.5) WITHIN GROUP (ORDER BY val) FROM osa_data"));
            assertEquals("22003", ex.getSQLState(),
                    "PG uses SQLSTATE 22003 but Memgres uses 22023");
        }
    }

    /**
     * PG raises SQLSTATE 22003 (numeric_value_out_of_range) for negative percentile.
     * Memgres raises SQLSTATE 22023 (invalid_parameter_value) instead.
     */
    @Test
    void testPercentileContNegativeSqlstate() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            SQLException ex = assertThrows(SQLException.class, () ->
                    stmt.executeQuery("SELECT percentile_cont(-0.1) WITHIN GROUP (ORDER BY val) FROM osa_data"));
            assertEquals("22003", ex.getSQLState(),
                    "PG uses SQLSTATE 22003 but Memgres uses 22023");
        }
    }

    /**
     * PG rejects FILTER on ordered-set aggregates after cast with SQLSTATE 42601 (syntax_error).
     * Memgres rejects it with SQLSTATE 42703 (undefined_column) instead.
     */
    @Test
    void testFilterNotValidOnOrderedSetAggregateAfterCast() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            SQLException ex = assertThrows(SQLException.class, () ->
                    stmt.executeQuery("SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY val)::integer FILTER (WHERE grp = 'a') AS p50_a FROM osa_data"));
            assertEquals("42601", ex.getSQLState(),
                    "PG uses SQLSTATE 42601 (syntax_error) but Memgres uses 42703");
        }
    }
}
