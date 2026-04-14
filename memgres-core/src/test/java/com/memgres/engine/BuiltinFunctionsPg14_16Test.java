package com.memgres.engine;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 18 Memgres-vs-Annotation failures from builtin-functions-pg14-16.sql.
 *
 * These cover PG 14-16 built-in functions where Memgres diverges from PG 18:
 *   Stmt 14: string_to_table('', ',') returns 1 row instead of 0
 *   Stmts 16-19, 22: array_sample does not exist
 *   Stmts 20-21: array_shuffle does not exist
 *   Stmt 28: range_agg on non-overlapping ranges returns wrong result
 *   Stmt 35: range_agg with NULLs returns wrong result
 *   Stmt 37: range_agg on empty table errors instead of returning NULL
 *   Stmt 40: range_intersect_agg returns 'empty' instead of correct intersection
 *   Stmts 45-50: OVERLAPS syntax returns wrong column name and wrong values
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BuiltinFunctionsPg14_16Test {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);

        try (Statement s = conn.createStatement()) {
            s.execute("DROP SCHEMA IF EXISTS bf_test CASCADE");
            s.execute("CREATE SCHEMA bf_test");
            s.execute("SET search_path = bf_test, public");

            // Create the ranges table used by range_agg / range_intersect_agg tests
            s.execute("CREATE TABLE bf_ranges (r int4range)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS bf_test CASCADE");
                s.execute("SET search_path = public");
            } catch (SQLException ignored) {
            }
            conn.close();
        }
        if (memgres != null) {
            memgres.close();
        }
    }

    // ========================================================================
    // Stmt 14: string_to_table('', ',') should return 0 rows
    // ========================================================================

    /**
     * Stmt 14: string_to_table('', ',') should return 0 rows.
     *
     * PG 18: OK (val) 0 rows
     * Memgres: OK (val) [] (1 row with empty string)
     */
    @Test
    @Order(1)
    void stringToTableEmptyStringReturnsZeroRows() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT val FROM string_to_table('', ',') AS val")) {
            assertFalse(rs.next(),
                    "string_to_table('', ',') should return 0 rows, not 1");
        }
    }

    // ========================================================================
    // Stmts 16-19, 22: array_sample function
    // ========================================================================

    /**
     * Stmt 16: array_sample returns correct number of elements.
     *
     * PG 18: OK (len) [3]
     * Memgres: ERROR [42883] function array_sample(text, integer) does not exist
     */
    @Test
    @Order(2)
    void arraySampleReturnsCorrectLength() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT array_length(array_sample(ARRAY[1,2,3,4,5,6,7,8,9,10], 3), 1) AS len")) {
            assertTrue(rs.next(), "Expected one result row");
            assertEquals(3, rs.getInt("len"),
                    "array_sample with n=3 should return array of length 3");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    /**
     * Stmt 17: array_sample with n=0 returns empty array.
     *
     * PG 18: OK (result) [{}]
     * Memgres: ERROR [42883] function array_sample(text, integer) does not exist
     */
    @Test
    @Order(3)
    void arraySampleZeroReturnsEmptyArray() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT array_sample(ARRAY[1,2,3], 0) AS result")) {
            assertTrue(rs.next(), "Expected one result row");
            Array arr = rs.getArray("result");
            assertNotNull(arr, "array_sample with n=0 should return an array, not NULL");
            Integer[] values = (Integer[]) arr.getArray();
            assertEquals(0, values.length,
                    "array_sample with n=0 should return empty array");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    /**
     * Stmt 18: array_sample with n=array length returns all elements.
     *
     * PG 18: OK (len) [3]
     * Memgres: ERROR [42883] function array_sample(text, integer) does not exist
     */
    @Test
    @Order(4)
    void arraySampleFullLengthReturnsCorrectLength() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT array_length(array_sample(ARRAY[1,2,3], 3), 1) AS len")) {
            assertTrue(rs.next(), "Expected one result row");
            assertEquals(3, rs.getInt("len"),
                    "array_sample with n=array length should return array of same length");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    /**
     * Stmt 19: array_sample elements come from source array.
     *
     * PG 18: OK (all_valid) [t]
     * Memgres: ERROR [42883] function array_sample(text, integer) does not exist
     */
    @Test
    @Order(5)
    void arraySampleElementsComeFromSource() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT bool_and(elem = ANY(ARRAY[10,20,30,40,50])) AS all_valid "
                     + "FROM unnest(array_sample(ARRAY[10,20,30,40,50], 3)) AS elem")) {
            assertTrue(rs.next(), "Expected one result row");
            assertTrue(rs.getBoolean("all_valid"),
                    "All sampled elements should come from the source array");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    /**
     * Stmt 22: array_sample works with text arrays.
     *
     * PG 18: OK (len) [2]
     * Memgres: ERROR [42883] function array_sample(text, integer) does not exist
     */
    @Test
    @Order(6)
    void arraySampleTextArrayReturnsCorrectLength() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT array_length(array_sample(ARRAY['a','b','c','d'], 2), 1) AS len")) {
            assertTrue(rs.next(), "Expected one result row");
            assertEquals(2, rs.getInt("len"),
                    "array_sample on text array with n=2 should return array of length 2");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    // ========================================================================
    // Stmts 20-21: array_shuffle function
    // ========================================================================

    /**
     * Stmt 20: array_shuffle preserves length and elements.
     *
     * PG 18: OK (same_length, same_elements) [t | t]
     * Memgres: ERROR [42883] function array_shuffle(text) does not exist
     */
    @Test
    @Order(7)
    void arrayShufflePreservesLengthAndElements() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT array_length(array_shuffle(ARRAY[1,2,3,4,5]), 1) = 5 AS same_length, "
                     + "(SELECT bool_and(elem = ANY(ARRAY[1,2,3,4,5])) "
                     + "FROM unnest(array_shuffle(ARRAY[1,2,3,4,5])) AS elem) AS same_elements")) {
            assertTrue(rs.next(), "Expected one result row");
            assertTrue(rs.getBoolean("same_length"),
                    "Shuffled array should have same length as original");
            assertTrue(rs.getBoolean("same_elements"),
                    "Shuffled array should contain same elements as original");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    /**
     * Stmt 21: array_shuffle on empty array returns empty array.
     *
     * PG 18: OK (result) [{}]
     * Memgres: ERROR [42883] function array_shuffle(text) does not exist
     */
    @Test
    @Order(8)
    void arrayShuffleEmptyArrayReturnsEmptyArray() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT array_shuffle('{}'::integer[]) AS result")) {
            assertTrue(rs.next(), "Expected one result row");
            Array arr = rs.getArray("result");
            assertNotNull(arr, "array_shuffle of empty array should return an array, not NULL");
            Integer[] values = (Integer[]) arr.getArray();
            assertEquals(0, values.length,
                    "array_shuffle of empty array should return empty array");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    // ========================================================================
    // Stmts 28, 35, 37: range_agg (stateful, order-dependent)
    // ========================================================================

    /**
     * Stmt 28: range_agg on non-overlapping ranges should keep them separate.
     *
     * PG 18: OK (result) [{[1,3),[5,7),[9,11)}]
     * Memgres: OK (result) [{[1,8),[9,15)}] (wrong -- appears to use stale data)
     */
    @Test
    @Order(9)
    void rangeAggNonOverlappingRangesStaySeparate() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("TRUNCATE bf_ranges");
            s.execute("INSERT INTO bf_ranges VALUES ('[1,3)'), ('[5,7)'), ('[9,11)')");

            try (ResultSet rs = s.executeQuery(
                    "SELECT range_agg(r) AS result FROM bf_ranges")) {
                assertTrue(rs.next(), "Expected one result row");
                assertEquals("{[1,3),[5,7),[9,11)}", rs.getString("result"),
                        "range_agg of non-overlapping ranges should keep them separate");
                assertFalse(rs.next(), "Expected exactly one row");
            }
        }
    }

    /**
     * Stmt 35: range_agg with NULL values should ignore NULLs and merge overlapping ranges.
     *
     * PG 18: OK (result) [{[1,8)}]
     * Memgres: OK (result) [{[1,8),[9,15)}] (wrong -- appears to use stale data)
     */
    @Test
    @Order(10)
    void rangeAggWithNullsIgnoresNullsAndMerges() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("TRUNCATE bf_ranges");
            s.execute("INSERT INTO bf_ranges VALUES ('[1,5)'), (NULL), ('[3,8)')");

            try (ResultSet rs = s.executeQuery(
                    "SELECT range_agg(r) AS result FROM bf_ranges")) {
                assertTrue(rs.next(), "Expected one result row");
                assertEquals("{[1,8)}", rs.getString("result"),
                        "range_agg should ignore NULLs and merge overlapping ranges");
                assertFalse(rs.next(), "Expected exactly one row");
            }
        }
    }

    /**
     * Stmt 37: range_agg on empty table should return NULL.
     *
     * PG 18: OK (result) [t]
     * Memgres: ERROR [42883] function range_agg(text) does not exist
     */
    @Test
    @Order(11)
    void rangeAggEmptyTableReturnsNull() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("TRUNCATE bf_ranges");

            try (ResultSet rs = s.executeQuery(
                    "SELECT range_agg(r) IS NULL AS result FROM bf_ranges")) {
                assertTrue(rs.next(), "Expected one result row");
                assertTrue(rs.getBoolean("result"),
                        "range_agg on empty table should return NULL (IS NULL = true)");
                assertFalse(rs.next(), "Expected exactly one row");
            }
        }
    }

    // ========================================================================
    // Stmt 40: range_intersect_agg
    // ========================================================================

    /**
     * Stmt 40: range_intersect_agg should return the intersection of all ranges.
     *
     * PG 18: OK (result) [[5,8)]
     * Memgres: OK (result) [empty]
     */
    @Test
    @Order(12)
    void rangeIntersectAggReturnsCorrectIntersection() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("TRUNCATE bf_ranges");
            s.execute("INSERT INTO bf_ranges VALUES ('[1,10)'), ('[3,8)'), ('[5,15)')");

            try (ResultSet rs = s.executeQuery(
                    "SELECT range_intersect_agg(r) AS result FROM bf_ranges")) {
                assertTrue(rs.next(), "Expected one result row");
                assertEquals("[5,8)", rs.getString("result"),
                        "range_intersect_agg should return [5,8) as the intersection");
                assertFalse(rs.next(), "Expected exactly one row");
            }
        }
    }

    // ========================================================================
    // Stmts 45-50: OVERLAPS syntax
    // ========================================================================

    /**
     * Stmt 45: Date OVERLAPS with overlapping periods should return true.
     *
     * PG 18: OK (result) [t] -- column named 'result', boolean value
     * Memgres: OK (overlaps) [(2026-01-01,2026-01-10)] -- wrong column name, wrong value
     */
    @Test
    @Order(13)
    void dateOverlapsOverlappingPeriodsReturnsTrue() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT (DATE '2026-01-01', DATE '2026-01-10') OVERLAPS "
                     + "(DATE '2026-01-05', DATE '2026-01-15') AS result")) {
            assertTrue(rs.next(), "Expected one result row");
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals("result", meta.getColumnLabel(1),
                    "OVERLAPS column should be named 'result' (from AS alias)");
            assertTrue(rs.getBoolean("result"),
                    "Overlapping date periods should return true");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    /**
     * Stmt 46: Date OVERLAPS with non-overlapping periods should return false.
     *
     * PG 18: OK (result) [f]
     * Memgres: OK (overlaps) [(2026-01-01,2026-01-05)]
     */
    @Test
    @Order(14)
    void dateOverlapsNonOverlappingPeriodsReturnsFalse() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT (DATE '2026-01-01', DATE '2026-01-05') OVERLAPS "
                     + "(DATE '2026-01-06', DATE '2026-01-10') AS result")) {
            assertTrue(rs.next(), "Expected one result row");
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals("result", meta.getColumnLabel(1),
                    "OVERLAPS column should be named 'result' (from AS alias)");
            assertFalse(rs.getBoolean("result"),
                    "Non-overlapping date periods should return false");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    /**
     * Stmt 47: Timestamp OVERLAPS with overlapping periods should return true.
     *
     * PG 18: OK (result) [t]
     * Memgres: OK (overlaps) [(2026-01-01T08:00,2026-01-01T17:00)]
     */
    @Test
    @Order(15)
    void timestampOverlapsOverlappingPeriodsReturnsTrue() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT (TIMESTAMP '2026-01-01 08:00', TIMESTAMP '2026-01-01 17:00') OVERLAPS "
                     + "(TIMESTAMP '2026-01-01 12:00', TIMESTAMP '2026-01-01 20:00') AS result")) {
            assertTrue(rs.next(), "Expected one result row");
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals("result", meta.getColumnLabel(1),
                    "OVERLAPS column should be named 'result' (from AS alias)");
            assertTrue(rs.getBoolean("result"),
                    "Overlapping timestamp periods should return true");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    /**
     * Stmt 48: OVERLAPS with interval should return true.
     *
     * PG 18: OK (result) [t]
     * Memgres: OK (overlaps) [(2026-01-01,5 days)]
     */
    @Test
    @Order(16)
    void overlapsWithIntervalReturnsTrue() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT (DATE '2026-01-01', INTERVAL '5 days') OVERLAPS "
                     + "(DATE '2026-01-03', INTERVAL '5 days') AS result")) {
            assertTrue(rs.next(), "Expected one result row");
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals("result", meta.getColumnLabel(1),
                    "OVERLAPS column should be named 'result' (from AS alias)");
            assertTrue(rs.getBoolean("result"),
                    "Overlapping intervals should return true");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    /**
     * Stmt 49: OVERLAPS with touching endpoints should return false (SQL standard).
     *
     * PG 18: OK (result) [f]
     * Memgres: OK (overlaps) [(2026-01-01,2026-01-05)]
     */
    @Test
    @Order(17)
    void overlapsTouchingEndpointsReturnsFalse() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT (DATE '2026-01-01', DATE '2026-01-05') OVERLAPS "
                     + "(DATE '2026-01-05', DATE '2026-01-10') AS result")) {
            assertTrue(rs.next(), "Expected one result row");
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals("result", meta.getColumnLabel(1),
                    "OVERLAPS column should be named 'result' (from AS alias)");
            assertFalse(rs.getBoolean("result"),
                    "Touching endpoints should NOT be considered overlapping per SQL standard");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    /**
     * Stmt 50: OVERLAPS with zero-length period at same start should return true.
     *
     * PG 18: OK (result) [t]
     * Memgres: OK (overlaps) [(2026-01-01,2026-01-01)]
     */
    @Test
    @Order(18)
    void overlapsZeroLengthPeriodAtSameStartReturnsTrue() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT (DATE '2026-01-01', DATE '2026-01-01') OVERLAPS "
                     + "(DATE '2026-01-01', DATE '2026-01-05') AS result")) {
            assertTrue(rs.next(), "Expected one result row");
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals("result", meta.getColumnLabel(1),
                    "OVERLAPS column should be named 'result' (from AS alias)");
            assertTrue(rs.getBoolean("result"),
                    "Zero-length period at same start should be considered overlapping");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }
}
