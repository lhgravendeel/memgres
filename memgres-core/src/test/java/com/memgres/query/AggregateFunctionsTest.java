package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 5 failures from aggregate-functions.sql where Memgres diverges from PG 18.
 *
 * Stmt 18: agg_mysum_no_init(val) IS NULL on empty table should return 1 row [t], Memgres returns 0 rows
 * Stmt 64: agg_sorted_array(val) should return {5,10,15,20,30,40}, Memgres returns NULL
 * Stmt 65: grouped agg_sorted_array should return {10,20,30} etc, Memgres returns NULL
 * Stmt 79: CREATE AGGREGATE with nonexistent SFUNC should error 42883, Memgres succeeds
 * Stmt 81: ORDER BY agg_mysum(val) should sort groups [b, c, a], Memgres returns [a, b, c]
 */
class AggregateFunctionsTest {

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
            s.execute("DROP SCHEMA IF EXISTS agg_test CASCADE");
            s.execute("CREATE SCHEMA agg_test");
            s.execute("SET search_path = agg_test, public");

            // Tables and data
            s.execute("CREATE TABLE agg_data (id serial PRIMARY KEY, grp text, val integer, label text)");
            s.execute("INSERT INTO agg_data (grp, val, label) VALUES "
                    + "('a', 10, 'x'), ('a', 20, 'y'), ('a', 30, 'z'), "
                    + "('b', 5, 'p'), ('b', 15, 'q'), "
                    + "('c', NULL, 'r'), ('c', 40, 's')");

            s.execute("CREATE TABLE agg_empty (id integer, val integer)");

            // SFUNC for integer sum
            s.execute("CREATE FUNCTION agg_int_add(state integer, val integer) RETURNS integer "
                    + "LANGUAGE sql IMMUTABLE AS $$ SELECT state + val $$");

            // Aggregate with INITCOND (used by Stmt 81)
            s.execute("CREATE AGGREGATE agg_mysum(integer) ("
                    + "SFUNC = agg_int_add, STYPE = integer, INITCOND = '0')");

            // Aggregate WITHOUT INITCOND (used by Stmt 18)
            s.execute("CREATE AGGREGATE agg_mysum_no_init(integer) ("
                    + "SFUNC = agg_int_add, STYPE = integer)");

            // Array accumulator sfunc and finalfunc (used by Stmts 64, 65)
            s.execute("CREATE FUNCTION agg_array_append_fn(state integer[], val integer) RETURNS integer[] "
                    + "LANGUAGE sql IMMUTABLE AS $$ SELECT state || val $$");

            s.execute("CREATE FUNCTION agg_array_sort_finish(state integer[]) RETURNS integer[] "
                    + "LANGUAGE sql IMMUTABLE AS $$ SELECT array_agg(x ORDER BY x) FROM unnest(state) AS x $$");

            s.execute("CREATE AGGREGATE agg_sorted_array(integer) ("
                    + "SFUNC = agg_array_append_fn, STYPE = integer[], "
                    + "INITCOND = '{}', FINALFUNC = agg_array_sort_finish)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS agg_test CASCADE");
                s.execute("SET search_path = public");
            } catch (SQLException ignored) {
            }
            conn.close();
        }
        if (memgres != null) {
            memgres.close();
        }
    }

    /**
     * Stmt 18: Aggregate without INITCOND on empty table should return one row with NULL.
     *
     * PG 18: returns 1 row with is_null = true
     * Memgres: returns 0 rows
     */
    @Test
    void aggMysumNoInitOnEmptyTableReturnsOneNullRow() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT agg_mysum_no_init(val) IS NULL AS is_null FROM agg_empty")) {
            assertTrue(rs.next(), "Expected one result row from aggregate on empty table");
            assertTrue(rs.getBoolean("is_null"),
                    "Aggregate without INITCOND on empty table should yield NULL (is_null = true)");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    /**
     * Stmt 64: agg_sorted_array over all non-NULL vals should return {5,10,15,20,30,40}.
     *
     * PG 18: returns {5,10,15,20,30,40}
     * Memgres: returns NULL
     */
    @Test
    void aggSortedArrayReturnsCorrectResult() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT agg_sorted_array(val) AS result FROM agg_data WHERE val IS NOT NULL")) {
            assertTrue(rs.next(), "Expected one result row");
            Array arr = rs.getArray("result");
            assertNotNull(arr, "agg_sorted_array should not return NULL");
            Integer[] values = (Integer[]) arr.getArray();
            assertArrayEquals(new Integer[]{5, 10, 15, 20, 30, 40}, values,
                    "agg_sorted_array should return all values sorted");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    /**
     * Stmt 65: Grouped agg_sorted_array should return sorted arrays per group.
     *
     * PG 18: a -> {10,20,30}, b -> {5,15}, c -> {40}
     * Memgres: all groups return NULL
     */
    @Test
    void aggSortedArrayGroupedReturnsCorrectResults() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT grp, agg_sorted_array(val) AS sorted_vals "
                     + "FROM agg_data WHERE val IS NOT NULL GROUP BY grp ORDER BY grp")) {

            assertTrue(rs.next(), "Expected row for group 'a'");
            assertEquals("a", rs.getString("grp"));
            Array arrA = rs.getArray("sorted_vals");
            assertNotNull(arrA, "Group 'a' sorted_vals should not be NULL");
            assertArrayEquals(new Integer[]{10, 20, 30}, (Integer[]) arrA.getArray(),
                    "Group 'a' should have {10,20,30}");

            assertTrue(rs.next(), "Expected row for group 'b'");
            assertEquals("b", rs.getString("grp"));
            Array arrB = rs.getArray("sorted_vals");
            assertNotNull(arrB, "Group 'b' sorted_vals should not be NULL");
            assertArrayEquals(new Integer[]{5, 15}, (Integer[]) arrB.getArray(),
                    "Group 'b' should have {5,15}");

            assertTrue(rs.next(), "Expected row for group 'c'");
            assertEquals("c", rs.getString("grp"));
            Array arrC = rs.getArray("sorted_vals");
            assertNotNull(arrC, "Group 'c' sorted_vals should not be NULL");
            assertArrayEquals(new Integer[]{40}, (Integer[]) arrC.getArray(),
                    "Group 'c' should have {40}");

            assertFalse(rs.next(), "Expected exactly three rows");
        }
    }

    /**
     * Stmt 79: CREATE AGGREGATE with a nonexistent SFUNC should fail with error 42883.
     *
     * PG 18: ERROR [42883] function agg_nonexistent_fn(integer, integer) does not exist
     * Memgres: succeeds (incorrectly)
     */
    @Test
    void createAggregateWithNonexistentSfuncShouldError() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement()) {
                s.execute("CREATE AGGREGATE agg_bad(integer) ("
                        + "SFUNC = agg_nonexistent_fn, STYPE = integer)");
            }
        });
        assertEquals("42883", ex.getSQLState(),
                "Expected SQL state 42883 for nonexistent function");
        assertTrue(ex.getMessage().contains("agg_nonexistent_fn"),
                "Error message should mention the nonexistent function name: " + ex.getMessage());
    }

    /**
     * Stmt 81: ORDER BY agg_mysum(val) should sort groups by their sum: b(20), c(40), a(60).
     *
     * PG 18: [b, c, a]
     * Memgres: [a, b, c] (appears to sort by group name instead of aggregate value)
     */
    @Test
    void orderByCustomAggregateSortsCorrectly() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT grp FROM agg_data WHERE val IS NOT NULL "
                     + "GROUP BY grp ORDER BY agg_mysum(val)")) {
            List<String> groups = new ArrayList<>();
            while (rs.next()) {
                groups.add(rs.getString("grp"));
            }
            assertEquals(List.of("b", "c", "a"), groups,
                    "Groups should be ordered by agg_mysum(val): b(20), c(40), a(60)");
        }
    }
}
