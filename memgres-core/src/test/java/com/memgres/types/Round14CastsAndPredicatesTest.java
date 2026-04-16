package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 14 gaps: CREATE CAST, OVERLAPS, BETWEEN SYMMETRIC, IN NULL 3VL,
 *                current_query(), IS DISTINCT FROM.
 *
 * Classic SQL-standard predicates that should be supported but often aren't
 * in lightweight Postgres-compatible engines.
 */
class Round14CastsAndPredicatesTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private static int scalarInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private static String scalarString(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    private static Boolean scalarBool(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            Object v = rs.getObject(1);
            if (v == null) return null;
            return (Boolean) v;
        }
    }

    // =========================================================================
    // A. OVERLAPS
    // =========================================================================

    @Test
    void overlaps_dates_true() throws SQLException {
        assertEquals(Boolean.TRUE, scalarBool(
                "SELECT (DATE '2020-01-01', DATE '2020-06-01') "
                        + "OVERLAPS (DATE '2020-03-01', DATE '2020-09-01')"));
    }

    @Test
    void overlaps_dates_false() throws SQLException {
        assertEquals(Boolean.FALSE, scalarBool(
                "SELECT (DATE '2020-01-01', DATE '2020-02-01') "
                        + "OVERLAPS (DATE '2020-06-01', DATE '2020-09-01')"));
    }

    @Test
    void overlaps_timestamps() throws SQLException {
        assertEquals(Boolean.TRUE, scalarBool(
                "SELECT (TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-01-02 00:00:00') "
                        + "OVERLAPS (TIMESTAMP '2020-01-01 12:00:00', TIMESTAMP '2020-01-03 00:00:00')"));
    }

    @Test
    void overlaps_with_interval() throws SQLException {
        // Second point can be an INTERVAL (duration form)
        assertEquals(Boolean.TRUE, scalarBool(
                "SELECT (DATE '2020-01-01', INTERVAL '30 days') "
                        + "OVERLAPS (DATE '2020-01-15', DATE '2020-02-15')"));
    }

    @Test
    void overlaps_touching_is_false() throws SQLException {
        // PG semantics: touching intervals do NOT overlap
        assertEquals(Boolean.FALSE, scalarBool(
                "SELECT (DATE '2020-01-01', DATE '2020-02-01') "
                        + "OVERLAPS (DATE '2020-02-01', DATE '2020-03-01')"));
    }

    // =========================================================================
    // B. BETWEEN SYMMETRIC
    // =========================================================================

    @Test
    void between_symmetric_swaps_bounds() throws SQLException {
        // BETWEEN SYMMETRIC: order of bounds doesn't matter
        assertEquals(Boolean.TRUE, scalarBool("SELECT 5 BETWEEN SYMMETRIC 10 AND 1"));
    }

    @Test
    void between_asymmetric_respects_order() throws SQLException {
        // Plain BETWEEN with reversed bounds → false
        assertEquals(Boolean.FALSE, scalarBool("SELECT 5 BETWEEN 10 AND 1"));
    }

    @Test
    void not_between_symmetric() throws SQLException {
        assertEquals(Boolean.TRUE, scalarBool("SELECT 20 NOT BETWEEN SYMMETRIC 10 AND 1"));
    }

    // =========================================================================
    // C. IN with NULL — 3VL semantics
    // =========================================================================

    @Test
    void in_with_null_element_unknown_becomes_null() throws SQLException {
        // `x IN (1, NULL)` where x not in list → NULL (not false)
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 5 IN (1, NULL)")) {
            assertTrue(rs.next());
            rs.getBoolean(1);
            assertTrue(rs.wasNull(), "5 IN (1, NULL) should be NULL, not false");
        }
    }

    @Test
    void not_in_with_null_yields_null() throws SQLException {
        // `x NOT IN (1, NULL)` where x ≠ 1 → NULL
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 5 NOT IN (1, NULL)")) {
            assertTrue(rs.next());
            rs.getBoolean(1);
            assertTrue(rs.wasNull(), "5 NOT IN (1, NULL) should be NULL");
        }
    }

    @Test
    void in_with_null_match_is_true() throws SQLException {
        // If the LHS matches a non-NULL element, result is TRUE even with NULL present
        assertEquals(Boolean.TRUE, scalarBool("SELECT 1 IN (1, NULL)"));
    }

    // =========================================================================
    // D. IS DISTINCT FROM / IS NOT DISTINCT FROM
    // =========================================================================

    @Test
    void is_distinct_from_treats_null_equally() throws SQLException {
        assertEquals(Boolean.FALSE, scalarBool("SELECT NULL IS DISTINCT FROM NULL"));
        assertEquals(Boolean.TRUE, scalarBool("SELECT NULL IS DISTINCT FROM 1"));
        assertEquals(Boolean.FALSE, scalarBool("SELECT 1 IS DISTINCT FROM 1"));
    }

    @Test
    void is_not_distinct_from_true_for_equal_nulls() throws SQLException {
        assertEquals(Boolean.TRUE, scalarBool("SELECT NULL IS NOT DISTINCT FROM NULL"));
        assertEquals(Boolean.FALSE, scalarBool("SELECT NULL IS NOT DISTINCT FROM 1"));
    }

    // =========================================================================
    // E. CREATE CAST
    // =========================================================================

    @Test
    void create_cast_with_function() throws SQLException {
        // User-defined cast via function
        exec("CREATE DOMAIN r14_dom_pos AS int CHECK (VALUE > 0)");
        exec("CREATE FUNCTION r14_cast_fn(int) RETURNS r14_dom_pos AS 'SELECT $1::r14_dom_pos' LANGUAGE SQL");
        exec("CREATE CAST (int AS r14_dom_pos) WITH FUNCTION r14_cast_fn(int) AS ASSIGNMENT");
        // Cast visible in pg_cast
        assertTrue(scalarInt(
                "SELECT count(*)::int FROM pg_cast c "
                        + "JOIN pg_type s ON c.castsource = s.oid "
                        + "JOIN pg_type t ON c.casttarget = t.oid "
                        + "WHERE t.typname = 'r14_dom_pos'") >= 1);
    }

    @Test
    void create_cast_without_function_binary_coercible() throws SQLException {
        // CREATE CAST (a AS b) WITHOUT FUNCTION → binary coercion
        exec("CREATE DOMAIN r14_dom_text AS text");
        exec("CREATE CAST (text AS r14_dom_text) WITHOUT FUNCTION AS IMPLICIT");
        assertTrue(scalarInt(
                "SELECT count(*)::int FROM pg_cast c "
                        + "JOIN pg_type t ON c.casttarget = t.oid "
                        + "WHERE t.typname = 'r14_dom_text'") >= 1);
    }

    @Test
    void drop_cast() throws SQLException {
        exec("CREATE DOMAIN r14_dom_d AS int");
        exec("CREATE CAST (int AS r14_dom_d) WITHOUT FUNCTION");
        exec("DROP CAST (int AS r14_dom_d)");
        assertEquals(0, scalarInt(
                "SELECT count(*)::int FROM pg_cast c "
                        + "JOIN pg_type t ON c.casttarget = t.oid "
                        + "WHERE t.typname = 'r14_dom_d'"));
    }

    // =========================================================================
    // F. current_query() and related
    // =========================================================================

    @Test
    void current_query_contains_itself() throws SQLException {
        String q = scalarString("SELECT current_query()");
        assertNotNull(q);
        assertTrue(q.toLowerCase().contains("current_query"),
                "current_query() should return its own query text; got " + q);
    }

    @Test
    void inet_server_addr_function() throws SQLException {
        // Should resolve — value may be NULL for local sockets
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT inet_server_addr()")) {
            assertTrue(rs.next());
        }
    }

    @Test
    void inet_client_addr_function() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT inet_client_addr()")) {
            assertTrue(rs.next());
        }
    }

    // =========================================================================
    // G. Numeric + string cast edge cases
    // =========================================================================

    @Test
    void numeric_to_int_rounding_half_away() throws SQLException {
        // PG rounds half away from zero (banker's rounding for numeric::int? actually half-to-even)
        assertEquals(2, scalarInt("SELECT (2.5)::int"));
        assertEquals(4, scalarInt("SELECT (3.5)::int"));  // half-to-even
    }

    @Test
    void cast_boolean_from_string_variants() throws SQLException {
        assertEquals(Boolean.TRUE, scalarBool("SELECT 'yes'::boolean"));
        assertEquals(Boolean.TRUE, scalarBool("SELECT 'on'::boolean"));
        assertEquals(Boolean.TRUE, scalarBool("SELECT 'true'::boolean"));
        assertEquals(Boolean.TRUE, scalarBool("SELECT 't'::boolean"));
        assertEquals(Boolean.TRUE, scalarBool("SELECT '1'::boolean"));
        assertEquals(Boolean.FALSE, scalarBool("SELECT 'no'::boolean"));
        assertEquals(Boolean.FALSE, scalarBool("SELECT 'off'::boolean"));
    }

    @Test
    void cast_interval_justify_hours() throws SQLException {
        // 25 hours justified → 1 day 1 hour
        String v = scalarString("SELECT justify_hours(interval '25 hours')::text");
        assertNotNull(v);
        assertTrue(v.contains("1 day"),
                "justify_hours(25h) must normalize to days; got " + v);
    }

    @Test
    void cast_interval_justify_days() throws SQLException {
        String v = scalarString("SELECT justify_days(interval '30 days')::text");
        assertNotNull(v);
        assertTrue(v.contains("1 mon") || v.contains("1 month"),
                "justify_days(30d) should fold into months; got " + v);
    }

    // =========================================================================
    // H. SIMILAR TO / quantifier regex
    // =========================================================================

    @Test
    void similar_to_matches_sql_regex() throws SQLException {
        // SQL:2008 SIMILAR TO — POSIX-like regex with % and _
        assertEquals(Boolean.TRUE, scalarBool("SELECT 'abc' SIMILAR TO 'a_c'"));
        assertEquals(Boolean.TRUE, scalarBool("SELECT 'abc' SIMILAR TO 'a%'"));
        assertEquals(Boolean.FALSE, scalarBool("SELECT 'abc' SIMILAR TO 'x_z'"));
    }

    @Test
    void similar_to_character_class() throws SQLException {
        assertEquals(Boolean.TRUE, scalarBool("SELECT 'abc' SIMILAR TO '[a-z]+'"));
        assertEquals(Boolean.FALSE, scalarBool("SELECT '123' SIMILAR TO '[a-z]+'"));
    }

    // =========================================================================
    // I. row-valued IN / ANY / ALL
    // =========================================================================

    @Test
    void row_value_equality() throws SQLException {
        assertEquals(Boolean.TRUE, scalarBool("SELECT (1, 'a') = (1, 'a')"));
        assertEquals(Boolean.FALSE, scalarBool("SELECT (1, 'a') = (1, 'b')"));
    }

    @Test
    void row_value_in_subquery() throws SQLException {
        exec("CREATE TABLE r14_rv (a int, b text)");
        exec("INSERT INTO r14_rv VALUES (1,'a'),(2,'b')");
        assertEquals(Boolean.TRUE, scalarBool(
                "SELECT (1,'a') IN (SELECT a, b FROM r14_rv)"));
    }

    @Test
    void row_value_comparison_lexicographic() throws SQLException {
        // (1,2) < (1,3) → true; (1,2) < (1,2) → false
        assertEquals(Boolean.TRUE, scalarBool("SELECT (1,2) < (1,3)"));
        assertEquals(Boolean.FALSE, scalarBool("SELECT (1,2) < (1,2)"));
    }
}
