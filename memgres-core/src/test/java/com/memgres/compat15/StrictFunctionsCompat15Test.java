package com.memgres.compat15;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 3 failures from strict-functions.sql where Memgres diverges from PG 18.
 *
 * Stmt 35: strict_sum_no_init with all-NULL input should return NULL (PG succeeds, Memgres errors)
 * Stmt 39: agg_track_nulls with ORDER BY on non-grouped column should error (PG errors, Memgres succeeds)
 * Stmt 49: expanding composite return from strict function via .* should work (PG succeeds, Memgres errors)
 */
class StrictFunctionsCompat15Test {

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
            s.execute("DROP SCHEMA IF EXISTS strict_test CASCADE");
            s.execute("CREATE SCHEMA strict_test");
            s.execute("SET search_path = strict_test, public");

            // Table and data
            s.execute("CREATE TABLE strict_data (id integer PRIMARY KEY, val integer, label text)");
            s.execute("INSERT INTO strict_data VALUES "
                    + "(1, 10, 'a'), (2, NULL, 'b'), (3, 30, 'c'), (4, NULL, NULL), (5, 50, 'e')");

            // STRICT sfunc used by aggregates
            s.execute("CREATE FUNCTION strict_sfunc_add(state integer, val integer) RETURNS integer "
                    + "LANGUAGE sql STRICT IMMUTABLE AS $$ SELECT state + val $$");

            // Aggregate with INITCOND (used indirectly, included for completeness)
            s.execute("CREATE AGGREGATE strict_sum(integer) ("
                    + "SFUNC = strict_sfunc_add, STYPE = integer, INITCOND = '0')");

            // Aggregate WITHOUT INITCOND -- needed for Stmt 35
            s.execute("CREATE AGGREGATE strict_sum_no_init(integer) ("
                    + "SFUNC = strict_sfunc_add, STYPE = integer)");

            // Non-strict sfunc and aggregate -- needed for Stmt 39
            s.execute("CREATE FUNCTION non_strict_sfunc(state text, val integer) RETURNS text "
                    + "LANGUAGE sql IMMUTABLE AS $$ "
                    + "SELECT state || CASE WHEN val IS NULL THEN 'N' ELSE val::text END $$");
            s.execute("CREATE AGGREGATE agg_track_nulls(integer) ("
                    + "SFUNC = non_strict_sfunc, STYPE = text, INITCOND = '')");

            // Composite type and function -- needed for Stmt 49
            s.execute("CREATE TYPE strict_pair AS (a integer, b integer)");
            s.execute("CREATE FUNCTION strict_make_pair(x integer, y integer) RETURNS strict_pair "
                    + "LANGUAGE plpgsql STRICT AS $$ "
                    + "DECLARE result strict_pair; "
                    + "BEGIN result.a := x; result.b := y; RETURN result; END; $$");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS strict_test CASCADE");
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
     * Stmt 35: STRICT aggregate with no INITCOND over all-NULL input should return NULL.
     *
     * PG 18: returns one row with is_null = true
     * Memgres: ERROR [42883] function strict_sum_no_init(unknown) does not exist
     */
    @Test
    void strictSumNoInitAllNullsReturnsNull() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT strict_sum_no_init(val) IS NULL AS is_null "
                     + "FROM (VALUES (NULL::integer), (NULL::integer)) AS t(val)")) {
            assertTrue(rs.next(), "Expected one result row");
            assertEquals(true, rs.getBoolean("is_null"), "All-NULL input with no INITCOND should yield NULL");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    /**
     * Stmt 39: SELECT with aggregate but ORDER BY on a non-grouped column should fail.
     *
     * PG 18: ERROR [42803] column "strict_data.id" must appear in the GROUP BY clause
     *        or be used in an aggregate function
     * Memgres: incorrectly succeeds with result [10N30N50]
     */
    @Test
    void aggTrackNullsWithOrderByNonGroupedColumnShouldError() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery(
                         "SELECT agg_track_nulls(val) AS result FROM strict_data ORDER BY id")) {
                rs.next();
            }
        });
        assertEquals("42803", ex.getSQLState(),
                "Expected SQL state 42803 for missing GROUP BY column");
        assertTrue(ex.getMessage().toLowerCase().contains("aggregate")
                        || ex.getMessage().toLowerCase().contains("group by"),
                "Error message should mention aggregate or GROUP BY: " + ex.getMessage());
    }

    /**
     * Stmt 49: Expanding composite return of a STRICT function via (func(args)).* should work.
     *
     * PG 18: returns one row with a=1, b=2
     * Memgres: ERROR [42601] "result.a" is not a known variable
     */
    @Test
    void strictMakePairExpandStar() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT (strict_make_pair(1, 2)).*")) {
            assertTrue(rs.next(), "Expected one result row");
            assertEquals(1, rs.getInt("a"), "First element of pair should be 1");
            assertEquals(2, rs.getInt("b"), "Second element of pair should be 2");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }
}
